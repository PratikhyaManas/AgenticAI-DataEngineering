"""
SAP / OData v4 Ingestion — ERP Data → Bronze Delta
===================================================
Agent: Data Ingestion Agent

Ingests data from SAP S/4HANA (or any OData v4 endpoint) into Bronze
Delta tables using:
  - OData $filter for incremental watermark-based extraction
  - @odata.nextLink cursor pagination (standard OData v4 pattern)
  - Fallback $skip/$top offset pagination for non-compliant APIs
  - Service principal OAuth2 (SAP BTP) or Basic Auth

SAP OData examples:
  API_BUSINESS_PARTNER     → /sap/opu/odata4/sap/api_business_partner/srvd/...
  API_SALES_ORDER_SRV      → /sap/opu/odata/sap/API_SALES_ORDER_SRV/
  API_PRODUCT_SRV          → /sap/opu/odata/sap/API_PRODUCT_SRV/

Reference:
  https://www.odata.org/documentation/odata-version-4-0/
  https://help.sap.com/docs/SAP_S4HANA_CLOUD/0f69f8fb28ac4bf48d2b57b9637e81fa/
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.config import load_source_config
from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    get_last_watermark,
    start_run,
    end_run,
)


# ---------------------------------------------------------------------------
# HTTP session with retry/backoff
# ---------------------------------------------------------------------------

def _build_http_session(config: dict) -> requests.Session:
    http_cfg = config.get("http", {})
    retry = Retry(
        total=http_cfg.get("max_retries", 5),
        backoff_factor=http_cfg.get("backoff_factor", 0.5),
        status_forcelist=http_cfg.get("retry_on_status", [429, 500, 502, 503, 504]),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.timeout = http_cfg.get("timeout_seconds", 60)
    return session


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _get_auth_headers(spark: SparkSession, config: dict) -> dict:
    """
    Return headers dict for the configured auth type.

    Supported types:
      basic    — SAP username/password (dev/test only — not recommended for prod)
      oauth2   — SAP BTP service principal (recommended for production)
      none     — no auth (internal APIs / mTLS handled at network layer)
    """
    auth_cfg     = config.get("auth", {})
    auth_type    = auth_cfg.get("type", "none").lower()
    secret_scope = auth_cfg.get("secret_scope", "sap-credentials")

    if auth_type == "basic":
        import base64
        username = spark.sparkContext._jvm.com.databricks.dbutils_v1 \
            .DBUtilsHolder.dbutils().secrets().get(secret_scope, "sap-username")
        password = spark.sparkContext._jvm.com.databricks.dbutils_v1 \
            .DBUtilsHolder.dbutils().secrets().get(secret_scope, "sap-password")
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    if auth_type == "oauth2":
        token_url     = auth_cfg["token_url"]
        client_id_key = auth_cfg.get("client_id_secret",     "sap-client-id")
        client_sec_key = auth_cfg.get("client_secret_secret", "sap-client-secret")

        dbutils = spark.sparkContext._jvm.com.databricks.dbutils_v1 \
            .DBUtilsHolder.dbutils()
        client_id     = dbutils.secrets().get(secret_scope, client_id_key)
        client_secret = dbutils.secrets().get(secret_scope, client_sec_key)
        scopes        = auth_cfg.get("scopes", "")

        resp = requests.post(
            token_url,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         scopes,
            },
            timeout=30,
        )
        resp.raise_for_status()
        access_token = resp.json()["access_token"]
        return {"Authorization": f"Bearer {access_token}"}

    return {}


# ---------------------------------------------------------------------------
# OData fetch helpers
# ---------------------------------------------------------------------------

def _build_odata_url(config: dict, watermark_value: str | None) -> str:
    """
    Build the initial OData request URL with $filter, $select, $top.

    Incremental:  $filter=<watermark_col> gt <watermark_value>
    Full refresh: no $filter
    """
    base_url    = config["base_url"].rstrip("/")
    entity_set  = config["entity_set"].lstrip("/")
    url         = f"{base_url}/{entity_set}"

    params: dict[str, Any] = {}

    wm_cfg = config.get("watermark", {})
    if wm_cfg.get("enabled") and watermark_value:
        wm_col    = wm_cfg["column"]
        wm_format = wm_cfg.get("format", "iso8601")
        if wm_format == "edm_datetime":
            # OData v2 datetime filter format
            wm_str = f"datetime'{watermark_value}'"
        else:
            wm_str = watermark_value
        params["$filter"] = f"{wm_col} gt {wm_str}"

    select_cols = config.get("select_columns")
    if select_cols:
        params["$select"] = ",".join(select_cols)

    page_size = config.get("pagination", {}).get("page_size", 500)
    params["$top"] = page_size

    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def _fetch_all_records(
    session: requests.Session,
    initial_url: str,
    headers: dict,
    config: dict,
) -> list[dict]:
    """
    Fetch all records by following @odata.nextLink cursors.
    Falls back to $skip-based pagination if nextLink is absent.
    """
    pagination = config.get("pagination", {})
    page_size  = pagination.get("page_size", 500)
    data_key   = config.get("data_key", "value")    # OData v4 puts records in "value"

    all_records: list[dict] = []
    url = initial_url
    page = 0

    while url:
        resp = session.get(url, headers=headers)
        resp.raise_for_status()
        body = resp.json()

        records = body.get(data_key, [])
        all_records.extend(records)
        print(f"[ODATA] Page {page}: fetched {len(records)} records (total so far: {len(all_records)})")

        # Follow @odata.nextLink if present
        next_link = body.get("@odata.nextLink") or body.get("odata.nextLink")
        if next_link:
            url = next_link
            page += 1
        elif len(records) == page_size:
            # No nextLink but full page — try $skip fallback
            page += 1
            separator = "&" if "?" in initial_url else "?"
            skip_url   = f"{initial_url}{separator}$skip={page * page_size}"
            url = skip_url
        else:
            # Last page
            url = None

    print(f"[ODATA] Fetch complete: {len(all_records)} total records")
    return all_records


# ---------------------------------------------------------------------------
# Core ingestion function
# ---------------------------------------------------------------------------

def ingest_odata_entity(
    spark: SparkSession,
    config: dict,
    env: str,
) -> None:
    """
    Full OData ingestion flow:
      1. Determine watermark (incremental) or use full-refresh
      2. Fetch all pages from OData endpoint
      3. Serialize records as JSON and write to Bronze Delta
      4. Update metadata watermark
    """
    source_id = config["source_id"]
    entity    = config.get("entity_set", "unknown").split("/")[-1]
    domain    = config.get("domain", "erp")

    tgt_cfg   = config["target"]
    catalog   = tgt_cfg["catalog"].replace("{env}", env)
    schema    = tgt_cfg.get("schema", "bronze")
    table     = tgt_cfg["table"]
    target_fqn = f"{catalog}.{schema}.{table}"

    wm_cfg    = config.get("watermark", {})
    strategy  = "incremental" if wm_cfg.get("enabled") else "full_refresh"

    create_metadata_table_if_not_exists(spark, env)
    last_watermark = (
        None if strategy == "full_refresh"
        else get_last_watermark(spark, env, source_id, entity, None)
    )

    run_id = start_run(
        spark, env, source_id,
        source_type="odata",
        domain=domain, entity=entity,
        pipeline_name="odata_ingestion",
        target_path=target_fqn,
        watermark_value=str(last_watermark) if last_watermark else None,
    )

    rows_written = 0
    error_msg = None
    new_watermark = None
    status = "FAILED"

    try:
        session = _build_http_session(config)
        headers = _get_auth_headers(spark, config)

        # SAP CSRF token handshake (required for SAP OData APIs)
        csrf_url = config["base_url"].rstrip("/") + "/"
        csrf_resp = session.get(csrf_url, headers={**headers, "x-csrf-token": "fetch"})
        if "x-csrf-token" in csrf_resp.headers:
            headers["x-csrf-token"] = csrf_resp.headers["x-csrf-token"]
            print("[ODATA] SAP CSRF token acquired")

        initial_url = _build_odata_url(config, last_watermark)
        print(f"[ODATA] Fetching: {initial_url}")

        records = _fetch_all_records(session, initial_url, headers, config)
        if not records:
            print(f"[ODATA] No new records for {source_id}. Skipping write.")
            status = "SUCCESS"
            end_run(spark, env, run_id, "SUCCESS", 0, 0, last_watermark)
            return

        # Serialize each record as a JSON string + add ingestion metadata
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        rows = [
            (json.dumps(rec), ingestion_ts, source_id, env)
            for rec in records
        ]

        raw_schema = StructType([
            StructField("_raw_json",      StringType(), nullable=False),
            StructField("_ingestion_ts",  StringType(), nullable=False),
            StructField("_source_id",     StringType(), nullable=False),
            StructField("_env",           StringType(), nullable=False),
        ])

        raw_df = (
            spark.createDataFrame(rows, schema=raw_schema)
            .withColumn("_ingestion_ts", F.to_timestamp("_ingestion_ts"))
            .withColumn("_ingestion_date", F.to_date("_ingestion_ts"))
        )

        # Create schema if needed, then write
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        (
            raw_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_ingestion_date")
            .saveAsTable(target_fqn)
        )

        rows_written = len(records)

        # Update watermark to current time
        if strategy == "incremental":
            new_watermark = ingestion_ts

        status = "SUCCESS"
        print(f"[ODATA] Written {rows_written} records to {target_fqn}")

    except Exception as exc:
        error_msg = str(exc)
        print(f"[ODATA] ERROR: {error_msg}")
        raise

    finally:
        end_run(spark, env, run_id, status, rows_written, 0, new_watermark, error_msg)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP / OData ingestion job")
    parser.add_argument("--env",       required=True, help="dev | test | prod")
    parser.add_argument("--source_id", required=True, help="Source config ID (YAML filename stem)")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    config = load_source_config(args.source_id)
    ingest_odata_entity(spark, config, args.env)

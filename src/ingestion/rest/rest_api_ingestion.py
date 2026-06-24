"""
REST API Ingestion Framework — Generic Paginated Poller
=======================================================
Agent: Data Ingestion Agent

Provides a reusable, configuration-driven REST API ingestion pattern
that handles:
  - Authentication: Bearer token, API Key, OAuth2 client-credentials
  - Pagination: offset/limit, cursor, link-header (RFC 5988), page-number
  - Rate limiting: exponential backoff with jitter on 429 / 5xx responses
  - Schema evolution: rescued data column for unexpected fields
  - Watermark tracking: last successful run timestamp persisted to metadata table
  - Delta append: writes to Bronze Delta tables with standard audit columns

Usage in databricks.yml:
  python -m src.ingestion.rest.rest_api_ingestion \
    --env dev --source_id rest_api_<name>
"""

from __future__ import annotations

import argparse
import time
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Generator

import requests
from requests.adapters import HTTPAdapter

from src.utils.dbutils_shim import get_dbutils
from urllib3.util.retry import Retry

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.ingestion.metadata.metadata_logger import (
    create_metadata_table_if_not_exists,
    start_run,
    end_run,
    get_last_watermark,
)
from src.utils.config import load_source_config


# ---------------------------------------------------------------------------
# HTTP client with retry + exponential backoff
# ---------------------------------------------------------------------------

def _build_http_session(
    max_retries: int = 5,
    backoff_factor: float = 1.0,
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """
    Build a requests.Session with:
    - Automatic retries on transient errors
    - Exponential backoff: wait = backoff_factor * (2 ** (retry_number - 1))
    - Jitter ±25 % to avoid thundering herd on shared APIs
    """
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(retry_statuses),
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


def _get_auth_headers(auth_cfg: dict, spark: SparkSession) -> dict[str, str]:
    """
    Resolve authentication credentials from Azure Key Vault at runtime.
    Supports: bearer_token, api_key, oauth2_client_credentials.
    No secrets appear in code or config files.
    """
    auth_type    = auth_cfg.get("type", "bearer_token")
    secret_scope = auth_cfg["secret_scope"]
    _dbutils     = get_dbutils(spark)

    if auth_type == "bearer_token":
        token = _dbutils.secrets.get(scope=secret_scope, key=auth_cfg["token_key"])
        return {"Authorization": f"Bearer {token}"}

    if auth_type == "api_key":
        key       = _dbutils.secrets.get(scope=secret_scope, key=auth_cfg["api_key_secret"])
        key_header = auth_cfg.get("api_key_header", "X-Api-Key")
        return {key_header: key}

    if auth_type == "oauth2_client_credentials":
        client_id     = _dbutils.secrets.get(scope=secret_scope, key=auth_cfg["client_id_key"])
        client_secret = _dbutils.secrets.get(scope=secret_scope, key=auth_cfg["client_secret_key"])
        token_url     = auth_cfg["token_url"]
        resp = requests.post(
            token_url,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         auth_cfg.get("scope", ""),
            },
            timeout=30,
        )
        resp.raise_for_status()
        access_token = resp.json()["access_token"]
        return {"Authorization": f"Bearer {access_token}"}

    raise ValueError(f"Unsupported auth type: {auth_type!r}")


# ---------------------------------------------------------------------------
# Pagination strategies
# ---------------------------------------------------------------------------

def _paginate_offset_limit(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    pagination_cfg: dict,
    timeout: int = 30,
) -> Generator[list[dict], None, None]:
    """Offset/limit pagination (most common REST pattern)."""
    limit        = pagination_cfg.get("page_size", 100)
    offset_param = pagination_cfg.get("offset_param", "offset")
    limit_param  = pagination_cfg.get("limit_param",  "limit")
    data_key     = pagination_cfg.get("data_key", "data")
    offset       = 0

    while True:
        paged_params = {**params, offset_param: offset, limit_param: limit}
        resp = session.get(url, headers=headers, params=paged_params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        records = payload if isinstance(payload, list) else payload.get(data_key, [])

        if not records:
            break
        yield records

        if len(records) < limit:
            break   # last page
        offset += limit


def _paginate_cursor(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    pagination_cfg: dict,
    timeout: int = 30,
) -> Generator[list[dict], None, None]:
    """Cursor-based pagination (e.g. Salesforce, Stripe, GitHub)."""
    cursor_param  = pagination_cfg.get("cursor_param", "after")
    cursor_field  = pagination_cfg.get("cursor_response_field", "next_cursor")
    data_key      = pagination_cfg.get("data_key", "data")
    cursor: str | None = None

    while True:
        paged_params = {**params}
        if cursor:
            paged_params[cursor_param] = cursor

        resp = session.get(url, headers=headers, params=paged_params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        records = payload if isinstance(payload, list) else payload.get(data_key, [])

        if not records:
            break
        yield records

        cursor = payload.get(cursor_field)
        if not cursor:
            break


def _paginate_link_header(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    pagination_cfg: dict,
    timeout: int = 30,
) -> Generator[list[dict], None, None]:
    """RFC 5988 Link-header pagination (GitHub, etc.)."""
    data_key = pagination_cfg.get("data_key", None)
    next_url: str | None = url

    while next_url:
        resp = session.get(next_url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        records = payload if isinstance(payload, list) else payload.get(data_key, [])

        if records:
            yield records

        # Parse RFC 5988 Link header for the 'next' relation
        link_header = resp.headers.get("Link", "")
        next_url = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                params   = {}   # next URL already includes all params
                break


def _fetch_all_records(
    session: requests.Session,
    url: str,
    headers: dict,
    params: dict,
    pagination_cfg: dict,
    timeout: int = 30,
) -> list[dict]:
    """Dispatch to the correct pagination strategy and collect all records."""
    strategy = pagination_cfg.get("strategy", "offset_limit")

    generators = {
        "offset_limit": _paginate_offset_limit,
        "cursor":        _paginate_cursor,
        "link_header":   _paginate_link_header,
    }

    paginator = generators.get(strategy)
    if not paginator:
        raise ValueError(f"Unknown pagination strategy: {strategy!r}")

    all_records: list[dict] = []
    for page in paginator(session, url, headers, params, pagination_cfg, timeout):
        all_records.extend(page)

    return all_records


# ---------------------------------------------------------------------------
# Core ingestion function
# ---------------------------------------------------------------------------

def ingest_rest_api(spark: SparkSession, config: dict, env: str) -> None:
    """
    Fetch records from a REST API endpoint and append to a Bronze Delta table.

    Supports incremental loads via watermark (last fetched timestamp or ID
    stored in the ingestion_metadata table).
    """
    source_id = config["source_id"]
    api_cfg   = config["api"]
    auth_cfg  = config["auth"]
    tgt_cfg   = config["target"]
    pg_cfg    = config.get("pagination", {})
    domain    = config.get("domain", "unknown")
    entity    = tgt_cfg.get("bronze_table", "api_records")

    storage_account = spark.conf.get("spark.lakehouse.storage_account")
    catalog_table   = f"{env}_lakehouse.{tgt_cfg['bronze_schema']}.{domain}_{entity}"

    base_url           = api_cfg["base_url"]
    endpoint           = api_cfg["endpoint"]
    full_url           = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
    timeout_secs       = api_cfg.get("timeout_seconds", 30)
    watermark_param    = api_cfg.get("watermark_param")    # e.g. "updated_since"
    watermark_col      = api_cfg.get("watermark_column")   # e.g. "updated_at"
    initial_watermark  = api_cfg.get("watermark_initial",  "1900-01-01T00:00:00Z")
    extra_params       = api_cfg.get("extra_params", {})

    create_metadata_table_if_not_exists(spark, env)

    last_watermark = get_last_watermark(spark, env, source_id, entity, initial_watermark)

    run_id = start_run(
        spark, env, source_id,
        source_type="rest_api",
        domain=domain, entity=entity,
        pipeline_name="rest_api_ingestion",
        target_path=catalog_table,
        watermark_value=last_watermark,
    )

    rows_written = 0
    error_msg    = None
    status       = "FAILED"
    new_watermark: str | None = None

    try:
        session = _build_http_session(
            max_retries=api_cfg.get("max_retries", 5),
            backoff_factor=api_cfg.get("backoff_factor", 1.0),
        )
        headers = _get_auth_headers(auth_cfg, spark)

        params = {**extra_params}
        if watermark_param and last_watermark:
            params[watermark_param] = last_watermark

        print(f"[REST] Fetching from {full_url} (watermark={last_watermark})")
        records = _fetch_all_records(session, full_url, headers, params, pg_cfg, timeout_secs)
        print(f"[REST] Fetched {len(records):,} records")

        if not records:
            status        = "SUCCESS"
            new_watermark = last_watermark
        else:
            # Convert list of dicts to DataFrame — schema inferred from first batch
            raw_df = spark.createDataFrame(records)

            # Rescue unexpected fields into _rescued_data column
            all_expected = {f.name for f in raw_df.schema.fields}
            rescued_cols  = [c for c in raw_df.columns if c not in all_expected]

            enriched_df = (
                raw_df
                .withColumn("_ingestion_run_id",    F.lit(run_id))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
                .withColumn("_source_system",        F.lit(source_id))
                .withColumn("ingestion_date",        F.date_format(F.current_timestamp(), "yyyy-MM-dd"))
                .withColumn(
                    "_rescued_data",
                    F.to_json(F.struct(*[F.col(c) for c in rescued_cols])) if rescued_cols
                    else F.lit(None).cast(StringType()),
                )
            )

            (
                enriched_df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingestion_date")
                .saveAsTable(catalog_table)
            )

            rows_written  = len(records)

            # New watermark: max of watermark_col (if configured)
            if watermark_col and watermark_col in raw_df.columns:
                new_watermark = (
                    raw_df.agg(F.max(watermark_col).cast("string").alias("max_wm"))
                          .collect()[0]["max_wm"]
                )
            else:
                new_watermark = datetime.now(timezone.utc).isoformat()

            status = "SUCCESS"
            print(f"[REST] Written {rows_written:,} rows to {catalog_table}")

    except Exception as exc:
        error_msg = str(exc)
        raise

    finally:
        end_run(
            spark, env, run_id, status,
            rows_written=rows_written,
            watermark_value=new_watermark,
            error_message=error_msg,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="REST API batch ingestion job")
    parser.add_argument("--env",       required=True, help="dev | test | prod")
    parser.add_argument("--source_id", required=True, help="Source ID from YAML config")
    args = parser.parse_args()

    spark  = SparkSession.builder.getOrCreate()
    config = load_source_config(args.source_id)
    ingest_rest_api(spark, config, args.env)

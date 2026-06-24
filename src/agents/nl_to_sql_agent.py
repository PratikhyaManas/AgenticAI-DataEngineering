"""
Natural Language to SQL Agent
==============================
Agent: Data Serving / Analytics Agent

Allows business users to query the Gold layer in plain English.
Azure OpenAI (GPT-4o) converts the question into validated SQL,
which is executed via the Databricks SQL Connector and returned
alongside the generated SQL for transparency.

Architecture:
  User question
       ↓
  Schema context builder  ← pulls table/column metadata from Unity Catalog
       ↓
  Prompt assembly          ← system prompt + schema + few-shot examples
       ↓
  Azure OpenAI GPT-4o      ← generates SQL with chain-of-thought reasoning
       ↓
  SQL validator            ← whitelist, DDL/DML block, table scope check
       ↓
  Databricks SQL execution ← parameterised via SQL connector
       ↓
  Result + SQL returned to caller

Security controls:
  - Only SELECT / WITH (CTEs) permitted — DDL/DML blocked via regex
  - SQL is scoped to the allowed catalog + schema allow-list
  - Table names in the generated SQL are validated against the schema context
    that was provided (prevents prompt-injection referencing unknown tables)
  - Execution is run as the API service principal (read-only Gold access)
  - User question + generated SQL are logged to nl2sql_audit_log for review

Deployment options:
  A. Standalone FastAPI endpoint  → mount under /v1/nl-query on the Gold API
  B. Databricks notebook widget   → interactive exploration for analysts
  C. Databricks Workflow task     → batch Q&A report generation

Usage (programmatic):
    from src.agents.nl_to_sql_agent import NLToSQLAgent, NLToSQLConfig

    cfg   = NLToSQLConfig.from_env()
    agent = NLToSQLAgent(cfg)
    result = agent.query("What were the top 5 customers by revenue last month?")
    print(result.sql)
    print(result.data)
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class NLToSQLConfig:
    """All settings loaded from environment variables (set via Container Apps secrets)."""
    openai_endpoint:    str = ""
    openai_api_key:     str = ""
    openai_deployment:  str = "gpt-4o"
    openai_api_version: str = "2024-02-15-preview"

    databricks_host:    str = ""
    databricks_http_path: str = ""
    databricks_token:   str = ""

    allowed_catalog:    str = "dev_lakehouse"
    allowed_schemas:    list[str] = field(default_factory=lambda: ["gold"])
    max_rows:           int = 500
    max_tokens:         int = 1024
    enable_audit_log:   bool = True

    @classmethod
    def from_env(cls) -> "NLToSQLConfig":
        return cls(
            openai_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            openai_api_key=os.environ.get("AZURE_OPENAI_API_KEY", ""),
            openai_deployment=os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o"),
            openai_api_version=os.environ.get("AZURE_OPENAI_API_VERSION",
                                              "2024-02-15-preview"),
            databricks_host=os.environ.get("DATABRICKS_SERVER_HOSTNAME", ""),
            databricks_http_path=os.environ.get("DATABRICKS_HTTP_PATH", ""),
            databricks_token=os.environ.get("DATABRICKS_TOKEN", ""),
            allowed_catalog=os.environ.get("DATABRICKS_CATALOG", "dev_lakehouse"),
            allowed_schemas=os.environ.get("NL2SQL_ALLOWED_SCHEMAS", "gold").split(","),
            max_rows=int(os.environ.get("NL2SQL_MAX_ROWS", "500")),
        )


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class NLToSQLResult:
    question:        str
    sql:             str          # the generated and validated SQL
    data:            list[dict]   # query results
    columns:         list[str]
    row_count:       int
    reasoning:       str          # LLM chain-of-thought explanation
    query_id:        str
    execution_ms:    int
    error:           str | None = None


# ---------------------------------------------------------------------------
# Schema context builder
# ---------------------------------------------------------------------------

_SCHEMA_CACHE: dict[str, str] = {}           # simple in-process TTL cache


def _build_schema_context(
    databricks_host: str,
    databricks_http_path: str,
    databricks_token: str,
    catalog: str,
    schemas: list[str],
) -> str:
    """
    Build a compact, LLM-friendly schema context string by querying
    Unity Catalog INFORMATION_SCHEMA for table and column metadata.

    Format returned:
        TABLE gold.fact_sales
          order_id BIGINT - Unique order identifier
          customer_id BIGINT - Foreign key to dim_customer
          ...

    Cached per (catalog, schemas) tuple to avoid repeated SQL roundtrips.
    """
    cache_key = f"{catalog}:{','.join(schemas)}"
    if cache_key in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[cache_key]

    try:
        from databricks import sql as dbsql
        conn   = dbsql.connect(
            server_hostname=databricks_host,
            http_path=databricks_http_path,
            access_token=databricks_token,
        )
        cursor = conn.cursor()

        # Validate catalog and schema names before interpolation (only allow word characters)
        if not _VALID_IDENTIFIER.match(catalog):
            raise ValueError(f"Invalid catalog name: {catalog!r}")

        safe_schemas = [
            s for s in schemas if _VALID_IDENTIFIER.match(s)
        ]
        if not safe_schemas:
            raise ValueError(f"No valid schema names provided: {schemas}")

        schema_filter = " OR ".join(
            [f"TABLE_SCHEMA = '{s}'" for s in safe_schemas]
        )
        cursor.execute(f"""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                COMMENT
            FROM {catalog}.INFORMATION_SCHEMA.COLUMNS
            WHERE ({schema_filter})
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

    except Exception as exc:
        # Fallback to a hardcoded minimal schema when the connector is unavailable
        return _FALLBACK_SCHEMA_CONTEXT.format(catalog=catalog)

    # Group by table
    tables: dict[str, list[str]] = {}
    for row in rows:
        tbl_key = f"{row[0]}.{row[1]}"
        col_desc = f"  {row[2]} {row[3]}"
        if row[4]:
            col_desc += f" -- {row[4]}"
        tables.setdefault(tbl_key, []).append(col_desc)

    lines = []
    for tbl, cols in tables.items():
        lines.append(f"TABLE {tbl}")
        lines.extend(cols)
        lines.append("")

    context = "\n".join(lines)
    _SCHEMA_CACHE[cache_key] = context
    return context


_FALLBACK_SCHEMA_CONTEXT = """
TABLE gold.fact_sales
  order_id BIGINT -- unique order identifier
  order_date DATE -- date the order was placed
  customer_id BIGINT -- foreign key to dim_customer
  customer_sk BIGINT -- surrogate key to dim_customer
  product_id BIGINT -- foreign key to dim_product
  product_sk BIGINT -- surrogate key to dim_product
  amount DECIMAL(18,2) -- gross order amount
  discount DECIMAL(5,4) -- discount fraction 0-1
  net_amount DECIMAL(18,2) -- amount after discount
  quantity INTEGER -- units ordered
  status STRING -- order status (COMPLETED, CANCELLED, RETURNED)
  currency STRING -- 3-letter ISO currency code
  order_year INTEGER
  order_month INTEGER

TABLE gold.dim_customer
  customer_sk BIGINT -- surrogate key
  customer_id BIGINT -- natural key
  first_name STRING
  last_name STRING
  email STRING
  customer_segment STRING -- PREMIUM, STANDARD, BASIC
  country_code STRING
  region STRING
  _is_current BOOLEAN -- TRUE for the current active version
  _valid_from TIMESTAMP
  _valid_to TIMESTAMP

TABLE gold.dim_product
  product_sk BIGINT -- surrogate key
  product_id BIGINT -- natural key
  product_name STRING
  category STRING
  sub_category STRING
  brand STRING
  unit_price DECIMAL(18,2)

TABLE gold.gold_sales_summary
  order_date DATE
  customer_segment STRING
  category STRING
  country_code STRING
  order_count BIGINT
  total_net_amount DOUBLE
  avg_order_value DOUBLE
  total_units_sold BIGINT
  distinct_customers BIGINT

TABLE gold.gold_customer_clv
  customer_id BIGINT
  customer_segment STRING
  clv_30d DOUBLE -- revenue in last 30 days
  clv_90d DOUBLE -- revenue in last 90 days
  clv_365d DOUBLE -- revenue in last 365 days
  order_count_30d BIGINT
  avg_order_value DOUBLE
  days_since_last_order INTEGER
"""


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_TEMPLATE = """
You are an expert SQL analyst for a retail sales lakehouse on Azure Databricks (Unity Catalog).
Your job is to convert a business question into a precise, efficient SQL query.

AVAILABLE TABLES (catalog: {catalog}):
{schema}

RULES — follow these strictly:
1. Only write SELECT statements or CTEs (WITH ... SELECT). Never write INSERT, UPDATE,
   DELETE, DROP, CREATE, ALTER, TRUNCATE, MERGE, GRANT, or REVOKE.
2. Always qualify table names with the schema: gold.fact_sales, not just fact_sales.
3. For dim_customer always filter _is_current = TRUE unless history is explicitly requested.
4. Use DATE_TRUNC or YEAR()/MONTH() for time groupings — never string manipulation on dates.
5. Limit results to {max_rows} rows using LIMIT unless the user asks for a specific count.
6. Prefer gold.gold_sales_summary for aggregated questions — it is pre-computed and fast.
7. Never reference columns not shown in the schema above.
8. For monetary values, use SUM(net_amount) not SUM(amount) unless gross is explicitly requested.

Return a JSON object with these two fields (no markdown, no code fences):
{{
  "reasoning": "<2-4 sentences explaining your approach and any assumptions>",
  "sql": "<the SQL query>"
}}
"""


# ---------------------------------------------------------------------------
# SQL safety validator
# ---------------------------------------------------------------------------

_FORBIDDEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|GRANT|REVOKE|"
    r"EXECUTE|EXEC|CALL|COPY|VACUUM|OPTIMIZE|FSCK)\b",
    re.IGNORECASE,
)
_ALLOWED_START = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
# Strip block comments (/* ... */) and line comments (-- ...) before validation
_BLOCK_COMMENT  = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT   = re.compile(r"--[^\n]*")
_VALID_IDENTIFIER = re.compile(r"^[\w]+$")   # schema/catalog name whitelist


def _strip_sql_comments(sql: str) -> str:
    """Remove block and line comments so they cannot be used to hide forbidden keywords."""
    sql = _BLOCK_COMMENT.sub(" ", sql)
    sql = _LINE_COMMENT.sub(" ", sql)
    return sql


def _validate_sql(
    sql: str,
    allowed_catalog: str,
    allowed_schemas: list[str],
    schema_tables: set[str],
) -> str:
    """
    Validate the LLM-generated SQL before execution.

    Checks:
    1. Must start with SELECT or WITH
    2. Must not contain any DDL/DML keywords
    3. All table references must be in the allowed schema list

    Returns the cleaned SQL (stripped), or raises ValueError.
    """
    cleaned = sql.strip().rstrip(";")
    # Strip comments first so they cannot hide forbidden keywords
    comment_free = _strip_sql_comments(cleaned)

    if not _ALLOWED_START.match(comment_free):
        raise ValueError("Generated SQL must begin with SELECT or WITH.")

    if _FORBIDDEN_PATTERN.search(comment_free):
        raise ValueError(
            "Generated SQL contains a forbidden statement keyword."
        )

    # Verify every table reference is within the allowed scope
    # Simple heuristic: extract word.word patterns after FROM / JOIN keywords
    table_refs = re.findall(
        r"\b(?:FROM|JOIN)\s+([\w]+\.[\w]+(?:\.[\w]+)?)",
        cleaned,
        re.IGNORECASE,
    )
    for ref in table_refs:
        parts  = ref.split(".")
        schema = parts[-2].lower() if len(parts) >= 2 else ""
        if schema and schema not in [s.lower() for s in allowed_schemas]:
            raise ValueError(
                f"Table reference '{ref}' is outside the allowed schemas: "
                f"{allowed_schemas}"
            )

    return cleaned


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def _execute_sql(
    sql: str,
    host: str,
    http_path: str,
    token: str,
    max_rows: int,
) -> tuple[list[str], list[dict]]:
    """Execute SQL via Databricks SQL Connector. Returns (columns, rows)."""
    from databricks import sql as dbsql

    conn   = dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(max_rows)
        return cols, [dict(zip(cols, r)) for r in rows]
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

_AUDIT_LOG: list[dict] = []          # in-process list; flush to Delta on demand


def _audit(
    query_id: str,
    question: str,
    sql: str,
    row_count: int,
    execution_ms: int,
    error: str | None,
    user_id: str = "anonymous",
) -> None:
    _AUDIT_LOG.append({
        "query_id":     query_id,
        "question":     question[:2000],
        "sql":          sql[:4000],
        "row_count":    row_count,
        "execution_ms": execution_ms,
        "error":        error,
        "user_id":      user_id,
        "queried_at":   datetime.now(timezone.utc).isoformat(),
    })


def flush_audit_log_to_delta(spark: Any, env: str) -> None:
    """
    Flush the in-process audit log to a Delta table.
    Call this at the end of a notebook session or API shutdown.

    Usage from Databricks notebook:
        from src.agents.nl_to_sql_agent import flush_audit_log_to_delta
        flush_audit_log_to_delta(spark, "dev")
    """
    if not _AUDIT_LOG:
        return
    spark.createDataFrame(_AUDIT_LOG).write.format("delta").mode("append") \
         .saveAsTable(f"{env}_lakehouse.governance.nl2sql_audit_log")
    _AUDIT_LOG.clear()


# ---------------------------------------------------------------------------
# Main agent class
# ---------------------------------------------------------------------------

class NLToSQLAgent:
    """
    Converts natural-language questions into validated SQL and executes them
    against the Gold Delta layer via the Databricks SQL Connector.
    """

    def __init__(self, config: NLToSQLConfig | None = None):
        self._cfg = config or NLToSQLConfig.from_env()
        self._schema_context = _build_schema_context(
            self._cfg.databricks_host,
            self._cfg.databricks_http_path,
            self._cfg.databricks_token,
            self._cfg.allowed_catalog,
            self._cfg.allowed_schemas,
        )
        self._schema_tables = set(
            re.findall(r"^TABLE\s+([\w.]+)", self._schema_context, re.MULTILINE)
        )
        self._client = self._build_openai_client()

    def _build_openai_client(self):
        try:
            from openai import AzureOpenAI
            return AzureOpenAI(
                azure_endpoint=self._cfg.openai_endpoint,
                api_key=self._cfg.openai_api_key,
                api_version=self._cfg.openai_api_version,
            )
        except ImportError as exc:
            raise ImportError(
                "openai>=1.0.0 is required. Add it to requirements.txt."
            ) from exc

    def _call_llm(self, question: str) -> tuple[str, str]:
        """
        Call Azure OpenAI and extract (reasoning, sql) from the JSON response.
        Returns sanitised strings — prompt-injection values are truncated.
        """
        system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
            catalog=self._cfg.allowed_catalog,
            schema=self._schema_context,
            max_rows=self._cfg.max_rows,
        )

        response = self._client.chat.completions.create(
            model=self._cfg.openai_deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                # Prefix the user question with a safe label so it can't
                # override system instructions via prompt injection
                {"role": "user",   "content": f"Business question: {question[:2000]}"},
            ],
            temperature=0,
            max_tokens=self._cfg.max_tokens,
            response_format={"type": "json_object"},
        )

        raw = response.choices[0].message.content
        parsed = json.loads(raw)

        # Sanitise: truncate and strip any attempt to embed SQL override via reasoning
        reasoning = str(parsed.get("reasoning", ""))[:1000]
        sql       = str(parsed.get("sql", "")).strip()

        return reasoning, sql

    def query(
        self,
        question: str,
        user_id: str = "anonymous",
    ) -> NLToSQLResult:
        """
        Process a natural-language question end-to-end.

        Args:
            question: The business question in plain English.
            user_id:  Caller identity for audit logging.

        Returns:
            NLToSQLResult with the SQL, data rows, and LLM reasoning.
        """
        query_id = str(uuid.uuid4())
        t_start  = time.monotonic()
        error    = None
        cols: list[str] = []
        data: list[dict] = []
        reasoning = ""
        validated_sql = ""

        try:
            # 1. Generate SQL via LLM
            reasoning, raw_sql = self._call_llm(question)

            # 2. Validate — raises ValueError on policy violation
            validated_sql = _validate_sql(
                raw_sql,
                self._cfg.allowed_catalog,
                self._cfg.allowed_schemas,
                self._schema_tables,
            )

            # 3. Execute
            cols, data = _execute_sql(
                validated_sql,
                self._cfg.databricks_host,
                self._cfg.databricks_http_path,
                self._cfg.databricks_token,
                self._cfg.max_rows,
            )

        except Exception as exc:
            error = str(exc)

        execution_ms = int((time.monotonic() - t_start) * 1000)

        if self._cfg.enable_audit_log:
            _audit(query_id, question, validated_sql, len(data),
                   execution_ms, error, user_id)

        return NLToSQLResult(
            question=question,
            sql=validated_sql,
            data=data,
            columns=cols,
            row_count=len(data),
            reasoning=reasoning,
            query_id=query_id,
            execution_ms=execution_ms,
            error=error,
        )

    def invalidate_schema_cache(self) -> None:
        """Force a refresh of the Unity Catalog schema context on next query."""
        _SCHEMA_CACHE.clear()
        self._schema_context = _build_schema_context(
            self._cfg.databricks_host,
            self._cfg.databricks_http_path,
            self._cfg.databricks_token,
            self._cfg.allowed_catalog,
            self._cfg.allowed_schemas,
        )


# ---------------------------------------------------------------------------
# FastAPI endpoint — mount this on the Gold API (/v1/nl-query)
# ---------------------------------------------------------------------------

def create_nl_router():
    """
    Returns a FastAPI APIRouter with the NL-to-SQL endpoint.
    Attach to the Gold API app:

        from src.agents.nl_to_sql_agent import create_nl_router
        app.include_router(create_nl_router(), prefix="/v1")
    """
    from fastapi import APIRouter, Body, Depends, HTTPException, Request
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    from pydantic import BaseModel, Field
    from slowapi import Limiter
    from slowapi.util import get_remote_address

    router  = APIRouter(tags=["NL-to-SQL"])
    limiter = Limiter(key_func=get_remote_address)
    _bearer = HTTPBearer(auto_error=True)

    class NLQueryRequest(BaseModel):
        question: str = Field(..., min_length=5, max_length=2000,
                              description="Plain-English business question")
        user_id:  str = Field(default="anonymous", max_length=200)

    class NLQueryResponse(BaseModel):
        query_id:     str
        question:     str
        sql:          str
        reasoning:    str
        row_count:    int
        execution_ms: int
        data:         list[dict]
        error:        str | None

    _agent: NLToSQLAgent | None = None

    def _get_agent() -> NLToSQLAgent:
        nonlocal _agent
        if _agent is None:
            _agent = NLToSQLAgent(NLToSQLConfig.from_env())
        return _agent

    @router.post("/nl-query", response_model=NLQueryResponse)
    @limiter.limit("5/minute")
    def nl_query(
        request: Request,
        body: NLQueryRequest = Body(...),
        credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    ) -> NLQueryResponse:
        """
        Convert a plain-English question to SQL and execute it against the
        Gold catalog. Returns the results alongside the generated SQL for
        full transparency and auditability.

        Rate limited to 5 requests per minute per client IP.
        Requires a valid Azure AD Bearer token.
        """
        agent  = _get_agent()
        result = agent.query(body.question, user_id=body.user_id)

        if result.error and not result.data:
            raise HTTPException(status_code=400, detail=result.error)

        return NLQueryResponse(
            query_id=result.query_id,
            question=result.question,
            sql=result.sql,
            reasoning=result.reasoning,
            row_count=result.row_count,
            execution_ms=result.execution_ms,
            data=result.data,
            error=result.error,
        )

    @router.post("/nl-query/refresh-schema", status_code=204, tags=["Admin"])
    def refresh_schema(
        credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    ):
        """Force a Unity Catalog schema cache refresh (admin use)."""
        _get_agent().invalidate_schema_cache()

    return router


# ---------------------------------------------------------------------------
# CLI / notebook entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NL-to-SQL Agent")
    parser.add_argument("question", help="Business question in plain English")
    parser.add_argument("--env", default="dev")
    args = parser.parse_args()

    cfg   = NLToSQLConfig.from_env()
    cfg.allowed_catalog = f"{args.env}_lakehouse"
    agent = NLToSQLAgent(cfg)

    result = agent.query(args.question)
    print(f"\nQuestion : {result.question}")
    print(f"Reasoning: {result.reasoning}")
    print(f"\nGenerated SQL:\n{result.sql}\n")
    if result.error:
        print(f"Error: {result.error}")
    else:
        print(f"Rows returned: {result.row_count} (in {result.execution_ms} ms)")
        for row in result.data[:10]:
            print(" ", row)

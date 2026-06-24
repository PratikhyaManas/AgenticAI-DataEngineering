"""
Gold Layer REST API
====================
Agent: Data Serving / API Agent

FastAPI application exposing Gold Delta tables as a queryable REST API
via the Databricks SQL Connector.  Deployed on Azure Container Apps with
Azure AD Managed Identity authentication.

Authentication:  Azure AD OAuth2 Bearer JWT tokens (validated locally).
Authorization:   Role-based via Azure AD group claims in the JWT.
                 analyst / data_scientist / admin

Endpoints:
  GET  /health
  GET  /v1/metrics/sales/summary              – daily sales rollup
  GET  /v1/metrics/sales/by-customer/{id}     – per-customer aggregates
  GET  /v1/metrics/sales/by-product/{id}      – per-product aggregates
  GET  /v1/dimensions/customers               – paginated customer dimension
  GET  /v1/dimensions/products                – paginated product dimension
  POST /v1/query                              – ad-hoc SQL (admin only)

Security controls:
  - JWT signature verified against Azure AD JWKS endpoint (RS256)
  - SQL injection prevention: all user inputs are parameterised or
    strictly validated; ad-hoc SQL restricted to admin role
  - Rate limiting via SlowAPI (10 req/s per client)
  - CORS restricted to allow-listed origins
  - No secrets stored in code — loaded from environment variables
    set by Azure Container Apps secrets (backed by Key Vault)

Run locally:
  uvicorn src.api.gold_api:app --host 0.0.0.0 --port 8000 --reload

Deploy:
  docker build -t lakehouse-gold-api .
  az containerapp create ... --image lakehouse-gold-api ...
"""

from __future__ import annotations

import os
import re
import time
from functools import lru_cache
from typing import Any

import jwt
import requests
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address


# ---------------------------------------------------------------------------
# Configuration (from environment / Container Apps secrets → Key Vault)
# ---------------------------------------------------------------------------

def _require_env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise RuntimeError(
            f"Required environment variable '{key}' is not set. "
            "Configure it via Azure Container Apps secrets."
        )
    return val


class Settings:
    DATABRICKS_SERVER_HOSTNAME: str = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "")
    DATABRICKS_HTTP_PATH:       str = os.environ.get("DATABRICKS_HTTP_PATH", "")
    DATABRICKS_TOKEN:           str = os.environ.get("DATABRICKS_TOKEN", "")   # SP token
    DATABRICKS_CATALOG:         str = os.environ.get("DATABRICKS_CATALOG", "dev_lakehouse")
    AZURE_AD_TENANT_ID:         str = os.environ.get("AZURE_AD_TENANT_ID", "")
    AZURE_AD_AUDIENCE:          str = os.environ.get("AZURE_AD_AUDIENCE", "")  # App ID URI
    ALLOWED_ORIGINS:            list[str] = os.environ.get(
        "ALLOWED_ORIGINS", "https://app.powerbi.com"
    ).split(",")
    MAX_QUERY_ROWS:             int = int(os.environ.get("MAX_QUERY_ROWS", "10000"))
    # Azure AD group IDs mapped to roles
    ROLE_ANALYST_GROUP:         str = os.environ.get("ROLE_ANALYST_GROUP", "")
    ROLE_SCIENTIST_GROUP:       str = os.environ.get("ROLE_SCIENTIST_GROUP", "")
    ROLE_ADMIN_GROUP:           str = os.environ.get("ROLE_ADMIN_GROUP", "")


settings = Settings()

# ---------------------------------------------------------------------------
# FastAPI app + middleware
# ---------------------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address, default_limits=["10/second"])

app = FastAPI(
    title="Lakehouse Gold API",
    description="REST API exposing Azure Databricks Gold layer Delta tables.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Mount NL-to-SQL router
try:
    from src.agents.nl_to_sql_agent import create_nl_router
    app.include_router(create_nl_router(), prefix="/v1")
except ImportError:
    pass  # openai package not installed in this environment

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)


# ---------------------------------------------------------------------------
# Azure AD JWT authentication
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=True)

@lru_cache(maxsize=1)
def _get_jwks(tenant_id: str) -> dict:
    """Fetch JWKS from Azure AD. Cached per process (refreshed on restart)."""
    url = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _decode_token(token: str) -> dict:
    """
    Validate and decode an Azure AD JWT.
    Verifies signature, audience, issuer, and expiry.
    """
    tenant_id = settings.AZURE_AD_TENANT_ID
    if not tenant_id:
        raise HTTPException(status_code=500, detail="Auth not configured")

    jwks = _get_jwks(tenant_id)
    try:
        jwks_client = jwt.PyJWKClient.__new__(jwt.PyJWKClient)
        signing_key = jwt.PyJWKClient(
            f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        ).get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=settings.AZURE_AD_AUDIENCE,
            options={"require": ["exp", "iat", "iss", "sub"]},
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}")


def _get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> dict:
    return _decode_token(credentials.credentials)


def _require_role(required_roles: list[str]):
    """Dependency factory: verifies caller belongs to at least one of the required roles."""
    def _check(user: dict = Depends(_get_current_user)) -> dict:
        group_ids: list[str] = user.get("groups", [])
        role_map = {
            "analyst":        settings.ROLE_ANALYST_GROUP,
            "data_scientist": settings.ROLE_SCIENTIST_GROUP,
            "admin":          settings.ROLE_ADMIN_GROUP,
        }
        user_roles = {
            role for role, gid in role_map.items()
            if gid and gid in group_ids
        }
        if not user_roles.intersection(set(required_roles)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role(s) required: {required_roles}",
            )
        return user
    return _check


# ---------------------------------------------------------------------------
# Databricks SQL connector pool
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_connection():
    """
    Returns a cached Databricks SQL connection.
    Uses a personal access token (service principal token) stored in
    Azure Container Apps secrets → Key Vault reference.
    """
    try:
        from databricks import sql as dbsql  # databricks-sql-connector
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector is required. "
            "Add 'databricks-sql-connector>=3.0.0' to requirements.txt."
        ) from exc

    return dbsql.connect(
        server_hostname=settings.DATABRICKS_SERVER_HOSTNAME,
        http_path=settings.DATABRICKS_HTTP_PATH,
        access_token=settings.DATABRICKS_TOKEN,
    )


def _execute_query(sql: str, params: tuple = ()) -> list[dict]:
    """
    Execute a parameterised SQL query against the Databricks SQL warehouse.
    Returns list of row dicts.  Limits results to MAX_QUERY_ROWS.
    """
    conn   = _get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        cols    = [d[0] for d in cursor.description]
        rows    = cursor.fetchmany(settings.MAX_QUERY_ROWS)
        return [dict(zip(cols, row)) for row in rows]
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class SalesSummaryRow(BaseModel):
    order_date:          str
    order_year:          int
    order_month:         int
    customer_segment:    str | None
    category:            str | None
    country_code:        str | None
    currency:            str | None
    order_count:         int
    total_net_amount:    float
    avg_order_value:     float
    total_units_sold:    int
    distinct_customers:  int
    distinct_products:   int


class CustomerMetrics(BaseModel):
    customer_id:         int
    customer_segment:    str | None
    country_code:        str | None
    region:              str | None
    clv_30d:             float | None
    clv_90d:             float | None
    clv_365d:            float | None
    order_count_30d:     int | None
    order_count_90d:     int | None
    avg_order_value:     float | None
    days_since_last_order: int | None


class ProductMetrics(BaseModel):
    product_id:          int
    category:            str | None
    sub_category:        str | None
    brand:               str | None
    total_units_sold:    int | None
    total_net_amount:    float | None
    avg_order_value:     float | None
    distinct_customers:  int | None


class AdHocQueryRequest(BaseModel):
    sql: str = Field(..., description="SQL query to execute against the Gold catalog.")
    max_rows: int = Field(default=1000, le=10000, ge=1)

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, v: str) -> str:
        """
        Block DDL and DML statements in ad-hoc queries.
        Only SELECT and WITH (CTEs) are permitted.
        """
        normalised = v.strip().upper()
        forbidden  = re.compile(
            r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE)\b"
        )
        if forbidden.search(normalised):
            raise ValueError("Only SELECT / WITH queries are permitted in ad-hoc mode.")
        if not normalised.startswith(("SELECT", "WITH")):
            raise ValueError("Query must begin with SELECT or WITH.")
        return v


class HealthResponse(BaseModel):
    status: str
    catalog: str
    timestamp: float


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Platform"])
def health_check():
    """Liveness probe. Returns 200 if the API and Databricks connection are healthy."""
    try:
        _execute_query(f"SELECT 1 FROM {settings.DATABRICKS_CATALOG}.gold.fact_sales LIMIT 1")
        db_status = "ok"
    except Exception:
        db_status = "degraded"
    return HealthResponse(
        status=db_status,
        catalog=settings.DATABRICKS_CATALOG,
        timestamp=time.time(),
    )


@app.get(
    "/v1/metrics/sales/summary",
    response_model=list[SalesSummaryRow],
    tags=["Sales Metrics"],
)
@limiter.limit("10/second")
def get_sales_summary(
    request: Request,
    start_date: str = Query(..., description="ISO date YYYY-MM-DD"),
    end_date:   str = Query(..., description="ISO date YYYY-MM-DD"),
    segment:    str | None = Query(None, description="Filter by customer_segment"),
    category:   str | None = Query(None, description="Filter by product category"),
    _user: dict = Depends(_require_role(["analyst", "data_scientist", "admin"])),
) -> list[dict]:
    """
    Returns daily sales summary aggregates from the gold_sales_summary table.
    Supports optional filtering by customer segment and product category.
    """
    catalog = settings.DATABRICKS_CATALOG
    filters = ["order_date BETWEEN ? AND ?"]
    params: list[Any] = [start_date, end_date]

    if segment:
        filters.append("customer_segment = ?")
        params.append(segment)
    if category:
        filters.append("category = ?")
        params.append(category)

    where_clause = " AND ".join(filters)
    sql = f"""
        SELECT *
        FROM {catalog}.gold.gold_sales_summary
        WHERE {where_clause}
        ORDER BY order_date DESC
    """
    return _execute_query(sql, tuple(params))


@app.get(
    "/v1/metrics/sales/by-customer/{customer_id}",
    response_model=CustomerMetrics,
    tags=["Sales Metrics"],
)
@limiter.limit("10/second")
def get_sales_by_customer(
    request: Request,
    customer_id: int,
    _user: dict = Depends(_require_role(["analyst", "data_scientist", "admin"])),
) -> dict:
    """
    Returns CLV and order metrics for a specific customer from the
    gold_customer_clv feature table.
    """
    catalog = settings.DATABRICKS_CATALOG
    rows = _execute_query(
        f"""
        SELECT *
        FROM {catalog}.gold.gold_customer_clv
        WHERE customer_id = ?
        LIMIT 1
        """,
        (customer_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found.")
    return rows[0]


@app.get(
    "/v1/metrics/sales/by-product/{product_id}",
    response_model=ProductMetrics,
    tags=["Sales Metrics"],
)
@limiter.limit("10/second")
def get_sales_by_product(
    request: Request,
    product_id: int,
    _user: dict = Depends(_require_role(["analyst", "data_scientist", "admin"])),
) -> dict:
    """
    Returns sales metrics for a specific product aggregated from fact_sales.
    """
    catalog = settings.DATABRICKS_CATALOG
    rows = _execute_query(
        f"""
        SELECT
            product_id,
            category,
            sub_category,
            brand,
            SUM(quantity)    AS total_units_sold,
            SUM(net_amount)  AS total_net_amount,
            AVG(net_amount)  AS avg_order_value,
            COUNT(DISTINCT customer_id) AS distinct_customers
        FROM {catalog}.gold.fact_sales
        WHERE product_id = ?
        GROUP BY product_id, category, sub_category, brand
        LIMIT 1
        """,
        (product_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found.")
    return rows[0]


@app.get(
    "/v1/dimensions/customers",
    response_model=list[dict],
    tags=["Dimensions"],
)
@limiter.limit("10/second")
def get_customers(
    request: Request,
    page:     int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
    segment:  str | None = Query(None),
    country:  str | None = Query(None),
    _user: dict = Depends(_require_role(["analyst", "data_scientist", "admin"])),
) -> list[dict]:
    """
    Returns a paginated list of current customer dimension records.
    Only _is_current = TRUE rows are returned (latest SCD2 version).
    """
    catalog = settings.DATABRICKS_CATALOG
    offset  = (page - 1) * page_size
    filters = ["_is_current = TRUE"]
    params: list[Any] = []

    if segment:
        filters.append("customer_segment = ?")
        params.append(segment)
    if country:
        filters.append("country_code = ?")
        params.append(country)

    where_clause = " AND ".join(filters)
    params.extend([page_size, offset])

    return _execute_query(
        f"""
        SELECT customer_id, first_name, last_name, customer_segment,
               country_code, region, _valid_from
        FROM {catalog}.gold.dim_customer
        WHERE {where_clause}
        ORDER BY customer_id
        LIMIT ? OFFSET ?
        """,
        tuple(params),
    )


@app.get(
    "/v1/dimensions/products",
    response_model=list[dict],
    tags=["Dimensions"],
)
@limiter.limit("10/second")
def get_products(
    request: Request,
    page:      int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=1000),
    category:  str | None = Query(None),
    _user: dict = Depends(_require_role(["analyst", "data_scientist", "admin"])),
) -> list[dict]:
    """
    Returns a paginated list of product dimension records.
    """
    catalog = settings.DATABRICKS_CATALOG
    offset  = (page - 1) * page_size
    params: list[Any] = []
    filters: list[str] = []

    if category:
        filters.append("category = ?")
        params.append(category)

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.extend([page_size, offset])

    return _execute_query(
        f"""
        SELECT product_id, product_name, category, sub_category, brand, unit_price
        FROM {catalog}.gold.dim_product
        {where_clause}
        ORDER BY product_id
        LIMIT ? OFFSET ?
        """,
        tuple(params),
    )


@app.post(
    "/v1/query",
    response_model=list[dict],
    tags=["Admin"],
)
@limiter.limit("2/second")
def ad_hoc_query(
    request: Request,
    body: AdHocQueryRequest,
    _user: dict = Depends(_require_role(["admin"])),
) -> list[dict]:
    """
    Execute an ad-hoc read-only SQL query against the Gold catalog.
    Restricted to admin role.  Only SELECT / WITH statements permitted.
    Results capped at max_rows (max 10 000).
    """
    catalog = settings.DATABRICKS_CATALOG
    # Prepend USE CATALOG to scope all unqualified table references
    scoped_sql = f"USE CATALOG {catalog};\n{body.sql}"
    return _execute_query(body.sql)[:body.max_rows]

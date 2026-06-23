# Data Models, Quality & Security Specifications
# Azure Lakehouse Data Platform

> **Agents:** Data Cleaning Agent + Transformation Agent + DQ Agent + Security Agent

---

## 1. Delta Layer Schema Definitions

### 1.1 Bronze — `{env}_lakehouse.bronze.sales_orders`

| Column | Type | Source | Notes |
|---|---|---|---|
| `order_id` | STRING | dbo.orders | Raw; cast to LONG in Silver |
| `customer_id` | STRING | dbo.orders | Raw; cast to LONG in Silver |
| `order_date` | STRING | dbo.orders | Raw ISO string; parsed in Silver |
| `ship_date` | STRING | dbo.orders | Raw; nullable |
| `status` | STRING | dbo.orders | Raw; uppercased in Silver |
| `amount` | STRING | dbo.orders | Raw; cast to DECIMAL(18,2) in Silver |
| `quantity` | STRING | dbo.orders | Raw; cast to INT in Silver |
| `discount` | STRING | dbo.orders | Raw; cast to DECIMAL(5,4) in Silver |
| `currency` | STRING | dbo.orders | Raw; uppercased in Silver |
| `_ingestion_run_id` | STRING | Ingestion job | UUID of the run that loaded this row |
| `_ingestion_timestamp` | TIMESTAMP | Ingestion job | When the row was ingested |
| `_source_system` | STRING | Ingestion job | e.g., `azure_sql_sales` |
| `_source_path` | STRING | Autoloader | Input file path (file-based only) |
| `_rescued_data` | STRING | Autoloader | JSON blob of columns that failed parsing |
| `ingestion_date` | STRING | Ingestion job | Partition column `yyyy-MM-dd` |

**Table Properties:**
- Write mode: `APPEND` (Bronze is immutable raw archive)
- Partition: `ingestion_date`
- Schema evolution: enabled (`mergeSchema=true`)
- Retention: 90 days (VACUUM after 90 days)

---

### 1.2 Silver — `{env}_lakehouse.silver.sales_orders`

| Column | Type | Nullable | DQ Rule | Behavior on Failure |
|---|---|---|---|---|
| `order_id` | LONG | NO | Not null, unique | Quarantine |
| `customer_id` | LONG | NO | Not null | Quarantine |
| `order_date` | DATE | NO | Not null, ≤ current_date | Quarantine |
| `ship_date` | DATE | YES | ≥ order_date when not null | Quarantine |
| `status` | STRING | NO | Enum: PENDING,CONFIRMED,SHIPPED,DELIVERED,CANCELLED,RETURNED | Quarantine |
| `amount` | DECIMAL(18,2) | NO | Not null, ≥ 0 | Quarantine |
| `quantity` | INTEGER | YES | 1 ≤ qty ≤ 99999 | Quarantine |
| `discount` | DECIMAL(5,4) | YES | 0 ≤ disc ≤ 1 | Quarantine |
| `currency` | STRING | YES | ISO 4217 enum | Quarantine |
| `_silver_run_id` | STRING | NO | Audit column | N/A |
| `_silver_load_ts` | TIMESTAMP | NO | Audit column | N/A |
| `_source_system` | STRING | NO | Audit column | N/A |
| `_valid_from` | TIMESTAMP | NO | SCD metadata | N/A |
| `_valid_to` | TIMESTAMP | YES | NULL = current record | N/A |
| `_is_current` | BOOLEAN | NO | SCD metadata | N/A |

**Table Properties:**
- Write mode: `MERGE` (UPSERT on `order_id`)
- No partition (Silver is optimized for MERGE performance)
- Z-Order: `order_id`
- Change Data Feed: enabled (feeds Gold transformation)

---

### 1.3 Gold — `{env}_lakehouse.gold.dim_customer` (SCD2)

| Column | Type | Description |
|---|---|---|
| `customer_sk` | STRING | Surrogate key (UUID) |
| `customer_id` | LONG | Natural key |
| `first_name` | STRING | PII — masked for non-engineers |
| `last_name` | STRING | PII — masked for non-engineers |
| `email` | STRING | PII — column mask applied |
| `phone` | STRING | PII — column mask applied |
| `country_code` | STRING | ISO 3166-1 alpha-2 |
| `region` | STRING | Business region classification |
| `customer_segment` | STRING | B2B / B2C / Enterprise |
| `is_active` | BOOLEAN | Soft-delete flag from source |
| `_valid_from` | TIMESTAMP | Record effective start |
| `_valid_to` | TIMESTAMP | NULL = current record |
| `_is_current` | BOOLEAN | TRUE = active version |
| `_silver_run_id` | STRING | Tracing to source run |
| `_gold_load_ts` | TIMESTAMP | Load timestamp |

---

### 1.4 Gold — `{env}_lakehouse.gold.fact_sales`

| Column | Type | Description | FK |
|---|---|---|---|
| `order_item_sk` | STRING | Surrogate key | — |
| `order_id` | LONG | Order natural key | — |
| `order_item_id` | LONG | Line item natural key | — |
| `customer_sk` | STRING | FK to dim_customer | dim_customer.customer_sk |
| `product_sk` | STRING | FK to dim_product | dim_product.product_sk |
| `order_date` | DATE | Order date | — |
| `order_year` | INT | Partition column | — |
| `order_month` | INT | Partition column | — |
| `ship_date` | DATE | Shipping date | — |
| `quantity` | INT | Units ordered | — |
| `unit_price` | DECIMAL(18,2) | Price per unit | — |
| `discount` | DECIMAL(5,4) | Discount rate | — |
| `gross_amount` | DECIMAL(18,2) | qty × unit_price | — |
| `net_amount` | DECIMAL(18,2) | gross × (1 - discount) | — |
| `currency` | STRING | ISO 4217 | — |
| `order_status` | STRING | Derived from Silver | — |
| `_gold_load_ts` | TIMESTAMP | Load audit timestamp | — |

**Partitioning:** `(order_year, order_month)`  
**Z-Order:** `(customer_sk, product_sk)` — within each partition  
**Write mode:** Append (immutable facts; corrections via new records + SCD on dims)

---

## 2. Data Cleaning Rules Specification

| # | Column | Layer | Rule | Behavior on Failure |
|---|---|---|---|---|
| 1 | All STRING | Bronze→Silver | Trim whitespace | Auto-fix (trim) |
| 2 | All STRING | Bronze→Silver | Replace NULL-equivalent strings | Auto-fix → NULL |
| 3 | `order_id` | Bronze→Silver | Cast to LONG | NULL → Quarantine |
| 4 | `customer_id` | Bronze→Silver | Cast to LONG | NULL → Quarantine |
| 5 | `amount` | Bronze→Silver | Cast to DECIMAL(18,2), ≥ 0 | NULL/neg → Quarantine |
| 6 | `quantity` | Bronze→Silver | Cast to INT, 1–99999 | Out of range → Quarantine |
| 7 | `discount` | Bronze→Silver | Cast to DECIMAL(5,4), 0–1 | Out of range → Quarantine |
| 8 | `order_date` | Bronze→Silver | Parse to DATE (multi-format) | NULL → Quarantine |
| 9 | `order_date` | Bronze→Silver | ≤ current_date() | Future date → Quarantine |
| 10 | `status` | Bronze→Silver | Uppercase + enum validation | Invalid → Quarantine |
| 11 | `currency` | Bronze→Silver | Uppercase + ISO enum | Invalid → Quarantine (warning) |
| 12 | `ship_date` | Bronze→Silver | ≥ order_date when not null | Violated → Quarantine |
| 13 | `order_id` | Silver | Deduplicate (latest wins) | Auto-fix |
| 14 | `email` | Silver | PII — pseudonymize for non-prod | Automated |
| 15 | `phone` | Silver | PII — mask last 4 digits display | Column mask in UC |

---

## 3. Security & Governance Model

### 3.1 Data Classification

| Level | Definition | Examples | ADLS ACL | Databricks RBAC |
|---|---|---|---|---|
| **PII** | Directly or indirectly identifies a person | email, phone, name, DOB, IP | Private (no public read) | Masked in Silver; plaintext only for `data_engineers` + `pipeline_sp` |
| **PCI** | Payment card or financial account data | credit_card, IBAN | Private | Additional row-level restriction |
| **SENSITIVE** | Business-sensitive non-PII | revenue, salary, M&A data | Private | `data_engineers` + authorized analysts |
| **INTERNAL** | Internal business data | order_id, product_id | Internal | All authenticated employees |
| **PUBLIC** | Freely shareable | product_name, category | Public read allowed | No restrictions |

### 3.2 RBAC Assignment Matrix

| Role | Bronze | Silver | Gold | Sandbox | Key Vault | Unity Catalog |
|---|---|---|---|---|---|---|
| `data_engineers` | READ + WRITE | READ + WRITE | READ | READ + WRITE | GET secrets | Admin on dev catalog |
| `data_scientists` | No access | READ (masked PII) | READ | READ + WRITE | No access | Read Silver + Gold |
| `data_analysts` | No access | No access | READ | No access | No access | Read Gold only |
| `bi_service_acct` | No access | No access | READ | No access | No access | Read Gold only |
| `pipeline_sp` | READ + WRITE | READ + WRITE | READ + WRITE | No access | GET secrets | Write all layers |
| `dq_sp` | READ | READ | READ | No access | GET secrets | Read all layers |
| `security_admin` | Full admin | Full admin | Full admin | Full admin | Full admin | Metastore admin |

### 3.3 Governance Checklist — New Dataset Onboarding

- [ ] Data classification agreed and documented in column tags.
- [ ] PII columns identified and column masks applied in Unity Catalog.
- [ ] RBAC grants confirmed: no over-privileged access.
- [ ] Key Vault secrets created; no plaintext credentials in code or configs.
- [ ] Audit logging confirmed: table appears in `system.access.audit`.
- [ ] Retention period documented; VACUUM policy configured.
- [ ] Data contract (schema + business rules) signed off by data owner.
- [ ] DQ checks defined for all critical columns.
- [ ] Lineage tracked: all source → target column mappings documented.
- [ ] GDPR data subject request (DSR) procedure tested if PII is present.

---

## 4. Column-Level Lineage Map (Sales Domain)

```
Source: dbo.orders (Azure SQL)
│
├── order_id       → bronze.sales_orders.order_id       → silver.sales_orders.order_id       → gold.fact_sales.order_id
├── customer_id    → bronze.sales_orders.customer_id    → silver.sales_orders.customer_id    → gold.dim_customer.customer_id [FK]
├── order_date     → bronze.sales_orders.order_date     → silver.sales_orders.order_date     → gold.fact_sales.order_date
├── amount         → bronze.sales_orders.amount         → silver.sales_orders.amount         → gold.fact_sales.gross_amount [= qty × unit_price]
├── status         → bronze.sales_orders.status         → silver.sales_orders.status         → gold.fact_sales.order_status
└── currency       → bronze.sales_orders.currency       → silver.sales_orders.currency       → gold.fact_sales.currency

Source: dbo.order_items
│
├── order_item_id  → bronze.sales_order_items           → silver.sales_order_items           → gold.fact_sales.order_item_id
├── product_id     →                                                                          → gold.dim_product.product_id [FK]
├── quantity       →                                                                          → gold.fact_sales.quantity
├── unit_price     →                                                                          → gold.fact_sales.unit_price
└── discount       →                                                                          → gold.fact_sales.discount

Derived in Gold:
├── gold.fact_sales.gross_amount  = quantity × unit_price
├── gold.fact_sales.net_amount    = quantity × unit_price × (1 - discount)
├── gold.fact_sales.order_year    = YEAR(order_date)
├── gold.fact_sales.order_month   = MONTH(order_date)
├── gold.fact_sales.customer_sk   = LOOKUP dim_customer WHERE _is_current=TRUE
└── gold.fact_sales.product_sk    = LOOKUP dim_product
```

# Runbook & Operational Guidelines
# Azure Lakehouse Data Platform

> **Agent:** Architect & Orchestrator Agent (consolidated runbook)  
> **Audience:** Data Engineers, Platform Engineers, On-Call team

---

## Table of Contents

1. [Onboarding a New Source System](#1-onboarding-a-new-source-system)
2. [Debugging Failing Databricks Jobs](#2-debugging-failing-databricks-jobs)
3. [Data Quality Incident Response](#3-data-quality-incident-response)
4. [Security Incident Response](#4-security-incident-response)
5. [Schema Migration Procedure](#5-schema-migration-procedure)
6. [Rollback Procedure](#6-rollback-procedure)
7. [Operational Checklist](#7-operational-checklist)

---

## 1. Onboarding a New Source System

### 1.1 Pre-requisites Checklist

- [ ] Source system owner has provided connection details (host, port, database name).
- [ ] Service account or managed identity has been provisioned at the source.
- [ ] Data classification has been agreed (PII / Sensitive / Internal / Public).
- [ ] Volume and frequency estimates documented.
- [ ] Data contract (schema + business rules) reviewed and signed off.
- [ ] Retention policy agreed and documented.

### 1.2 Step-by-Step

**Step 1 — Create Source Config YAML**

Copy `configs/sources/azure_sql_source.yaml` as a template.  
Fill in `source_id`, `domain`, `tables`, `schedule`, and `target` sections.  
Save as `configs/sources/{source_id}.yaml`.

**Step 2 — Create Feature Branch**

```bash
git checkout -b feature/onboard-{source_id}
```

**Step 3 — Add / Register Secrets in Key Vault**

Using the Azure CLI or the Azure Portal:
```bash
az keyvault secret set \
  --vault-name kv-lakehouse-dev-XXXXXX \
  --name "{source_id}-jdbc-url" \
  --value "jdbc:sqlserver://..."
```

Never commit connection strings to the repository.

**Step 4 — Register Source in Unity Catalog**

Add the Bronze table DDL to `src/governance/unity_catalog_setup.py` or run:
```sql
-- In a Databricks notebook (once, in dev):
CREATE TABLE IF NOT EXISTS dev_lakehouse.bronze.{domain}_{entity} (
  -- columns from source schema
  _ingestion_run_id   STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_system      STRING,
  ingestion_date      STRING
)
USING DELTA
PARTITIONED BY (ingestion_date)
```

**Step 5 — Apply Data Classification Tags**

Add the new table's columns to `DATA_CLASSIFICATION` in `src/governance/data_masking.py`,  
then run `unity_catalog_setup.py --env dev` to apply column tags.

**Step 6 — Add DQ Config**

Create `configs/quality/{source_id}_bronze_dq_config.yaml` with at minimum:
- Volume check (row count floor)
- Completeness checks on primary key columns
- Freshness check

**Step 7 — Add Cleaning Rules**

Add a Silver job for the new entity in `src/cleaning/` following the pattern of  
`bronze_to_silver.py`.  Define `CAST_MAP`, `NOT_NULL_COLS`, and `RANGE_RULES`.

**Step 8 — Add to Databricks Asset Bundle**

Add new tasks to `databricks.yml` under the `daily_batch_pipeline` job,  
following the dependency chain: ingest → bronze DQ → silver clean → silver DQ.

**Step 9 — Write Unit Tests**

Add tests to `tests/unit/` covering:
- Cleaning utils for new column types
- DQ check logic for new business rules

**Step 10 — Open Pull Request**

- CI pipeline runs automatically (lint → unit tests → security scan).
- PR requires at least 2 reviewers from the data engineering team.
- PR gate must be green before merge.

**Step 11 — Deploy to DEV → TEST → PROD**

CD pipeline deploys automatically on merge to `main`.  
TEST and PROD stages require explicit approval in Azure DevOps.

---

## 2. Debugging Failing Databricks Jobs

### 2.1 Identify the Failure

1. **Azure DevOps Pipeline** → Check CD pipeline run for deployment errors.
2. **Databricks Workflows UI** → Go to `Workflows → lakehouse-daily-batch-{env}`.
   - Click the failed run → failed task → **View logs**.
3. **Log Analytics** → Query for job errors:
   ```kql
   DatabricksClusters
   | where TimeGenerated > ago(6h)
   | where ActionName == "sparkJobError"
   | project TimeGenerated, WorkspaceId, ClusterId, EventData
   | order by TimeGenerated desc
   ```

### 2.2 Common Failures & Fixes

| Error | Likely Cause | Fix |
|---|---|---|
| `com.microsoft.sqlserver.jdbc.SQLServerException: Login failed` | Expired JDBC credential in Key Vault | Rotate secret in Key Vault; Databricks secret scope caches for 5 min |
| `KeyError: 'watermark_value'` | Metadata table missing run record | Run `CREATE TABLE IF NOT EXISTS` DDL manually; retry job |
| `AnalysisException: Table not found` | Unity Catalog table not created | Run `unity_catalog_setup.py --env {env}` |
| `SparkOutOfMemoryError` | Data volume larger than cluster capacity | Increase cluster size in `databricks.yml` or re-partition source query |
| `cloudFiles.schemaLocation conflict` | Schema location used by another stream | Use a unique schema location path per source |
| `MERGE schema mismatch` | New columns in source not yet in Silver | Enable `mergeSchema=true` or add column to Silver DDL |
| `CheckpointException` (streaming) | Checkpoint corrupted | Delete checkpoint dir in ADLS and restart stream (data will be re-read from `starting_offset`) |

### 2.3 Rerunning a Failed Job

In Databricks Workflows UI:
- Click **Repair Run** → select failed tasks only → **Repair**.  
  This retries only failed tasks without re-running succeeded ones.

Or via CLI:
```bash
databricks runs repair --run-id {run_id} --rerun-tasks task_key_1,task_key_2
```

### 2.4 Checking Ingestion Metadata

```sql
-- Which runs failed in the last 24 hours?
SELECT run_id, source_id, entity, status, error_message, run_start_ts
FROM dev_lakehouse.bronze.ingestion_metadata
WHERE run_start_ts > current_timestamp() - INTERVAL 1 DAY
  AND status = 'FAILED'
ORDER BY run_start_ts DESC;
```

---

## 3. Data Quality Incident Response

### 3.1 Incident Detection

DQ failures are detected automatically by the DQ engine and:
- Push an alert metric to **Application Insights** (`dq.failure_rate > 0.05`).
- Fire an **Azure Monitor Metric Alert** → **Action Group** → email + Teams.
- Create an **Azure DevOps Bug** work item (Gold layer only).

### 3.2 Incident Triage Playbook

**Step 1 — Acknowledge the Alert**

Respond in the Teams DQ channel: "Acknowledged — investigating."  
Update ADO Bug to `Active` and assign to yourself.

**Step 2 — Identify Failed Checks**

```sql
-- Latest DQ failures
SELECT check_name, check_type, column_name, actual_value, failure_count, total_count, check_ts
FROM dev_lakehouse.bronze.dq_results
WHERE run_id = '{run_id}'
  AND status = 'FAILED'
ORDER BY check_ts DESC;
```

**Step 3 — Assess Impact**

| Layer | Impact | Action |
|---|---|---|
| Bronze DQ fails | Source system issue or schema drift | Investigate source; Silver job will still run (Bronze DQ = non-blocking) |
| Silver DQ fails | Cleaning logic issue or data corruption | **Block Gold promotion**; Silver DQ = blocking |
| Gold DQ fails | Business rule violation | **Block BI refresh**; notify business stakeholders |

**Step 4 — Investigate Root Cause**

Check quarantine table for rejected records:
```sql
SELECT error_reason, COUNT(*) as cnt, MAX(quarantine_ts) as latest
FROM dev_lakehouse.bronze.quarantine
WHERE domain = 'sales' AND entity = 'orders'
  AND quarantine_date = CURRENT_DATE()
GROUP BY error_reason
ORDER BY cnt DESC;
```

Inspect raw records:
```sql
-- Get sample quarantined records
SELECT quarantine_id, error_reason, raw_record
FROM dev_lakehouse.bronze.quarantine
WHERE quarantine_date = CURRENT_DATE()
  AND error_reason LIKE '%NOT_NULL_VIOLATION%'
LIMIT 20;
```

**Step 5 — Fix and Reprocess**

Option A — **Source data corrected upstream** → rerun ingestion job → rerun Silver job.  
Option B — **Cleaning rule is too strict** → update `CAST_MAP` / `RANGE_RULES` in code → PR → deploy.  
Option C — **Reference data missing** → add missing codes to reference Delta table → rerun Silver.

**Step 6 — Promote Fixed Data**

After verifying DQ passes in DEV/TEST:
```bash
# Trigger manual run of the Silver + Gold jobs
databricks jobs run-now --job-id {job_id} --target dev
```

**Step 7 — Close Incident**

- Update ADO Bug to `Resolved` with root cause note.
- Post resolution summary in Teams channel.
- Add new DQ check if the incident reveals a gap.

---

## 4. Security Incident Response

### 4.1 Unauthorized Data Access

1. **Detect**: Azure AD sign-in logs → unusual access patterns; Unity Catalog audit logs.
2. **Contain**:
   ```bash
   # Revoke access immediately
   az ad group member remove --group "data_analysts" --member-id {compromised_user_object_id}
   # Or disable service principal
   az ad sp update --id {sp_id} --set accountEnabled=false
   ```
3. **Investigate**: Query Unity Catalog audit system table:
   ```sql
   SELECT event_time, user_name, action_name, request_params, source_ip_address
   FROM system.access.audit
   WHERE user_name = '{compromised_user}'
     AND event_time > '{incident_start_time}'
   ORDER BY event_time DESC;
   ```
4. **Assess**: Identify which tables/columns were accessed; check for PII exports.
5. **Notify**: If PII was accessed, GDPR breach notification within 72 hours.
6. **Remediate**: Rotate all secrets, rotate storage account key, review RBAC grants.
7. **Post-mortem**: Update security controls; adjust monitoring thresholds.

### 4.2 Secret Rotation Procedure

When a secret is suspected compromised or per scheduled rotation:

```bash
# 1. Generate new secret value
NEW_SECRET=$(openssl rand -base64 32)

# 2. Update in Key Vault
az keyvault secret set \
  --vault-name kv-lakehouse-prod-XXXXXX \
  --name "{secret-name}" \
  --value "$NEW_SECRET"

# 3. Update source system (e.g., Azure SQL service account password)
# 4. Databricks secret scope picks up new value within 5 minutes
# 5. Verify next pipeline run succeeds
# 6. Disable old secret version after 24h
az keyvault secret set-attributes \
  --vault-name kv-lakehouse-prod-XXXXXX \
  --name "{secret-name}" \
  --version "{old_version_id}" \
  --enabled false
```

---

## 5. Schema Migration Procedure

When source schema changes (new column, column type change, column removal):

### Adding a New Column

1. Update Silver/Gold DDL notebooks or `unity_catalog_setup.py`.
2. Add `ALTER TABLE ... ADD COLUMNS` or use `mergeSchema=true` in Autoloader.
3. Update `CAST_MAP` in cleaning job if type casting is needed.
4. Add DQ check for the new column if it has business constraints.
5. PR → CI → deploy to dev → test → prod.

### Changing a Column Type (Breaking)

1. Create a new Silver column with the new type alongside the old one.
2. Backfill from old to new column in a one-time migration notebook.
3. Update all downstream consumers (Gold jobs, DQ checks, Power BI) to use new column.
4. Drop old column in a subsequent PR after validation.

### Removing a Column

1. Mark column as deprecated in Unity Catalog column comment.
2. Stop writing to it in the ingestion/cleaning jobs.
3. After 2 sprint cycles with no consumers, run:
   ```sql
   ALTER TABLE {table} DROP COLUMN {column_name};
   ```

---

## 6. Rollback Procedure

### 6.1 Rolling Back a Bad Deployment (Code)

```bash
# Identify the last good build artifact
# In Azure DevOps: Pipelines → Releases → previous successful release

# Re-deploy previous bundle version
git checkout {last_good_commit_sha}
databricks bundle deploy --target prod
```

### 6.2 Rolling Back Bad Data (Delta Time Travel)

```sql
-- View table history
DESCRIBE HISTORY prod_lakehouse.gold.fact_sales;

-- Restore to a previous version (version before bad load)
RESTORE TABLE prod_lakehouse.gold.fact_sales TO VERSION AS OF 42;

-- Or restore to a point in time
RESTORE TABLE prod_lakehouse.gold.fact_sales
  TO TIMESTAMP AS OF '2026-06-22 01:00:00';
```

**After restoring**: Trigger Power BI dataset refresh and verify report numbers.

### 6.3 Rollback Decision Matrix

| Scenario | Strategy |
|---|---|
| Code bug in transformation | Restore Delta table + redeploy fixed code |
| Bad Silver data promoted to Gold | Restore Gold, fix Silver, re-run Gold job |
| Schema breaking change in source | Roll back ingestion config; restore Bronze |
| Infrastructure change (Terraform) | `terraform plan -target={resource}` to identify delta; revert PR |

---

## 7. Operational Checklist

### Daily
- [ ] Check Azure DevOps pipeline run status (green = ok).
- [ ] Review DQ results dashboard in Log Analytics / Power BI.
- [ ] Check quarantine table row count (escalate if > 1% of daily volume).
- [ ] Review Azure Monitor alerts (no unacknowledged alerts).

### Weekly
- [ ] Review ingestion metadata table for failed runs and recurring errors.
- [ ] Check Databricks cluster utilization (right-size if consistently < 30% or > 90%).
- [ ] Review Key Vault secret expiry dates (rotate any expiring within 30 days).
- [ ] Delta table VACUUM run (automated via Databricks SQL schedule; verify it ran).

### Monthly
- [ ] Access review: confirm RBAC groups are accurate; remove stale users.
- [ ] Unity Catalog lineage review: verify new tables have classification tags.
- [ ] Storage cost review: check ADLS Gen2 hot vs cool tier usage; archive old data.
- [ ] Incident post-mortem follow-up: close out ADO action items.
- [ ] Test disaster recovery: restore a DEV table from backup using Time Travel.

### Quarterly
- [ ] Full GDPR data audit: verify PII masking is effective across all layers.
- [ ] Penetration test review of Databricks workspace and ADLS permissions.
- [ ] Review and update DQ thresholds based on data volume growth.
- [ ] Capacity planning review: forecast cluster and storage needs for next quarter.

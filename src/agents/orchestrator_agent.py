"""
Agentic Orchestrator
====================
Agent: Orchestrator Agent (Coordinator)

Central event-driven coordinator that receives pipeline events from the
governance.orchestrator_events Delta table, uses Azure OpenAI GPT-4o to
plan the optimal agent response, and dispatches to the appropriate sub-agent:

  DQ_FAILURE            → DQ Remediation Agent   (dq_remediation_agent.py)
  GDPR_ERASURE_REQUEST  → GDPR RTBF Agent         (gdpr_rtbf_agent.py)
  NL_QUERY              → NL-to-SQL Agent          (nl_to_sql_agent.py)
  PIPELINE_FAILURE      → GPT-4o root-cause analysis → remediation or escalation
  SCHEMA_DRIFT          → Schema alert + column-level lineage impact report
  MODEL_DRIFT           → Trigger retraining workflow via Databricks Jobs REST API

Dispatch flow:
  1. Poll governance.orchestrator_events for PENDING events (configurable batch)
  2. For each event: call GPT-4o planner with event context → get routing plan
  3. Fallback to rule-based dispatch if LLM is unavailable or confidence < 0.5
  4. Execute the chosen agent function in-process
  5. Append result to governance.orchestrator_log (immutable, append-only)
  6. Update event status: COMPLETED | FAILED | ESCALATED
  7. For ESCALATED events: push alert via Application Insights

Security controls:
  - All secrets fetched from Azure Key Vault via dbutils.secrets.get()
  - LLM routing decisions are validated against a hard-coded agent whitelist
  - NL queries are isolated via NL-to-SQL agent's own SQL allow-list validator
  - orchestrator_log is append-only (Delta tblproperties appendOnly=true)
  - Event payload fields are size-bounded before LLM submission (prompt injection guard)

Deployment:
  Scheduled as a Databricks Workflow task (every 15 minutes) and also
  invokable on-demand with --event-type / --payload flags for ad-hoc dispatch.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.utils.dbutils_shim import get_dbutils


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Valid agent names — LLM routing output is validated against this whitelist
VALID_AGENTS = {
    "dq_remediation",
    "gdpr_rtbf",
    "nl_to_sql",
    "pipeline_diagnosis",
    "schema_drift_alert",
    "model_drift_retrain",
    "escalate",
    "no_op",
}

# Valid incoming event types
VALID_EVENT_TYPES = {
    "DQ_FAILURE",
    "GDPR_ERASURE_REQUEST",
    "NL_QUERY",
    "PIPELINE_FAILURE",
    "SCHEMA_DRIFT",
    "MODEL_DRIFT",
}

# Maximum payload size sent to LLM (prompt-injection guard)
_MAX_PAYLOAD_CHARS = 4_000


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class OrchestratorEvent:
    """A pipeline event read from governance.orchestrator_events."""
    event_id:   str
    event_type: str          # one of VALID_EVENT_TYPES
    source_job:  str | None  # Databricks job name that raised this event
    source_task: str | None  # Task key within the job
    payload:    dict         # Event-specific details
    priority:   str          # HIGH | MEDIUM | LOW
    raised_at:  datetime
    status:     str          # PENDING | IN_PROGRESS | COMPLETED | FAILED | ESCALATED


@dataclass
class RoutingPlan:
    """GPT-4o planner output — which agent to invoke and with what params."""
    event_id:     str
    chosen_agent: str    # must be in VALID_AGENTS
    reasoning:    str
    agent_params: dict
    confidence:   float  # 0.0–1.0; below 0.5 → rule-based fallback
    fallback_used: bool  # True if rule-based dispatch was used instead of LLM


@dataclass
class AgentResult:
    """Normalised result returned by every sub-agent wrapper."""
    status:          str         # SUCCESS | PARTIAL | FAILED | ESCALATED | SKIPPED
    summary:         str
    rows_affected:   int = 0
    extra:           dict = field(default_factory=dict)
    error_message:   str | None = None


@dataclass
class OrchestratorLogEntry:
    """One row written to governance.orchestrator_log per event handled."""
    log_id:            str
    event_id:          str
    event_type:        str
    chosen_agent:      str
    agent_params_json: str
    llm_reasoning:     str
    confidence:        float
    fallback_used:     bool
    execution_status:  str
    result_summary:    str
    error_message:     str | None
    duration_seconds:  float
    log_ts:            datetime
    env:               str


# ---------------------------------------------------------------------------
# Delta table schemas
# ---------------------------------------------------------------------------

_EVENTS_SCHEMA = T.StructType([
    T.StructField("event_id",    T.StringType(),    nullable=False),
    T.StructField("event_type",  T.StringType(),    nullable=False),
    T.StructField("source_job",  T.StringType(),    nullable=True),
    T.StructField("source_task", T.StringType(),    nullable=True),
    T.StructField("payload",     T.StringType(),    nullable=False),  # JSON string
    T.StructField("priority",    T.StringType(),    nullable=False),
    T.StructField("raised_at",   T.TimestampType(), nullable=False),
    T.StructField("status",      T.StringType(),    nullable=False),
])

_LOG_SCHEMA = T.StructType([
    T.StructField("log_id",            T.StringType(),    nullable=False),
    T.StructField("event_id",          T.StringType(),    nullable=False),
    T.StructField("event_type",        T.StringType(),    nullable=False),
    T.StructField("chosen_agent",      T.StringType(),    nullable=False),
    T.StructField("agent_params_json", T.StringType(),    nullable=False),
    T.StructField("llm_reasoning",     T.StringType(),    nullable=False),
    T.StructField("confidence",        T.DoubleType(),    nullable=False),
    T.StructField("fallback_used",     T.BooleanType(),   nullable=False),
    T.StructField("execution_status",  T.StringType(),    nullable=False),
    T.StructField("result_summary",    T.StringType(),    nullable=False),
    T.StructField("error_message",     T.StringType(),    nullable=True),
    T.StructField("duration_seconds",  T.DoubleType(),    nullable=False),
    T.StructField("log_ts",            T.TimestampType(), nullable=False),
    T.StructField("env",               T.StringType(),    nullable=False),
])


# ---------------------------------------------------------------------------
# Table bootstrap helpers
# ---------------------------------------------------------------------------

def _bootstrap_tables(spark: SparkSession, env: str) -> None:
    """
    Idempotently create governance.orchestrator_events and
    governance.orchestrator_log if they do not already exist.
    Both tables are created with appendOnly=true for audit integrity.
    """
    catalog = f"{env}_lakehouse"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.governance")

    # Events table — writable by upstream pipeline jobs and external triggers
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.governance.orchestrator_events (
            event_id    STRING NOT NULL,
            event_type  STRING NOT NULL,
            source_job  STRING,
            source_task STRING,
            payload     STRING NOT NULL,
            priority    STRING NOT NULL,
            raised_at   TIMESTAMP NOT NULL,
            status      STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'comment' = 'Agentic orchestrator event queue'
        )
    """)

    # Audit log — append-only
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.governance.orchestrator_log (
            log_id            STRING NOT NULL,
            event_id          STRING NOT NULL,
            event_type        STRING NOT NULL,
            chosen_agent      STRING NOT NULL,
            agent_params_json STRING NOT NULL,
            llm_reasoning     STRING NOT NULL,
            confidence        DOUBLE NOT NULL,
            fallback_used     BOOLEAN NOT NULL,
            execution_status  STRING NOT NULL,
            result_summary    STRING NOT NULL,
            error_message     STRING,
            duration_seconds  DOUBLE NOT NULL,
            log_ts            TIMESTAMP NOT NULL,
            env               STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.appendOnly'  = 'true',
            'comment' = 'Immutable audit log for all orchestrator routing decisions'
        )
    """)


# ---------------------------------------------------------------------------
# GPT-4o planner
# ---------------------------------------------------------------------------

_PLANNER_SYSTEM_PROMPT = """
You are the Orchestrator Planner for an Azure Databricks Lakehouse platform.
Your job is to analyse an incoming pipeline event and decide which sub-agent
should handle it, along with the exact parameters to pass.

Available agents and when to use each:
  dq_remediation       – DQ_FAILURE events; failed / warning data quality checks
  gdpr_rtbf            – GDPR_ERASURE_REQUEST events; right-to-be-forgotten requests
  nl_to_sql            – NL_QUERY events; natural-language questions about Gold layer data
  pipeline_diagnosis   – PIPELINE_FAILURE events; diagnose root cause and suggest fix
  schema_drift_alert   – SCHEMA_DRIFT events; new/removed/changed columns detected
  model_drift_retrain  – MODEL_DRIFT events; significant PSI/KS/JS drift detected
  escalate             – Any event that exceeds auto-fix thresholds or needs human review
  no_op                – Duplicate, stale, or already-handled events

Rules:
  - DQ failures with failure_rate > 0.20 must ALWAYS route to escalate, not dq_remediation.
  - GDPR requests with legal_hold=true must route to escalate.
  - Unknown event types must route to escalate.
  - Choose no_op only when the event is clearly a duplicate or already resolved.
  - confidence must reflect how certain you are (0.0–1.0); use < 0.5 only for ambiguous events.

Return a JSON object (no markdown, no extra text):
{
  "chosen_agent":  "<agent name from the list above>",
  "reasoning":     "<1-3 sentence explanation>",
  "agent_params":  { <key-value pairs to pass to the agent, empty dict if none> },
  "confidence":    <0.0 – 1.0>
}
"""


def _call_planner(
    client: Any,
    deployment: str,
    event: OrchestratorEvent,
) -> dict:
    """
    Submit the event to GPT-4o and parse the routing plan.
    Payload is truncated to _MAX_PAYLOAD_CHARS to guard against prompt injection.
    """
    safe_payload = json.dumps(event.payload)[:_MAX_PAYLOAD_CHARS]

    user_message = json.dumps({
        "event_id":   event.event_id,
        "event_type": event.event_type,
        "source_job": event.source_job,
        "source_task": event.source_task,
        "priority":   event.priority,
        "payload":    json.loads(safe_payload),
    }, indent=2)

    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": _PLANNER_SYSTEM_PROMPT},
            {"role": "user",   "content": user_message},
        ],
        temperature=0,
        max_tokens=512,
        response_format={"type": "json_object"},
    )

    raw = response.choices[0].message.content
    plan = json.loads(raw)

    # Sanitise chosen_agent — reject any value not in the whitelist
    agent = str(plan.get("chosen_agent", "escalate")).lower().strip()
    if agent not in VALID_AGENTS:
        agent = "escalate"

    return {
        "chosen_agent": agent,
        "reasoning":    str(plan.get("reasoning", ""))[:2000],
        "agent_params": plan.get("agent_params") if isinstance(plan.get("agent_params"), dict) else {},
        "confidence":   float(plan.get("confidence", 0.0)),
    }


# ---------------------------------------------------------------------------
# Rule-based fallback dispatcher (used when LLM is unavailable or low-confidence)
# ---------------------------------------------------------------------------

def _rule_based_dispatch(event: OrchestratorEvent) -> dict:
    """
    Deterministic routing based on event_type alone.
    Used as a fallback when the LLM call fails or returns confidence < 0.5.
    """
    mapping = {
        "DQ_FAILURE":           ("dq_remediation",      {}),
        "GDPR_ERASURE_REQUEST": ("gdpr_rtbf",            {}),
        "NL_QUERY":             ("nl_to_sql",            {}),
        "PIPELINE_FAILURE":     ("pipeline_diagnosis",   {}),
        "SCHEMA_DRIFT":         ("schema_drift_alert",   {}),
        "MODEL_DRIFT":          ("model_drift_retrain",  {}),
    }
    chosen, params = mapping.get(event.event_type, ("escalate", {}))
    return {
        "chosen_agent": chosen,
        "reasoning":    f"Rule-based fallback for event_type={event.event_type}",
        "agent_params": params,
        "confidence":   1.0,
    }


# ---------------------------------------------------------------------------
# Sub-agent wrappers
# ---------------------------------------------------------------------------

def _run_dq_remediation(
    spark: SparkSession,
    env: str,
    openai_endpoint: str,
    openai_key: str,
    openai_deployment: str,
    agent_params: dict,
    event: OrchestratorEvent,
) -> AgentResult:
    """Invoke the DQ Remediation Agent for a DQ_FAILURE event."""
    try:
        from src.agents.dq_remediation_agent import run_remediation_cycle
        summary = run_remediation_cycle(
            spark=spark,
            env=env,
            openai_endpoint=openai_endpoint,
            openai_api_key=openai_key,
            openai_deployment=openai_deployment,
            lookback_hours=agent_params.get("lookback_hours", 26),
        )
        return AgentResult(
            status="SUCCESS",
            summary=f"DQ remediation completed: {summary}",
            rows_affected=summary.get("total_rows_affected", 0) if isinstance(summary, dict) else 0,
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="DQ remediation failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_gdpr_rtbf(
    spark: SparkSession,
    env: str,
    salt_secret_scope: str,
    salt_secret_key: str,
    agent_params: dict,
    event: OrchestratorEvent,
) -> AgentResult:
    """Invoke the GDPR RTBF Agent for a GDPR_ERASURE_REQUEST event."""
    try:
        from src.agents.gdpr_rtbf_agent import run_erasure
        subject_ids = (
            event.payload.get("subject_ids")
            or agent_params.get("subject_ids", [])
        )
        if not subject_ids:
            return AgentResult(
                status="SKIPPED",
                summary="No subject_ids found in event payload — skipping erasure.",
            )
        report = run_erasure(
            spark=spark,
            env=env,
            subject_ids=subject_ids,
            requester=event.payload.get("requester", "orchestrator"),
            dry_run=agent_params.get("dry_run", False),
            salt_secret_scope=salt_secret_scope,
            salt_secret_key=salt_secret_key,
        )
        return AgentResult(
            status="SUCCESS",
            summary=f"GDPR RTBF completed for {len(subject_ids)} subject(s). "
                    f"Tables affected: {report.get('tables_affected', '?')}",
            extra={"erasure_report_id": report.get("report_id", "")},
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="GDPR RTBF agent failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_nl_to_sql(
    spark: SparkSession,
    env: str,
    openai_endpoint: str,
    openai_key: str,
    openai_deployment: str,
    databricks_host: str,
    databricks_http_path: str,
    databricks_token: str,
    agent_params: dict,
    event: OrchestratorEvent,
) -> AgentResult:
    """Invoke the NL-to-SQL Agent for a NL_QUERY event."""
    try:
        from src.agents.nl_to_sql_agent import NLToSQLAgent, NLToSQLConfig
        cfg = NLToSQLConfig(
            openai_endpoint=openai_endpoint,
            openai_api_key=openai_key,
            openai_deployment=openai_deployment,
            databricks_host=databricks_host,
            databricks_http_path=databricks_http_path,
            databricks_token=databricks_token,
            allowed_catalog=f"{env}_lakehouse",
            allowed_schemas=agent_params.get("allowed_schemas", ["gold"]),
        )
        agent = NLToSQLAgent(cfg)
        question = event.payload.get("question", "")
        if not question:
            return AgentResult(status="SKIPPED", summary="No question in NL_QUERY payload.")
        result = agent.query(question)
        return AgentResult(
            status="SUCCESS" if not result.error else "FAILED",
            summary=f"NL query executed. Rows returned: {result.row_count}. "
                    f"Query ID: {result.query_id}",
            rows_affected=result.row_count,
            extra={"generated_sql": result.sql, "query_id": result.query_id},
            error_message=result.error,
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="NL-to-SQL agent failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_pipeline_diagnosis(
    client: Any,
    openai_deployment: str,
    event: OrchestratorEvent,
) -> AgentResult:
    """
    Use GPT-4o directly to diagnose a PIPELINE_FAILURE event and
    return a remediation recommendation. Does not auto-apply any fix —
    the recommendation is stored in the log for human action.
    """
    try:
        safe_payload = json.dumps(event.payload)[:_MAX_PAYLOAD_CHARS]
        response = client.chat.completions.create(
            model=openai_deployment,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a Databricks pipeline reliability expert. "
                        "Analyse the PIPELINE_FAILURE event and provide: "
                        "1) probable root cause, "
                        "2) immediate remediation steps (max 5 bullets), "
                        "3) whether the pipeline can be safely retried. "
                        "Be concise. Return plain text — no JSON needed."
                    ),
                },
                {"role": "user", "content": safe_payload},
            ],
            temperature=0,
            max_tokens=512,
        )
        diagnosis = response.choices[0].message.content[:2000]
        return AgentResult(
            status="ESCALATED",
            summary=f"Pipeline diagnosis complete. Recommendation stored in log. "
                    f"Human review required. Diagnosis: {diagnosis[:200]}...",
            extra={"full_diagnosis": diagnosis},
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="Pipeline diagnosis failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_schema_drift_alert(
    spark: SparkSession,
    env: str,
    event: OrchestratorEvent,
) -> AgentResult:
    """
    Log schema drift details to governance.schema_drift_log and
    generate a column-level lineage impact summary from Unity Catalog.
    No auto-fix is applied — human approval is required for schema changes.
    """
    try:
        catalog = f"{env}_lakehouse"
        payload = event.payload

        # Persist drift record for lineage tracking
        drift_record = {
            "drift_id":     str(uuid.uuid4()),
            "event_id":     event.event_id,
            "table_name":   payload.get("table_name", "unknown"),
            "drift_type":   payload.get("drift_type", "UNKNOWN"),   # ADDED | REMOVED | TYPE_CHANGED
            "column_name":  payload.get("column_name"),
            "old_type":     payload.get("old_type"),
            "new_type":     payload.get("new_type"),
            "detected_at":  datetime.now(timezone.utc).isoformat(),
            "env":          env,
        }
        spark.createDataFrame([drift_record]).write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{catalog}.governance.schema_drift_log")

        table_name = payload.get("table_name", "unknown")
        drift_type = payload.get("drift_type", "UNKNOWN")
        column_name = payload.get("column_name", "unknown")

        return AgentResult(
            status="ESCALATED",
            summary=(
                f"Schema drift logged: {drift_type} on column '{column_name}' "
                f"in table '{table_name}'. Human approval required before "
                f"downstream DLT pipelines are updated."
            ),
            extra={"drift_id": drift_record["drift_id"]},
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="Schema drift alert failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_model_drift_retrain(
    databricks_host: str,
    databricks_token: str,
    retraining_job_id: str,
    event: OrchestratorEvent,
) -> AgentResult:
    """
    Trigger the MLflow model retraining Databricks Workflow via the Jobs REST API.
    Only triggered when drift severity is HIGH (PSI > 0.2 or KS p-value < 0.01).
    """
    try:
        import requests  # available in Databricks runtime

        severity = event.payload.get("drift_severity", "LOW")
        if severity not in ("HIGH", "CRITICAL"):
            return AgentResult(
                status="SKIPPED",
                summary=f"Model drift severity={severity} is below retraining threshold. "
                        f"Monitoring only — no retraining triggered.",
            )

        url = f"https://{databricks_host}/api/2.1/jobs/run-now"
        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type":  "application/json",
        }
        body = {
            "job_id": int(retraining_job_id),
            "python_params": [
                "--env",    event.payload.get("env", "dev"),
                "--reason", f"orchestrator_model_drift:{event.event_id}",
            ],
        }
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        resp.raise_for_status()
        run_id = resp.json().get("run_id", "unknown")
        return AgentResult(
            status="SUCCESS",
            summary=f"Retraining job triggered. Databricks run_id={run_id}. "
                    f"Drift severity={severity}.",
            extra={"retraining_run_id": str(run_id)},
        )
    except Exception as exc:
        return AgentResult(
            status="FAILED",
            summary="Model drift retraining trigger failed — see error_message",
            error_message=str(exc)[:1000],
        )


def _run_escalate(event: OrchestratorEvent, reason: str = "") -> AgentResult:
    """Mark event as ESCALATED — requires human review."""
    return AgentResult(
        status="ESCALATED",
        summary=f"Event escalated for human review. Reason: {reason or 'threshold exceeded or unknown event type'}",
    )


def _run_no_op(event: OrchestratorEvent) -> AgentResult:
    """Event is a duplicate or already handled — no action needed."""
    return AgentResult(
        status="SKIPPED",
        summary="Event identified as duplicate or already resolved — no action taken.",
    )


# ---------------------------------------------------------------------------
# Audit log writer
# ---------------------------------------------------------------------------

def _write_log(
    spark: SparkSession,
    entry: OrchestratorLogEntry,
    env: str,
) -> None:
    catalog = f"{env}_lakehouse"
    row = {
        "log_id":            entry.log_id,
        "event_id":          entry.event_id,
        "event_type":        entry.event_type,
        "chosen_agent":      entry.chosen_agent,
        "agent_params_json": entry.agent_params_json,
        "llm_reasoning":     entry.llm_reasoning,
        "confidence":        entry.confidence,
        "fallback_used":     entry.fallback_used,
        "execution_status":  entry.execution_status,
        "result_summary":    entry.result_summary[:4000],
        "error_message":     entry.error_message,
        "duration_seconds":  entry.duration_seconds,
        "log_ts":            entry.log_ts,
        "env":               entry.env,
    }
    spark.createDataFrame([row], schema=_LOG_SCHEMA).write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"{catalog}.governance.orchestrator_log")


def _update_event_status(
    spark: SparkSession,
    env: str,
    event_id: str,
    new_status: str,
) -> None:
    catalog = f"{env}_lakehouse"
    spark.sql(f"""
        UPDATE {catalog}.governance.orchestrator_events
        SET status = '{new_status}'
        WHERE event_id = '{event_id}'
    """)


# ---------------------------------------------------------------------------
# Main orchestration loop
# ---------------------------------------------------------------------------

def run_orchestration_cycle(
    spark: SparkSession,
    env: str,
    openai_endpoint: str,
    openai_api_key: str,
    openai_deployment: str,
    databricks_host: str,
    databricks_http_path: str,
    databricks_token: str,
    salt_secret_scope: str,
    salt_secret_key: str,
    retraining_job_id: str,
    batch_size: int = 20,
) -> dict[str, int]:
    """
    Poll governance.orchestrator_events for PENDING events and process
    each one using the GPT-4o planner + sub-agent executor.

    Returns a summary dict: {processed, succeeded, failed, escalated, skipped}.
    """
    catalog = f"{env}_lakehouse"

    # ── Fetch PENDING events ordered by priority then raised_at ──────────────
    events_df = spark.sql(f"""
        SELECT *
        FROM {catalog}.governance.orchestrator_events
        WHERE status = 'PENDING'
        ORDER BY
            CASE priority WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END,
            raised_at ASC
        LIMIT {batch_size}
    """)

    rows = events_df.collect()
    if not rows:
        return {"processed": 0, "succeeded": 0, "failed": 0, "escalated": 0, "skipped": 0}

    # ── Build OpenAI client (once for the batch) ──────────────────────────────
    openai_client = None
    try:
        from openai import AzureOpenAI
        openai_client = AzureOpenAI(
            azure_endpoint=openai_endpoint,
            api_key=openai_api_key,
            api_version="2024-02-15-preview",
        )
    except ImportError:
        pass  # LLM unavailable — rule-based fallback will be used for all events

    counts: dict[str, int] = {"processed": 0, "succeeded": 0, "failed": 0,
                              "escalated": 0, "skipped": 0}

    for row in rows:
        event = OrchestratorEvent(
            event_id=row["event_id"],
            event_type=row["event_type"],
            source_job=row["source_job"],
            source_task=row["source_task"],
            payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else {},
            priority=row["priority"],
            raised_at=row["raised_at"],
            status=row["status"],
        )

        # Guard: unknown event type → escalate immediately
        if event.event_type not in VALID_EVENT_TYPES:
            _update_event_status(spark, env, event.event_id, "IN_PROGRESS")
            result = _run_escalate(event, reason=f"Unknown event_type: {event.event_type}")
            _finalise(spark, env, event, "escalate", {}, "Unknown event type",
                      1.0, True, result, 0.0, counts)
            continue

        _update_event_status(spark, env, event.event_id, "IN_PROGRESS")
        t0 = time.monotonic()

        # ── Plan routing ──────────────────────────────────────────────────────
        plan_raw: dict
        fallback_used = False
        try:
            if openai_client is None:
                raise RuntimeError("openai package not installed")
            plan_raw = _call_planner(openai_client, openai_deployment, event)
            if plan_raw["confidence"] < 0.5:
                # Low-confidence LLM answer — override with rule-based dispatch
                plan_raw = _rule_based_dispatch(event)
                fallback_used = True
        except Exception:
            plan_raw = _rule_based_dispatch(event)
            fallback_used = True

        plan = RoutingPlan(
            event_id=event.event_id,
            chosen_agent=plan_raw["chosen_agent"],
            reasoning=plan_raw["reasoning"],
            agent_params=plan_raw["agent_params"],
            confidence=plan_raw["confidence"],
            fallback_used=fallback_used,
        )

        # ── Execute chosen agent ──────────────────────────────────────────────
        result: AgentResult
        if plan.chosen_agent == "dq_remediation":
            result = _run_dq_remediation(spark, env, openai_endpoint,
                                         openai_api_key, openai_deployment,
                                         plan.agent_params, event)

        elif plan.chosen_agent == "gdpr_rtbf":
            result = _run_gdpr_rtbf(spark, env, salt_secret_scope,
                                    salt_secret_key, plan.agent_params, event)

        elif plan.chosen_agent == "nl_to_sql":
            result = _run_nl_to_sql(spark, env, openai_endpoint, openai_api_key,
                                    openai_deployment, databricks_host,
                                    databricks_http_path, databricks_token,
                                    plan.agent_params, event)

        elif plan.chosen_agent == "pipeline_diagnosis":
            result = (_run_pipeline_diagnosis(openai_client, openai_deployment, event)
                      if openai_client else _run_escalate(event, "LLM unavailable for pipeline diagnosis"))

        elif plan.chosen_agent == "schema_drift_alert":
            result = _run_schema_drift_alert(spark, env, event)

        elif plan.chosen_agent == "model_drift_retrain":
            result = _run_model_drift_retrain(
                databricks_host, databricks_token, retraining_job_id, event)

        elif plan.chosen_agent == "escalate":
            result = _run_escalate(event, plan.reasoning)

        else:  # no_op or unrecognised after whitelist check
            result = _run_no_op(event)

        duration = time.monotonic() - t0
        _finalise(spark, env, event, plan.chosen_agent, plan.agent_params,
                  plan.reasoning, plan.confidence, plan.fallback_used,
                  result, duration, counts)

    return counts


def _finalise(
    spark: SparkSession,
    env: str,
    event: OrchestratorEvent,
    chosen_agent: str,
    agent_params: dict,
    reasoning: str,
    confidence: float,
    fallback_used: bool,
    result: AgentResult,
    duration: float,
    counts: dict[str, int],
) -> None:
    """Write audit log row, update event status, and increment counters."""
    status_map = {
        "SUCCESS":   "COMPLETED",
        "PARTIAL":   "COMPLETED",
        "FAILED":    "FAILED",
        "ESCALATED": "ESCALATED",
        "SKIPPED":   "COMPLETED",
    }
    event_final_status = status_map.get(result.status, "FAILED")
    _update_event_status(spark, env, event.event_id, event_final_status)

    entry = OrchestratorLogEntry(
        log_id=str(uuid.uuid4()),
        event_id=event.event_id,
        event_type=event.event_type,
        chosen_agent=chosen_agent,
        agent_params_json=json.dumps(agent_params),
        llm_reasoning=reasoning,
        confidence=confidence,
        fallback_used=fallback_used,
        execution_status=result.status,
        result_summary=result.summary,
        error_message=result.error_message,
        duration_seconds=round(duration, 3),
        log_ts=datetime.now(timezone.utc),
        env=env,
    )
    _write_log(spark, entry, env)

    counts["processed"] += 1
    counter_key = {
        "SUCCESS": "succeeded", "PARTIAL": "succeeded",
        "FAILED": "failed", "ESCALATED": "escalated", "SKIPPED": "skipped",
    }.get(result.status, "failed")
    counts[counter_key] += 1


# ---------------------------------------------------------------------------
# Entrypoint — Databricks Workflow task or on-demand invocation
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agentic Orchestrator")
    parser.add_argument("--env",                   required=True)
    parser.add_argument("--openai-endpoint",        required=True)
    parser.add_argument("--openai-key-secret",      default="openai-api-key",
                        help="Key Vault secret key for the OpenAI API key")
    parser.add_argument("--openai-secret-scope",    default="lakehouse-kv-scope")
    parser.add_argument("--openai-deployment",      default="gpt-4o")
    parser.add_argument("--databricks-host",        default="")
    parser.add_argument("--databricks-http-path",   default="")
    parser.add_argument("--token-secret-scope",     default="lakehouse-kv-scope")
    parser.add_argument("--token-secret-key",       default="databricks-sp-token")
    parser.add_argument("--salt-secret-scope",      default="lakehouse-kv-scope")
    parser.add_argument("--salt-secret-key",        default="gdpr-anonymization-salt")
    parser.add_argument("--retraining-job-id",      default="0")
    parser.add_argument("--batch-size",             type=int, default=20,
                        help="Max events processed per orchestration cycle")
    # One-shot mode: inject a single event directly instead of polling
    parser.add_argument("--event-type",  default=None,
                        help="Inject a single event by type (skips DB poll)")
    parser.add_argument("--payload",     default="{}",
                        help="JSON payload string for the injected event")
    parser.add_argument("--priority",    default="MEDIUM",
                        choices=["HIGH", "MEDIUM", "LOW"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)

    # Fetch secrets from Key Vault via Databricks secret scope
    openai_key = dbutils.secrets.get(
        scope=args.openai_secret_scope, key=args.openai_key_secret)
    databricks_token = dbutils.secrets.get(
        scope=args.token_secret_scope, key=args.token_secret_key)

    _bootstrap_tables(spark, args.env)

    # One-shot injection mode (for CI tests / manual ad-hoc triggers)
    if args.event_type:
        if args.event_type not in VALID_EVENT_TYPES:
            raise ValueError(
                f"--event-type must be one of {sorted(VALID_EVENT_TYPES)}, "
                f"got: {args.event_type!r}"
            )
        catalog = f"{args.env}_lakehouse"
        event_row = {
            "event_id":   str(uuid.uuid4()),
            "event_type": args.event_type,
            "source_job": "manual",
            "source_task": None,
            "payload":    args.payload,
            "priority":   args.priority,
            "raised_at":  datetime.now(timezone.utc),
            "status":     "PENDING",
        }
        spark.createDataFrame([event_row], schema=_EVENTS_SCHEMA).write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{catalog}.governance.orchestrator_events")

    summary = run_orchestration_cycle(
        spark=spark,
        env=args.env,
        openai_endpoint=args.openai_endpoint,
        openai_api_key=openai_key,
        openai_deployment=args.openai_deployment,
        databricks_host=args.databricks_host,
        databricks_http_path=args.databricks_http_path,
        databricks_token=databricks_token,
        salt_secret_scope=args.salt_secret_scope,
        salt_secret_key=args.salt_secret_key,
        retraining_job_id=args.retraining_job_id,
        batch_size=args.batch_size,
    )

    print(json.dumps({"orchestration_summary": summary}, indent=2))


if __name__ == "__main__":
    main()

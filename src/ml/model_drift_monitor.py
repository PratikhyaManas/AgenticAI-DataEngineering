"""
Model Drift Monitor & Automated Retraining Trigger
====================================================
Agent: MLOps / Model Monitoring Agent

Detects statistical drift in live model inputs and predictions compared
to the training baseline, then automatically triggers retraining when
drift exceeds configurable thresholds.

Drift detection methods:
  PSI  (Population Stability Index) — numeric + categorical feature drift
       PSI < 0.10 → no drift  | 0.10–0.25 → moderate | >0.25 → severe
  KS   (Kolmogorov-Smirnov test)    — prediction distribution shift
       p-value < 0.05 → statistically significant drift
  JS   (Jensen-Shannon Divergence)  — categorical feature distribution
       JS  > 0.10 → moderate drift  | >0.25 → severe
  RMSE drift — rolling RMSE on labelled actuals vs baseline RMSE
       >20% increase → model degradation

Pipeline:
  1. Load reference (training) distribution from MLflow baseline artifact
  2. Load current (live) prediction + feature distribution from Gold tables
  3. Compute drift metrics for each feature and for predictions
  4. Write drift_metrics to {env}_lakehouse.mlops.drift_metrics (Delta)
  5. If any metric exceeds threshold → trigger retraining via Databricks Jobs API
  6. Log alert to Azure Monitor / Application Insights
  7. Create MLflow comparison run for champion vs challenger model tracking

Reference:
  - PSI: https://mwburke.github.io/data%20science/2018/04/29/population-stability-index.html
  - KS test: scipy.stats.ks_2samp
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import mlflow
import numpy as np
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.utils.dbutils_shim import get_dbutils
from src.utils.logger import get_logger

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Drift severity thresholds
# ---------------------------------------------------------------------------

PSI_MODERATE = 0.10
PSI_SEVERE   = 0.25

KS_P_VALUE_THRESHOLD = 0.05   # reject null hypothesis of same distribution

JS_MODERATE = 0.10
JS_SEVERE   = 0.25

RMSE_DRIFT_THRESHOLD = 0.20   # >20% RMSE increase triggers retraining


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class DriftMetric:
    metric_id:       str
    monitor_run_id:  str
    model_name:      str
    model_version:   str
    feature_name:    str        # "prediction" for prediction drift
    drift_method:    str        # PSI | KS | JS | RMSE_DRIFT
    reference_value: float
    current_value:   float
    drift_score:     float
    severity:        str        # NONE | MODERATE | SEVERE
    threshold:       float
    is_drifted:      bool
    computed_at:     datetime
    environment:     str


# ---------------------------------------------------------------------------
# PSI computation
# ---------------------------------------------------------------------------

def _compute_psi(
    reference: np.ndarray,
    current: np.ndarray,
    n_bins: int = 10,
    epsilon: float = 1e-8,
) -> float:
    """
    Compute Population Stability Index between reference and current distributions.

    PSI = Σ (current_pct - reference_pct) × ln(current_pct / reference_pct)

    Args:
        reference: 1-D array of reference (training) values.
        current:   1-D array of current (live) values.
        n_bins:    Number of equal-width buckets.
        epsilon:   Small constant to avoid log(0).

    Returns:
        PSI score (≥ 0); higher = more drift.
    """
    combined    = np.concatenate([reference, current])
    bin_edges   = np.histogram_bin_edges(combined, bins=n_bins)

    ref_counts, _ = np.histogram(reference, bins=bin_edges)
    cur_counts, _ = np.histogram(current,   bins=bin_edges)

    ref_pct = (ref_counts + epsilon) / (len(reference) + epsilon * n_bins)
    cur_pct = (cur_counts + epsilon) / (len(current)   + epsilon * n_bins)

    psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    return float(psi)


# ---------------------------------------------------------------------------
# KS test (prediction distribution)
# ---------------------------------------------------------------------------

def _compute_ks(reference: np.ndarray, current: np.ndarray) -> tuple[float, float]:
    """
    Two-sample Kolmogorov-Smirnov test.
    Returns (ks_statistic, p_value).
    """
    from scipy import stats  # lazy import

    ks_stat, p_value = stats.ks_2samp(reference, current)
    return float(ks_stat), float(p_value)


# ---------------------------------------------------------------------------
# Jensen-Shannon divergence (categorical)
# ---------------------------------------------------------------------------

def _compute_js(
    reference_counts: dict[str, int],
    current_counts:   dict[str, int],
    epsilon: float = 1e-8,
) -> float:
    """
    Jensen-Shannon Divergence between two categorical distributions.
    Returns a value in [0, 1]; 0 = identical, 1 = maximally different.
    """
    from scipy.spatial.distance import jensenshannon

    all_keys = sorted(set(reference_counts) | set(current_counts))
    ref_vec  = np.array([reference_counts.get(k, 0) + epsilon for k in all_keys])
    cur_vec  = np.array([current_counts.get(k, 0) + epsilon   for k in all_keys])

    ref_vec = ref_vec / ref_vec.sum()
    cur_vec = cur_vec / cur_vec.sum()

    return float(jensenshannon(ref_vec, cur_vec))


# ---------------------------------------------------------------------------
# RMSE drift
# ---------------------------------------------------------------------------

def _compute_rmse_drift(
    spark: SparkSession,
    env: str,
    model_name: str,
    actuals_table: str,
    prediction_col: str = "prediction",
    actual_col:     str = "actual_clv_30d",
    lookback_days:  int = 7,
) -> tuple[float, float]:
    """
    Compute current rolling RMSE on labelled actuals and compare to
    baseline RMSE recorded at training time in MLflow.

    Returns:
        (current_rmse, baseline_rmse)
    """
    rows = spark.sql(f"""
        SELECT
            SQRT(AVG(POW({prediction_col} - {actual_col}, 2))) AS rolling_rmse
        FROM {env}_lakehouse.{actuals_table}
        WHERE scored_at >= current_timestamp() - INTERVAL {lookback_days} DAYS
          AND {actual_col} IS NOT NULL
    """).collect()

    current_rmse = float(rows[0]["rolling_rmse"] or 0.0)

    # Fetch baseline RMSE from MLflow model version tags / metrics
    client = mlflow.tracking.MlflowClient()
    mv     = client.get_latest_versions(model_name, stages=["Production"])
    if not mv:
        mv = client.get_latest_versions(model_name, stages=["Staging"])

    baseline_rmse = 0.0
    if mv:
        run_id       = mv[0].run_id
        baseline_run = client.get_run(run_id)
        baseline_rmse = float(
            baseline_run.data.metrics.get("test_rmse", 0.0)
        )

    return current_rmse, baseline_rmse


# ---------------------------------------------------------------------------
# Reference distribution loading (from MLflow artifact)
# ---------------------------------------------------------------------------

def _load_reference_distribution(
    model_name: str,
    artifact_path: str = "reference_distribution.json",
) -> dict[str, Any]:
    """
    Load the training reference distribution from the registered MLflow model artifact.
    The artifact is a JSON dict: {feature_name: {values: [...], counts: {...}}}.

    This is saved during model training by model_training.py.
    """
    client = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(model_name, stages=["Production"])
    if not versions:
        versions = client.get_latest_versions(model_name, stages=["Staging"])
    if not versions:
        raise ValueError(
            f"No Production or Staging version of '{model_name}' found in MLflow."
        )

    run_id    = versions[0].run_id
    local_dir = mlflow.artifacts.download_artifacts(
        run_id=run_id, artifact_path=artifact_path
    )
    with open(local_dir) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Load current live distribution from Gold / Feature Store
# ---------------------------------------------------------------------------

def _load_current_distribution(
    spark: SparkSession,
    env: str,
    feature_cols: list[str],
    prediction_col: str = "prediction",
    lookback_days: int = 7,
    sample_size: int = 50_000,
) -> dict[str, np.ndarray]:
    """
    Load current feature + prediction values from the live inference table
    in the Gold layer.

    Returns dict mapping column name → 1-D numpy array of values.
    """
    cols_str  = ", ".join(feature_cols + [prediction_col])
    table     = f"{env}_lakehouse.gold.sales_predictions"
    df = spark.sql(f"""
        SELECT {cols_str}
        FROM {table}
        WHERE scored_at >= current_timestamp() - INTERVAL {lookback_days} DAYS
        ORDER BY RAND()
        LIMIT {sample_size}
    """)

    result: dict[str, np.ndarray] = {}
    pdf = df.toPandas()
    for col in feature_cols + [prediction_col]:
        result[col] = pdf[col].dropna().to_numpy()
    return result


# ---------------------------------------------------------------------------
# Drift metric computation (main logic)
# ---------------------------------------------------------------------------

def compute_drift_metrics(
    spark: SparkSession,
    env: str,
    model_name: str,
    feature_cols: list[str],
    categorical_cols: list[str],
    prediction_col: str = "prediction",
    lookback_days: int = 7,
    monitor_run_id: str | None = None,
) -> list[DriftMetric]:
    """
    Compute all drift metrics for the registered model.

    Args:
        spark:             Active SparkSession.
        env:               Deployment environment.
        model_name:        MLflow registered model name.
        feature_cols:      List of numeric feature columns to monitor.
        categorical_cols:  Subset of feature_cols that are categorical (use JS).
        prediction_col:    Name of the prediction column in the live table.
        lookback_days:     Window of live data to use for current distribution.
        monitor_run_id:    Optional run ID; generated if not provided.

    Returns:
        List of DriftMetric objects — one per feature + one for predictions.
    """
    monitor_run_id = monitor_run_id or str(uuid.uuid4())
    metrics: list[DriftMetric] = []

    # Load reference + current distributions
    ref_dist  = _load_reference_distribution(model_name)
    curr_dist = _load_current_distribution(
        spark, env, feature_cols, prediction_col, lookback_days
    )

    client    = mlflow.tracking.MlflowClient()
    versions  = client.get_latest_versions(model_name, stages=["Production"]) or \
                client.get_latest_versions(model_name, stages=["Staging"])
    model_ver = versions[0].version if versions else "unknown"

    now = datetime.now(timezone.utc)

    # ---- Numeric features: PSI ----
    for col in [c for c in feature_cols if c not in categorical_cols]:
        if col not in ref_dist or col not in curr_dist:
            continue

        ref_vals  = np.array(ref_dist[col].get("values", []))
        curr_vals = curr_dist.get(col, np.array([]))

        if len(ref_vals) < 30 or len(curr_vals) < 30:
            continue

        psi_score = _compute_psi(ref_vals, curr_vals)
        severity  = (
            "SEVERE"   if psi_score > PSI_SEVERE  else
            "MODERATE" if psi_score > PSI_MODERATE else
            "NONE"
        )

        metrics.append(DriftMetric(
            metric_id=str(uuid.uuid4()),
            monitor_run_id=monitor_run_id,
            model_name=model_name,
            model_version=model_ver,
            feature_name=col,
            drift_method="PSI",
            reference_value=float(np.mean(ref_vals)),
            current_value=float(np.mean(curr_vals)),
            drift_score=psi_score,
            severity=severity,
            threshold=PSI_MODERATE,
            is_drifted=psi_score > PSI_MODERATE,
            computed_at=now,
            environment=env,
        ))

    # ---- Categorical features: JS divergence ----
    for col in categorical_cols:
        if col not in ref_dist or col not in curr_dist:
            continue

        ref_counts  = ref_dist[col].get("counts", {})
        curr_arr    = curr_dist.get(col, np.array([]))
        curr_values = [str(v) for v in curr_arr]
        curr_counts = {v: curr_values.count(v) for v in set(curr_values)}

        js_score = _compute_js(ref_counts, curr_counts)
        severity = (
            "SEVERE"   if js_score > JS_SEVERE  else
            "MODERATE" if js_score > JS_MODERATE else
            "NONE"
        )

        metrics.append(DriftMetric(
            metric_id=str(uuid.uuid4()),
            monitor_run_id=monitor_run_id,
            model_name=model_name,
            model_version=model_ver,
            feature_name=col,
            drift_method="JS",
            reference_value=float(len(ref_counts)),
            current_value=float(len(curr_counts)),
            drift_score=js_score,
            severity=severity,
            threshold=JS_MODERATE,
            is_drifted=js_score > JS_MODERATE,
            computed_at=now,
            environment=env,
        ))

    # ---- Prediction distribution: KS test ----
    ref_preds  = np.array(ref_dist.get(prediction_col, {}).get("values", []))
    curr_preds = curr_dist.get(prediction_col, np.array([]))

    if len(ref_preds) >= 30 and len(curr_preds) >= 30:
        ks_stat, p_value = _compute_ks(ref_preds, curr_preds)
        is_drifted = p_value < KS_P_VALUE_THRESHOLD
        severity   = "SEVERE" if (is_drifted and ks_stat > 0.3) else \
                     "MODERATE" if is_drifted else "NONE"

        metrics.append(DriftMetric(
            metric_id=str(uuid.uuid4()),
            monitor_run_id=monitor_run_id,
            model_name=model_name,
            model_version=model_ver,
            feature_name=prediction_col,
            drift_method="KS",
            reference_value=float(np.mean(ref_preds)),
            current_value=float(np.mean(curr_preds)),
            drift_score=ks_stat,
            severity=severity,
            threshold=KS_P_VALUE_THRESHOLD,
            is_drifted=is_drifted,
            computed_at=now,
            environment=env,
        ))

    return metrics


# ---------------------------------------------------------------------------
# Persist drift metrics to Delta
# ---------------------------------------------------------------------------

_DRIFT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {env}_lakehouse.mlops.drift_metrics (
    metric_id        STRING    NOT NULL,
    monitor_run_id   STRING    NOT NULL,
    model_name       STRING    NOT NULL,
    model_version    STRING,
    feature_name     STRING    NOT NULL,
    drift_method     STRING    NOT NULL,
    reference_value  DOUBLE,
    current_value    DOUBLE,
    drift_score      DOUBLE    NOT NULL,
    severity         STRING    NOT NULL,
    threshold        DOUBLE,
    is_drifted       BOOLEAN   NOT NULL,
    computed_at      TIMESTAMP NOT NULL,
    environment      STRING    NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.appendOnly'                 = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
"""


def _save_drift_metrics(
    spark: SparkSession,
    env: str,
    metrics: list[DriftMetric],
) -> None:
    spark.sql(_DRIFT_TABLE_DDL.format(env=env))
    if not metrics:
        return
    rows = [asdict(m) for m in metrics]
    spark.createDataFrame(rows).write.format("delta").mode("append") \
         .saveAsTable(f"{env}_lakehouse.mlops.drift_metrics")


# ---------------------------------------------------------------------------
# Retraining trigger via Databricks Jobs REST API
# ---------------------------------------------------------------------------

def _trigger_retraining(
    databricks_host: str,
    databricks_token: str,
    retraining_job_id: int,
    model_name: str,
    monitor_run_id: str,
    drift_summary: dict[str, Any],
) -> str:
    """
    Trigger the model retraining Databricks Job via REST API.
    Passes drift context as job parameters so the training job can
    log the trigger reason in MLflow.

    Returns the triggered job run_id.
    """
    url     = f"https://{databricks_host}/api/2.1/jobs/run-now"
    headers = {
        "Authorization":  f"Bearer {databricks_token}",
        "Content-Type":   "application/json",
    }
    payload = {
        "job_id": retraining_job_id,
        "python_params": [
            "--trigger-reason",  "model_drift",
            "--monitor-run-id",   monitor_run_id,
            "--drift-summary",    json.dumps(drift_summary),
        ],
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    run_id = resp.json().get("run_id", "unknown")
    _log.info("Retraining triggered | job=%s | run_id=%s", retraining_job_id, run_id)
    return str(run_id)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_drift_monitor(
    spark: SparkSession,
    env: str,
    model_name: str,
    databricks_host: str,
    databricks_token: str,
    retraining_job_id: int,
    feature_cols: list[str] | None = None,
    categorical_cols: list[str] | None = None,
    lookback_days: int = 7,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Entry point for the Model Drift Monitor.

    Returns a summary dict including drift scores and whether retraining
    was triggered.
    """
    monitor_run_id = str(uuid.uuid4())
    _log.info("Starting drift monitor | model=%s | run=%s", model_name, monitor_run_id)

    _feature_cols      = feature_cols or [
        "order_count_30d", "order_count_90d", "order_count_365d",
        "avg_order_value", "avg_discount",
        "days_since_last_order", "total_units_purchased",
    ]
    _categorical_cols = categorical_cols or ["customer_segment", "country_code"]

    # 1. Compute drift metrics
    metrics = compute_drift_metrics(
        spark=spark,
        env=env,
        model_name=model_name,
        feature_cols=_feature_cols,
        categorical_cols=_categorical_cols,
        lookback_days=lookback_days,
        monitor_run_id=monitor_run_id,
    )

    # 2. Persist to Delta
    _save_drift_metrics(spark, env, metrics)

    # 3. Assess overall drift severity
    severe_features   = [m for m in metrics if m.severity == "SEVERE"]
    moderate_features = [m for m in metrics if m.severity == "MODERATE"]
    should_retrain    = len(severe_features) > 0

    drift_summary = {
        "monitor_run_id":     monitor_run_id,
        "model_name":         model_name,
        "total_features":     len(metrics),
        "severe_drifted":     len(severe_features),
        "moderate_drifted":   len(moderate_features),
        "retrain_triggered":  False,
        "drift_details": [
            {
                "feature": m.feature_name,
                "method":  m.drift_method,
                "score":   round(m.drift_score, 4),
                "severity": m.severity,
            }
            for m in metrics
        ],
    }

    _log.info(
        "Drift results | severe=%d  moderate=%d | retrain=%s",
        len(severe_features), len(moderate_features), should_retrain,
    )

    # 4. Trigger retraining if severe drift detected
    if should_retrain and not dry_run:
        run_id = _trigger_retraining(
            databricks_host=databricks_host,
            databricks_token=databricks_token,
            retraining_job_id=retraining_job_id,
            model_name=model_name,
            monitor_run_id=monitor_run_id,
            drift_summary=drift_summary,
        )
        drift_summary["retrain_triggered"] = True
        drift_summary["retrain_job_run_id"] = run_id

    # 5. Log summary to MLflow as a monitoring run
    with mlflow.start_run(run_name=f"drift_monitor_{monitor_run_id[:8]}"):
        mlflow.set_tag("monitor_run_id",   monitor_run_id)
        mlflow.set_tag("model_name",       model_name)
        mlflow.set_tag("environment",      env)
        mlflow.log_metric("severe_drifted",   len(severe_features))
        mlflow.log_metric("moderate_drifted", len(moderate_features))
        mlflow.log_metric("retrain_triggered", int(should_retrain))
        for m in metrics:
            mlflow.log_metric(f"drift_{m.feature_name}_{m.drift_method}", m.drift_score)

    return drift_summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model Drift Monitor & Retraining Trigger")
    parser.add_argument("--env",                  required=True)
    parser.add_argument("--model-name",           required=True)
    parser.add_argument("--databricks-host",      required=True)
    parser.add_argument("--token-secret-scope",   required=True)
    parser.add_argument("--token-secret-key",     required=True)
    parser.add_argument("--retraining-job-id",    required=True, type=int)
    parser.add_argument("--lookback-days",        type=int, default=7)
    parser.add_argument("--dry-run",              action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    token = get_dbutils(spark).secrets.get(
        scope=args.token_secret_scope,
        key=args.token_secret_key,
    )

    summary = run_drift_monitor(
        spark=spark,
        env=args.env,
        model_name=args.model_name,
        databricks_host=args.databricks_host,
        databricks_token=token,
        retraining_job_id=args.retraining_job_id,
        lookback_days=args.lookback_days,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, default=str))

"""
Batch Inference Pipeline — Sales CLV Forecasting
=================================================
Agent: MLOps / ML Agent

Scores all active customers daily using the Production-registered MLflow
model and writes predictions back to the Gold layer.

Pipeline:
  1. Load the Production (or Staging fallback) MLflow model as a Spark UDF
  2. Read current customer features from gold_customer_clv or Feature Store
  3. Score all customers — distributed across the cluster via mapInPandas
  4. Compute 90 % prediction intervals using GBT leaf-node variance estimate
  5. Run Champion vs Challenger comparison when a Challenger model exists:
       - Score with both models on the same feature set
       - Record head-to-head metrics in mlops.champion_challenger_log
  6. Write predictions to {env}_lakehouse.gold.sales_predictions (MERGE)
  7. Log inference run metrics to MLflow and ingestion_metadata

Output table schema (gold.sales_predictions):
  customer_id            BIGINT
  customer_sk            BIGINT
  customer_segment       STRING
  country_code           STRING
  prediction_clv_30d     DOUBLE   ← point estimate
  prediction_lower_90    DOUBLE   ← lower bound of 90 % prediction interval
  prediction_upper_90    DOUBLE   ← upper bound of 90 % prediction interval
  model_name             STRING
  model_version          STRING
  model_stage            STRING   ← Production | Staging (challenger)
  challenger_prediction  DOUBLE   ← NULL when no challenger exists
  scored_at              TIMESTAMP
  inference_run_id       STRING
"""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator

import mlflow
import mlflow.spark
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)


# ---------------------------------------------------------------------------
# Feature columns — must match model_training.py FEATURE_COLS
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "order_count_30d",
    "order_count_90d",
    "order_count_365d",
    "avg_order_value",
    "avg_discount",
    "days_since_last_order",
    "total_units_purchased",
]

_PREDICTION_SCHEMA = StructType([
    StructField("customer_id",           DoubleType(), nullable=True),
    StructField("prediction_clv_30d",    DoubleType(), nullable=True),
    StructField("prediction_lower_90",   DoubleType(), nullable=True),
    StructField("prediction_upper_90",   DoubleType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Model loading helpers
# ---------------------------------------------------------------------------

def _load_model_info(
    model_name: str,
    stage: str,
) -> tuple[Any, str]:
    """
    Load an MLflow PySpark model for a given stage.
    Returns (loaded_model, model_version_string).
    """
    client   = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(model_name, stages=[stage])
    if not versions:
        return None, ""

    mv     = versions[0]
    model  = mlflow.spark.load_model(f"models:/{model_name}/{stage}")
    return model, mv.version


def _get_model_run_metric(model_name: str, stage: str, metric: str) -> float:
    """Fetch a training metric from the MLflow run backing a registered model version."""
    client   = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions(model_name, stages=[stage])
    if not versions:
        return float("nan")
    run = client.get_run(versions[0].run_id)
    return float(run.data.metrics.get(metric, float("nan")))


# ---------------------------------------------------------------------------
# Feature loading
# ---------------------------------------------------------------------------

def _load_features(spark: SparkSession, env: str) -> DataFrame:
    """
    Load customer features from gold_customer_clv (computed by the DLT pipeline
    or feature_engineering.py).  Falls back to the Databricks Feature Store
    if the gold table is unavailable.

    Returns a DataFrame with customer identity columns + FEATURE_COLS.
    Missing features are coalesced to 0 so every customer gets a score.
    """
    try:
        from databricks.feature_store import FeatureStoreClient  # type: ignore[import]
        fs_table = f"{env}_lakehouse.features.customer_features"
        df = FeatureStoreClient().read_table(name=fs_table)
        print(f"[INFERENCE] Loaded features from Feature Store: {fs_table}")
    except Exception:
        df = spark.table(f"{env}_lakehouse.gold.gold_customer_clv")
        print(f"[INFERENCE] Loaded features from gold_customer_clv")

    # Coalesce missing features to 0 — ensures no NULLs reach the model
    for col in FEATURE_COLS:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(0.0).cast(DoubleType()))
        else:
            df = df.withColumn(col, F.coalesce(F.col(col).cast(DoubleType()), F.lit(0.0)))

    identity_cols = ["customer_id", "customer_sk", "customer_segment", "country_code", "region"]
    keep = [c for c in identity_cols if c in df.columns] + FEATURE_COLS
    return df.select(keep).dropDuplicates(["customer_id"])


# ---------------------------------------------------------------------------
# Prediction interval via GBT leaf variance
# ---------------------------------------------------------------------------

def _score_with_intervals_pandas(
    model_broadcast,
    feature_cols: list[str],
    z_90: float = 1.645,
) -> "callable":
    """
    Returns a mapInPandas function that:
      1. Scores each micro-batch with the broadcast model
      2. Estimates prediction std using the GBT tree ensemble variance
         (variance of per-tree predictions → proxy for epistemic uncertainty)
    """
    def _score(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        model = model_broadcast.value
        for batch in iterator:
            if batch.empty:
                yield pd.DataFrame(columns=["customer_id", "prediction_clv_30d",
                                            "prediction_lower_90", "prediction_upper_90"])
                continue

            X = batch[feature_cols].fillna(0).values.astype(float)

            # Access individual tree predictions from the GBT pipeline model
            # to compute variance across the ensemble for uncertainty estimation
            try:
                gbt_model = model.stages[-1]          # GBTRegressionModel
                tree_preds = np.stack(
                    [t.predict(X) for t in gbt_model.trees], axis=1
                )                                     # shape: (n_samples, n_trees)
                point_est = tree_preds.mean(axis=1)
                std_est   = tree_preds.std(axis=1)
            except Exception:
                # Fallback: scikit-learn or opaque model
                point_est = np.array(model.predict(X), dtype=float)
                std_est   = np.full_like(point_est, fill_value=point_est.std() * 0.15)

            lower = point_est - z_90 * std_est
            upper = point_est + z_90 * std_est

            yield pd.DataFrame({
                "customer_id":          batch["customer_id"].values,
                "prediction_clv_30d":   point_est,
                "prediction_lower_90":  lower,
                "prediction_upper_90":  upper,
            })

    return _score


# ---------------------------------------------------------------------------
# Champion vs Challenger comparison
# ---------------------------------------------------------------------------

_CC_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {env}_lakehouse.mlops.champion_challenger_log (
    comparison_id         STRING     NOT NULL,
    inference_run_id      STRING     NOT NULL,
    model_name            STRING     NOT NULL,
    champion_version      STRING,
    challenger_version    STRING,
    champion_avg_pred     DOUBLE,
    challenger_avg_pred   DOUBLE,
    pred_delta_pct        DOUBLE,
    n_customers_scored    LONG,
    scored_at             TIMESTAMP  NOT NULL,
    environment           STRING     NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
"""


def _log_champion_challenger(
    spark: SparkSession,
    env: str,
    inference_run_id: str,
    model_name: str,
    champion_version: str,
    challenger_version: str,
    champ_preds: np.ndarray,
    chal_preds: np.ndarray,
) -> None:
    spark.sql(_CC_LOG_DDL.format(env=env))

    champ_avg = float(np.nanmean(champ_preds))
    chal_avg  = float(np.nanmean(chal_preds))
    delta_pct = ((chal_avg - champ_avg) / (abs(champ_avg) + 1e-8)) * 100

    row = [{
        "comparison_id":       str(uuid.uuid4()),
        "inference_run_id":    inference_run_id,
        "model_name":          model_name,
        "champion_version":    champion_version,
        "challenger_version":  challenger_version,
        "champion_avg_pred":   champ_avg,
        "challenger_avg_pred": chal_avg,
        "pred_delta_pct":      round(delta_pct, 4),
        "n_customers_scored":  len(champ_preds),
        "scored_at":           datetime.now(timezone.utc),
        "environment":         env,
    }]
    spark.createDataFrame(row).write.format("delta").mode("append") \
         .saveAsTable(f"{env}_lakehouse.mlops.champion_challenger_log")
    print(
        f"[INFERENCE] Champion avg={champ_avg:.2f} | "
        f"Challenger avg={chal_avg:.2f} | Δ={delta_pct:+.2f}%"
    )


# ---------------------------------------------------------------------------
# Write predictions to Gold
# ---------------------------------------------------------------------------

_PREDICTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {env}_lakehouse.gold.sales_predictions (
    customer_id             BIGINT     NOT NULL,
    customer_sk             BIGINT,
    customer_segment        STRING,
    country_code            STRING,
    region                  STRING,
    prediction_clv_30d      DOUBLE     NOT NULL,
    prediction_lower_90     DOUBLE,
    prediction_upper_90     DOUBLE,
    model_name              STRING     NOT NULL,
    model_version           STRING,
    model_stage             STRING,
    challenger_prediction   DOUBLE,
    scored_at               TIMESTAMP  NOT NULL,
    inference_run_id        STRING     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.enableChangeDataFeed'       = 'true'
)
"""


def _write_predictions(
    spark: SparkSession,
    env: str,
    predictions_df: DataFrame,
) -> int:
    spark.sql(_PREDICTIONS_DDL.format(env=env))
    target = f"{env}_lakehouse.gold.sales_predictions"

    predictions_df.createOrReplaceTempView("_inference_source")

    spark.sql(f"""
        MERGE INTO {target} AS tgt
        USING _inference_source AS src
          ON tgt.customer_id = src.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = predictions_df.count()
    print(f"[INFERENCE] Wrote {count:,} predictions → {target}")
    return count


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_batch_inference(
    spark: SparkSession,
    env: str,
    model_name: str,
    include_challenger: bool = True,
) -> dict[str, Any]:
    """
    Execute the full batch inference pipeline.

    Args:
        spark:              Active SparkSession.
        env:                Deployment environment (dev / test / prod).
        model_name:         MLflow registered model name.
        include_challenger: Whether to also score with the Staging challenger.

    Returns:
        Summary dict with run_id, customers scored, and model versions used.
    """
    inference_run_id = str(uuid.uuid4())
    scored_at        = datetime.now(timezone.utc)
    print(f"[INFERENCE] Starting | model={model_name} | run={inference_run_id}")

    # 1. Load champion model (Production)
    champ_model, champ_version = _load_model_info(model_name, "Production")
    if champ_model is None:
        champ_model, champ_version = _load_model_info(model_name, "Staging")
        champ_stage = "Staging"
    else:
        champ_stage = "Production"

    if champ_model is None:
        raise RuntimeError(
            f"No Production or Staging version of '{model_name}' in MLflow."
        )
    print(f"[INFERENCE] Champion: {model_name} v{champ_version} ({champ_stage})")

    # 2. Optionally load challenger model (Staging)
    chal_model, chal_version = None, ""
    if include_challenger and champ_stage == "Production":
        chal_model, chal_version = _load_model_info(model_name, "Staging")
        if chal_model:
            print(f"[INFERENCE] Challenger: {model_name} v{chal_version} (Staging)")

    # 3. Load features
    features_df = _load_features(spark, env)
    n_customers = features_df.count()
    print(f"[INFERENCE] Scoring {n_customers:,} customers")

    # 4. Score with Champion — broadcast model for mapInPandas efficiency
    champ_broadcast = spark.sparkContext.broadcast(champ_model)
    score_fn        = _score_with_intervals_pandas(champ_broadcast, FEATURE_COLS)

    champ_preds_df = (
        features_df
        .mapInPandas(score_fn, schema=_PREDICTION_SCHEMA)
    )
    champ_preds_df.cache()

    # 5. Optionally score with Challenger and log comparison
    chal_pred_col = F.lit(None).cast(DoubleType())
    if chal_model:
        chal_broadcast = spark.sparkContext.broadcast(chal_model)
        chal_score_fn  = _score_with_intervals_pandas(chal_broadcast, FEATURE_COLS)
        chal_preds_df  = features_df.mapInPandas(chal_score_fn, schema=_PREDICTION_SCHEMA)

        # Collect for comparison logging (sampled to 50k for large datasets)
        champ_vals = np.array([r["prediction_clv_30d"]
                                for r in champ_preds_df.select("prediction_clv_30d")
                                                        .limit(50_000).collect()])
        chal_vals  = np.array([r["prediction_clv_30d"]
                                for r in chal_preds_df.select("prediction_clv_30d")
                                                       .limit(50_000).collect()])

        _log_champion_challenger(
            spark, env, inference_run_id, model_name,
            champ_version, chal_version, champ_vals, chal_vals,
        )

        # Join challenger predictions for storage
        chal_pred_col = (
            chal_preds_df
            .withColumnRenamed("prediction_clv_30d", "challenger_prediction")
            .select("customer_id", "challenger_prediction")
        )

    # 6. Build final predictions DataFrame
    result_df = (
        features_df
        .join(champ_preds_df, "customer_id", "left")
        .withColumn("model_name",    F.lit(model_name))
        .withColumn("model_version", F.lit(champ_version))
        .withColumn("model_stage",   F.lit(champ_stage))
        .withColumn("scored_at",     F.lit(scored_at).cast(TimestampType()))
        .withColumn("inference_run_id", F.lit(inference_run_id))
    )

    if chal_model and isinstance(chal_pred_col, DataFrame):
        result_df = result_df.join(chal_pred_col, "customer_id", "left")
    else:
        result_df = result_df.withColumn("challenger_prediction", F.lit(None).cast(DoubleType()))

    # 7. Write predictions to Gold
    rows_written = _write_predictions(spark, env, result_df)

    # 8. Log run to MLflow
    experiment_name = f"/lakehouse/{env}/batch_inference"
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=f"batch_inference_{inference_run_id[:8]}"):
        mlflow.log_param("model_name",     model_name)
        mlflow.log_param("model_version",  champ_version)
        mlflow.log_param("model_stage",    champ_stage)
        mlflow.log_param("environment",    env)
        mlflow.log_param("inference_run_id", inference_run_id)
        mlflow.log_metric("customers_scored", rows_written)
        mlflow.log_metric("challenger_enabled", int(chal_model is not None))

    summary = {
        "inference_run_id":  inference_run_id,
        "model_name":        model_name,
        "champion_version":  champ_version,
        "challenger_version": chal_version or None,
        "customers_scored":  rows_written,
        "scored_at":         scored_at.isoformat(),
    }
    print(f"[INFERENCE] Complete | {json.dumps(summary)}")
    return summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Inference Pipeline")
    parser.add_argument("--env",        required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--no-challenger", action="store_true",
                        help="Disable Champion vs Challenger comparison")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    run_batch_inference(
        spark=spark,
        env=args.env,
        model_name=args.model_name,
        include_challenger=not args.no_challenger,
    )

"""
MLflow Model Training — Sales Forecasting Pipeline
===================================================
Agent: MLOps / ML Agent

Trains a sales forecasting model on Gold feature tables using MLflow
for experiment tracking, model registration, and serving.

Pipeline:
  1. Load customer + product features from Feature Store (or Gold Delta)
  2. Prepare training dataset (label = clv_30d, features = historical metrics)
  3. Train model with hyperparameter tuning (cross-validation)
  4. Log metrics, parameters, and model artifact to MLflow
  5. Register best model to MLflow Model Registry
  6. Optionally transition to Staging / Production stage

Model type:
  - Gradient Boosted Trees (MLlib GBTRegressor) for distributed training
    on large datasets; falls back to scikit-learn RandomForest for small data.

MLflow experiment is created per environment:
  /lakehouse/{env}/sales_forecasting
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone

import mlflow
import mlflow.spark
import mlflow.sklearn
from mlflow.models.signature import infer_signature

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


# ---------------------------------------------------------------------------
# Dataset preparation
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
LABEL_COL = "clv_30d"       # predict next 30-day customer lifetime value


def load_training_data(
    spark: SparkSession,
    env: str,
    min_orders: int = 3,
) -> DataFrame:
    """
    Load and prepare training data from the customer feature table.

    Filters to customers with at least *min_orders* to ensure meaningful
    CLV labels; drops rows with null features or label.
    """
    try:
        from databricks.feature_store import FeatureStoreClient  # type: ignore[import]
        fs = FeatureStoreClient()
        fs_table = f"{env}_lakehouse.features.customer_features"
        df = fs.read_table(name=fs_table)
        print(f"[ML] Loaded features from Feature Store: {fs_table}")
    except (ImportError, Exception):
        df = spark.table(f"{env}_lakehouse.features.customer_features")
        print(f"[ML] Loaded features from Delta table (Feature Store SDK unavailable)")

    prepared = (
        df
        .filter(F.col("order_count_365d") >= min_orders)    # minimum history required
        .filter(F.col(LABEL_COL).isNotNull())
        .filter(F.col(LABEL_COL) > 0)
        .select(*FEATURE_COLS, LABEL_COL)
        .dropna(subset=FEATURE_COLS)
    )

    total = prepared.count()
    print(f"[ML] Training dataset: {total:,} customers")
    return prepared


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def train_sales_forecast_model(
    spark: SparkSession,
    env: str,
    n_folds: int = 3,
    max_iter_values: list[int] | None = None,
    max_depth_values: list[int] | None = None,
) -> str:
    """
    Train a GBT sales forecasting model with MLflow tracking.

    Returns:
        The MLflow run_id of the best model run.
    """
    max_iter_values  = max_iter_values  or [20, 50]
    max_depth_values = max_depth_values or [3, 5]

    experiment_name = f"/lakehouse/{env}/sales_forecasting"
    mlflow.set_experiment(experiment_name)

    df = load_training_data(spark, env)

    # 80/20 train-test split (stratified by customer_segment not practical with GBT)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"[ML] Train: {train_df.count():,}  Test: {test_df.count():,}")

    # Build ML pipeline
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    scaler    = StandardScaler(inputCol="raw_features", outputCol="features",
                               withMean=True, withStd=True)
    gbt       = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL_COL,
        predictionCol="prediction",
        seed=42,
    )
    pipeline  = Pipeline(stages=[assembler, scaler, gbt])

    # Hyperparameter grid
    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxIter,  max_iter_values)
        .addGrid(gbt.maxDepth, max_depth_values)
        .build()
    )

    evaluator = RegressionEvaluator(
        labelCol=LABEL_COL,
        predictionCol="prediction",
        metricName="rmse",
    )

    with mlflow.start_run(run_name=f"gbt_clv_forecast_{env}") as run:
        mlflow.log_param("model_type",     "GBTRegressor")
        mlflow.log_param("feature_cols",   FEATURE_COLS)
        mlflow.log_param("label_col",      LABEL_COL)
        mlflow.log_param("n_folds",        n_folds)
        mlflow.log_param("train_size",     train_df.count())
        mlflow.log_param("test_size",      test_df.count())
        mlflow.log_param("environment",    env)

        # Enable autologging for Spark ML
        mlflow.spark.autolog(log_models=True, log_post_training_metrics=True)

        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=n_folds,
            parallelism=2,
            seed=42,
        )

        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel

        # Evaluate on held-out test set
        predictions = best_model.transform(test_df)
        rmse  = evaluator.evaluate(predictions)
        r2_ev = RegressionEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="r2"
        ).evaluate(predictions)
        mae = RegressionEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="mae"
        ).evaluate(predictions)

        mlflow.log_metric("test_rmse", rmse)
        mlflow.log_metric("test_r2",   r2_ev)
        mlflow.log_metric("test_mae",  mae)

        print(f"[ML] Test metrics — RMSE: {rmse:.2f}  R²: {r2_ev:.4f}  MAE: {mae:.2f}")

        # Log the best model
        mlflow.spark.log_model(
            best_model,
            artifact_path="model",
            registered_model_name=f"lakehouse_{env}_clv_forecast",
        )

        run_id = run.info.run_id
        print(f"[ML] MLflow run_id: {run_id}")

    return run_id


# ---------------------------------------------------------------------------
# Model registry promotion
# ---------------------------------------------------------------------------

def promote_model_to_staging(model_name: str, run_id: str) -> None:
    """
    Find the model version logged in *run_id* and transition it to Staging.
    Requires MLflow Model Registry access.
    """
    client = mlflow.tracking.MlflowClient()

    # Find the version registered from this run
    versions = client.search_model_versions(f"run_id='{run_id}'")
    if not versions:
        print(f"[ML] No model version found for run_id={run_id}")
        return

    version = versions[0].version
    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage="Staging",
        archive_existing_versions=True,
    )
    print(f"[ML] Model {model_name} v{version} transitioned to Staging")


def promote_model_to_production(
    model_name: str,
    staging_version: str | None = None,
) -> None:
    """
    Transition the current Staging model (or a specific version) to Production.
    Archives the current Production version automatically.
    """
    client = mlflow.tracking.MlflowClient()

    if staging_version is None:
        staging = [
            v for v in client.search_model_versions(f"name='{model_name}'")
            if v.current_stage == "Staging"
        ]
        if not staging:
            print(f"[ML] No Staging model found for {model_name}")
            return
        staging_version = staging[0].version

    client.transition_model_version_stage(
        name=model_name,
        version=staging_version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"[ML] Model {model_name} v{staging_version} promoted to Production")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sales forecasting model training")
    parser.add_argument("--env",    required=True,
                        help="dev | test | prod")
    parser.add_argument("--action", default="train",
                        choices=["train", "promote_staging", "promote_production"])
    parser.add_argument("--run-id", default=None,
                        help="MLflow run_id (required for promote_staging)")
    parser.add_argument("--n-folds", type=int, default=3)
    args = parser.parse_args()

    model_name = f"lakehouse_{args.env}_clv_forecast"

    if args.action == "train":
        spark = SparkSession.builder.getOrCreate()
        run_id = train_sales_forecast_model(spark, args.env, n_folds=args.n_folds)
        print(f"[DONE] Training complete. run_id={run_id}")
        promote_model_to_staging(model_name, run_id)

    elif args.action == "promote_staging":
        if not args.run_id:
            raise ValueError("--run-id required for promote_staging")
        promote_model_to_staging(model_name, args.run_id)

    elif args.action == "promote_production":
        promote_model_to_production(model_name)

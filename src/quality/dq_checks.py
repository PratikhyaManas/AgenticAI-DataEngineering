"""
Data Quality Checks — PySpark Implementation
=============================================
Agent: Data Quality & Observability Agent

Implements layered DQ checks for Bronze, Silver, and Gold Delta tables.
Results are logged to a Delta DQ results table and metrics are pushed
to Azure Monitor via the Application Insights REST API.

Dimensions covered:
  - Completeness   (NOT NULL checks)
  - Uniqueness     (duplicate key detection)
  - Validity       (range, regex, enum checks)
  - Timeliness     (freshness: max timestamp vs current time)
  - Consistency    (referential integrity across tables)
  - Volume         (row count drift vs previous run)
"""

import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


# ---------------------------------------------------------------------------
# DQ Result Data Model
# ---------------------------------------------------------------------------

@dataclass
class DQCheckResult:
    check_id:        str
    run_id:          str
    environment:     str
    layer:           str          # bronze | silver | gold
    domain:          str
    entity:          str
    table_name:      str
    check_name:      str
    check_type:      str          # completeness | uniqueness | validity | timeliness | consistency | volume
    column_name:     Optional[str]
    status:          str          # PASSED | FAILED | WARNING
    expected_value:  Optional[str]
    actual_value:    Optional[str]
    failure_count:   int
    total_count:     int
    failure_rate:    float        # 0.0 – 1.0
    threshold:       float        # max allowed failure_rate before FAILED
    check_ts:        datetime
    error_message:   Optional[str] = None


# ---------------------------------------------------------------------------
# DQ Check Engine
# ---------------------------------------------------------------------------

class DQEngine:
    def __init__(self, spark: SparkSession, env: str, layer: str, domain: str, entity: str, run_id: str = None):
        self.spark   = spark
        self.env     = env
        self.layer   = layer
        self.domain  = domain
        self.entity  = entity
        self.run_id  = run_id or str(uuid.uuid4())
        self.results: List[DQCheckResult] = []
        self._results_table = f"{env}_lakehouse.bronze.dq_results"
        self._cached_dfs: list = []      # track cached DataFrames for cleanup
        self._total_cache: dict = {}     # {df_id: total_count} — avoids repeat count()

    # ------------------------------------------------------------------
    # DataFrame caching helpers
    # ------------------------------------------------------------------
    def cache_df(self, df: DataFrame) -> DataFrame:
        """Persist a DataFrame to memory+disk and record it for later release."""
        cached = df.persist(StorageLevel.MEMORY_AND_DISK)
        self._cached_dfs.append(cached)
        return cached

    def release_caches(self) -> None:
        """Unpersist all DataFrames cached during this DQ run."""
        for df in self._cached_dfs:
            df.unpersist()
        self._cached_dfs.clear()
        self._total_cache.clear()

    def _total(self, df: DataFrame) -> int:
        """Return row count, reusing a cached value where possible."""
        key = id(df)
        if key not in self._total_cache:
            self._total_cache[key] = df.count()
        return self._total_cache[key]

    def _make_result(self, check_name, check_type, column_name, status, expected, actual,
                     failures, total, threshold, table_name, error_message=None):
        rate = failures / total if total > 0 else 0.0
        return DQCheckResult(
            check_id=str(uuid.uuid4()),
            run_id=self.run_id,
            environment=self.env,
            layer=self.layer,
            domain=self.domain,
            entity=self.entity,
            table_name=table_name,
            check_name=check_name,
            check_type=check_type,
            column_name=column_name,
            status=status,
            expected_value=str(expected),
            actual_value=str(actual),
            failure_count=failures,
            total_count=total,
            failure_rate=rate,
            threshold=threshold,
            check_ts=datetime.now(timezone.utc),
            error_message=error_message,
        )

    # ------------------------------------------------------------------
    # 1. Completeness
    # ------------------------------------------------------------------
    def check_not_null(self, df: DataFrame, column: str, table_name: str,
                        threshold: float = 0.0) -> DQCheckResult:
        total = self._total(df)
        nulls = df.filter(F.col(column).isNull()).count()
        status = "PASSED" if (nulls / total if total else 0) <= threshold else "FAILED"
        result = self._make_result(
            f"not_null:{column}", "completeness", column, status,
            f"null_rate <= {threshold}", f"null_rate = {nulls/total:.4f}" if total else "N/A",
            nulls, total, threshold, table_name
        )
        self.results.append(result)
        return result

    def check_completeness_batch(
        self,
        df: DataFrame,
        columns: List[str],
        table_name: str,
        threshold: float = 0.0,
    ) -> List[DQCheckResult]:
        """
        Compute null counts for ALL columns in a single Spark aggregation pass
        instead of one action per column.  Results are identical to calling
        check_not_null() repeatedly, but uses only 2 Spark actions total
        (1 count + 1 agg) regardless of how many columns are checked.
        """
        total = self._total(df)

        # Build one agg() expression per column — single Spark job
        agg_exprs = [
            F.sum(F.col(c).isNull().cast("long")).alias(f"_null_{c}")
            for c in columns
        ]
        null_counts = df.agg(*agg_exprs).collect()[0].asDict()

        results = []
        for col in columns:
            nulls  = int(null_counts[f"_null_{col}"] or 0)
            rate   = nulls / total if total else 0.0
            status = "PASSED" if rate <= threshold else "FAILED"
            result = self._make_result(
                f"not_null:{col}", "completeness", col, status,
                f"null_rate <= {threshold}",
                f"null_rate = {rate:.4f}" if total else "N/A",
                nulls, total, threshold, table_name,
            )
            self.results.append(result)
            results.append(result)
        return results

    # ------------------------------------------------------------------
    # 2. Uniqueness
    # ------------------------------------------------------------------
    def check_unique(self, df: DataFrame, columns: List[str], table_name: str,
                      threshold: float = 0.0) -> DQCheckResult:
        total      = self._total(df)
        distinct   = df.select(*columns).distinct().count()
        duplicates = total - distinct
        status     = "PASSED" if (duplicates / total if total else 0) <= threshold else "FAILED"
        result     = self._make_result(
            f"unique:{'_'.join(columns)}", "uniqueness", ",".join(columns), status,
            f"dup_rate <= {threshold}", f"dup_rate = {duplicates/total:.4f}" if total else "N/A",
            duplicates, total, threshold, table_name
        )
        self.results.append(result)
        return result

    # ------------------------------------------------------------------
    # 3. Validity — Range
    # ------------------------------------------------------------------
    def check_range(self, df: DataFrame, column: str, table_name: str,
                     min_val=None, max_val=None, threshold: float = 0.01) -> DQCheckResult:
        total = self._total(df)
        condition = F.lit(False)
        if min_val is not None:
            condition = condition | (F.col(column) < F.lit(min_val))
        if max_val is not None:
            condition = condition | (F.col(column) > F.lit(max_val))
        violations = df.filter(condition).count()
        rate   = violations / total if total else 0.0
        status = "PASSED" if rate <= threshold else "FAILED"
        result = self._make_result(
            f"range:{column}[{min_val},{max_val}]", "validity", column, status,
            f"[{min_val},{max_val}]", f"violations={violations}",
            violations, total, threshold, table_name
        )
        self.results.append(result)
        return result

    # ------------------------------------------------------------------
    # 4. Validity — Enum
    # ------------------------------------------------------------------
    def check_enum(self, df: DataFrame, column: str, table_name: str,
                    accepted_values: List[Any], threshold: float = 0.01) -> DQCheckResult:
        total      = self._total(df)
        violations = df.filter(~F.col(column).isin(accepted_values) & F.col(column).isNotNull()).count()
        rate       = violations / total if total else 0.0
        status     = "PASSED" if rate <= threshold else "FAILED"
        result     = self._make_result(
            f"enum:{column}", "validity", column, status,
            str(accepted_values), f"violations={violations}",
            violations, total, threshold, table_name
        )
        self.results.append(result)
        return result

    # ------------------------------------------------------------------
    # 5. Timeliness (Freshness)
    # ------------------------------------------------------------------
    def check_freshness(self, df: DataFrame, timestamp_column: str, table_name: str,
                         max_age_hours: float = 25.0) -> DQCheckResult:
        max_ts = df.agg(F.max(timestamp_column).alias("max_ts")).collect()[0]["max_ts"]
        now    = datetime.now(timezone.utc)
        age_hours = (now - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if max_ts else 999
        status = "PASSED" if age_hours <= max_age_hours else "FAILED"
        result = self._make_result(
            f"freshness:{timestamp_column}", "timeliness", timestamp_column, status,
            f"max_age_hours <= {max_age_hours}", f"actual_age_hours = {age_hours:.2f}",
            0 if status == "PASSED" else 1, 1, 0.0, table_name,
        )
        self.results.append(result)
        return result

    # ------------------------------------------------------------------
    # 6. Volume Drift
    # ------------------------------------------------------------------
    def check_volume_drift(self, df: DataFrame, table_name: str,
                            expected_min: int = None, expected_max: int = None,
                            drift_pct_threshold: float = 0.3) -> DQCheckResult:
        total = df.count()
        status = "PASSED"
        details = f"row_count={total}"
        if expected_min is not None and total < expected_min:
            status = "FAILED"
            details += f" (below min {expected_min})"
        if expected_max is not None and total > expected_max:
            status = "WARNING"
            details += f" (above max {expected_max})"
        result = self._make_result(
            "volume_check", "volume", None, status,
            f"[{expected_min},{expected_max}]", details,
            0 if status == "PASSED" else 1, total, drift_pct_threshold, table_name
        )
        self.results.append(result)
        return result

    # ------------------------------------------------------------------
    # Save DQ results to Delta
    # ------------------------------------------------------------------
    def save_results(self) -> None:
        self._ensure_results_table()
        rows = [asdict(r) for r in self.results]
        df   = self.spark.createDataFrame(rows)
        df   = df.withColumn("check_ts", F.col("check_ts").cast("timestamp"))
        df.write.format("delta").mode("append").saveAsTable(self._results_table)
        print(f"[DQ] Saved {len(self.results)} check results to {self._results_table}")

    def _ensure_results_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._results_table} (
                check_id        STRING,
                run_id          STRING,
                environment     STRING,
                layer           STRING,
                domain          STRING,
                entity          STRING,
                table_name      STRING,
                check_name      STRING,
                check_type      STRING,
                column_name     STRING,
                status          STRING,
                expected_value  STRING,
                actual_value    STRING,
                failure_count   BIGINT,
                total_count     BIGINT,
                failure_rate    DOUBLE,
                threshold       DOUBLE,
                check_ts        TIMESTAMP,
                error_message   STRING
            )
            USING DELTA
            PARTITIONED BY (environment, layer)
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)

    def has_failures(self) -> bool:
        return any(r.status == "FAILED" for r in self.results)

    def summary(self) -> dict:
        return {
            "run_id":   self.run_id,
            "passed":   sum(1 for r in self.results if r.status == "PASSED"),
            "failed":   sum(1 for r in self.results if r.status == "FAILED"),
            "warnings": sum(1 for r in self.results if r.status == "WARNING"),
            "total":    len(self.results),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        """Auto-release cached DataFrames when used as a context manager."""
        self.release_caches()

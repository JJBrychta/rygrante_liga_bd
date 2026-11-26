"""Bronze ingestion skeleton for HR domain (`employees` CSV and `timesheets` Parquet).

TODO (Student Deliverable):
1. Read daily Hive-partitioned files for `batch_date`:
   - `<landing_root>/employees/date=<batch_date>/part-000.csv`
   - `<landing_root>/timesheets/date=<batch_date>/part-000.parquet`
2. Preserve raw values with zero lossy transformations.
3. Append `_batch_date`, `_ingested_at`, `_source_file`.
4. Write idempotently partitioned by `_batch_date` (replace only `batch_date`
   via `replaceWhere` or Delta `delete` + `append`) to `bronze/employees` and
   `bronze/timesheets`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

EMPLOYEES_RAW_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("seniority_level", StringType(), nullable=True),
        StructField("hourly_rate_eur", StringType(), nullable=True),
        StructField("hire_date", StringType(), nullable=True),
        StructField("last_promotion_date", StringType(), nullable=True),
        StructField("work_mode", StringType(), nullable=True),
        StructField("distance_km", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("is_active", StringType(), nullable=True),
        StructField("termination_date", StringType(), nullable=True),
        StructField("churn_type", StringType(), nullable=True),
        StructField("snapshot_date", StringType(), nullable=True),
    ]
)


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Ingest employees and timesheets into Bronze Delta."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    # 1. Read the daily files.
    employees_path = join_path(resolved_landing, "employees", f"date={batch_date}", "part-000.csv")
    timesheets_path = join_path(
        resolved_landing, "timesheets", f"date={batch_date}", "part-000.parquet"
    )

    # 2. Preserve raw values.
    employees_df = (
        spark.read.option("header", "true").schema(EMPLOYEES_RAW_SCHEMA).csv(employees_path)
    )
    timesheets_df = spark.read.parquet(timesheets_path)

    for table_name, raw_df in [
        ("employees", employees_df),
        ("timesheets", timesheets_df),
    ]:
        # 3. Add technical metadata.
        bronze_df = (
            raw_df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )

        target_path = join_path(resolved_lakehouse, "bronze", table_name)

        # 4. Replace only the requested day's partition.
        (
            bronze_df.write.format("delta")
            .mode("overwrite")
            .partitionBy("_batch_date")
            .option("replaceWhere", f"_batch_date = '{batch_date}'")
            .save(target_path)
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

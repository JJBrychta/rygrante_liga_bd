"""Bronze ingestion skeleton for `crm_events` and `employee_surveys` (JSONL batches).

TODO (Student Deliverable):
1. Read `<landing_root>/crm_events/date=<batch_date>/part-000.jsonl`, preserving
   the polymorphic `payload` JSON untouched (read as raw string or map/struct
   without dropping event-specific fields).
2. Read `<landing_root>/employee_surveys/date=<batch_date>/part-000.jsonl`
   (note: pulse surveys only drop on survey wave dates, e.g. Days 9-16 for Wave 1;
   handle dates without survey drops gracefully!).
3. Append `_batch_date`, `_ingested_at`, `_source_file` and write idempotently
   partitioned by `_batch_date` to `bronze/crm_events` and `bronze/employee_surveys`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)


def _field(name: str, alias: str | None = None) -> Column:
    return F.get_json_object(F.col("value"), f"$.{name}").alias(alias or name)


def _write_daily(df: DataFrame, target: str, batch_date: str) -> DataFrame:
    """Add Bronze metadata and replace only this `_batch_date` partition (idempotent)."""
    out = (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    (
        out.write.format("delta")
        .mode("overwrite")
        .partitionBy("_batch_date")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .save(target)
    )
    return out


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Ingest one day of `crm_events` and `employee_surveys` into Bronze Delta."""
    landing = get_landing_root(landing_root)
    lakehouse = get_lakehouse_root(lakehouse_root)

    # --- crm_events -------------------------------------------------------
    # Read each JSONL line as raw text so the polymorphic `payload` stays untouched.
    crm_src = join_path(landing, "crm_events", f"date={batch_date}", "part-000.jsonl")
    crm = spark.read.text(crm_src).select(
        _field("event_id"),
        _field("client_id"),
        _field("event_type"),
        _field("timestamp"),
        _field("payload", "payload_raw"),
    )

    crm_out = _write_daily(crm, join_path(lakehouse, "bronze", "crm_events"), batch_date)
    print(f"bronze/crm_events: {crm_out.count()} rows for batch_date={batch_date}")

    # --- employee_surveys -------------------------------------------------
    # Pulse surveys only drop on survey wave days -> skip days without a file.
    surveys_src = join_path(landing, "employee_surveys", f"date={batch_date}", "part-000.jsonl")
    if "://" not in surveys_src and not os.path.exists(surveys_src):
        print(f"bronze/employee_surveys: no file for batch_date={batch_date}, skipping.")
        return

    surveys = spark.read.text(surveys_src).select(
        _field("survey_id"),
        _field("emp_id"),
        _field("submitted_at"),
        _field("enps_score"),
        _field("overall_comment"),
        _field("answers", "answers_raw"),
    )
    surveys_out = _write_daily(
        surveys, join_path(lakehouse, "bronze", "employee_surveys"), batch_date
    )
    print(f"bronze/employee_surveys: {surveys_out.count()} rows for batch_date={batch_date}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CRM & Survey datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-crm-surveys")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

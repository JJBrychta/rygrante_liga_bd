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

from pyspark.sql import SparkSession

from spark_jobs.common.spark_session import get_spark_session


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """TODO: Implement Bronze ingestion for `crm_events` and `employee_surveys`."""
    raise NotImplementedError("TODO: Implement Bronze CRM & Surveys ingestion.")


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

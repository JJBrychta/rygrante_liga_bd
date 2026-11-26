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

from spark_jobs.common.spark_session import get_spark_session


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """TODO: Implement Bronze ingestion for `employees` and `timesheets`."""
    raise NotImplementedError("TODO: Implement Bronze HR ingestion for employees and timesheets.")


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

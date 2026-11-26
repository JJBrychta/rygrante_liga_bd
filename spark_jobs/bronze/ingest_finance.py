"""Bronze ingestion skeleton for Finance domain (`fx_rates`, `invoices`, `expenses`).

TODO (Student Deliverable):
1. Read daily Hive-partitioned files for `batch_date`:
   - `<landing_root>/fx_rates/date=<batch_date>/part-000.json`
     IMPORTANT: `fx_rates` is delivered on **weekdays only** (no Saturday/Sunday
     files). Handle missing weekend `fx_rates` partitions gracefully without failing!
   - `<landing_root>/invoices/date=<batch_date>/part-000.jsonl`
     Keep raw `issued_at` and `paid_at` strings (ISO & `DD.MM.YYYY`) unchanged in Bronze.
   - `<landing_root>/expenses/date=<batch_date>/part-000.csv`
2. Append `_batch_date`, `_ingested_at`, `_source_file`.
3. Write idempotently partitioned by `_batch_date` to `bronze/fx_rates`,
   `bronze/invoices`, and `bronze/expenses`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 4.2.A) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Implement Bronze ingestion for `fx_rates`, `invoices`, and `expenses`."""
    raise NotImplementedError("TODO: Implement Bronze Finance ingestion.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

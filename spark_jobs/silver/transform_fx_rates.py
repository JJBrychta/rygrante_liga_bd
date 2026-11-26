"""Silver FX calendar spine & LOCF forward-fill skeleton for `silver_fx_rates_daily`.

TODO (Student Deliverable):
1. Generate a complete calendar spine (`start_date`..`batch_date`) cross-joined
   with all 6 currency pairs (`EUR`, `USD`, `CZK`).
2. Left-join `bronze/fx_rates` and forward-fill missing Saturday/Sunday/holiday
   rates using Spark window functions (`F.last("rate", ignorenulls=True).over(...)`).
3. Verify `silver_fx_rates_daily` has 0 NULL rates (e.g., 84 rows for Days 1-14).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 4.2.B.3) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_jobs.common.spark_session import get_spark_session


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> None:
    """TODO: Build `silver/silver_fx_rates_daily` using calendar spine + LOCF."""
    raise NotImplementedError("TODO: Implement Silver FX rates forward-fill transformation.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_fx_rates_daily.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-fx-rates")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

"""Silver SCD Type 2 transformation skeleton for `silver_employees_scd2`.

TODO (Student Deliverable):
1. Read `bronze/employees` snapshots up to `batch_date` (or incrementally apply
   snapshot changes via Delta MERGE / window-based change detection).
2. Track Slowly Changing Dimension Type 2 history (`valid_from`, `valid_to`,
   `is_current`) for changes in `department`, `role`, `hourly_rate_eur`,
   `is_active`, `termination_date`, and `churn_type`.
3. Choose and document your interval convention (closed `[valid_from, valid_to]`
   vs. half-open `[valid_from, valid_to)`) in `docs/architecture.md`.
4. Verify zero overlapping intervals and unique `(emp_id, valid_from)`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 4.2.B.1, 4.3) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Build `silver/silver_employees_scd2` from `bronze/employees`."""
    raise NotImplementedError("TODO: Implement Silver SCD Type 2 transformation for employees.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_employees_scd2.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-employees-scd2")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

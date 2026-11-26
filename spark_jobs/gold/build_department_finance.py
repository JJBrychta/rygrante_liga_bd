"""Gold mart skeleton for `gold_department_finance`.

TODO (Student Deliverable):
- Grain: `(department, category, year_month)`.
- Metrics (see `docs/STUDENT_ASSIGNMENT.md` Section 4.2.C):
  - `total_expense_eur`
  - `expense_count`
  - Departmental headcount & billable utilization
- Write idempotently to `gold/gold_department_finance` (Delta).
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
    """TODO: Build `gold/gold_department_finance`."""
    raise NotImplementedError("TODO: Implement gold_department_finance.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_department_finance.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("gold-department-finance")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

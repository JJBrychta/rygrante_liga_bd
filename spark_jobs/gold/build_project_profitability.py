"""Gold mart skeleton for `gold_project_profitability`.

TODO (Student Deliverable):
- Grain: `(project_id, year_month)` (plus optional overall project summary view).
- Metrics (see `docs/STUDENT_ASSIGNMENT.md` Section 4.2.C & `docs/SANITY_CHECKS.md`):
  - `invoiced_revenue_eur`, `credit_notes_eur`, `net_revenue_eur`
  - `billable_hours`
  - `labor_cost_eur`: join `silver_timesheets` to `silver_employees_scd2` using the
    employee's point-in-time `hourly_rate_eur` valid on `work_date`
  - `gross_margin_eur`, `gross_margin_pct` (safe division -> NULL when net_revenue_eur = 0)
  - `budget_utilization_pct`
- Write idempotently to `gold/gold_project_profitability` (Delta).
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
    """TODO: Build `gold/gold_project_profitability`."""
    raise NotImplementedError("TODO: Implement gold_project_profitability.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_project_profitability.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("gold-project-profitability")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

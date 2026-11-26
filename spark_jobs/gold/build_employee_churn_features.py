"""Gold point-in-time ML feature table skeleton for `gold_employee_churn_features`.

TODO (Student Deliverable):
- Grain: `emp_id` (leak-free snapshot cut at day 17 / Wave 1 so features can be trained/validated
  on the 200-day student dataset and evaluated out-of-time on the 70-day holdout).
- Features (see `docs/STUDENT_ASSIGNMENT.md` Section 4.2.C & `docs/SANITY_CHECKS.md`):
  - `compa_ratio` (employee hourly rate / `(department, seniority_level)` peer median)
  - `tenure_years`, `months_since_promotion`, `work_mode`, `distance_km`, `manager_id`
  - Early average daily hours & weekend work counts from `silver_timesheets`
  - Wave 1 `enps_score` & `Q02`–`Q10` numeric scores
  - Target `churned_within_quarter` (`0`/`1`, churn label in the prediction horizon)
- Write idempotently to `gold/gold_employee_churn_features` (Delta).
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
    """TODO: Build `gold/gold_employee_churn_features`."""
    raise NotImplementedError("TODO: Implement gold_employee_churn_features.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_employee_churn_features.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("gold-employee-churn-features")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

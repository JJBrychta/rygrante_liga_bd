"""Silver transformation & quarantine skeleton for `silver_timesheets`.

TODO (Student Deliverable):
1. Deduplicate exact and near-duplicates by `ts_id` using a deterministic
   `Window.partitionBy("ts_id").orderBy(...)` rule (document in `docs/architecture.md`).
2. Route invalid rows to `silver_quarantine/timesheets` with `dq_reasons: array<string>`
   and `_batch_date`:
   - `project_id IS NULL`
   - `hours <= 0` or `hours > 24`
   - Post-termination entries (`work_date >= employee.termination_date`)
3. Write valid rows idempotently to `silver/silver_timesheets`.
4. Emit DQ check results to `dq/dq_audit_results` (e.g., quarantine ratio WARN,
   zombie timesheet count WARN, non-null PK ERROR).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.2, 3.5, 4.2.B.2) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Build `silver/silver_timesheets` and `silver_quarantine/timesheets`."""
    raise NotImplementedError("TODO: Implement Silver transformation for timesheets.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform bronze/timesheets into silver_timesheets."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-timesheets")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

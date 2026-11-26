"""Silver transformation & quarantine skeleton for `silver_invoices` and `silver_expenses`.

TODO (Student Deliverable):
1. `silver_invoices`:
   - Parse both ISO (`YYYY-MM-DD`) and European (`DD.MM.YYYY`) date strings in
     `issued_at` and `paid_at` into canonical `DATE` columns (`issued_date`, `paid_date`).
   - Quarantine orphan `project_id` references (`PRJ_ORPHAN_*`) via `left_anti`
     against `silver_projects` into `silver_quarantine/invoices`.
   - Retain negative amounts as valid credit notes (`is_credit_note: boolean`,
     `net_amount_eur`) and retain unpaid invoices (`paid_date IS NULL`).
   - Join with `silver_fx_rates_daily` to compute `amount_eur` (EUR rate = 1.0).
2. `silver_expenses`:
   - Deduplicate exact & near-duplicates by `exp_id` deterministically.
   - Join with `silver_fx_rates_daily` to compute `amount_eur`.
3. Enforce hard DQ check: zero `NULL` EUR amounts after FX conversion.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 3.5, 4.2.B.4-5) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Build `silver/silver_invoices` and `silver/silver_expenses`."""
    raise NotImplementedError("TODO: Implement Silver invoices and expenses transformations.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_invoices and silver_expenses.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-invoices-expenses")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

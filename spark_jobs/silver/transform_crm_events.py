"""Silver skew-optimized transformation skeleton for `silver_crm_events`.

TODO (Student Deliverable):
1. Parse `timestamp` into UTC `TIMESTAMP` and extract structured attributes from
   the polymorphic `payload` JSON across all 5 `event_type` variants (`page_view`,
   `support_ticket`, `contract_renewal`, `api_call`, `meeting_logged`).
2. Enrich events by joining with `silver_clients` using an explicit skew-handling
   strategy (Broadcast Join, AQE skew join, or Key Salting) to handle `VIP_CORP_001`
   (~45% of all events).
3. Capture baseline vs. optimized Spark UI metrics/screenshots from the Spark
   History Server (`http://localhost:18080`) for `docs/architecture.md`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 4.2.B.6) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Build `silver/silver_crm_events` with documented skew mitigation."""
    raise NotImplementedError(
        "TODO: Implement Silver CRM events transformation with skew mitigation."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_crm_events.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-crm-events")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

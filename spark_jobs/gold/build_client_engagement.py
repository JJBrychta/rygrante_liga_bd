"""Gold mart skeleton for `gold_client_engagement`.

TODO (Student Deliverable):
- Grain: `(client_id, client_tier, event_type, event_date)`.
- Metrics (see `docs/STUDENT_ASSIGNMENT.md` Section 4.2.C):
  - `event_count`
  - `avg_api_latency_ms`
  - `critical_ticket_count`
  - `pipeline_arr_delta_eur`
- Write idempotently to `gold/gold_client_engagement` (Delta).
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
    """TODO: Build `gold/gold_client_engagement`."""
    raise NotImplementedError("TODO: Implement gold_client_engagement.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_client_engagement.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("gold-client-engagement")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

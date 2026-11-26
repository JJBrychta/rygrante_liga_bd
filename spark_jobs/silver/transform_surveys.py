"""Silver transformation & quarantine skeleton for survey responses and exploded answers.

TODO (Student Deliverable):
1. Deduplicate survey submissions by `survey_id`.
2. Derive `enps_category` (`Promoter`: 9-10, `Passive`: 7-8, `Detractor`: 0-6) and
   write response-level records to `silver/silver_survey_responses`.
3. `explode()` the 10-element `answers` array and join with `bronze/survey_questions`.
4. Quarantine answers violating `[scale_min, scale_max]` bounds (e.g., `-1`, `11`)
   to `silver_quarantine/survey_answers` with `dq_reasons`.
5. Flag blank/NULL `written_answer` cleanly with `has_written_comment: boolean`
   and write valid exploded answers to `silver/silver_survey_answers`.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.3, 3.5, 4.2.B.7) and `docs/SANITY_CHECKS.md`.
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
    """TODO: Build `silver/silver_survey_responses` and `silver/silver_survey_answers`."""
    raise NotImplementedError("TODO: Implement Silver survey transformation.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver_survey_responses and silver_survey_answers."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-surveys")
    try:
        run(spark, args.batch_date, args.lakehouse_root, args.run_id)
    finally:
        spark.stop()

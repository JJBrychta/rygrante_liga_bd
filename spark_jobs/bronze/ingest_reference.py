"""Bronze ingestion skeleton for `projects` and `survey_questions` (full-load reference datasets).

TODO (Student Deliverable):
1. Ingest `<landing_root>/projects/projects.csv` (header=True, explicit schema:
   `project_id`, `client_id`, `budget_eur`, `status`).
2. Ingest `<landing_root>/survey_questions/survey_questions.json` (multiline JSON array
   of 10 question definitions: `question_id`, `dimension`, `scale_min`, `scale_max`,
   `prompt_text`).
3. Append standardized Bronze metadata (`_batch_date`, `_ingested_at`, `_source_file`).
4. Write idempotently to `<lakehouse_root>/bronze/projects` and
   `<lakehouse_root>/bronze/survey_questions` (Delta Lake).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from spark_jobs.common.spark_session import get_spark_session


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """TODO: Implement Bronze ingestion for `projects` and `survey_questions`."""
    raise NotImplementedError(
        "TODO: Implement Bronze ingestion for projects & survey_questions (see ingest_clients.py)."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

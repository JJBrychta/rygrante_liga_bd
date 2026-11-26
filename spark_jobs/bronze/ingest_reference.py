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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", StringType(), nullable=True),  # raw; cast to double in Silver
        StructField("status", StringType(), nullable=True),
    ]
)

SURVEY_QUESTIONS_RAW_SCHEMA = StructType(
    [
        StructField("question_id", StringType(), nullable=True),
        StructField("dimension", StringType(), nullable=True),
        StructField("scale_min", IntegerType(), nullable=True),
        StructField("scale_max", IntegerType(), nullable=True),
        StructField("prompt_text", StringType(), nullable=True),
    ]
)


def _add_bronze_metadata(df: DataFrame, batch_date: str) -> DataFrame:
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Ingest `projects` and `survey_questions` into Bronze Delta (full load)."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    projects_path = join_path(resolved_landing, "projects", "projects.csv")
    questions_path = join_path(resolved_landing, "survey_questions", "survey_questions.json")

    projects_raw = (
        spark.read.option("header", "true").schema(PROJECTS_RAW_SCHEMA).csv(projects_path)
    )

    questions_raw = (
        spark.read.option("multiLine", "true")
        .schema(SURVEY_QUESTIONS_RAW_SCHEMA)
        .json(questions_path)
    )

    projects_bronze = _add_bronze_metadata(projects_raw, batch_date)
    questions_bronze = _add_bronze_metadata(questions_raw, batch_date)

    projects_target = join_path(resolved_lakehouse, "bronze", "projects")
    questions_target = join_path(resolved_lakehouse, "bronze", "survey_questions")

    for df, target in (
        (projects_bronze, projects_target),
        (questions_bronze, questions_target),
    ):
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target))

    return projects_bronze, questions_bronze


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
        projects_df, questions_df = run(
            spark, args.batch_date, args.landing_root, args.lakehouse_root
        )
        print(
            f"Ingested {projects_df.count()} rows into bronze/projects and "
            f"{questions_df.count()} rows into bronze/survey_questions "
            f"for batch_date={args.batch_date}"
        )
    finally:
        spark.stop()

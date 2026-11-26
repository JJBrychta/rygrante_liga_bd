"""Unit tests for Bronze ingestion of `projects` and `survey_questions`."""

from __future__ import annotations

import json
from pathlib import Path

from spark_jobs.bronze.ingest_reference import run
from spark_jobs.common.spark_session import join_path

METADATA_COLUMNS = {"_batch_date", "_ingested_at", "_source_file"}


def _write_landing(landing_root: Path) -> None:
    projects_dir = landing_root / "projects"
    projects_dir.mkdir(parents=True)
    (projects_dir / "projects.csv").write_text(
        "project_id,client_id,budget_eur,status\n"
        "PRJ_0001,CLI_0009,12200.0,Active\n"
        "PRJ_0002,CLI_0064,105900.0,Completed\n",
        encoding="utf-8",
    )

    questions_dir = landing_root / "survey_questions"
    questions_dir.mkdir(parents=True)
    questions = [
        {
            "question_id": "Q01",
            "dimension": "enps",
            "scale_min": 0,
            "scale_max": 10,
            "prompt_text": "Would you recommend us?",
        },
        {
            "question_id": "Q02",
            "dimension": "work_life_balance",
            "scale_min": 1,
            "scale_max": 5,
            "prompt_text": "Balance?",
        },
    ]
    # indent=2 -> multi-line JSON array, same shape as the real file
    (questions_dir / "survey_questions.json").write_text(
        json.dumps(questions, indent=2), encoding="utf-8"
    )


def test_ingest_reference_metadata_types_and_idempotency(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_landing(landing_root)

    for _ in range(2):  # run twice: overwrite must not duplicate rows
        run(spark, "2026-01-01", str(landing_root), str(lakehouse_root))

    projects = spark.read.format("delta").load(join_path(str(lakehouse_root), "bronze", "projects"))
    questions = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "bronze", "survey_questions")
    )

    # 1. Idempotency: two runs of a full load still give exactly the source rows
    assert projects.count() == 2
    assert questions.count() == 2

    # 2. Standard Bronze metadata columns exist on both tables
    for df in (projects, questions):
        assert METADATA_COLUMNS.issubset(set(df.columns))

        # 3. ...and are always populated
        assert (
            df.filter("_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL").count()
            == 0
        )

    # 4. Explicit raw schema: budget kept lossless as string, scales typed as int
    assert dict(projects.dtypes)["budget_eur"] == "string"
    assert dict(questions.dtypes)["scale_min"] == "int"
    assert dict(questions.dtypes)["scale_max"] == "int"

    # 5. Multi-line JSON array parsed into real rows (no corrupt-record fallback)
    assert "_corrupt_record" not in questions.columns
    q01 = questions.filter("question_id = 'Q01'").first()
    assert q01["dimension"] == "enps"
    assert q01["scale_max"] == 10

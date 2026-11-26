"""Unit tests for Bronze `crm_events` & `employee_surveys` ingestion."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable

from spark_jobs.bronze.ingest_crm_surveys import run
from spark_jobs.common.spark_session import join_path

CRM_EVENTS = [
    {
        "event_id": "E1",
        "client_id": "VIP_CORP_001",
        "event_type": "api_call",
        "timestamp": "2026-01-01T08:00:00Z",
        "payload": {"endpoint": "/v1/orders", "latency_ms": 120},
    },
    {
        "event_id": "E2",
        "client_id": "CLI_0002",
        "event_type": "support_ticket",
        "timestamp": "2026-01-01T09:15:00Z",
        "payload": {"priority": "critical", "tags": ["billing", "sla"]},
    },
    {
        "event_id": "E3",
        "client_id": "VIP_CORP_001",
        "event_type": "page_view",
        "timestamp": "2026-01-01T10:30:00Z",
        "payload": {"url": "/pricing"},
    },
]

SURVEYS = [
    {
        "survey_id": "SRV-1",
        "emp_id": "EMP_0001",
        "submitted_at": "2026-01-09T07:00:00Z",
        "enps_score": 9,
        "overall_comment": "Great team",
        "answers": [
            {"question_id": "Q01", "dimension": "enps", "numeric_score": 9, "written_answer": "ok"},
            {
                "question_id": "Q02",
                "dimension": "work_life_balance",
                "numeric_score": 4,
                "written_answer": None,
            },
        ],
    },
    # Exact duplicate + out-of-range score: Bronze must keep both untouched (Silver handles them).
    {
        "survey_id": "SRV-1",
        "emp_id": "EMP_0001",
        "submitted_at": "2026-01-09T07:00:00Z",
        "enps_score": 9,
        "overall_comment": "Great team",
        "answers": [],
    },
    {
        "survey_id": "SRV-2",
        "emp_id": "EMP_0002",
        "submitted_at": "2026-01-09T08:00:00Z",
        "enps_score": 11,
        "overall_comment": "",
        "answers": [],
    },
]


def _write_jsonl(landing_root: Path, dataset: str, batch_date: str, rows: list[dict]) -> None:
    day_dir = landing_root / dataset / f"date={batch_date}"
    day_dir.mkdir(parents=True, exist_ok=True)
    content = "\n".join(json.dumps(r) for r in rows) + "\n"
    (day_dir / "part-000.jsonl").write_text(content, encoding="utf-8")


def _load(spark, lakehouse_root: Path, table: str):
    return spark.read.format("delta").load(join_path(str(lakehouse_root), "bronze", table))


def test_crm_events_raw_payload_metadata_and_idempotency(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_jsonl(landing_root, "crm_events", "2026-01-01", CRM_EVENTS)
    _write_jsonl(landing_root, "crm_events", "2026-01-02", CRM_EVENTS[:1])

    # Same day twice -> no duplicates; a second day must not wipe the first one.
    for batch_date in ("2026-01-01", "2026-01-01", "2026-01-02"):
        run(spark, batch_date, str(landing_root), str(lakehouse_root))

    crm = _load(spark, lakehouse_root, "crm_events")

    counts = {r["_batch_date"]: r["count"] for r in crm.groupBy("_batch_date").count().collect()}
    assert counts == {date(2026, 1, 1): 3, date(2026, 1, 2): 1}

    assert {"event_id", "client_id", "event_type", "timestamp", "payload_raw"}.issubset(crm.columns)
    assert all(f.dataType.simpleString() == "string" for f in crm.schema.fields[:5])
    assert crm.filter("_ingested_at IS NULL OR _source_file IS NULL").count() == 0

    # Polymorphic payloads are preserved losslessly per event (no merged struct / NULL padding).
    day1 = crm.filter("_batch_date = DATE '2026-01-01'").collect()
    payloads = {r["event_id"]: json.loads(r["payload_raw"]) for r in day1}
    assert payloads == {e["event_id"]: e["payload"] for e in CRM_EVENTS}


def test_employee_surveys_lossless_ingest(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_jsonl(landing_root, "crm_events", "2026-01-09", CRM_EVENTS)
    _write_jsonl(landing_root, "employee_surveys", "2026-01-09", SURVEYS)

    run(spark, "2026-01-09", str(landing_root), str(lakehouse_root))
    run(spark, "2026-01-09", str(landing_root), str(lakehouse_root))

    surveys = _load(spark, lakehouse_root, "employee_surveys")

    # Duplicates and invalid scores are kept as-is in Bronze (dedup/validation is Silver's job).
    assert surveys.count() == 3
    assert sorted(r["enps_score"] for r in surveys.collect()) == ["11", "9", "9"]

    first = surveys.filter("survey_id = 'SRV-1' AND answers_raw != '[]'").first()
    assert json.loads(first["answers_raw"]) == SURVEYS[0]["answers"]


def test_days_without_survey_file_are_skipped(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_jsonl(landing_root, "crm_events", "2026-01-01", CRM_EVENTS)

    run(spark, "2026-01-01", str(landing_root), str(lakehouse_root))

    assert _load(spark, lakehouse_root, "crm_events").count() == 3
    surveys_path = join_path(str(lakehouse_root), "bronze", "employee_surveys")
    assert not DeltaTable.isDeltaTable(spark, surveys_path)

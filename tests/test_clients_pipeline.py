"""Unit tests for the reference `clients` Bronze -> Silver pipeline and DQ audit."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from dq.framework import (
    DQ_AUDIT_SCHEMA,
    DQCheckResult,
    DQGateFailedError,
    enforce_dq_gate,
)
from spark_jobs.bronze.ingest_clients import ingest_clients
from spark_jobs.common.spark_session import join_path
from spark_jobs.silver.transform_clients import transform_clients


def _write_clients_csv(landing_root: Path, rows: list[str]) -> None:
    clients_dir = landing_root / "clients"
    clients_dir.mkdir(parents=True, exist_ok=True)
    content = "\n".join(["client_id,client_name,tier", *rows]) + "\n"
    (clients_dir / "clients.csv").write_text(content, encoding="utf-8")


def test_ingest_clients_bronze_metadata_and_idempotency(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_clients_csv(
        landing_root,
        [
            "VIP_CORP_001,VIP Corporation,Enterprise",
            "CLI_0002,Acme Industries,Gold",
            "CLI_0003,Northstar Analytics,Silver",
        ],
    )

    ingest_clients(
        spark,
        batch_date="2026-01-01",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )
    ingest_clients(
        spark,
        batch_date="2026-01-01",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )

    bronze_path = join_path(str(lakehouse_root), "bronze", "clients")
    bronze_df = spark.read.format("delta").load(bronze_path)

    assert bronze_df.count() == 3
    assert {"_batch_date", "_ingested_at", "_source_file"}.issubset(set(bronze_df.columns))
    assert (
        bronze_df.filter(
            "_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL"
        ).count()
        == 0
    )
    batch_dates = {
        row["_batch_date"] for row in bronze_df.select("_batch_date").distinct().collect()
    }
    assert batch_dates == {date(2026, 1, 1)}


def test_transform_clients_silver_quarantine_dq_audit_and_idempotency(
    spark, tmp_path: Path
) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    _write_clients_csv(
        landing_root,
        [
            " VIP_CORP_001 , VIP Corporation , Enterprise ",
            "CLI_0002,Acme Industries,Gold",
            ",Missing ID Corp,Silver",
            "CLI_BAD_TIER,Bad Tier Co,Platinum",
        ],
    )

    ingest_clients(
        spark,
        batch_date="2026-01-01",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )

    # Run Silver twice for the same batch_date to verify idempotency.
    transform_clients(
        spark,
        batch_date="2026-01-01",
        lakehouse_root=str(lakehouse_root),
        run_id="test-run-1",
    )
    transform_clients(
        spark,
        batch_date="2026-01-01",
        lakehouse_root=str(lakehouse_root),
        run_id="test-run-2",
    )

    silver_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver", "silver_clients")
    )
    quarantine_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver_quarantine", "clients")
    )
    audit_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "dq", "dq_audit_results")
    )

    # Valid rows: trimmed & cleansed.
    silver_rows = {
        row["client_id"]: (row["client_name"], row["tier"]) for row in silver_df.collect()
    }
    assert silver_rows == {
        "VIP_CORP_001": ("VIP Corporation", "Enterprise"),
        "CLI_0002": ("Acme Industries", "Gold"),
    }

    # Quarantined rows: exactly 2 rows with expected dq_reasons, no duplicates after re-run.
    assert quarantine_df.count() == 2
    reasons = [tuple(row["dq_reasons"]) for row in quarantine_df.collect()]
    assert ("missing_client_id",) in reasons
    assert ("invalid_client_tier",) in reasons

    # DQ audit results: schema matches DQ_AUDIT_SCHEMA and contains 1 PASS record after re-run.
    assert audit_df.schema.names == DQ_AUDIT_SCHEMA.names
    assert [f.dataType for f in audit_df.schema.fields] == [
        f.dataType for f in DQ_AUDIT_SCHEMA.fields
    ]
    assert audit_df.count() == 1
    audit_row = audit_df.first()
    assert audit_row["run_id"] == "test-run-2"
    assert audit_row["check_name"] == "not_null_client_id"
    assert audit_row["status"] == "PASS"
    assert audit_row["failed_count"] == 0
    assert audit_row["total_count"] == 2

    # Re-running the same batch_date with clean data must clear stale quarantined rows.
    _write_clients_csv(
        landing_root,
        [
            "VIP_CORP_001,VIP Corporation,Enterprise",
            "CLI_0002,Acme Industries,Gold",
        ],
    )
    ingest_clients(
        spark,
        batch_date="2026-01-01",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )
    transform_clients(
        spark,
        batch_date="2026-01-01",
        lakehouse_root=str(lakehouse_root),
        run_id="test-run-3",
    )
    quarantine_after_fix = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver_quarantine", "clients")
    )
    assert quarantine_after_fix.count() == 0


def test_enforce_dq_gate_behavior() -> None:
    now = datetime.now(timezone.utc)
    warn_only = DQCheckResult(
        run_id="r1",
        batch_date=date(2026, 1, 1),
        layer="silver",
        table_name="silver_clients",
        check_name="sample_warn",
        severity="WARN",
        status="WARN",
        failed_count=1,
        total_count=10,
        checked_at=now,
    )
    enforce_dq_gate([warn_only])

    hard_fail = DQCheckResult(
        run_id="r1",
        batch_date=date(2026, 1, 1),
        layer="silver",
        table_name="silver_clients",
        check_name="not_null_client_id",
        severity="ERROR",
        status="FAIL",
        failed_count=1,
        total_count=10,
        checked_at=now,
    )
    with pytest.raises(DQGateFailedError, match="not_null_client_id"):
        enforce_dq_gate([warn_only, hard_fail])

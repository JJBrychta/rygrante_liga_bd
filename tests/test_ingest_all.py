"""Unit tests for the Bronze ingestion runner used by `make ingest*`."""

from __future__ import annotations

import pytest

from spark_jobs.bronze import ingest_all


def test_date_range_is_inclusive_and_crosses_months() -> None:
    assert ingest_all.date_range("2026-01-30", "2026-02-02") == [
        "2026-01-30",
        "2026-01-31",
        "2026-02-01",
        "2026-02-02",
    ]
    assert ingest_all.date_range("2026-01-05", "2026-01-05") == ["2026-01-05"]


def test_date_range_rejects_reversed_dates() -> None:
    with pytest.raises(ValueError, match="before"):
        ingest_all.date_range("2026-01-14", "2026-01-01")


def test_parse_jobs_keeps_pipeline_order_and_rejects_unknown() -> None:
    assert ingest_all.parse_jobs("all") == list(ingest_all.JOBS)
    assert ingest_all.parse_jobs("finance, clients") == ["clients", "finance"]
    with pytest.raises(ValueError, match="Unknown"):
        ingest_all.parse_jobs("finance,payroll")


def test_ingest_runs_full_loads_once_and_daily_jobs_every_day(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def recorder(name: str):
        return lambda spark, batch_date, landing_root, lakehouse_root: calls.append(
            (name, batch_date)
        )

    fake_jobs = {
        "clients": (False, recorder("clients")),
        "finance": (True, recorder("finance")),
    }
    monkeypatch.setattr(ingest_all, "JOBS", fake_jobs)

    ingest_all.ingest(None, ["clients", "finance"], ["2026-01-01", "2026-01-02", "2026-01-03"])

    assert calls == [
        ("clients", "2026-01-03"),  # full load: once, stamped with the last date
        ("finance", "2026-01-01"),
        ("finance", "2026-01-02"),
        ("finance", "2026-01-03"),
    ]

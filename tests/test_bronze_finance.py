"""Unit tests for Bronze Finance ingestion (`fx_rates`, `invoices`, `expenses`)."""

from __future__ import annotations

import json
from pathlib import Path

from spark_jobs.bronze.ingest_finance import run
from spark_jobs.common.spark_session import join_path

BRONZE_METADATA = {"_batch_date", "_ingested_at", "_source_file"}


def _write(landing_root: Path, dataset: str, batch_date: str, file_name: str, text: str) -> None:
    partition = landing_root / dataset / f"date={batch_date}"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / file_name).write_text(text, encoding="utf-8")


def _write_finance_day(landing_root: Path, batch_date: str, with_fx: bool = True) -> None:
    """Write one small fake day of fx_rates (optional), invoices and expenses."""
    if with_fx:
        fx_rows = [
            {"rate_date": batch_date, "currency_from": "EUR", "currency_to": "USD", "rate": 1.08},
            {"rate_date": batch_date, "currency_from": "USD", "currency_to": "EUR", "rate": 0.92},
        ]
        _write(landing_root, "fx_rates", batch_date, "part-000.json", json.dumps(fx_rows, indent=2))

    invoices = [
        {
            "inv_id": "INV-1",
            "client_id": "CLI_1",
            "project_id": "PRJ_1",
            "amount": 100.5,
            "currency": "EUR",
            "issued_at": batch_date,
            "paid_at": batch_date,
        },
        # European date + credit note + unpaid: all must survive Bronze untouched
        {
            "inv_id": "INV-2",
            "client_id": "CLI_2",
            "project_id": "PRJ_ORPHAN_9",
            "amount": -40.0,
            "currency": "USD",
            "issued_at": "05.01.2026",
            "paid_at": None,
        },
    ]
    _write(
        landing_root,
        "invoices",
        batch_date,
        "part-000.jsonl",
        "\n".join(json.dumps(row) for row in invoices) + "\n",
    )

    # The duplicate row is intentional: Bronze keeps raw duplicates, Silver removes them.
    _write(
        landing_root,
        "expenses",
        batch_date,
        "part-000.csv",
        "exp_id,department,category,amount,currency,expense_date\n"
        f"EXP-1,Engineering,Cloud,10.00,EUR,{batch_date}\n"
        f"EXP-1,Engineering,Cloud,10.00,EUR,{batch_date}\n",
    )


def _bronze(spark, lakehouse_root: Path, dataset: str):
    return spark.read.format("delta").load(join_path(str(lakehouse_root), "bronze", dataset))


def test_finance_bronze_metadata_raw_values_and_idempotency(spark, tmp_path: Path) -> None:
    landing, lakehouse = tmp_path / "landing", tmp_path / "lakehouse"
    _write_finance_day(landing, "2026-01-05")
    _write_finance_day(landing, "2026-01-06")

    for _ in range(2):  # every day is ingested twice: re-runs must not duplicate rows
        for day in ("2026-01-05", "2026-01-06"):
            run(spark, day, str(landing), str(lakehouse))

    for dataset in ("fx_rates", "invoices", "expenses"):
        df = _bronze(spark, lakehouse, dataset)
        assert df.count() == 4, dataset  # 2 rows per day x 2 days, not 8
        assert BRONZE_METADATA.issubset(df.columns), dataset
        assert df.filter("_batch_date IS NULL OR _source_file IS NULL").count() == 0, dataset

    invoices = _bronze(spark, lakehouse, "invoices").filter("_batch_date = '2026-01-05'")
    raw = {row["inv_id"]: row for row in invoices.collect()}
    assert raw["INV-2"]["issued_at"] == "05.01.2026"  # raw date string preserved
    assert raw["INV-2"]["amount"] == "-40.0"  # credit note kept as raw text
    assert raw["INV-2"]["paid_at"] is None  # open invoice kept


def test_finance_bronze_skips_missing_weekend_fx_rates(spark, tmp_path: Path) -> None:
    landing, lakehouse = tmp_path / "landing", tmp_path / "lakehouse"
    _write_finance_day(landing, "2026-01-02")  # Friday
    _write_finance_day(landing, "2026-01-03", with_fx=False)  # Saturday: no fx_rates file

    friday = run(spark, "2026-01-02", str(landing), str(lakehouse))
    saturday = run(spark, "2026-01-03", str(landing), str(lakehouse))  # must not raise

    # run() reports rows ingested per dataset; None = no landing file that day
    assert friday == {"fx_rates": 2, "invoices": 2, "expenses": 2}
    assert saturday == {"fx_rates": None, "invoices": 2, "expenses": 2}

    fx = _bronze(spark, lakehouse, "fx_rates")
    assert fx.count() == 2  # only Friday's rates
    assert fx.filter("_batch_date = '2026-01-03'").count() == 0
    assert _bronze(spark, lakehouse, "invoices").count() == 4  # both days loaded


def test_finance_bronze_tolerates_schema_drift(spark, tmp_path: Path) -> None:
    landing, lakehouse = tmp_path / "landing", tmp_path / "lakehouse"
    _write_finance_day(landing, "2026-01-05")

    # Day 2: invoices gain a new field and contain one malformed line;
    # expenses arrive with their columns in a different order.
    new_field = {
        "inv_id": "INV-3",
        "client_id": "CLI_3",
        "project_id": "PRJ_3",
        "amount": 7.25,
        "currency": "CZK",
        "issued_at": "2026-01-06",
        "paid_at": None,
        "vat_rate": 0.21,
    }
    _write(
        landing,
        "invoices",
        "2026-01-06",
        "part-000.jsonl",
        json.dumps(new_field) + '\n{"inv_id":"INV-4","amount":1\n',
    )
    _write(
        landing,
        "expenses",
        "2026-01-06",
        "part-000.csv",
        "currency,amount,exp_id,department,category,expense_date\n"
        "USD,55.10,EXP-9,Sales,Travel,2026-01-06\n",
    )

    for day in ("2026-01-05", "2026-01-06", "2026-01-06"):  # day 2 twice: still idempotent
        run(spark, day, str(landing), str(lakehouse))

    invoices = _bronze(spark, lakehouse, "invoices")
    assert invoices.count() == 4  # 2 + (1 good + 1 malformed)
    assert "vat_rate" in invoices.columns  # new field kept as a new column
    assert invoices.filter("inv_id = 'INV-3'").first()["vat_rate"] == "0.21"
    assert invoices.filter("_batch_date = '2026-01-05'").filter("vat_rate IS NOT NULL").count() == 0
    malformed = invoices.filter("_corrupt_record IS NOT NULL").collect()
    assert len(malformed) == 1 and malformed[0]["_corrupt_record"].startswith('{"inv_id":"INV-4"')

    expenses = _bronze(spark, lakehouse, "expenses").filter("_batch_date = '2026-01-06'").first()
    assert expenses["exp_id"] == "EXP-9"  # matched by header name, not position
    assert expenses["amount"] == "55.10"
    assert expenses["currency"] == "USD"


def test_finance_bronze_fills_missing_expected_columns_with_null(spark, tmp_path: Path) -> None:
    landing, lakehouse = tmp_path / "landing", tmp_path / "lakehouse"
    # First-ever write already lacks `category`: the table must still get the column.
    _write(
        landing,
        "expenses",
        "2026-01-05",
        "part-000.csv",
        "exp_id,department,amount,currency,expense_date\nEXP-1,Engineering,10.00,EUR,2026-01-05\n",
    )
    run(spark, "2026-01-05", str(landing), str(lakehouse))

    expenses = _bronze(spark, lakehouse, "expenses")
    assert "category" in expenses.columns
    row = expenses.first()
    assert row["category"] is None
    assert row["exp_id"] == "EXP-1"
    assert dict(expenses.dtypes)["category"] == "string"

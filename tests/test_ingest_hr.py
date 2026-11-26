"""Unit tests for Bronze ingestion of daily employees and timesheets."""

from __future__ import annotations

import csv
from collections import Counter
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable

from spark_jobs.bronze.ingest_hr import run
from spark_jobs.common.spark_session import join_path

EMPLOYEE_COLUMNS = [
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "hourly_rate_eur",
    "hire_date",
    "last_promotion_date",
    "work_mode",
    "distance_km",
    "manager_id",
    "is_active",
    "termination_date",
    "churn_type",
    "snapshot_date",
]
TIMESHEET_COLUMNS = ["ts_id", "emp_id", "project_id", "work_date", "hours"]
TIMESHEET_SCHEMA = "ts_id string, emp_id string, project_id string, work_date date, hours double"
METADATA_COLUMNS = {"_batch_date", "_ingested_at", "_source_file"}


def _employee(emp_id: str, batch_date: str) -> tuple:
    # Keep whitespace, decimal formatting and an invalid date unchanged in Bronze.
    return (
        emp_id,
        " Alice Smith ",
        "Engineering",
        "Developer",
        "Senior",
        "063.690",
        "not-a-date",
        "2025-01-01",
        "Hybrid",
        "012.60",
        None,
        "true",
        None,
        None,
        batch_date,
    )


def _write_landing(spark, landing_root: Path, batch_date: str, employees, timesheets) -> None:
    employees_dir = landing_root / "employees" / f"date={batch_date}"
    employees_dir.mkdir(parents=True, exist_ok=True)
    with (employees_dir / "part-000.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(EMPLOYEE_COLUMNS)
        writer.writerows(employees)

    timesheets_path = landing_root / "timesheets" / f"date={batch_date}" / "part-000.parquet"
    spark.createDataFrame(timesheets, TIMESHEET_SCHEMA).coalesce(1).write.mode("overwrite").parquet(
        str(timesheets_path)
    )


def test_ingest_hr_raw_values_metadata_types_and_idempotency(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"
    batch_date = "2026-01-01"
    employees = [_employee("EMP_0001", batch_date)] * 2
    timesheets = [
        ("TS_001", "EMP_0001", "PRJ_001", date(2026, 1, 1), 8.0),
        ("TS_001", "EMP_0001", "PRJ_001", date(2026, 1, 1), 8.0),
        ("TS_001", "EMP_0001", "PRJ_001", date(2026, 1, 1), 8.5),
        ("TS_002", "EMP_0001", None, date(2026, 1, 1), 0.0),
        ("TS_003", "EMP_0001", "PRJ_001", date(2026, 1, 1), -1.0),
        ("TS_004", "EMP_0001", "PRJ_001", date(2026, 1, 1), 25.0),
    ]
    _write_landing(spark, landing_root, batch_date, employees, timesheets)

    for _ in range(2):
        run(spark, batch_date, str(landing_root), str(lakehouse_root))

    for table, columns, expected in [
        ("employees", EMPLOYEE_COLUMNS, employees),
        ("timesheets", TIMESHEET_COLUMNS, timesheets),
    ]:
        path = join_path(str(lakehouse_root), "bronze", table)
        df = spark.read.format("delta").load(path)

        # Compare all raw values including duplicate occurrences and anomalies.
        assert Counter(tuple(row) for row in df.select(columns).collect()) == Counter(expected)
        assert set(df.columns) == set(columns) | METADATA_COLUMNS
        types = dict(df.dtypes)
        assert types["_batch_date"] == "date"
        assert types["_ingested_at"] == "timestamp"
        assert types["_source_file"] == "string"
        for row in df.select(*sorted(METADATA_COLUMNS)).collect():
            assert row["_batch_date"] == date(2026, 1, 1)
            assert row["_ingested_at"] is not None
            assert f"/{table}/date={batch_date}/part-000." in row["_source_file"]

        assert DeltaTable.forPath(spark, path).detail().first()["partitionColumns"] == [
            "_batch_date"
        ]
        if table == "employees":
            assert all(types[column] == "string" for column in EMPLOYEE_COLUMNS)
        else:
            assert types["work_date"] == "date"
            assert types["hours"] == "double"


def test_ingest_hr_replaces_changed_batch_and_preserves_other_dates(spark, tmp_path: Path) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"

    for day in (1, 2):
        batch_date = f"2026-01-{day:02d}"
        _write_landing(
            spark,
            landing_root,
            batch_date,
            [_employee(f"EMP_000{day}", batch_date)],
            [(f"TS_00{day}", f"EMP_000{day}", "PRJ_001", date(2026, 1, day), 8.0)],
        )
        run(spark, batch_date, str(landing_root), str(lakehouse_root))

    # Snapshot day 2 including metadata: rerunning day 1 must leave it untouched.
    unchanged = {}
    for table in ("employees", "timesheets"):
        df = spark.read.format("delta").load(join_path(str(lakehouse_root), "bronze", table))
        unchanged[table] = df.filter("_batch_date = DATE '2026-01-02'").collect()

    _write_landing(
        spark,
        landing_root,
        "2026-01-01",
        [_employee("EMP_NEW", "2026-01-01")],
        [("TS_NEW", "EMP_NEW", "PRJ_002", date(2026, 1, 1), 4.0)],
    )
    run(spark, "2026-01-01", str(landing_root), str(lakehouse_root))

    for table, id_column, new_id in [
        ("employees", "emp_id", "EMP_NEW"),
        ("timesheets", "ts_id", "TS_NEW"),
    ]:
        df = spark.read.format("delta").load(join_path(str(lakehouse_root), "bronze", table))
        assert df.count() == 2
        replaced = df.filter("_batch_date = DATE '2026-01-01'").collect()
        assert len(replaced) == 1
        assert replaced[0][id_column] == new_id
        assert df.filter("_batch_date = DATE '2026-01-02'").collect() == unchanged[table]

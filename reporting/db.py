"""DuckDB helper utilities for reading Delta Lake tables in the Reporting Layer."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd


def get_lakehouse_root() -> Path:
    """Return the configured local lakehouse directory path."""
    return Path(os.getenv("LAKEHOUSE_ROOT", "./lakehouse"))


def resolve_table_path(relative_path: str) -> Path:
    """Resolve a relative layer/table path inside the lakehouse root."""
    return get_lakehouse_root() / relative_path


def table_exists(relative_path: str) -> bool:
    """Check whether a Delta Lake table exists at the given relative path."""
    table_dir = resolve_table_path(relative_path)
    return (table_dir / "_delta_log").is_dir()


def _connect_with_delta() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the Delta extension loaded."""
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("LOAD delta;")
    except duckdb.Error:
        conn.execute("INSTALL delta; LOAD delta;")
    return conn


def _delta_scan_sql(table_dir: Path) -> str:
    """Return a properly escaped SQL `delta_scan(...)` expression."""
    escaped = table_dir.resolve().as_posix().replace("'", "''")
    return f"delta_scan('{escaped}')"


def get_table_row_count(relative_path: str) -> int | None:
    """Return the row count of a Delta table, or None if it has not been created yet."""
    if not table_exists(relative_path):
        return None
    table_dir = resolve_table_path(relative_path)
    with _connect_with_delta() as conn:
        result = conn.execute(f"SELECT COUNT(*) FROM {_delta_scan_sql(table_dir)}").fetchone()
    return int(result[0]) if result is not None else 0


def read_delta_table(relative_path: str, limit: int | None = None) -> pd.DataFrame | None:
    """Read a Delta table into a pandas DataFrame, or return None if missing."""
    if not table_exists(relative_path):
        return None
    table_dir = resolve_table_path(relative_path)
    query = f"SELECT * FROM {_delta_scan_sql(table_dir)}"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    with _connect_with_delta() as conn:
        return conn.execute(query).df()

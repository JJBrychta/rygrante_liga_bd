"""Student Data Quality check skeletons.

TODO: Implement the full Bronze, Silver, and Gold DQ rule suite defined in
`docs/STUDENT_ASSIGNMENT.md` (Section 4.3) and `docs/SANITY_CHECKS.md`.
Each function should return a `DQCheckResult` from `dq.framework`.
"""

from __future__ import annotations

from datetime import date
from typing import Sequence

from pyspark.sql import DataFrame

from dq.framework import DQCheckResult


def check_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify uniqueness of primary/composite keys (e.g. `(emp_id, valid_from)`)."""
    raise NotImplementedError("TODO: Implement check_unique_key in dq/checks.py")


def check_non_empty_partition(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    allow_weekends_empty: bool = False,
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify mandatory partitions contain >0 rows (accounting for weekend fx_rates)."""
    raise NotImplementedError("TODO: Implement check_non_empty_partition in dq/checks.py")


def check_scd2_no_overlaps(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str = "silver_employees_scd2",
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify no overlapping [valid_from, valid_to] intervals per emp_id."""
    raise NotImplementedError("TODO: Implement check_scd2_no_overlaps in dq/checks.py")


def check_zero_null_eur(
    df: DataFrame,
    eur_column: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify zero NULL EUR amounts after FX conversion (invoices & expenses)."""
    raise NotImplementedError("TODO: Implement check_zero_null_eur in dq/checks.py")


def check_quarantine_ratio(
    valid_count: int,
    quarantined_count: int,
    *,
    max_ratio: float = 0.08,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str,
    severity: str = "WARN",
) -> DQCheckResult:
    """TODO: Emit WARN when quarantined_count / (valid_count + quarantined_count) > max_ratio."""
    raise NotImplementedError("TODO: Implement check_quarantine_ratio in dq/checks.py")

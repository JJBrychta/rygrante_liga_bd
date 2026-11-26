"""Lightweight Data Quality framework and `dq_audit_results` Delta logger."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

DQ_AUDIT_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("batch_date", DateType(), nullable=False),
        StructField("layer", StringType(), nullable=False),
        StructField("table_name", StringType(), nullable=False),
        StructField("check_name", StringType(), nullable=False),
        StructField("severity", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("failed_count", LongType(), nullable=False),
        StructField("total_count", LongType(), nullable=False),
        StructField("checked_at", TimestampType(), nullable=False),
    ]
)


class DQGateFailedError(RuntimeError):
    """Raised when one or more hard (ERROR severity) DQ checks fail."""


@dataclass(frozen=True)
class DQCheckResult:
    """Structured result emitted by a single Data Quality check."""

    run_id: str
    batch_date: date
    layer: str
    table_name: str
    check_name: str
    severity: str
    status: str
    failed_count: int
    total_count: int
    checked_at: datetime


def _parse_batch_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def check_not_null(
    df: DataFrame,
    column_name: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify that `column_name` contains no NULL values (reference sample check)."""
    parsed_date = _parse_batch_date(batch_date)
    total_count = df.count()
    failed_count = df.filter(F.col(column_name).isNull()).count()

    if failed_count == 0:
        status = "PASS"
    elif severity.upper() == "WARN":
        status = "WARN"
    else:
        status = "FAIL"

    return DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer=layer,
        table_name=table_name,
        check_name=f"not_null_{column_name}",
        severity=severity.upper(),
        status=status,
        failed_count=failed_count,
        total_count=total_count,
        checked_at=datetime.now(timezone.utc),
    )


def write_audit_results(
    spark: SparkSession,
    results: Sequence[DQCheckResult],
    audit_table_path: str,
) -> DataFrame:
    """Persist DQCheckResult records idempotently to the `dq_audit_results` Delta table."""
    if not results:
        return spark.createDataFrame([], schema=DQ_AUDIT_SCHEMA)

    rows = [
        (
            r.run_id,
            r.batch_date,
            r.layer,
            r.table_name,
            r.check_name,
            r.severity,
            r.status,
            int(r.failed_count),
            int(r.total_count),
            r.checked_at,
        )
        for r in results
    ]
    audit_df = spark.createDataFrame(rows, schema=DQ_AUDIT_SCHEMA)

    if DeltaTable.isDeltaTable(spark, audit_table_path):
        # Replace existing audit records for the same (batch_date, layer, table_name)
        # so re-running a batch remains strictly idempotent.
        unique_scopes = {(r.batch_date, r.layer, r.table_name) for r in results}
        predicates = [
            (
                f"(batch_date = DATE '{bd.isoformat()}' "
                f"AND layer = '{_escape_sql_literal(lyr)}' "
                f"AND table_name = '{_escape_sql_literal(tbl)}')"
            )
            for bd, lyr, tbl in sorted(unique_scopes)
        ]
        DeltaTable.forPath(spark, audit_table_path).delete(" OR ".join(predicates))
        audit_df.write.format("delta").mode("append").partitionBy("batch_date").save(
            audit_table_path
        )
    else:
        audit_df.write.format("delta").mode("overwrite").partitionBy("batch_date").save(
            audit_table_path
        )

    return audit_df


def enforce_dq_gate(results: Sequence[DQCheckResult]) -> None:
    """Log all check outcomes and raise DQGateFailedError if any ERROR check failed."""
    hard_failures: list[DQCheckResult] = []
    for res in results:
        msg = (
            f"[DQ {res.status}] {res.layer}.{res.table_name}::{res.check_name} "
            f"(severity={res.severity}, failed={res.failed_count}/{res.total_count}, "
            f"batch_date={res.batch_date.isoformat()})"
        )
        if res.status == "FAIL":
            logger.error(msg)
            if res.severity == "ERROR":
                hard_failures.append(res)
        elif res.status == "WARN":
            logger.warning(msg)
        else:
            logger.info(msg)

    if hard_failures:
        summary = ", ".join(
            f"{r.table_name}.{r.check_name} ({r.failed_count})" for r in hard_failures
        )
        raise DQGateFailedError(f"Hard DQ gate failed: {summary}")

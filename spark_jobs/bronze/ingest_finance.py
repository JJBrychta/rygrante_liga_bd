"""Bronze ingestion for the Finance domain (`fx_rates`, `invoices`, `expenses`).

For one `batch_date`, each dataset is read from its daily partition:
   - `<landing_root>/fx_rates/date=<batch_date>/part-000.json`  (JSON array, weekdays only)
   - `<landing_root>/invoices/date=<batch_date>/part-000.jsonl` (JSON Lines)
   - `<landing_root>/expenses/date=<batch_date>/part-000.csv`   (CSV with header)

Bronze rules (STUDENT_ASSIGNMENT.md section 4.2.A):
- Zero lossy transformations: every field is read as a string, so raw values such as
  `issued_at` in both `YYYY-MM-DD` and `DD.MM.YYYY` formats stay exactly as delivered.
  Raw duplicates are kept; deduplication happens in Silver.
- Schema drift tolerant: all columns present in the file are kept (CSV matched by header
  name), new source fields become new Delta columns (`mergeSchema`), expected columns
  missing from a file are added as NULL, and malformed JSON lines are preserved in
  `_corrupt_record`. Every difference from the expected schema is logged.
- Metadata columns `_batch_date`, `_ingested_at`, `_source_file` are appended.
- Idempotent: tables are partitioned by `_batch_date` and only the `batch_date`
  partition is replaced (`replaceWhere`), so re-runs never duplicate rows.
- Missing partitions (no `fx_rates` on weekends) are skipped instead of failing.

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.4, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

# Expected raw columns per dataset. Files are NOT forced into these schemas (that would
# silently drop new fields); they are used to log drift and to add missing columns as NULL.
FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), nullable=True),
        StructField("currency_from", StringType(), nullable=True),
        StructField("currency_to", StringType(), nullable=True),
        StructField("rate", StringType(), nullable=True),
    ]
)

EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField("exp_id", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("expense_date", StringType(), nullable=True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("amount", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("issued_at", StringType(), nullable=True),
        StructField("paid_at", StringType(), nullable=True),
    ]
)


@dataclass(frozen=True)
class DailySource:
    """Everything that differs between the daily Finance datasets."""

    dataset: str  # landing folder name and Bronze table name
    file_name: str  # file inside the `date=<batch_date>` folder
    file_format: str  # Spark reader format: "json" or "csv"
    expected_schema: StructType
    read_options: dict[str, str] = field(default_factory=dict)


FINANCE_SOURCES = (
    DailySource(
        dataset="fx_rates",
        file_name="part-000.json",
        file_format="json",
        expected_schema=FX_RATES_RAW_SCHEMA,
        # One JSON array spread over many lines; keep every value as raw text.
        read_options={"multiLine": "true", "primitivesAsString": "true"},
    ),
    DailySource(
        dataset="invoices",
        file_name="part-000.jsonl",
        file_format="json",
        expected_schema=INVOICES_RAW_SCHEMA,
        read_options={"primitivesAsString": "true"},
    ),
    DailySource(
        dataset="expenses",
        file_name="part-000.csv",
        file_format="csv",
        expected_schema=EXPENSES_RAW_SCHEMA,
        # No schema: CSV columns are matched by header name and all read as strings.
        read_options={"header": "true"},
    ),
)


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a file exists (uses Spark's file system, so it also works for S3/MinIO)."""
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)  # noqa: SLF001
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    return fs.exists(jvm_path)


def _align_to_expected(dataset: str, df: DataFrame, expected: StructType) -> DataFrame:
    """Log schema drift and add expected columns missing from the file as NULL strings.

    New (unexpected) columns are kept untouched; `mergeSchema` adds them to the table.
    """
    actual = set(df.columns) - {"_corrupt_record"}
    expected_cols = expected.fieldNames()

    new_cols = sorted(actual - set(expected_cols))
    missing_cols = [col for col in expected_cols if col not in actual]

    if new_cols:
        print(f"[bronze/{dataset}] WARNING new columns kept: {new_cols}")
    if missing_cols:
        print(f"[bronze/{dataset}] WARNING missing columns filled with NULL: {missing_cols}")
    if "_corrupt_record" in df.columns:
        print(f"[bronze/{dataset}] WARNING malformed lines kept in _corrupt_record")

    for col in missing_cols:
        df = df.withColumn(col, F.lit(None).cast(StringType()))
    return df


def _write_bronze(raw_df: DataFrame, batch_date: str, target_path: str) -> None:
    """Add Bronze metadata columns and replace only this batch_date's partition."""
    bronze_df = (
        raw_df.withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = '{batch_date}'")
        .option("mergeSchema", "true")  # new source fields become new columns
        .partitionBy("_batch_date")
        .save(target_path)
    )


def ingest_daily_source(
    spark: SparkSession,
    source: DailySource,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> int | None:
    """Ingest one day of one dataset: check file -> read -> align -> write Bronze.

    Returns the number of rows stored for `batch_date`, or None if there was no file.
    """
    source_path = join_path(
        get_landing_root(landing_root), source.dataset, f"date={batch_date}", source.file_name
    )
    target_path = join_path(get_lakehouse_root(lakehouse_root), "bronze", source.dataset)

    if not _path_exists(spark, source_path):
        print(f"[bronze/{source.dataset}] no file for {batch_date}, skipping")
        return None

    raw_df = spark.read.format(source.file_format).options(**source.read_options).load(source_path)
    raw_df = _align_to_expected(source.dataset, raw_df, source.expected_schema)
    _write_bronze(raw_df, batch_date, target_path)

    # Count what actually landed in the Bronze partition (not just what was read).
    rows = (
        spark.read.format("delta")
        .load(target_path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(batch_date)))
        .count()
    )
    print(f"[bronze/{source.dataset}] ingested {rows} rows for batch_date={batch_date}")
    return rows


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int | None]:
    """Ingest fx_rates, invoices and expenses for one batch_date into Bronze.

    Returns `{dataset: rows ingested}`; None means no landing file for that day.
    """
    ingested: dict[str, int | None] = {}
    for source in FINANCE_SOURCES:
        ingested[source.dataset] = ingest_daily_source(
            spark, source, batch_date, landing_root, lakehouse_root
        )

    summary = ", ".join(
        f"{dataset}={'skipped (no file)' if rows is None else rows}"
        for dataset, rows in ingested.items()
    )
    print(f"[bronze/finance] {batch_date} summary: {summary}")
    return ingested


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

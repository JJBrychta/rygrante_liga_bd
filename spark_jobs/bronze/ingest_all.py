"""Run Bronze ingestion jobs for one day or a date range in a single Spark session.

Starting Spark once and looping inside it is much faster than one `spark-submit`
per job per day. Used by the `make ingest*` targets.

Examples (inside the spark-master container):
    spark-submit spark_jobs/bronze/ingest_all.py --from 2026-01-05 --to 2026-01-05
    spark-submit spark_jobs/bronze/ingest_all.py --from 2026-01-01 --to 2026-01-14 \
        --jobs finance,hr

Full-load jobs (`clients`, `reference`) reload the whole file, so they run once per
call, stamped with the last date of the range. Daily jobs run for every date.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable
from datetime import date, timedelta

from pyspark.sql import SparkSession

from spark_jobs.bronze import ingest_crm_surveys, ingest_finance, ingest_hr, ingest_reference
from spark_jobs.bronze.ingest_clients import ingest_clients
from spark_jobs.common.spark_session import get_spark_session

# job name -> (is it a daily job?, function(spark, batch_date, landing_root, lakehouse_root))
JOBS: dict[str, tuple[bool, Callable[..., object]]] = {
    "clients": (False, ingest_clients),
    "reference": (False, ingest_reference.run),
    "hr": (True, ingest_hr.run),
    "finance": (True, ingest_finance.run),
    "crm-surveys": (True, ingest_crm_surveys.run),
}


def date_range(start: str, end: str) -> list[str]:
    """Return every date from `start` to `end` inclusive as `YYYY-MM-DD` strings."""
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if last < first:
        raise ValueError(f"--to ({end}) is before --from ({start})")
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def parse_jobs(value: str) -> list[str]:
    """Turn `finance,hr` (or `all`) into a list of known job names, in pipeline order."""
    if value.strip() == "all":
        return list(JOBS)
    requested = [name.strip() for name in value.split(",") if name.strip()]
    unknown = [name for name in requested if name not in JOBS]
    if unknown:
        raise ValueError(f"Unknown job(s) {unknown}. Choose from: {', '.join(JOBS)}, all")
    return [name for name in JOBS if name in requested]


def ingest(
    spark: SparkSession,
    jobs: list[str],
    dates: list[str],
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Run the selected jobs; full loads once (last date), daily jobs for each date."""
    started = time.monotonic()
    for name in jobs:
        is_daily, job = JOBS[name]
        job_dates = dates if is_daily else dates[-1:]
        job_started = time.monotonic()
        for batch_date in job_dates:
            print(f"[ingest] {name} batch_date={batch_date}")
            job(spark, batch_date, landing_root, lakehouse_root)
        took = time.monotonic() - job_started
        print(f"[ingest] {name} done: {len(job_dates)} day(s) in {took:.0f}s")
    print(
        f"[ingest] finished {', '.join(jobs)} for {dates[0]}..{dates[-1]} "
        f"in {time.monotonic() - started:.0f}s"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze ingestion for a date range.")
    parser.add_argument("--from", dest="start", required=True, help="First date (YYYY-MM-DD).")
    parser.add_argument("--to", dest="end", required=True, help="Last date (YYYY-MM-DD).")
    parser.add_argument(
        "--jobs", default="all", help=f"Comma-separated jobs: {', '.join(JOBS)} (default: all)."
    )
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    selected_jobs = parse_jobs(args.jobs)
    batch_dates = date_range(args.start, args.end)
    spark = get_spark_session("bronze-ingest-all")
    spark.sparkContext.setLogLevel("WARN")
    try:
        ingest(spark, selected_jobs, batch_dates, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()

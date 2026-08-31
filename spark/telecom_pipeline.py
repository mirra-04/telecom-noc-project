"""
telecom_pipeline.py — Reusable Spark ETL Job for Milan Telecom Activity Data

JOB CONTRACT
============
Purpose:
    Read raw daily "sms-call-internet-mi-*.csv" files, clean them,
    aggregate to grid_id + timestamp grain, enrich with geographic
    reference data, and write processed/analytics outputs.

Expected inputs:
    --input-dir   Directory containing sms-call-internet-mi-*.csv files.
                  MUST contain at least one matching file, or the job
                  fails immediately (see "Failure conditions" below).
    --reference   Path to milano-grid.geojson (grid boundary reference data).
    --output-dir  Directory under which processed/ and analytics/ subfolders
                  are created.

Outputs (all written under --output-dir):
    processed/activity/date=YYYY-MM-DD/part-0.parquet   (one folder per date)
    analytics/hourly_grid_summary/part-0.parquet          (one row per grid+hour)
    dashboard_summary.csv                                  (one row per date)

Failure conditions (job exits non-zero and logs an ERROR):
    - --input-dir contains zero matching CSV files
    - --reference file does not exist
    - Row-count reconciliation fails at any stage (input != accepted + rejected,
      or written != read-back on validation)
    - Any unhandled exception during read/clean/aggregate/enrich/write

Idempotency:
    Re-running the job with the same inputs overwrites prior outputs at
    the same --output-dir (safe to re-run).

Non-goals:
    This job does not handle Airflow orchestration, retries, or
    notification on failure — those are handled by the DE-phase
    orchestration layer that calls this script.
"""

import argparse
import glob
import json
import logging
import os
import sys
import time
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, date_format, when, lit,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

RAW_SCHEMA = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

logger = logging.getLogger("telecom_pipeline")


def setup_logging(log_path=None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_path:
        handlers.append(logging.FileHandler(log_path))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers,
    )


# =====================================================================
# read_raw
# =====================================================================
def read_raw(spark, input_dir):
    """Read all sms-call-internet-mi-*.csv files from input_dir.
    Fails loudly (raises) if no matching files are found."""
    pattern = os.path.join(input_dir, "sms-call-internet-mi-*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No input files matched pattern '{pattern}'. "
            f"Job cannot proceed with zero input files."
        )

    logger.info("read_raw(): found %d input file(s): %s", len(files), files)

    df = (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(files)
        .withColumnRenamed("datetime", "timestamp")
        .withColumnRenamed("CellID", "grid_id")
        .withColumnRenamed("countrycode", "country_code")
        .withColumnRenamed("smsin", "sms_in")
        .withColumnRenamed("smsout", "sms_out")
        .withColumnRenamed("callin", "call_in")
        .withColumnRenamed("callout", "call_out")
        .withColumnRenamed("internet", "internet_activity")
    )

    row_count = df.count()
    logger.info("read_raw(): loaded %d rows from %d file(s)", row_count, len(files))
    return df, row_count


# =====================================================================
# clean
# =====================================================================
def clean(df):
    """Cast types, quarantine (not silently drop) rows with missing
    grid_id/timestamp or negative activity values. Returns
    (accepted_df, rejected_df, stats dict)."""
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    bad_id_condition = col("grid_id").isNull() | col("timestamp").isNull()

    negative_condition = None
    for c in ACTIVITY_COLUMNS:
        cond = col(c).isNotNull() & (col(c) < 0)
        negative_condition = cond if negative_condition is None else (negative_condition | cond)

    reject_condition = bad_id_condition | negative_condition

    rejected_df = df.filter(reject_condition).withColumn(
        "reject_reason",
        when(bad_id_condition, lit("missing_grid_id_or_timestamp"))
        .when(negative_condition, lit("negative_activity_value"))
        .otherwise(lit("unknown")),
    )
    accepted_df = df.filter(~reject_condition)

    accepted_count = accepted_df.count()
    rejected_count = rejected_df.count()

    null_counts = {}
    for c in ACTIVITY_COLUMNS:
        null_counts[c] = accepted_df.filter(col(c).isNull()).count()

    stats = {
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "null_counts": null_counts,
    }

    logger.info(
        "clean(): accepted=%d rejected=%d null_counts=%s",
        accepted_count, rejected_count, null_counts,
    )

    for c in ACTIVITY_COLUMNS:
        accepted_df = accepted_df.withColumn(
            c + "_curated", when(col(c).isNull(), lit(0.0)).otherwise(col(c))
        )

    accepted_df = (
        accepted_df
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
    )

    return accepted_df, rejected_df, stats


# =====================================================================
# aggregate
# =====================================================================
def aggregate(clean_df):
    """Collapse country_code rows into hourly_grid_summary — exactly
    one row per grid_id + timestamp."""
    grouped = (
        clean_df
        .groupBy("grid_id", "timestamp", "date", "hour", "day_of_week")
        .agg(
            spark_sum("sms_in_curated").alias("sms_in"),
            spark_sum("sms_out_curated").alias("sms_out"),
            spark_sum("call_in_curated").alias("call_in"),
            spark_sum("call_out_curated").alias("call_out"),
            spark_sum("internet_activity_curated").alias("internet_activity"),
        )
        .withColumn("total_sms", col("sms_in") + col("sms_out"))
        .withColumn("total_calls", col("call_in") + col("call_out"))
        .withColumn("total_activity", col("total_sms") + col("total_calls") + col("internet_activity"))
    )

    row_count = grouped.count()
    distinct_grid_ts = grouped.select("grid_id", "timestamp").distinct().count()
    if row_count != distinct_grid_ts:
        raise ValueError(
            f"GRAIN VIOLATION in aggregate(): {row_count} rows but only "
            f"{distinct_grid_ts} distinct (grid_id, timestamp) pairs."
        )

    logger.info("aggregate(): produced %d grid/hour rows (grain verified)", row_count)
    return grouped


# =====================================================================
# enrich
# =====================================================================
def enrich(hourly_grid_summary, reference_path):
    """Attach centroid lon/lat from the reference GeoJSON. Uses
    properties.cellId (1-based) as the join key — NEVER the top-level
    'id' field (0-based), which would silently shift every grid by one."""
    if not os.path.exists(reference_path):
        raise FileNotFoundError(f"Reference file not found: {reference_path}")

    with open(reference_path, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    lookup = {}
    for feature in geojson["features"]:
        cell_id = feature["properties"]["cellId"]
        ring = feature["geometry"]["coordinates"][0]
        lons = [pt[0] for pt in ring]
        lats = [pt[1] for pt in ring]
        lookup[cell_id] = (sum(lons) / len(lons), sum(lats) / len(lats))

    logger.info("enrich(): loaded %d grid geometries from reference file", len(lookup))
    return hourly_grid_summary, lookup  # lookup applied at write time (pandas stage)


# =====================================================================
# write_outputs
# =====================================================================
def write_outputs(clean_df, hourly_grid_summary, geo_lookup, output_dir):
    """Write processed activity (partitioned by date), analytics
    hourly_grid_summary, and dashboard_summary.csv. Uses pandas/pyarrow
    for the actual file write (see module docstring in SP6 context:
    avoids a Windows Hadoop-native-library incompatibility found during
    development)."""
    processed_path = os.path.join(output_dir, "processed", "activity")
    analytics_path = os.path.join(output_dir, "analytics", "hourly_grid_summary")
    os.makedirs(processed_path, exist_ok=True)
    os.makedirs(analytics_path, exist_ok=True)

    distinct_dates = [row["date"] for row in clean_df.select("date").distinct().collect()]
    distinct_dates.sort()

    written_rows = 0
    for d in distinct_dates:
        date_str = d.isoformat() if hasattr(d, "isoformat") else str(d)
        partition_dir = os.path.join(processed_path, f"date={date_str}")
        os.makedirs(partition_dir, exist_ok=True)
        day_pdf = clean_df.filter(col("date") == d).toPandas()
        day_pdf.to_parquet(
            os.path.join(partition_dir, "part-0.parquet"),
            engine="pyarrow", compression="snappy", index=False,
        )
        written_rows += len(day_pdf)

    logger.info("write_outputs(): wrote %d rows to %s (%d date partitions)",
                written_rows, processed_path, len(distinct_dates))

    summary_pdf = hourly_grid_summary.toPandas()
    summary_pdf["centroid_lon"] = summary_pdf["grid_id"].map(lambda g: geo_lookup.get(g, (None, None))[0])
    summary_pdf["centroid_lat"] = summary_pdf["grid_id"].map(lambda g: geo_lookup.get(g, (None, None))[1])
    summary_pdf.to_parquet(
        os.path.join(analytics_path, "part-0.parquet"),
        engine="pyarrow", compression="snappy", index=False,
    )
    logger.info("write_outputs(): wrote %d rows to %s", len(summary_pdf), analytics_path)

    dashboard = (
        summary_pdf.groupby("date")
        .agg(
            total_activity=("total_activity", "sum"),
            total_sms=("total_sms", "sum"),
            total_calls=("total_calls", "sum"),
            total_internet=("internet_activity", "sum"),
        )
        .reset_index()
        .sort_values("date")
    )
    dashboard_path = os.path.join(output_dir, "dashboard_summary.csv")
    dashboard.to_csv(dashboard_path, index=False)
    logger.info("write_outputs(): wrote %d rows to %s", len(dashboard), dashboard_path)

    # ---- Round-trip validation ----
    reread_count = sum(
        len(pd.read_parquet(os.path.join(processed_path, entry, "part-0.parquet")))
        for entry in os.listdir(processed_path)
        if os.path.isdir(os.path.join(processed_path, entry))
    )
    if reread_count != written_rows:
        raise ValueError(
            f"ROUND-TRIP VALIDATION FAILED: wrote {written_rows} rows, "
            f"read back {reread_count} rows."
        )
    logger.info("write_outputs(): round-trip validation PASSED (%d rows)", reread_count)

    return {
        "processed_rows": written_rows,
        "analytics_rows": len(summary_pdf),
        "dashboard_rows": len(dashboard),
    }


# =====================================================================
# main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Milan Telecom Activity ETL Pipeline")
    parser.add_argument("--input-dir", required=True, help="Directory of sms-call-internet-mi-*.csv files")
    parser.add_argument("--reference", required=True, help="Path to milano-grid.geojson")
    parser.add_argument("--output-dir", required=True, help="Output directory for processed/analytics data")
    parser.add_argument("--log-file", default=None, help="Optional path to also write logs to a file")
    args = parser.parse_args()

    setup_logging(args.log_file)

    job_start = time.time()
    logger.info("=" * 60)
    logger.info("JOB START: %s", datetime.now().isoformat())
    logger.info("input_dir=%s reference=%s output_dir=%s", args.input_dir, args.reference, args.output_dir)
    logger.info("=" * 60)

    spark = (
        SparkSession.builder
        .appName("TelecomPipeline")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df, input_row_count = read_raw(spark, args.input_dir)
        clean_df, rejected_df, clean_stats = clean(raw_df)

        reconciled = clean_stats["accepted_count"] + clean_stats["rejected_count"] == input_row_count
        if not reconciled:
            raise ValueError(
                f"ROW RECONCILIATION FAILED: input={input_row_count}, "
                f"accepted+rejected={clean_stats['accepted_count'] + clean_stats['rejected_count']}"
            )
        logger.info("Row reconciliation OK: input=%d = accepted=%d + rejected=%d",
                    input_row_count, clean_stats["accepted_count"], clean_stats["rejected_count"])

        hourly_grid_summary = aggregate(clean_df)
        hourly_grid_summary, geo_lookup = enrich(hourly_grid_summary, args.reference)
        output_stats = write_outputs(clean_df, hourly_grid_summary, geo_lookup, args.output_dir)

        elapsed = time.time() - job_start
        logger.info("=" * 60)
        logger.info("JOB STATUS: SUCCESS")
        logger.info("Input rows: %d", input_row_count)
        logger.info("Rejected rows: %d", clean_stats["rejected_count"])
        logger.info("Null values handled (per column): %s", clean_stats["null_counts"])
        logger.info("Output rows (processed): %d", output_stats["processed_rows"])
        logger.info("Output rows (analytics): %d", output_stats["analytics_rows"])
        logger.info("Elapsed time: %.2fs", elapsed)
        logger.info("JOB END: %s", datetime.now().isoformat())
        logger.info("=" * 60)

    except Exception as e:
        elapsed = time.time() - job_start
        logger.error("=" * 60)
        logger.error("JOB STATUS: FAILED")
        logger.error("Error: %s", str(e))
        logger.error("Elapsed time before failure: %.2fs", elapsed)
        logger.error("=" * 60)
        spark.stop()
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
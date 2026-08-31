"""
SP6 — Write Processed & Analytics Data

Persists the SP2 clean data and SP3 hourly_grid_summary as Parquet,
partitioned appropriately, plus a small dashboard CSV summary. Keeps
milano-grid.geojson as-is under data/reference/ rather than folding
geometry into every analytics row. Validates every write with a
round-trip read.

Expected output:
    data/processed/activity/          (Parquet, partitioned by date)
    data/analytics/hourly_grid_summary/  (Parquet, one row per grid+hour)
    dashboard_summary.csv
    data/reference/milano-grid.geojson (copied, unchanged)
"""

import glob
import os
import shutil
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, date_format, when, lit,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

DATA_PATH = sorted(glob.glob("../data/sms-call-internet-mi-*.csv"))
GEOJSON_SOURCE = "../data/milano-grid.geojson"

spark = (
    SparkSession.builder
    .appName("SP6-WriteData")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

manual_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

# -----------------------------------------------------------------
# Rebuild SP2's clean_network_df (row-level, still country_code grain)
# -----------------------------------------------------------------
raw_df = (
    spark.read
    .option("header", True)
    .schema(manual_schema)
    .csv(DATA_PATH)
    .withColumnRenamed("datetime", "timestamp")
    .withColumnRenamed("CellID", "grid_id")
    .withColumnRenamed("countrycode", "country_code")
    .withColumnRenamed("smsin", "sms_in")
    .withColumnRenamed("smsout", "sms_out")
    .withColumnRenamed("callin", "call_in")
    .withColumnRenamed("callout", "call_out")
    .withColumnRenamed("internet", "internet_activity")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
)

for c in ACTIVITY_COLUMNS:
    raw_df = raw_df.withColumn(c + "_curated", when(col(c).isNull(), lit(0.0)).otherwise(col(c)))

clean_network_df = (
    raw_df
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
)

# -----------------------------------------------------------------
# Rebuild SP3's hourly_grid_summary (grid_id + timestamp grain)
# -----------------------------------------------------------------
hourly_grid_summary = (
    clean_network_df
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

# -----------------------------------------------------------------
# Step 1: Write clean activity data as Parquet, partitioned by date
#
# NOTE: Spark's native .write.parquet() goes through Hadoop's
# FileOutputCommitter, which on this machine hits a persistent
# UnsatisfiedLinkError in NativeIO$Windows.access0 — the installed
# winutils.exe/hadoop.dll build doesn't match what this Hadoop
# client expects internally, even with HADOOP_HOME correctly set.
# Rather than keep chasing DLL versions, we do the heavy lifting
# (filtering) in Spark, then write the actual Parquet files with
# pandas + pyarrow per date — this never touches Hadoop's commit
# protocol at all, so the broken native call is never invoked.
# -----------------------------------------------------------------
PROCESSED_PATH = "../data/processed/activity"
os.makedirs("../data/processed", exist_ok=True)

print("=" * 60)
print("STEP 1: Writing clean activity data as Parquet, partitioned by date")
print("(via Spark filter + pandas/pyarrow write, bypassing Hadoop commit)")
print("=" * 60)

distinct_dates = [row["date"] for row in clean_network_df.select("date").distinct().collect()]
distinct_dates.sort()
print(f"Writing {len(distinct_dates)} date partitions: {distinct_dates}")

start = time.time()
total_written = 0
for d in distinct_dates:
    date_str = d.isoformat() if hasattr(d, "isoformat") else str(d)
    partition_dir = os.path.join(PROCESSED_PATH, f"date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)

    day_pdf = clean_network_df.filter(col("date") == d).toPandas()
    day_pdf.to_parquet(
        os.path.join(partition_dir, "part-0.parquet"),
        engine="pyarrow",
        compression="snappy",
        index=False,
    )
    total_written += len(day_pdf)
    print(f"  {partition_dir}: {len(day_pdf)} rows written")

write1_time = time.time() - start
print(f"Written {total_written} total rows to {PROCESSED_PATH} in {write1_time:.2f}s")

print("\nOn-disk partition layout:")
for entry in sorted(os.listdir(PROCESSED_PATH)):
    full_path = os.path.join(PROCESSED_PATH, entry)
    if os.path.isdir(full_path):
        files = os.listdir(full_path)
        parquet_files = [f for f in files if f.endswith(".parquet")]
        total_size = sum(os.path.getsize(os.path.join(full_path, f)) for f in parquet_files)
        print(f"  {entry}/  ({len(parquet_files)} parquet file(s), {total_size/1024:.1f} KB)")

# -----------------------------------------------------------------
# Step 2: Write hourly_grid_summary as Parquet — one record per
# grid + hour. NO geometry here — geometry stays in the static
# reference GeoJSON, joined in only when needed (per SP4's note).
# Same Hadoop-bypass approach as Step 1: Spark computes, pandas writes.
# -----------------------------------------------------------------
ANALYTICS_PATH = "../data/analytics/hourly_grid_summary"
os.makedirs("../data/analytics", exist_ok=True)

print("\n" + "=" * 60)
print("STEP 2: Writing hourly_grid_summary as Parquet")
print("(via Spark aggregation + pandas/pyarrow write)")
print("=" * 60)

start = time.time()
summary_pdf = hourly_grid_summary.toPandas()
os.makedirs(ANALYTICS_PATH, exist_ok=True)
summary_pdf.to_parquet(
    os.path.join(ANALYTICS_PATH, "part-0.parquet"),
    engine="pyarrow",
    compression="snappy",
    index=False,
)
write2_time = time.time() - start
print(f"Written {len(summary_pdf)} rows to {ANALYTICS_PATH} in {write2_time:.2f}s")

parquet_files_2 = [f for f in os.listdir(ANALYTICS_PATH) if f.endswith(".parquet")]
total_size_2 = sum(os.path.getsize(os.path.join(ANALYTICS_PATH, f)) for f in parquet_files_2)
print(f"{len(parquet_files_2)} parquet file(s), {total_size_2/1024:.1f} KB total")

# -----------------------------------------------------------------
# Step 3: Small dashboard summary as CSV — easy to open in Excel/
# eyeball without any tooling
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 3: Writing dashboard_summary.csv")
print("=" * 60)

dashboard_summary = (
    hourly_grid_summary
    .groupBy("date")
    .agg(
        spark_sum("total_activity").alias("total_activity"),
        spark_sum("total_sms").alias("total_sms"),
        spark_sum("total_calls").alias("total_calls"),
        spark_sum("internet_activity").alias("total_internet"),
    )
    .orderBy("date")
)

# collect() is fine here — dashboard_summary is tiny (7 rows), so we
# write it with plain Python to guarantee a SINGLE clean CSV file,
# rather than Spark's default multi-part-file CSV output.
rows = dashboard_summary.collect()
dashboard_csv_path = "dashboard_summary.csv"
with open(dashboard_csv_path, "w", encoding="utf-8") as f:
    f.write("date,total_activity,total_sms,total_calls,total_internet\n")
    for row in rows:
        f.write(f"{row['date']},{row['total_activity']},{row['total_sms']},"
                f"{row['total_calls']},{row['total_internet']}\n")
print(f"Written {dashboard_csv_path} ({len(rows)} rows)")

# -----------------------------------------------------------------
# Step 4: Copy milano-grid.geojson unchanged into data/reference/
# (NOT re-written through Spark — it's static reference data, not
# a Spark output, so a plain file copy is the correct/simplest tool)
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 4: Copying milano-grid.geojson to data/reference/")
print("=" * 60)

REFERENCE_DIR = "../data/reference"
os.makedirs(REFERENCE_DIR, exist_ok=True)
reference_dest = os.path.join(REFERENCE_DIR, "milano-grid.geojson")
shutil.copyfile(GEOJSON_SOURCE, reference_dest)
print(f"Copied to {reference_dest}")
print("Kept as a plain file copy, not reprocessed through Spark, because "
      "it is static reference/dimension data (10,000 fixed shapes), not "
      "a fact table that changes daily like the activity data.")

# -----------------------------------------------------------------
# Step 5: Round-trip validation — READ the Parquet back and check
# schema + counts match what was written
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 5: Round-trip validation")
print("(reading back via pandas/pyarrow, matching how we wrote it)")
print("=" * 60)

import pandas as pd

reread_frames = []
for entry in sorted(os.listdir(PROCESSED_PATH)):
    full_path = os.path.join(PROCESSED_PATH, entry)
    if os.path.isdir(full_path):
        for f in os.listdir(full_path):
            if f.endswith(".parquet"):
                reread_frames.append(pd.read_parquet(os.path.join(full_path, f)))
reread_processed_count = sum(len(f) for f in reread_frames)
original_count = clean_network_df.count()
print(f"data/processed/activity: written {original_count} rows, "
      f"read back {reread_processed_count} rows -> match: "
      f"{original_count == reread_processed_count}")
assert original_count == reread_processed_count, "ROUND-TRIP MISMATCH on processed activity data!"

print("Schema read back (from one partition file):")
print(reread_frames[0].dtypes)

reread_analytics_pdf = pd.read_parquet(os.path.join(ANALYTICS_PATH, "part-0.parquet"))
reread_analytics_count = len(reread_analytics_pdf)
original_analytics_count = hourly_grid_summary.count()
print(f"\ndata/analytics/hourly_grid_summary: written {original_analytics_count} rows, "
      f"read back {reread_analytics_count} rows -> match: "
      f"{original_analytics_count == reread_analytics_count}")
assert original_analytics_count == reread_analytics_count, "ROUND-TRIP MISMATCH on hourly_grid_summary!"

print("Schema read back:")
print(reread_analytics_pdf.dtypes)

# -----------------------------------------------------------------
# Step 6: Compare file sizes — CSV source vs Parquet output
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("STEP 6: File size comparison — source CSV vs Parquet output")
print("=" * 60)

csv_total_size = sum(os.path.getsize(f) for f in DATA_PATH)
parquet_processed_size = sum(
    os.path.getsize(os.path.join(root, f))
    for root, _, files in os.walk(PROCESSED_PATH)
    for f in files if f.endswith(".parquet")
)

print(f"Original 7 CSV files (raw, country_code grain): {csv_total_size/1024/1024:.2f} MB")
print(f"data/processed/activity (Parquet, same grain, all columns retained): "
      f"{parquet_processed_size/1024/1024:.2f} MB")
print(f"Ratio: {parquet_processed_size/csv_total_size:.2%} of original CSV size")
print("""
Why Parquet is smaller AND faster to query, even at the same row grain:
1. COLUMNAR layout: Parquet stores each column contiguously, enabling
   type-specific compression (e.g. run-length/dictionary encoding on
   grid_id, delta encoding on timestamps) far more effective than
   generic text compression on a row-based CSV.
2. Binary types: numbers are stored as actual binary doubles/ints, not
   as ASCII text digits — CSV "57.7729" is 8 bytes of text; Parquet
   stores the equivalent double in far fewer bytes with no parsing cost.
3. Embedded schema + statistics: Parquet stores min/max per column per
   file, letting Spark SKIP entire files/row-groups that can't match a
   filter, without reading them at all — impossible with CSV, which
   must be scanned start-to-finish every time regardless of filters.
""")

print("""
--- Learner Validation: Why Parquet for processed/analytics, but NOT raw ---
Raw data should stay in its original CSV form because:
1. It is the audit trail / source of truth — converting it changes
   nothing about its content, but re-encoding the ORIGINAL ingest
   artifact removes the ability to prove "this is exactly what we
   received," which matters for debugging and compliance.
2. Raw files are read only once or twice (during initial processing),
   so Parquet's read-optimized benefits are wasted on them — the cost
   of converting isn't repaid.
3. Processed/analytics data, by contrast, gets read repeatedly by many
   downstream consumers (APIs, dashboards, ML training) — this is
   exactly where Parquet's columnar, compressed, statistics-aware
   format earns back its conversion cost many times over.

--- Learner Validation: Why GeoJSON stays as static reference, not fact data ---
milano-grid.geojson describes 10,000 FIXED grid cell boundaries that do
not change day to day. It is DIMENSION-like data (a lookup/reference
table), not FACT data (which grows every day, one record per grid+hour).
Baking geometry into every analytics row would mean repeating the same
handful of coordinate pairs across ~1.68 million+ rows — pure redundant
storage with zero informational gain, since the geometry for grid_id=1
never differs between hour 0 and hour 23. Keeping it as a separate small
reference file, joined in only when a map/geometry view is actually
needed (as SP4 demonstrated with a broadcast join), is both smaller AND
philosophically correct: it's a dimension table, not a fact table.
""")

print("SP6 complete.")
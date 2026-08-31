"""
SP5 — Performance & Execution Behaviour

This script does NOT tell you which optimizations "win" — it measures
things and prints evidence. YOU read the timings and explain() output,
then decide what to keep. Fill in your own conclusions in the
DISCUSSION section at the bottom based on what you actually observe
when you run this (numbers vary by machine/run).
"""

import glob
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, date_format, when, lit,
    sum as spark_sum, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

DATA_PATH = sorted(glob.glob("../data/sms-call-internet-mi-*.csv"))
GEOJSON_PATH = "../data/milano-grid.geojson"

spark = SparkSession.builder.appName("SP5-Performance").getOrCreate()
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
    raw_df = raw_df.withColumn(c + "_z", when(col(c).isNull(), lit(0.0)).otherwise(col(c)))

clean_df = raw_df.withColumn("date", to_date(col("timestamp")))

print(f"Input partitions: {clean_df.rdd.getNumPartitions()}")
print("Note: .rdd.getNumPartitions() itself is an action-like call in some Spark "
      "versions/configs and may trigger a small amount of work — timings below "
      "start fresh from this point.\n")


def time_action(label, fn):
    """Run fn(), time it, and print the result. Returns the elapsed time."""
    start = time.time()
    result = fn()
    elapsed = time.time() - start
    print(f"[{label}] {elapsed:.2f}s  (result: {result})")
    return elapsed


# -----------------------------------------------------------------
# 1. explain() on a hotspot-style aggregation — read this yourself
# -----------------------------------------------------------------
print("=" * 60)
print("1. EXPLAIN — hotspot aggregation physical plan")
print("=" * 60)
hotspot_agg = (
    clean_df.groupBy("grid_id")
    .agg(spark_sum("internet_activity_z").alias("total_internet"))
    .orderBy(col("total_internet").desc())
)
hotspot_agg.explain()
print("""
READ THIS YOURSELF: look for HashAggregate (there should be a partial +
final stage — Spark pre-aggregates per-partition before combining, to
reduce shuffle volume), Exchange (the shuffle boundary — this is the
expensive part), and FileScan at the bottom (confirms lazy evaluation:
nothing has actually run yet, this is just the PLAN).
""")

# -----------------------------------------------------------------
# 2. Cache vs no-cache — compare REPEATED action timings
# -----------------------------------------------------------------
print("=" * 60)
print("2. CACHE COMPARISON — same DataFrame, reused 3 times")
print("=" * 60)

print("--- WITHOUT cache ---")
no_cache_df = clean_df.filter(col("grid_id") <= 100)
t1 = time_action("run 1 (no cache)", lambda: no_cache_df.count())
t2 = time_action("run 2 (no cache)", lambda: no_cache_df.count())
t3 = time_action("run 3 (no cache)", lambda: no_cache_df.count())
print(f"Without cache, each .count() re-reads and re-filters from scratch: "
      f"{t1:.2f}s, {t2:.2f}s, {t3:.2f}s")

print("\n--- WITH cache ---")
cached_df = clean_df.filter(col("grid_id") <= 100).cache()
t1c = time_action("run 1 (cache, first call materializes it)", lambda: cached_df.count())
t2c = time_action("run 2 (cache, should be faster)", lambda: cached_df.count())
t3c = time_action("run 3 (cache, should be faster)", lambda: cached_df.count())
print(f"With cache: {t1c:.2f}s, {t2c:.2f}s, {t3c:.2f}s "
      f"(first call pays the caching cost; later calls should be cheaper)")
cached_df.unpersist()

# -----------------------------------------------------------------
# 3. Repartition by date — observe partition counts, not just "faster"
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("3. REPARTITION BY DATE")
print("=" * 60)
original_partitions = clean_df.rdd.getNumPartitions()
repartitioned_df = clean_df.repartition("date")
new_partitions = repartitioned_df.rdd.getNumPartitions()
print(f"Original partitions: {original_partitions}")
print(f"Partitions after repartition('date'): {new_partitions}")
print(f"Distinct dates in data: {clean_df.select('date').distinct().count()}")
print("Note: repartition() triggers a full shuffle — it is NOT free. "
      "It only pays off if the new partitioning is reused for MULTIPLE "
      "downstream operations that benefit from it (e.g. many per-date "
      "aggregations), not for a single one-off query.")

# -----------------------------------------------------------------
# 4. Column pruning — select only needed columns BEFORE aggregating
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("4. COLUMN PRUNING")
print("=" * 60)

t_wide = time_action(
    "aggregation on FULL row (all columns carried through)",
    lambda: clean_df.groupBy("grid_id").agg(spark_sum("internet_activity_z")).count()
)

pruned_df = clean_df.select("grid_id", "internet_activity_z")
t_pruned = time_action(
    "aggregation on PRUNED columns only (grid_id, internet_activity_z)",
    lambda: pruned_df.groupBy("grid_id").agg(spark_sum("internet_activity_z")).count()
)
print(f"Wide: {t_wide:.2f}s vs Pruned: {t_pruned:.2f}s")
print("Note: for a CSV source, Spark's predicate/column pushdown behavior "
      "differs from columnar formats like Parquet — CSV must still be "
      "read row-by-row, so pruning benefits may be smaller here than they "
      "would be in a columnar warehouse (see SP6/DE-phase).")

# -----------------------------------------------------------------
# 5. Broadcast the SP4 grid lookup — compare plans
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("5. BROADCAST JOIN — grid lookup from SP4")
print("=" * 60)

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

lookup_records = [{"grid_id": feat["properties"]["cellId"]} for feat in geojson["features"]]
lookup_json_path = "grid_lookup_ids_only.jsonl"
with open(lookup_json_path, "w", encoding="utf-8") as f:
    for r in lookup_records:
        f.write(json.dumps(r) + "\n")

grid_ids_df = spark.read.schema(
    StructType([StructField("grid_id", IntegerType(), False)])
).json(lookup_json_path)

daily_totals = clean_df.groupBy("grid_id").agg(spark_sum("internet_activity_z").alias("total"))

print("--- Standard join plan ---")
standard = daily_totals.join(grid_ids_df, on="grid_id", how="inner")
standard.explain()

print("\n--- Explicit broadcast join plan ---")
broadcasted = daily_totals.join(broadcast(grid_ids_df), on="grid_id", how="inner")
broadcasted.explain()
print("Compare: does the plan actually differ, or did Spark's optimizer "
      "already choose to broadcast automatically (as we saw in SP4)? "
      "Look for 'BroadcastHashJoin' vs 'SortMergeJoin' in each plan.")

# -----------------------------------------------------------------
# 6. Discuss: why over-partitioning a small dataset hurts
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("6. OVER-PARTITIONING DEMONSTRATION")
print("=" * 60)
small_df = grid_ids_df  # only 10,000 rows

over_partitioned = small_df.repartition(200)
t_over = time_action(
    "count() on 10,000-row dataset FORCED into 200 partitions",
    lambda: over_partitioned.count()
)

right_sized = small_df.repartition(2)
t_right = time_action(
    "count() on same dataset with 2 partitions",
    lambda: right_sized.count()
)
print(f"200 partitions: {t_over:.4f}s vs 2 partitions: {t_right:.4f}s")
print("Reasoning to evaluate yourself: with only 10,000 rows split across "
      "200 partitions, each partition holds ~50 rows — the overhead of "
      "scheduling, serializing, and coordinating 200 tiny tasks can exceed "
      "the actual work being done in each one. This is 'over-partitioning'.")

print("\nSP5 measurement harness complete.")

"""
SP3 — Network Activity Aggregations

Collapses SP2's clean_network_df (still at country_code grain) into
hourly_grid_summary — exactly one row per grid_id + timestamp — the
canonical downstream analytics table for SP4, DE6, RE2.

Expected output: hourly_grid_summary, daily_traffic_summary, hotspot ranking.
"""

import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, date_format, when, lit,
    sum as spark_sum, avg as spark_avg, max as spark_max,
    row_number, count
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

DATA_PATH = sorted(glob.glob("../data/sms-call-internet-mi-*.csv"))

spark = SparkSession.builder.appName("SP3-Aggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

# -----------------------------------------------------------------
# Rebuild clean_network_df (SP2's output) so this script is self-contained
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

clean_network_df = raw_df  # still at country_code grain — this is SP3's input

country_code_row_count = clean_network_df.count()
print(f"Input row count (country_code grain, from SP2): {country_code_row_count}")

# -----------------------------------------------------------------
# BEFORE/AFTER inspection for ONE grid + hour, by hand
# (Learner Validation: confirm the sum by hand)
# -----------------------------------------------------------------
sample_grid = 1
sample_hour = "2013-11-01 00:00:00"

print(f"\n--- BEFORE consolidation: grid_id={sample_grid}, timestamp={sample_hour} ---")
before_rows = clean_network_df.filter(
    (col("grid_id") == sample_grid) & (col("timestamp") == sample_hour)
)
before_rows.select("grid_id", "timestamp", "country_code", *ACTIVITY_COLUMNS).show(truncate=False)

# -----------------------------------------------------------------
# Step 1: Collapse country-code rows -> one row per grid_id + timestamp
# Sum across ACTIVITY_COLUMNS, treating null as 0 (curated-layer rule,
# consistent with NP2/SP2). minCount not directly available in agg sum,
# so we zero-fill just for this summation.
# -----------------------------------------------------------------
for c in ACTIVITY_COLUMNS:
    clean_network_df = clean_network_df.withColumn(
        c + "_z", when(col(c).isNull(), lit(0.0)).otherwise(col(c))
    )

hourly_grid_summary = (
    clean_network_df
    .groupBy("grid_id", "timestamp")
    .agg(
        spark_sum("sms_in_z").alias("sms_in"),
        spark_sum("sms_out_z").alias("sms_out"),
        spark_sum("call_in_z").alias("call_in"),
        spark_sum("call_out_z").alias("call_out"),
        spark_sum("internet_activity_z").alias("internet_activity"),
    )
    .withColumn("total_sms", col("sms_in") + col("sms_out"))
    .withColumn("total_calls", col("call_in") + col("call_out"))
    .withColumn("total_activity", col("total_sms") + col("total_calls") + col("internet_activity"))
    .withColumn("internet_share", col("internet_activity") / col("total_activity"))
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
)

after_row = hourly_grid_summary.filter(
    (col("grid_id") == sample_grid) & (col("timestamp") == sample_hour)
)
print(f"\n--- AFTER consolidation: grid_id={sample_grid}, timestamp={sample_hour} ---")
after_row.select("grid_id", "timestamp", *ACTIVITY_COLUMNS, "total_activity").show(truncate=False)
print("^ Compare this total_activity by hand against the sum of the BEFORE rows above.")

# -----------------------------------------------------------------
# Step 2: Grain check — hourly_grid_summary must have EXACTLY one
# row per grid_id + timestamp
# -----------------------------------------------------------------
total_summary_rows = hourly_grid_summary.count()
distinct_grid_ts = hourly_grid_summary.select("grid_id", "timestamp").distinct().count()

print(f"\n--- Grain Check on hourly_grid_summary ---")
print(f"Total rows: {total_summary_rows}")
print(f"Distinct (grid_id, timestamp) pairs: {distinct_grid_ts}")
print(f"Exactly one record per grid+hour: {total_summary_rows == distinct_grid_ts}")
assert total_summary_rows == distinct_grid_ts, "GRAIN VIOLATION: duplicate grid_id+timestamp rows exist!"

expected_rows = 10000 * 168  # 10,000 grids x (7 days x 24 hours)
print(f"Expected rows (10,000 grids x 168 hours): {expected_rows}")
print(f"Matches expectation: {total_summary_rows == expected_rows}")

# -----------------------------------------------------------------
# Step 3: Top 10 high-activity grids (overall, across the full week)
# -----------------------------------------------------------------
print("\n--- Top 10 High-Activity Grids (summed across full week) ---")
top_grids = (
    hourly_grid_summary
    .groupBy("grid_id")
    .agg(spark_sum("total_activity").alias("week_total_activity"))
    .orderBy(col("week_total_activity").desc())
    .limit(10)
)
top_grids.show(truncate=False)

# -----------------------------------------------------------------
# Step 4: Peak activity hour (across the whole dataset, by hour-of-day)
# -----------------------------------------------------------------
print("\n--- Peak Activity Hour (summed across all grids/days, by hour-of-day) ---")
peak_hour_df = (
    hourly_grid_summary
    .groupBy("hour")
    .agg(spark_sum("total_activity").alias("total_activity_at_hour"))
    .orderBy(col("total_activity_at_hour").desc())
)
peak_hour_df.show(24, truncate=False)

# -----------------------------------------------------------------
# Step 5: daily_traffic_summary — one row per date
# -----------------------------------------------------------------
daily_traffic_summary = (
    hourly_grid_summary
    .groupBy("date")
    .agg(
        spark_sum("total_activity").alias("total_activity"),
        spark_sum("total_sms").alias("total_sms"),
        spark_sum("total_calls").alias("total_calls"),
        spark_sum("internet_activity").alias("total_internet"),
        spark_avg("internet_share").alias("avg_internet_share"),
    )
    .orderBy("date")
)
print("\n--- daily_traffic_summary ---")
daily_traffic_summary.show(truncate=False)

# -----------------------------------------------------------------
# Step 6: Hotspot ranking per day (top 10 grids per date, using a
# window function — this is the "window function concepts" requirement)
# -----------------------------------------------------------------
print("\n--- Hotspot Ranking: Top 10 grids PER DAY (window function) ---")
daily_grid_totals = (
    hourly_grid_summary
    .groupBy("date", "grid_id")
    .agg(spark_sum("total_activity").alias("daily_grid_activity"))
)

rank_window = Window.partitionBy("date").orderBy(col("daily_grid_activity").desc())
hotspot_ranking = (
    daily_grid_totals
    .withColumn("rank", row_number().over(rank_window))
    .filter(col("rank") <= 10)
    .orderBy("date", "rank")
)
hotspot_ranking.show(30, truncate=False)

# -----------------------------------------------------------------
# Step 7: Persist outputs
# -----------------------------------------------------------------
hourly_grid_summary.write.mode("overwrite").option("header", True).csv("hourly_grid_summary")
daily_traffic_summary.write.mode("overwrite").option("header", True).csv("daily_traffic_summary")
hotspot_ranking.write.mode("overwrite").option("header", True).csv("hotspot_ranking")

print("\nSP3 complete. Outputs written: hourly_grid_summary/, daily_traffic_summary/, hotspot_ranking/")
print(f"hourly_grid_summary is ready for SP4, DE6, RE2 — {total_summary_rows} rows, "
      f"grain verified as exactly one record per grid_id + timestamp.")

# -----------------------------------------------------------------
# Step 8: Investigate the 6 missing grid+hour combinations
# (expected 1,680,000 but got 1,679,994)
# -----------------------------------------------------------------
if total_summary_rows != expected_rows:
    print(f"\n--- Investigating {expected_rows - total_summary_rows} missing grid+hour combinations ---")

    all_grids = spark.range(1, 10001).withColumnRenamed("id", "grid_id")
    all_hours = hourly_grid_summary.select("timestamp").distinct()
    full_grid_hour_space = all_grids.crossJoin(all_hours)

    missing = full_grid_hour_space.join(
        hourly_grid_summary.select("grid_id", "timestamp"),
        on=["grid_id", "timestamp"],
        how="left_anti",
    )
    print(f"Missing combinations found: {missing.count()}")
    missing.orderBy("timestamp", "grid_id").show(20, truncate=False)
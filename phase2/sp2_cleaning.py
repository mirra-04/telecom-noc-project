"""
SP2 — Cleaning & Standardization

Takes SP1's raw, multi-file Spark DataFrame and produces a trusted,
cleaned DataFrame: canonical names, correct types, quarantined bad
rows (not silently dropped), and curated-layer activity measures.

Expected output: clean_network_df, a rejected-record summary, and a
null-handling report.
"""

import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, count, isnan, isnull, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

DATA_PATH = sorted(glob.glob("../data/sms-call-internet-mi-*.csv"))

spark = SparkSession.builder.appName("SP2-Cleaning").getOrCreate()
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

# -----------------------------------------------------------------
# Step 1: Load + rename to canonical names (SP1's job, repeated here
# so this script is self-contained)
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
)

initial_count = raw_df.count()
print(f"Initial row count (raw, renamed, RAW measures preserved): {initial_count}")

# -----------------------------------------------------------------
# Step 2: Cast types — timestamp to real datetime, activity columns
# to double (already double from schema, but cast defensively in
# case this ever runs against inferSchema-loaded data instead)
# -----------------------------------------------------------------
ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

typed_df = raw_df.withColumn("timestamp", to_timestamp(col("timestamp")))
for c in ACTIVITY_COLUMNS:
    typed_df = typed_df.withColumn(c, col(c).cast("double"))

# verify cadence still holds across all 7 files: expect 168 unique hours
unique_hours = typed_df.select("timestamp").distinct().count()
print(f"Unique hourly timestamps across all files: {unique_hours} (expected 168 = 7 days x 24 hours)")
assert unique_hours == 168, "Cadence check failed — unexpected number of unique hours"

# -----------------------------------------------------------------
# Step 3: Quarantine bad rows — do NOT silently drop.
# Bad = missing grid_id/timestamp, OR any negative activity value.
# -----------------------------------------------------------------
bad_id_condition = col("grid_id").isNull() | col("timestamp").isNull()

negative_condition = None
for c in ACTIVITY_COLUMNS:
    # IMPORTANT: col(c) < 0 evaluates to NULL (not False) when col(c) is
    # NULL, due to Spark's three-valued logic. Since nulls here are
    # expected/valid (NP1 finding: null = no activity, not bad data),
    # we must explicitly exclude nulls from the negative check —
    # otherwise filter() silently drops NULL-condition rows from BOTH
    # accepted_df and quarantined_df.
    cond = col(c).isNotNull() & (col(c) < 0)
    negative_condition = cond if negative_condition is None else (negative_condition | cond)

reject_condition = bad_id_condition | negative_condition

quarantined_df = typed_df.filter(reject_condition).withColumn(
    "reject_reason",
    when(bad_id_condition, lit("missing_grid_id_or_timestamp"))
    .when(negative_condition, lit("negative_activity_value"))
    .otherwise(lit("unknown")),
)
accepted_df = typed_df.filter(~reject_condition)

quarantined_count = quarantined_df.count()
accepted_count = accepted_df.count()

print(f"\n--- Rejected-Record Summary ---")
print(f"Rows accepted: {accepted_count}")
print(f"Rows quarantined: {quarantined_count}")
print(f"Total (should equal initial): {accepted_count + quarantined_count} vs {initial_count}")
assert accepted_count + quarantined_count == initial_count, "Row counts don't reconcile!"

print("\nQuarantine breakdown by reason:")
quarantined_df.groupBy("reject_reason").count().show()

quarantined_df.write.mode("overwrite").option("header", True).csv("quarantined_rows")
print("Quarantined rows written to quarantined_rows/ (for inspection)")

# -----------------------------------------------------------------
# Step 4: Null-handling report — profile blanks BEFORE applying the
# curated-layer null-to-zero rule, so raw nullness is documented.
# -----------------------------------------------------------------
print("\n--- Null-Handling Report (on accepted rows, RAW measures, before null->0) ---")
null_counts_before = accepted_df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in ACTIVITY_COLUMNS
])
null_counts_before.show()

null_report = {row[0]: row.asDict() for row in [null_counts_before.collect()[0]]}
print("Raw null counts per activity column (these represent 'no measurable activity "
      "from this country code in this cell/hour', per the NP1 finding):")
for c in ACTIVITY_COLUMNS:
    n = null_counts_before.collect()[0][c]
    print(f"  {c}: {n} nulls out of {accepted_count} accepted rows")

# Now apply the curated-layer null-to-zero rule — this is where the
# rule is actually applied, only AFTER the raw nullness above has
# been measured and reported.
curated_df = accepted_df
for c in ACTIVITY_COLUMNS:
    curated_df = curated_df.withColumn(
        c + "_curated", when(col(c).isNull(), lit(0.0)).otherwise(col(c))
    )

print("\nCurated-layer rule applied: nulls in the ACTIVITY_COLUMNS converted to 0.0 "
      "in new *_curated columns. Original raw columns are retained UNCHANGED "
      "alongside them, per the raw-preservation requirement.")

# -----------------------------------------------------------------
# Step 5: total_sms / total_calls / total_activity — built from the
# curated (null->0) columns, while keeping the raw originals intact.
# -----------------------------------------------------------------
clean_network_df = (
    curated_df
    .withColumn("total_sms", col("sms_in_curated") + col("sms_out_curated"))
    .withColumn("total_calls", col("call_in_curated") + col("call_out_curated"))
    .withColumn(
        "total_activity",
        col("total_sms") + col("total_calls") + col("internet_activity_curated"),
    )
)

# -----------------------------------------------------------------
# Step 6: Derive date, hour, day_of_week
# -----------------------------------------------------------------
from pyspark.sql.functions import to_date, hour, date_format

clean_network_df = (
    clean_network_df
    .withColumn("date", to_date(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
    .withColumn("day_of_week", date_format(col("timestamp"), "EEEE"))
)

print("\n--- clean_network_df schema ---")
clean_network_df.printSchema()

final_count = clean_network_df.count()
print(f"\n--- Before/After Summary ---")
print(f"Initial raw rows loaded : {initial_count}")
print(f"Rows quarantined        : {quarantined_count}")
print(f"Rows in clean_network_df: {final_count}")
print(f"Reconciles              : {final_count == accepted_count}")

print("\nSample of clean_network_df (5 rows):")
clean_network_df.select(
    "grid_id", "timestamp", "sms_in", "sms_in_curated",
    "total_sms", "total_calls", "total_activity", "day_of_week"
).show(5, truncate=False)

print("\nSP2 complete. clean_network_df is ready for SP3.")

nov1 = clean_network_df.filter(col("date") == "2013-11-01")
print("Nov 1 row count in Spark:", nov1.count())
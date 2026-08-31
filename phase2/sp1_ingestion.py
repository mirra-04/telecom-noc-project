"""
SP1 — Distributed Ingestion

Reads all daily Milan telecom activity files with Spark, compares
manual schema vs inference, and produces traceability + partition
diagnostics.

Expected output: raw_network_df (a Spark DataFrame), a validated
schema, and a file-level row count report.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, countDistinct
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

DATA_PATH = "../data/sms-call-internet-mi-*.csv"  # the -mi- glob matters — excludes any -tn- files

# -----------------------------------------------------------------
# Step 1: Create SparkSession
# -----------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SP1-DistributedIngestion")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # quiet down Spark's default chatty logging

print(f"Spark version: {spark.version}")

# -----------------------------------------------------------------
# Step 2a: Read with inferSchema=True — Spark reads the data TWICE:
# once to guess types, once to actually load it.
# -----------------------------------------------------------------
print("\n" + "=" * 60)
print("READ 1: inferSchema=True")
print("=" * 60)

start = time.time()
df_inferred = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
)
inferred_count = df_inferred.count()  # forces the actual read (lazy evaluation)
inferred_time = time.time() - start

print(f"inferSchema=True took {inferred_time:.2f}s for {inferred_count} rows")
print("Inferred schema:")
df_inferred.printSchema()

# -----------------------------------------------------------------
# Step 2b: Read with a manually defined StructType — Spark reads
# the data ONCE, because it already knows the types.
# -----------------------------------------------------------------
manual_schema = StructType([
    StructField("datetime", StringType(), True),     # kept as string here;
                                                       # parsed to timestamp in SP2 (cleaning stage)
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

print("\n" + "=" * 60)
print("READ 2: manual StructType")
print("=" * 60)

start = time.time()
raw_network_df = (
    spark.read
    .option("header", True)
    .schema(manual_schema)
    .csv(DATA_PATH)
    .withColumn("source_file", input_file_name())  # traceability, per SP1 requirement
)
manual_count = raw_network_df.count()
manual_time = time.time() - start

print(f"manual schema took {manual_time:.2f}s for {manual_count} rows")
print("Manual schema:")
raw_network_df.printSchema()

print(f"""
--- Schema comparison summary ---
inferSchema=True : {inferred_time:.2f}s  (Spark reads the file TWICE — once to sample
                    and guess types, once to actually load — extra pass = extra cost)
manual StructType: {manual_time:.2f}s  (Spark reads the file ONCE, since types are
                    already known up front — faster and predictable)
Row counts match : {inferred_count == manual_count}  ({inferred_count} vs {manual_count})
""")

# -----------------------------------------------------------------
# Step 3: Counts — rows, source files, unique grids, country codes, hours
# -----------------------------------------------------------------
print("=" * 60)
print("DATASET COUNTS")
print("=" * 60)

total_rows = raw_network_df.count()
num_source_files = raw_network_df.select("source_file").distinct().count()
unique_grids = raw_network_df.select("CellID").distinct().count()
unique_country_codes = raw_network_df.select("countrycode").distinct().count()
unique_hours = raw_network_df.select("datetime").distinct().count()

print(f"Total rows: {total_rows}")
print(f"Source files: {num_source_files}")
print(f"Unique grid IDs (CellID): {unique_grids}")
print(f"Unique country codes: {unique_country_codes}")
print(f"Unique hourly timestamps: {unique_hours}")

# -----------------------------------------------------------------
# Step 3b: File-level row count report
# -----------------------------------------------------------------
print("\n--- File-level row count report ---")
raw_network_df.groupBy("source_file").count().orderBy("source_file").show(truncate=False)

# -----------------------------------------------------------------
# Step 4: Partition inspection
# -----------------------------------------------------------------
print("=" * 60)
print("PARTITION DIAGNOSTICS")
print("=" * 60)

num_partitions = raw_network_df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")
print(f"""
Why this matters: Spark splits work across partitions to process data
in parallel. With {num_source_files} input files, Spark by default tends
to create partitions roughly aligned to file splits/block size — too FEW
partitions means poor parallelism (some cores sit idle), too MANY means
excessive scheduling overhead for tiny amounts of work per task. File
layout (number of files, file sizes) directly drives this, which is why
production pipelines often repartition() or coalesce() explicitly rather
than relying on whatever the raw file layout happens to produce.
""")

# -----------------------------------------------------------------
# Step 5: Confirm raw grain has NOT been collapsed
# (Learner Validation check — Spark must not have prematurely
# aggregated country-code rows)
# -----------------------------------------------------------------
print("=" * 60)
print("GRAIN CHECK — confirming Spark has not collapsed country-code rows")
print("=" * 60)

expected_min_rows = unique_grids * unique_hours  # would be the collapsed grid+hour row count
print(f"If Spark HAD collapsed to grid+hour grain, we'd expect close to: {unique_grids * 24 * num_source_files} rows")
print(f"Actual total rows: {total_rows}")
print(f"Actual rows are much larger than the collapsed estimate -> grain preserved: "
      f"{total_rows > unique_grids * 24 * num_source_files}")

sample_grid = raw_network_df.select("CellID").first()["CellID"]
sample_grid_rows = raw_network_df.filter(col("CellID") == sample_grid).groupBy("datetime").count()
print(f"\nRow counts per hour for a sample grid_id={sample_grid} "
      f"(should show MULTIPLE rows per hour, one per country code, not 1):")
sample_grid_rows.orderBy("datetime").show(5, truncate=False)

# -----------------------------------------------------------------
# Step 6: Intentional break — change a field type and observe
# what happens. Run this section, read the output, then decide
# whether Spark errored or silently coerced.
# -----------------------------------------------------------------
print("=" * 60)
print("INTENTIONAL SCHEMA BREAK TEST")
print("=" * 60)

broken_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", StringType(), True),   # <-- deliberately wrong: should be IntegerType
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True),
])

broken_df = (
    spark.read
    .option("header", True)
    .schema(broken_schema)
    .csv(DATA_PATH)
)
print("Schema with CellID intentionally set to StringType instead of IntegerType:")
broken_df.printSchema()
broken_df.select("CellID").show(3)
print("""
Observe: Spark did NOT throw an error here — CellID just silently loaded
as a string instead of a number. This is the real risk of getting schema
types wrong: no crash, no warning, just quietly wrong types that could
break downstream numeric operations (sorting, math, joins) without any
obvious signal that something's off.
""")

print("\nSP1 complete.")
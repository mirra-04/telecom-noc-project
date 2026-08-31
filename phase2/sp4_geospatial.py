"""
SP4 — Geospatial Enrichment Using the Milan Grid

Joins hourly_grid_summary (SP3's output) with milano-grid.geojson to
attach real geographic boundaries to each grid_id.

CRITICAL: the join key is properties.cellId (1-based), NOT the
top-level "id" field (0-based). Using "id" silently shifts every
grid cell by one — this is the single highest-risk defect in the
whole project (flagged explicitly in the trainer guide).

Expected output: grid_activity_geo_df, an enrichment coverage
report, a list of unmatched grid_ids (if any), and top high-activity
grids with geometry retained.
"""

import glob
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, hour, date_format, when, lit, broadcast,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

DATA_PATH = sorted(glob.glob("../data/sms-call-internet-mi-*.csv"))
GEOJSON_PATH = "../data/milano-grid.geojson"

spark = SparkSession.builder.appName("SP4-GeoEnrichment").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]

# -----------------------------------------------------------------
# Step 1: Rebuild hourly_grid_summary (SP3's output) so this script
# is self-contained
# -----------------------------------------------------------------
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

hourly_grid_summary = (
    raw_df
    .groupBy("grid_id", "timestamp")
    .agg(
        spark_sum("sms_in_z").alias("sms_in"),
        spark_sum("sms_out_z").alias("sms_out"),
        spark_sum("call_in_z").alias("call_in"),
        spark_sum("call_out_z").alias("call_out"),
        spark_sum("internet_activity_z").alias("internet_activity"),
    )
    .withColumn("total_activity",
                col("sms_in") + col("sms_out") + col("call_in") + col("call_out") + col("internet_activity"))
)

activity_row_count = hourly_grid_summary.count()
distinct_activity_grids = hourly_grid_summary.select("grid_id").distinct().count()
print(f"hourly_grid_summary: {activity_row_count} rows, {distinct_activity_grids} distinct grid_ids")

# -----------------------------------------------------------------
# Step 2: Load and inspect milano-grid.geojson structure
# -----------------------------------------------------------------
with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    geojson = json.load(f)

print(f"\n--- GeoJSON structure inspection ---")
print(f"Top-level type: {geojson['type']}")
print(f"Number of features: {len(geojson['features'])}")

first_feature = geojson["features"][0]
print(f"\nFirst feature keys: {list(first_feature.keys())}")
print(f"First feature 'properties': {first_feature['properties']}")
print(f"First feature top-level 'id' (if present): {first_feature.get('id', 'NOT PRESENT')}")
print(f"First feature geometry type: {first_feature['geometry']['type']}")

# -----------------------------------------------------------------
# Step 3: THE CRITICAL STEP — build the grid_id -> geometry lookup
# using properties.cellId, NEVER the top-level "id" field.
# -----------------------------------------------------------------
print("\n--- Building grid lookup: properties.cellId -> grid_id (1-based, CORRECT) ---")

lookup_records = []
mismatch_examples = []

def compute_centroid_from_ring(ring):
    """Simple average of ring coordinates — a lightweight centroid
    approximation, sufficient for map markers / API responses.
    Not a true geometric (area-weighted) centroid; documented here
    for honesty rather than implying more precision than this gives."""
    lons = [pt[0] for pt in ring]
    lats = [pt[1] for pt in ring]
    return sum(lons) / len(lons), sum(lats) / len(lats)

for feature in geojson["features"]:
    cell_id = feature["properties"]["cellId"]   # <-- the correct key
    top_level_id = feature.get("id")            # <-- the WRONG key, captured only to prove the trap
    geometry = feature["geometry"]

    # coordinates for a Polygon: [ [ [lon, lat], [lon, lat], ... ] ]
    coords = geometry["coordinates"][0][0] if geometry["type"] == "Polygon" else None
    ring = geometry["coordinates"][0] if geometry["type"] == "Polygon" else []
    centroid_lon, centroid_lat = compute_centroid_from_ring(ring) if ring else (None, None)

    lookup_records.append({
        "grid_id": cell_id,
        "geometry_type": geometry["type"],
        "coordinates_json": json.dumps(geometry["coordinates"]),
        "centroid_lon": centroid_lon,
        "centroid_lat": centroid_lat,
    })

    if top_level_id is not None and top_level_id != cell_id:
        mismatch_examples.append((top_level_id, cell_id))

print(f"Sample mismatch between top-level 'id' and properties.cellId "
      f"(proves why 'id' must NEVER be used as the join key):")
for top_id, cell_id in mismatch_examples[:5]:
    print(f"  top-level id={top_id}  vs  properties.cellId={cell_id}  (off by {cell_id - top_id})")

grid_lookup_schema = StructType([
    StructField("grid_id", IntegerType(), False),
    StructField("geometry_type", StringType(), True),
    StructField("coordinates_json", StringType(), True),
    StructField("centroid_lon", DoubleType(), True),
    StructField("centroid_lat", DoubleType(), True),
])

# IMPORTANT: spark.createDataFrame() on a Python list still requires a
# Python worker process to serialize the data into Spark (even with no
# UDF involved) — and that's exactly what's timing out on this machine
# (likely a firewall/antivirus blocking the local loopback socket Spark
# needs). To avoid Python workers entirely, we write the lookup data to
# a plain local file ourselves, then have Spark READ it back using its
# native JVM-based file reader, which needs no Python worker at all.
import os
lookup_json_path = "grid_lookup_temp.jsonl"
with open(lookup_json_path, "w", encoding="utf-8") as f:
    for record in lookup_records:
        f.write(json.dumps(record) + "\n")

grid_lookup_df = spark.read.schema(grid_lookup_schema).json(lookup_json_path)

lookup_count = grid_lookup_df.count()
print(f"\nGrid lookup size: {lookup_count} rows (expected 10,000)")
print(f"Activity DataFrame size: {activity_row_count} rows "
      f"(lookup is ~{activity_row_count // lookup_count}x smaller -> broadcast candidate)")

print("\nSample grid lookup with centroids (computed in plain Python, no Spark UDF needed):")
grid_lookup_df.select("grid_id", "geometry_type", "centroid_lon", "centroid_lat").show(5)

# (Step 4 — centroid computation — already done above in plain Python,
# no Spark UDF needed. This avoids company-laptop firewall/antivirus
# issues that can block the local socket Spark's Python UDF workers
# need to establish.)

# -----------------------------------------------------------------
# Step 5: LEFT join (deliberately, not inner) — see justification below
# Broadcast the small grid_lookup_df (10,000 rows) against the large
# activity DataFrame (millions of rows).
# -----------------------------------------------------------------
print("\n--- Standard join plan ---")
standard_join = hourly_grid_summary.join(grid_lookup_df, on="grid_id", how="left")
standard_join.explain()

print("\n--- Broadcast join plan ---")
broadcast_join = hourly_grid_summary.join(broadcast(grid_lookup_df), on="grid_id", how="left")
broadcast_join.explain()

print("""
Why LEFT join, not INNER:
An INNER join would silently DROP any activity row whose grid_id has no
matching geometry — those rows would simply vanish with no trace, no
warning. A LEFT join keeps every activity row regardless of match status,
and any row lacking geometry ends up with NULLs there instead of
disappearing. This makes the failure VISIBLE (as null geometry we can
count and report) rather than SILENT (as missing rows we might never
notice). Given the whole project's theme of catching silent data loss,
left join is the safer, more honest default here.

Why broadcast join is appropriate:
grid_lookup_df has only 10,000 rows; hourly_grid_summary has ~1.68 million.
A standard (shuffle) join would redistribute BOTH large datasets across
the cluster to co-locate matching keys — expensive. A broadcast join instead
sends the small lookup table (10,000 rows) to every executor as-is, so each
executor can join locally without any shuffle of the large dataset at all.
The size asymmetry here (roughly 168x) is exactly the kind of situation
broadcast joins are designed for.
""")

grid_activity_geo_df = broadcast_join

# -----------------------------------------------------------------
# Step 6: Validate the join — numerically
# -----------------------------------------------------------------
distinct_grids_before = distinct_activity_grids
distinct_grids_after = grid_activity_geo_df.select("grid_id").distinct().count()
rows_before = activity_row_count
rows_after = grid_activity_geo_df.count()
missing_geometry_count = grid_activity_geo_df.filter(col("geometry_type").isNull()).count()
missing_geometry_grids = (
    grid_activity_geo_df.filter(col("geometry_type").isNull())
    .select("grid_id").distinct()
)
coverage_pct = 100.0 * (rows_after - missing_geometry_count) / rows_after

print(f"\n--- Enrichment Coverage Report ---")
print(f"Distinct activity grid_ids before join: {distinct_grids_before}")
print(f"Distinct grid_ids after join: {distinct_grids_after}")
print(f"Row count before join: {rows_before}")
print(f"Row count after join: {rows_after}")
print(f"Row count unchanged (as expected for LEFT join): {rows_before == rows_after}")
print(f"Rows with missing geometry: {missing_geometry_count}")
print(f"Coverage: {coverage_pct:.4f}%")

print("\nUnmatched grid_ids (if any):")
missing_geometry_grids.show(50)

# -----------------------------------------------------------------
# Step 7: Validate the join GEOGRAPHICALLY, not just numerically.
# A high coverage percentage alone does not prove the join used the
# right key — it only proves keys matched something. We must confirm
# the geometry attached to a KNOWN busy grid actually sits inside
# Milan's real geographic bounds.
# -----------------------------------------------------------------
print("\n--- Geographic Spot-Check (not just numeric coverage) ---")
# Milan city center is approximately lat 45.46, lon 9.19
top_grid_row = (
    grid_activity_geo_df
    .groupBy("grid_id")
    .agg(spark_sum("total_activity").alias("week_total"))
    .orderBy(col("week_total").desc())
    .limit(1)
    .join(grid_lookup_df, on="grid_id")
    .select("grid_id", "centroid_lon", "centroid_lat")
    .collect()[0]
)
print(f"Busiest grid_id={top_grid_row['grid_id']}: "
      f"centroid = (lon={top_grid_row['centroid_lon']:.4f}, lat={top_grid_row['centroid_lat']:.4f})")
print(f"Milan city center is approximately (lon=9.19, lat=45.46).")
lon_diff = abs(top_grid_row['centroid_lon'] - 9.19)
lat_diff = abs(top_grid_row['centroid_lat'] - 45.46)
print(f"Difference from city center: lon_diff={lon_diff:.4f}, lat_diff={lat_diff:.4f}")
print("A busy grid landing reasonably close to central Milan (not off the coast, "
      "not in another country) is a strong sign the join key is correct. "
      "If we had used the WRONG key (top-level 'id'), every grid's geometry "
      "would be shifted by one cell — small in absolute distance, but easy "
      "to confirm is wrong by cross-referencing against known city landmarks "
      "at the grid boundary, not just checking that SOME geometry attached.")

# -----------------------------------------------------------------
# Step 8: Top high-activity grids with geometry retained
# -----------------------------------------------------------------
print("\n--- Top 10 High-Activity Grids WITH Geometry Retained ---")
top_grids_geo = (
    grid_activity_geo_df
    .groupBy("grid_id", "centroid_lon", "centroid_lat")
    .agg(spark_sum("total_activity").alias("week_total_activity"))
    .orderBy(col("week_total_activity").desc())
    .limit(10)
)
top_grids_geo.show(truncate=False)

# -----------------------------------------------------------------
# Step 9: Final enriched dataset — the required column set
# -----------------------------------------------------------------
final_enriched_df = grid_activity_geo_df.select(
    "timestamp", "grid_id", "sms_in", "sms_out", "call_in", "call_out",
    "internet_activity", "total_activity", "geometry_type", "coordinates_json",
    "centroid_lon", "centroid_lat",
)

final_enriched_df.write.mode("overwrite").option("header", True).csv("grid_activity_geo_sample")
top_grids_geo.write.mode("overwrite").option("header", True).csv("top_grids_with_geometry")

print(f"""
--- Notes for later phases ---
1. grid_id is a GEOGRAPHIC CELL identifier — one of 10,000 squares in a
   fixed map grid over Milan. It is NOT a phone tower, base station (BTS),
   or physical network asset. Multiple towers could plausibly serve one
   grid cell, or none directly — the grid is an analysis unit, not
   infrastructure.
2. Full polygon geometry (coordinates_json) should NOT be duplicated into
   every fact row of the future data warehouse (DE6) — that would repeat
   the same ~dozens of coordinate pairs across 1.68 million+ rows. The
   warehouse should instead keep a separate small grid dimension table
   (10,000 rows) and join geometry in only when needed for display,
   exactly as this lab treats it as a broadcastable lookup, not a
   per-row payload.

SP4 complete.
""")
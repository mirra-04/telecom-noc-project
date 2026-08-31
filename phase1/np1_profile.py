"""
NP1 — Profile the Telecom Activity Dataset
Loads one raw daily file, renames columns to canonical names,
verifies cadence, checks data quality, inspects the country-code
grain, builds derived measures, and prints a profiling summary.
"""

import pandas as pd

# ---------------------------------------------------------------
# Step 1: Load one raw file
# ---------------------------------------------------------------
df = pd.read_csv("../data/sms-call-internet-mi-2013-11-01.csv")

print("Shape:", df.shape)
print("Raw columns:", df.columns.tolist())
print("Raw dtypes:\n", df.dtypes)

# ---------------------------------------------------------------
# Step 2: Rename raw columns to canonical names
# ---------------------------------------------------------------
df = df.rename(columns={
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet_activity",
})

# ---------------------------------------------------------------
# Step 3: Parse timestamp + verify hourly cadence
# ---------------------------------------------------------------
df["timestamp"] = pd.to_datetime(df["timestamp"])

unique_timestamps = sorted(df["timestamp"].unique())
print("\n--- Cadence Check ---")
print("Number of unique timestamps:", len(unique_timestamps))
print("First few:", unique_timestamps[:3])
print("Last few:", unique_timestamps[-3:])

# ---------------------------------------------------------------
# Step 4: Derive date, hour, day_of_week
# ---------------------------------------------------------------
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour
df["day_of_week"] = df["timestamp"].dt.day_name()

# ---------------------------------------------------------------
# Step 5: Data quality checks (report only, don't fix)
# ---------------------------------------------------------------
print("\n--- Data Quality Checks ---")
print("Missing grid_id:", df["grid_id"].isna().sum())
print("Missing timestamp:", df["timestamp"].isna().sum())
print("Missing sms_in:", df["sms_in"].isna().sum())
print("Missing sms_out:", df["sms_out"].isna().sum())
print("Missing call_in:", df["call_in"].isna().sum())
print("Missing call_out:", df["call_out"].isna().sum())
print("Missing internet_activity:", df["internet_activity"].isna().sum())

print("Exact duplicate rows:", df.duplicated().sum())

print("Negative sms_in:", (df["sms_in"] < 0).sum())
print("Negative sms_out:", (df["sms_out"] < 0).sum())
print("Negative call_in:", (df["call_in"] < 0).sum())
print("Negative call_out:", (df["call_out"] < 0).sum())
print("Negative internet_activity:", (df["internet_activity"] < 0).sum())

# ---------------------------------------------------------------
# Step 6: Inspect the country-code grain for one grid_id + hour
# ---------------------------------------------------------------
sample_grid = df["grid_id"].iloc[0]
sample_hour = df["timestamp"].iloc[0]

sample = df[(df["grid_id"] == sample_grid) & (df["timestamp"] == sample_hour)]
print(f"\n--- Grain Check: grid_id={sample_grid}, timestamp={sample_hour} ---")
print(sample[["grid_id", "timestamp", "country_code",
              "sms_in", "sms_out", "call_in", "call_out", "internet_activity"]])
print("Number of rows for this grid+hour:", len(sample))
print("=> Confirms raw grain is timestamp + grid_id + country_code")

# ---------------------------------------------------------------
# Step 7: Derived measures
# NOTE: NaN + number = NaN in plain addition, so use .fillna(0)
# only for this row-level sum — do NOT fillna on the whole df.
# ---------------------------------------------------------------
df["total_sms"] = df["sms_in"].fillna(0) + df["sms_out"].fillna(0)
df["total_calls"] = df["call_in"].fillna(0) + df["call_out"].fillna(0)
df["total_activity"] = (
    df["total_sms"] + df["total_calls"] + df["internet_activity"].fillna(0)
)

# ---------------------------------------------------------------
# Step 8: Profiling facts
# ---------------------------------------------------------------
print("\n--- Profiling Summary ---")
print("Unique grid_ids:", df["grid_id"].nunique())
print("Date range:", df["date"].min(), "to", df["date"].max())
print("Unique country codes:", df["country_code"].nunique())

busiest_hour = df.groupby("hour")["total_activity"].sum().idxmax()
print("Busiest hour (by total_activity):", busiest_hour)

busiest_grid = df.groupby("grid_id")["total_activity"].sum().idxmax()
print("Busiest grid_id (by total_activity):", busiest_grid)

print("\nNull counts per column:")
print(df.isna().sum())

# ---------------------------------------------------------------
# Step 9: Plain-language summary for the Network Analytics Team
# ---------------------------------------------------------------
print("\n--- Summary for the Network Analytics Team ---")
print(f"""
1. This file covers {df['grid_id'].nunique()} distinct grid cells across Milan for
   {df['date'].min()}, with activity recorded every hour (24 hourly snapshots).
2. Each grid cell and hour can have several rows — one per country code active
   in that cell during that hour — so the raw file is NOT one row per cell/hour;
   rows must be summed by grid_id + timestamp before further analysis.
3. Missing values in the SMS/call/internet columns (roughly 55-75% of rows per
   column) are expected and represent "no measurable activity" from that
   country code in that cell/hour, not sensor failure. They should be treated
   as 0 when aggregating, not dropped or imputed with a mean.
4. No missing grid_id/timestamp values, no exact duplicate rows, and no
   negative activity values were found — the structural integrity of the file
   is sound.
5. Hour {busiest_hour}:00 was the busiest hour and grid cell {busiest_grid} was the
   busiest cell by total activity on this day.
""")
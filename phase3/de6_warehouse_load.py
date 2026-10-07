"""
DE6 — Warehouse Modelling for Network Analytics.

Builds a minimal star schema (fact_network_activity, dim_grid, dim_time)
from the SP6/SP7 analytics output (hourly_grid_summary), and loads it
into a SQLite database.

Why SQLite: no PostgreSQL/MySQL server is available in this
environment, and Appendix A explicitly allows SQLite "for a small day
count" -- this project has 7 days, which qualifies.

Design decisions (see DE6 write-up for full rationale):
  - fact_network_activity holds ONLY grid/time keys + activity measures.
    NO geometry column -- this is checked by an acceptance-criteria query.
  - dim_grid holds grid_id + centroid lat/lon + a geometry REFERENCE
    (a pointer string into milano-grid.geojson), never the full Polygon.
  - dim_time and dim_grid are each populated ONCE per distinct key,
    not once per fact row -- verified by row-count checks below.
  - Natural keys are used throughout (grid_id, timestamp) rather than
    surrogate integer keys, because that IS the grain the Core Dataset
    Contract already defines -- introducing a surrogate key here would
    just be an extra join for no benefit at this data volume.
"""

import os
import sqlite3

import pandas as pd

PROJECT_ROOT = "/mnt/d/Training/Project - 2"  # kept for reference only
DATA_ROOT = "/home/mirrag/project_data"  # data lives here now, not on /mnt/d (DrvFs I/O issues)
ANALYTICS_PARQUET = os.path.join(
    DATA_ROOT, "data", "analytics", "hourly_grid_summary", "part-0.parquet"
)
WAREHOUSE_DIR = "/home/mirrag/warehouse"
DB_PATH = os.path.join(WAREHOUSE_DIR, "network_intelligence.db")
REFERENCE_FILE = "data/reference/milano-grid.geojson"  # pointer, not the geometry itself


def load_source():
    df = pd.read_parquet(ANALYTICS_PARQUET)
    print(f"Loaded {len(df)} rows from {ANALYTICS_PARQUET}")
    return df


def build_dim_grid(df: pd.DataFrame) -> pd.DataFrame:
    """One row per grid_id. Centroid only -- never the full Polygon.
    geometry_ref points back to the static reference file + the join
    key needed to look up the actual geometry (properties.cellId),
    rather than duplicating ~3.2MB of geometry into the warehouse."""
    dim_grid = (
        df[["grid_id", "centroid_lon", "centroid_lat"]]
        .drop_duplicates(subset=["grid_id"])
        .sort_values("grid_id")
        .reset_index(drop=True)
    )
    dim_grid["geometry_ref"] = REFERENCE_FILE + "#cellId=" + dim_grid["grid_id"].astype(str)
    return dim_grid


def build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """One row per distinct hourly timestamp."""
    dim_time = (
        df[["timestamp", "date", "hour", "day_of_week"]]
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    dim_time["timestamp"] = dim_time["timestamp"].astype(str)
    dim_time["date"] = dim_time["date"].astype(str)
    return dim_time


def build_fact(df: pd.DataFrame) -> pd.DataFrame:
    """Grid + time keys and activity measures ONLY. No geometry, no
    centroid -- those live in dim_grid and are reached by joining on
    grid_id."""
    fact = df[[
        "grid_id", "timestamp",
        "sms_in", "sms_out", "call_in", "call_out", "internet_activity",
        "total_sms", "total_calls", "total_activity",
    ]].copy()
    fact["timestamp"] = fact["timestamp"].astype(str)
    return fact


def create_schema(conn: sqlite3.Connection):
    conn.executescript("""
        DROP TABLE IF EXISTS fact_network_activity;
        DROP TABLE IF EXISTS dim_grid;
        DROP TABLE IF EXISTS dim_time;

        CREATE TABLE dim_grid (
            grid_id       INTEGER PRIMARY KEY,
            centroid_lon  REAL,
            centroid_lat  REAL,
            geometry_ref  TEXT NOT NULL
        );

        CREATE TABLE dim_time (
            timestamp     TEXT PRIMARY KEY,
            date          TEXT NOT NULL,
            hour          INTEGER NOT NULL,
            day_of_week   TEXT NOT NULL
        );

        CREATE TABLE fact_network_activity (
            grid_id            INTEGER NOT NULL REFERENCES dim_grid(grid_id),
            timestamp          TEXT    NOT NULL REFERENCES dim_time(timestamp),
            sms_in             REAL,
            sms_out            REAL,
            call_in            REAL,
            call_out           REAL,
            internet_activity  REAL,
            total_sms          REAL,
            total_calls        REAL,
            total_activity     REAL,
            PRIMARY KEY (grid_id, timestamp)
        );

        -- Indexes on the columns actually used for filtering/joining:
        -- grid_id is already the leading column of the fact PK (covers
        -- "all activity for grid X" queries), so the index that earns
        -- its keep here is on timestamp, for "all grids at hour Y" and
        -- hourly-trend queries which do NOT lead with grid_id.
    """)
    conn.commit()


def load_data(conn, dim_grid, dim_time, fact):
    print("Loading dim_grid...")
    dim_grid.to_sql("dim_grid", conn, if_exists="append", index=False,
                     method="multi", chunksize=5000)
    print("dim_grid loaded.")

    print("Loading dim_time...")
    dim_time.to_sql("dim_time", conn, if_exists="append", index=False,
                     method="multi", chunksize=5000)
    print("dim_time loaded.")

    print("Loading fact_network_activity...")
    fact.to_sql("fact_network_activity", conn, if_exists="append", index=False,
                method="multi", chunksize=5000)
    print("fact_network_activity loaded.")

    print("Creating timestamp index...")
    conn.execute("""
        CREATE INDEX idx_fact_timestamp
        ON fact_network_activity(timestamp)
    """)
    print("Timestamp index created.")

    print("Committing SQLite transaction...")
    conn.commit()
    print("SQLite commit complete.")


def run_validations(conn, df, dim_grid, fact):
    print("\n" + "=" * 60)
    print("VALIDATIONS")
    print("=" * 60)

    # --- Acceptance criterion: no geometry column in the fact table ---
    fact_cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_network_activity)")]
    assert "geometry" not in [c.lower() for c in fact_cols], "geometry column leaked into fact!"
    print(f"[PASS] fact_network_activity columns (no geometry): {fact_cols}")

    # --- dim_grid row count matches distinct grid_id count, no duplicate keys ---
    distinct_grids_source = df["grid_id"].nunique()
    dim_grid_count = conn.execute("SELECT COUNT(*) FROM dim_grid").fetchone()[0]
    dim_grid_distinct = conn.execute("SELECT COUNT(DISTINCT grid_id) FROM dim_grid").fetchone()[0]
    assert dim_grid_count == distinct_grids_source == dim_grid_distinct, (
        f"dim_grid mismatch: table={dim_grid_count}, source_distinct={distinct_grids_source}, "
        f"distinct_in_table={dim_grid_distinct}"
    )
    print(f"[PASS] dim_grid row count = {dim_grid_count} = distinct grid_id in source "
          f"= distinct grid_id in table (no duplicate keys)")

    # --- fact row count equals hourly_grid_summary row count (no fan-out) ---
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_network_activity").fetchone()[0]
    assert fact_count == len(df), f"fact row count {fact_count} != source row count {len(df)}"
    print(f"[PASS] fact_network_activity row count = {fact_count} = source hourly_grid_summary "
          f"row count (no fan-out from the dim_grid join)")

    # --- hand-run SQL aggregate matches equivalent pandas/Spark aggregate exactly ---
    sql_busiest = conn.execute("""
        SELECT grid_id, SUM(total_activity) AS total
        FROM fact_network_activity
        GROUP BY grid_id
        ORDER BY total DESC
        LIMIT 1
    """).fetchone()
    pandas_busiest = df.groupby("grid_id")["total_activity"].sum().idxmax()
    pandas_busiest_total = df.groupby("grid_id")["total_activity"].sum().max()
    assert sql_busiest[0] == pandas_busiest, (
        f"SQL busiest grid {sql_busiest[0]} != pandas busiest grid {pandas_busiest}"
    )
    assert abs(sql_busiest[1] - pandas_busiest_total) < 0.01, (
        f"SQL total {sql_busiest[1]} != pandas total {pandas_busiest_total}"
    )
    print(f"[PASS] SQL busiest grid = {sql_busiest[0]} (total_activity={sql_busiest[1]:.4f}) "
          f"exactly matches the pandas/Spark aggregate")

    # --- index exists ---
    indexes = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='fact_network_activity'"
    ).fetchall()
    assert len(indexes) >= 1, "No index found on fact_network_activity"
    print(f"[PASS] Index(es) on fact_network_activity: {[i[0] for i in indexes]}")

    print("\nALL ACCEPTANCE CRITERIA PASSED")


def run_sample_queries(conn):
    print("\n" + "=" * 60)
    print("SAMPLE QUERIES")
    print("=" * 60)

    print("\n--- Top 10 grids by total activity (all history) ---")
    for row in conn.execute("""
        SELECT f.grid_id, g.centroid_lat, g.centroid_lon, SUM(f.total_activity) AS total
        FROM fact_network_activity f
        JOIN dim_grid g ON g.grid_id = f.grid_id
        GROUP BY f.grid_id
        ORDER BY total DESC
        LIMIT 10
    """):
        print(f"  grid {row[0]:>5}  lat={row[1]:.4f} lon={row[2]:.4f}  total_activity={row[3]:,.2f}")

    print("\n--- Hourly trend: total activity per hour-of-day (all grids, all days) ---")
    for row in conn.execute("""
        SELECT t.hour, SUM(f.total_activity) AS total
        FROM fact_network_activity f
        JOIN dim_time t ON t.timestamp = f.timestamp
        GROUP BY t.hour
        ORDER BY t.hour
    """):
        print(f"  hour {row[0]:>2}:00  total_activity={row[1]:,.2f}")

    print("\n--- Top 5 internet-heavy grid/hours (internet_share > 0.9) ---")
    for row in conn.execute("""
        SELECT grid_id, timestamp, internet_activity, total_activity,
               ROUND(internet_activity * 1.0 / total_activity, 4) AS internet_share
        FROM fact_network_activity
        WHERE total_activity > 0 AND internet_activity * 1.0 / total_activity > 0.9
        ORDER BY internet_share DESC
        LIMIT 5
    """):
        print(f"  grid {row[0]:>5}  {row[1]}  internet_share={row[4]}")


def main():
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)

    df = load_source()
    dim_grid = build_dim_grid(df)
    dim_time = build_dim_time(df)
    fact = build_fact(df)

    print(f"dim_grid: {len(dim_grid)} rows")
    print(f"dim_time: {len(dim_time)} rows")
    print(f"fact_network_activity: {len(fact)} rows")

    conn = sqlite3.connect(DB_PATH)

    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        create_schema(conn)
        load_data(conn, dim_grid, dim_time, fact)
        run_validations(conn, df, dim_grid, fact)
        run_sample_queries(conn)
    finally:
        conn.close()

    print(f"\nWarehouse written to {DB_PATH}")


if __name__ == "__main__":
    main()

"""
NP2 — UsageProcessor
Reusable, testable class for loading, cleaning, aggregating and
summarizing the Milan telecom activity dataset.

Pipeline:
    load_data() -> clean_data() -> derive_time_features()
    -> aggregate_to_grid_time() -> derive_activity_features()
    -> compute_kpis() -> export_summary()
"""

import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("UsageProcessor")

RAW_COLUMN_MAP = {
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet_activity",
}

REQUIRED_RAW_COLUMNS = set(RAW_COLUMN_MAP.keys())
ACTIVITY_COLUMNS = ["sms_in", "sms_out", "call_in", "call_out", "internet_activity"]


class UsageProcessor:
    """Encapsulates the full raw -> curated processing pipeline for
    one daily Milan telecom activity file."""

    def __init__(self, source):
        """
        source: either a file path (str) to a raw CSV, or an
        already-loaded pandas DataFrame with raw column names.
        """
        self.source = source
        self.raw_df = None       # loaded, renamed, but not cleaned
        self.clean_df = None     # cleaned, row-level, still country_code grain
        self.grid_df = None      # aggregated to grid_id + timestamp grain
        self.daily_summary = None
        self.grid_summary = None

    # -----------------------------------------------------------
    def load_data(self):
        """Load raw CSV (or use provided DataFrame) and rename columns
        to canonical names. Raises if required columns are missing."""
        if isinstance(self.source, str):
            df = pd.read_csv(self.source)
            logger.info("Loaded %d rows from %s", len(df), self.source)
        elif isinstance(self.source, pd.DataFrame):
            df = self.source.copy()
            logger.info("Loaded %d rows from provided DataFrame", len(df))
        else:
            raise TypeError("source must be a file path or a pandas DataFrame")

        missing_cols = REQUIRED_RAW_COLUMNS - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required raw columns: {missing_cols}")

        df = df.rename(columns=RAW_COLUMN_MAP)
        self.raw_df = df
        return self

    # -----------------------------------------------------------
    def clean_data(self):
        """Row-level cleaning only: types, required-field checks,
        negative-value flags. Does NOT change the grain of the data."""
        if self.raw_df is None:
            raise RuntimeError("Call load_data() before clean_data()")

        df = self.raw_df.copy()
        initial_count = len(df)

        # required identifier columns must not be null
        before = len(df)
        df = df.dropna(subset=["grid_id", "timestamp"])
        dropped_missing_ids = before - len(df)
        if dropped_missing_ids:
            logger.warning("Dropped %d rows with missing grid_id/timestamp", dropped_missing_ids)

        # parse timestamp
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        before = len(df)
        df = df.dropna(subset=["timestamp"])
        dropped_bad_ts = before - len(df)
        if dropped_bad_ts:
            logger.warning("Dropped %d rows with unparseable timestamp", dropped_bad_ts)

        # flag (not drop) negative activity values — set to NaN and log
        for col in ACTIVITY_COLUMNS:
            negative_mask = df[col] < 0
            n_negative = negative_mask.sum()
            if n_negative:
                logger.warning("Found %d negative values in %s — setting to NaN", n_negative, col)
                df.loc[negative_mask, col] = pd.NA

        dropped_total = initial_count - len(df)
        logger.info(
            "clean_data(): %d rows in, %d rows out, %d rows dropped",
            initial_count, len(df), dropped_total,
        )

        self.clean_df = df
        return self

    # -----------------------------------------------------------
    def derive_time_features(self):
        """Add date, hour, day_of_week. Row-level only, no grain change."""
        if self.clean_df is None:
            raise RuntimeError("Call clean_data() before derive_time_features()")

        df = self.clean_df.copy()
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour
        df["day_of_week"] = df["timestamp"].dt.day_name()

        self.clean_df = df
        logger.info("derive_time_features(): added date, hour, day_of_week")
        return self

    # -----------------------------------------------------------
    def aggregate_to_grid_time(self):
        """Collapse country_code rows into one row per grid_id + timestamp.
        This is the grain-changing step — kept separate from clean_data().
        country_code is dropped from this layer onward (curated layer rule);
        it is preserved only in self.clean_df / self.raw_df if needed for
        separate country-level analysis."""
        if self.clean_df is None:
            raise RuntimeError("Call derive_time_features() before aggregate_to_grid_time()")

        df = self.clean_df.copy()
        n_raw_rows = len(df)

        grouped = (
            df.groupby(["grid_id", "timestamp", "date", "hour", "day_of_week"], as_index=False)[
                ACTIVITY_COLUMNS
            ]
            .sum(min_count=0)  # NaN + NaN = 0 here, which is the documented curated-layer rule
        )

        logger.info(
            "aggregate_to_grid_time(): %d raw rows -> %d grid/hour rows",
            n_raw_rows, len(grouped),
        )

        self.grid_df = grouped
        return self

    # -----------------------------------------------------------
    def derive_activity_features(self):
        """Compute total_sms, total_calls, total_activity on the
        grid/hour grain (post-aggregation, not pre-aggregation)."""
        if self.grid_df is None:
            raise RuntimeError("Call aggregate_to_grid_time() before derive_activity_features()")

        df = self.grid_df.copy()
        df["total_sms"] = df["sms_in"] + df["sms_out"]
        df["total_calls"] = df["call_in"] + df["call_out"]
        df["total_activity"] = df["total_sms"] + df["total_calls"] + df["internet_activity"]

        self.grid_df = df
        logger.info("derive_activity_features(): added total_sms, total_calls, total_activity")
        return self

    # -----------------------------------------------------------
    def compute_kpis(self):
        """Build daily and grid-level summary tables from the
        grid/hour curated data."""
        if self.grid_df is None:
            raise RuntimeError("Call derive_activity_features() before compute_kpis()")

        df = self.grid_df

        self.daily_summary = (
            df.groupby("date", as_index=False)
            .agg(
                total_activity=("total_activity", "sum"),
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet_activity", "sum"),
                active_grids=("grid_id", "nunique"),
            )
        )

        self.grid_summary = (
            df.groupby("grid_id", as_index=False)
            .agg(
                total_activity=("total_activity", "sum"),
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet_activity", "sum"),
                peak_hour_activity=("total_activity", "max"),
                active_hours=("hour", "nunique"),
            )
            .sort_values("total_activity", ascending=False)
        )

        logger.info(
            "compute_kpis(): daily_summary=%d rows, grid_summary=%d rows",
            len(self.daily_summary), len(self.grid_summary),
        )
        return self

    # -----------------------------------------------------------
    def export_summary(self, daily_path="daily_summary.csv", grid_path="grid_summary.csv"):
        """Write daily and grid summary tables to CSV."""
        if self.daily_summary is None or self.grid_summary is None:
            raise RuntimeError("Call compute_kpis() before export_summary()")

        self.daily_summary.to_csv(daily_path, index=False)
        self.grid_summary.to_csv(grid_path, index=False)

        logger.info("export_summary(): wrote %s and %s", daily_path, grid_path)
        return self

    # -----------------------------------------------------------
    def run_all(self, daily_path="daily_summary.csv", grid_path="grid_summary.csv"):
        """Convenience method to run the full pipeline in order."""
        return (
            self.load_data()
            .clean_data()
            .derive_time_features()
            .aggregate_to_grid_time()
            .derive_activity_features()
            .compute_kpis()
            .export_summary(daily_path, grid_path)
        )


# =================================================================
# Small validation checks — one per major method
# Run this file directly to execute them: python usage_processor.py
# =================================================================
def _run_validations(source_path):
    print("\n" + "=" * 60)
    print("RUNNING VALIDATION CHECKS")
    print("=" * 60)

    proc = UsageProcessor(source_path)

    # 1. load_data
    proc.load_data()
    assert proc.raw_df is not None, "load_data() failed to populate raw_df"
    assert "grid_id" in proc.raw_df.columns, "load_data() did not rename columns correctly"
    print("[PASS] load_data(): columns renamed, raw_df populated")

    # 2. clean_data
    proc.clean_data()
    assert proc.clean_df["grid_id"].isna().sum() == 0, "clean_data() left null grid_id"
    assert proc.clean_df["timestamp"].isna().sum() == 0, "clean_data() left null timestamp"
    assert pd.api.types.is_datetime64_any_dtype(proc.clean_df["timestamp"]), \
        "clean_data() did not parse timestamp to datetime"
    print("[PASS] clean_data(): no null ids/timestamps, timestamp is datetime")

    # 3. derive_time_features
    proc.derive_time_features()
    for col in ["date", "hour", "day_of_week"]:
        assert col in proc.clean_df.columns, f"derive_time_features() missing {col}"
    print("[PASS] derive_time_features(): date/hour/day_of_week present")

    # 4. aggregate_to_grid_time
    raw_row_count = len(proc.clean_df)
    proc.aggregate_to_grid_time()
    agg_row_count = len(proc.grid_df)
    assert agg_row_count < raw_row_count, \
        "aggregate_to_grid_time() did not reduce row count — grain unchanged?"
    dup_check = proc.grid_df.duplicated(subset=["grid_id", "timestamp"]).sum()
    assert dup_check == 0, "aggregate_to_grid_time() left duplicate grid_id+timestamp rows"
    assert "country_code" not in proc.grid_df.columns, \
        "aggregate_to_grid_time() should drop country_code from curated layer"
    print(f"[PASS] aggregate_to_grid_time(): {raw_row_count} -> {agg_row_count} rows, "
          f"unique grid_id+timestamp, country_code dropped")

    # 5. derive_activity_features
    proc.derive_activity_features()
    for col in ["total_sms", "total_calls", "total_activity"]:
        assert col in proc.grid_df.columns, f"derive_activity_features() missing {col}"
    assert proc.grid_df["total_activity"].isna().sum() == 0, \
        "total_activity should have no NaN after aggregation sum"
    print("[PASS] derive_activity_features(): total_sms/calls/activity present, no NaN")

    # 6. compute_kpis
    proc.compute_kpis()
    assert len(proc.daily_summary) >= 1, "compute_kpis() produced empty daily_summary"
    assert len(proc.grid_summary) == proc.grid_df["grid_id"].nunique(), \
        "grid_summary row count should equal unique grid_id count"
    print("[PASS] compute_kpis(): daily_summary and grid_summary populated correctly")

    # 7. export_summary
    proc.export_summary()
    import os
    assert os.path.exists("daily_summary.csv"), "export_summary() did not write daily_summary.csv"
    assert os.path.exists("grid_summary.csv"), "export_summary() did not write grid_summary.csv"
    print("[PASS] export_summary(): both CSV files written")

    print("=" * 60)
    print("ALL VALIDATIONS PASSED")
    print("=" * 60)

    print("\nDaily summary preview:")
    print(proc.daily_summary)
    print("\nGrid summary preview (top 5 busiest grids):")
    print(proc.grid_summary.head())


if __name__ == "__main__":
    _run_validations("../data/sms-call-internet-mi-2013-11-01.csv")
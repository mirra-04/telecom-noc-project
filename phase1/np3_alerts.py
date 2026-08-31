"""
NP3 — Rule-Based Network Activity Alert Generator

Builds a within-day median baseline per grid_id, applies an activity
floor, and raises three transparent business-rule alerts:
    HIGH_ACTIVITY, ACTIVITY_SPIKE, ACTIVITY_DROP

This is a pre-ML operational alert layer: an alert is a request to
investigate, not a diagnosis.
"""

import logging
import pandas as pd

from usage_processor import UsageProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("AlertGenerator")

# -------------------------------------------------------------------
# Thresholds — chosen from the data, documented below.
# You (the learner) must look at the printed diagnostics and confirm
# or adjust these before trusting the alert output.
# -------------------------------------------------------------------
HIGH_ACTIVITY_RATIO = 1.5    # current must be >= 1.5x baseline
DROP_RATIO = 0.5             # current must be <= 0.5x baseline
SPIKE_RATIO_VS_PREV_HOUR = 2.0  # current must be >= 2x the previous hour
ACTIVITY_FLOOR_PERCENTILE = 0.25  # floor = 25th percentile of daily grid totals


def build_grid_hour_table(csv_path: str) -> pd.DataFrame:
    """Run the NP2 pipeline up through derive_activity_features() to
    get the grid/hour analytics table this lab builds on."""
    proc = UsageProcessor(csv_path)
    proc.load_data().clean_data().derive_time_features()
    proc.aggregate_to_grid_time().derive_activity_features()
    logger.info("Grid/hour table ready: %d rows", len(proc.grid_df))
    return proc.grid_df


def choose_activity_floor(df: pd.DataFrame) -> float:
    """Choose the activity floor from the data itself: the
    ACTIVITY_FLOOR_PERCENTILE of each grid's total daily activity.
    Grids below this floor are excluded from alerting entirely,
    since their ratios are dominated by noise."""
    daily_totals = df.groupby("grid_id")["total_activity"].sum()
    floor = daily_totals.quantile(ACTIVITY_FLOOR_PERCENTILE)
    logger.info(
        "Activity floor set to %.2f (%.0f%% percentile of daily grid totals). "
        "Min=%.2f, Median=%.2f, Max=%.2f",
        floor, ACTIVITY_FLOOR_PERCENTILE * 100,
        daily_totals.min(), daily_totals.median(), daily_totals.max(),
    )
    return floor


def compute_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """For each grid_id + hour, compute the median total_activity
    across that grid's OTHER 23 hours that day (excludes current hour).

    Implemented as an explicit per-grid loop (not groupby().apply())
    because pandas 3.x's groupby().apply() drops the grouping column
    from the group DataFrame by default, which would silently lose
    grid_id here."""
    df = df.sort_values(["grid_id", "hour"]).reset_index(drop=True)

    baseline_values = pd.Series(index=df.index, dtype="float64")

    for grid_id, group in df.groupby("grid_id"):
        values = group["total_activity"].to_numpy()
        idx = group.index
        for pos, row_idx in enumerate(idx):
            others = pd.Series(
                values[:pos].tolist() + values[pos + 1:].tolist()
            )
            baseline_values[row_idx] = others.median()

    df = df.copy()
    df["baseline_activity"] = baseline_values
    return df


def apply_rules(df: pd.DataFrame, floor: float) -> pd.DataFrame:
    """Apply HIGH_ACTIVITY, ACTIVITY_SPIKE, ACTIVITY_DROP rules.
    Returns one row per alert (a grid/hour can trigger 0, 1, or more)."""
    df = df.sort_values(["grid_id", "hour"]).reset_index(drop=True)

    # previous-hour activity, per grid (NaN for hour 0 — no prior hour that day)
    df["prev_hour_activity"] = df.groupby("grid_id")["total_activity"].shift(1)

    # only consider grids above the activity floor
    daily_totals = df.groupby("grid_id")["total_activity"].transform("sum")
    df["above_floor"] = daily_totals >= floor

    alerts = []
    for _, row in df.iterrows():
        if not row["above_floor"] or pd.isna(row["baseline_activity"]) or row["baseline_activity"] == 0:
            continue

        current = row["total_activity"]
        baseline = row["baseline_activity"]
        ratio_vs_baseline = current / baseline

        # HIGH_ACTIVITY
        if ratio_vs_baseline >= HIGH_ACTIVITY_RATIO:
            alerts.append({
                "grid_id": row["grid_id"],
                "timestamp": row["timestamp"],
                "alert_type": "HIGH_ACTIVITY",
                "current_activity": round(current, 2),
                "baseline_activity": round(baseline, 2),
                "reason": (
                    f"Current activity ({current:.1f}) is {ratio_vs_baseline:.2f}x "
                    f"this grid's within-day baseline ({baseline:.1f})"
                ),
            })

        # ACTIVITY_DROP
        if ratio_vs_baseline <= DROP_RATIO:
            alerts.append({
                "grid_id": row["grid_id"],
                "timestamp": row["timestamp"],
                "alert_type": "ACTIVITY_DROP",
                "current_activity": round(current, 2),
                "baseline_activity": round(baseline, 2),
                "reason": (
                    f"Current activity ({current:.1f}) is only {ratio_vs_baseline:.2f}x "
                    f"this grid's within-day baseline ({baseline:.1f})"
                ),
            })

        # ACTIVITY_SPIKE — vs immediately preceding hour, not the baseline
        prev = row["prev_hour_activity"]
        if pd.notna(prev) and prev > 0:
            ratio_vs_prev = current / prev
            if ratio_vs_prev >= SPIKE_RATIO_VS_PREV_HOUR:
                alerts.append({
                    "grid_id": row["grid_id"],
                    "timestamp": row["timestamp"],
                    "alert_type": "ACTIVITY_SPIKE",
                    "current_activity": round(current, 2),
                    "baseline_activity": round(baseline, 2),
                    "reason": (
                        f"Current activity ({current:.1f}) is {ratio_vs_prev:.2f}x "
                        f"the immediately preceding hour ({prev:.1f})"
                    ),
                })

    alerts_df = pd.DataFrame(alerts)
    logger.info("apply_rules(): generated %d alert records", len(alerts_df))
    return alerts_df


def print_operational_summary(alerts_df: pd.DataFrame, grid_hour_df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("OPERATIONAL ALERT SUMMARY")
    print("=" * 60)

    if alerts_df.empty:
        print("No alerts generated.")
        return

    print("\nAlerts by type:")
    print(alerts_df["alert_type"].value_counts())

    print("\nTop 10 grids by alert count:")
    print(alerts_df["grid_id"].value_counts().head(10))

    total_grid_hours = len(grid_hour_df)
    alerted_grid_hours = alerts_df[["grid_id", "timestamp"]].drop_duplicates().shape[0]
    proportion = alerted_grid_hours / total_grid_hours
    print(f"\nTotal grid/hours: {total_grid_hours}")
    print(f"Grid/hours with at least one alert: {alerted_grid_hours}")
    print(f"Proportion of grid/hours that alerted: {proportion:.2%}")


def print_baseline_limitations():
    print("\n" + "=" * 60)
    print("WRITTEN LIMITATIONS OF THIS BASELINE (read this)")
    print("=" * 60)
    print("""
1. This is a WITHIN-DAY baseline only. It compares each hour against
   the other hours of the SAME day for the SAME grid. It has no memory
   of what previous days or previous weeks looked like.

2. Blind spot (explicitly required by NP3): this baseline cannot tell
   "this grid is normally quiet at 03:00" from "this grid has dropped."
   A grid that is always near-zero at 3am will look identical, on a
   single day's data, to a grid whose 3am activity has genuinely
   collapsed compared to its usual 3am — because this method has no
   concept of a normal 03:00 to compare against. It only knows what
   THIS grid did on THIS day.

3. An alert here is a request to investigate, not a diagnosis. A
   HIGH_ACTIVITY alert does not mean "there is a problem" — it could
   be a concert, a public event, a rush hour, or a data artifact. The
   rule has no way to distinguish these causes.

4. The activity floor removes near-empty grids from alerting, but is
   still an arbitrary percentile choice — it should be revisited once
   more days of data are available.

5. Because only one day of data exists at this stage, weekday/weekend
   and seasonal patterns cannot be accounted for at all. This is a
   known limitation to be addressed later (see ML1/ML4).
""")


def main():
    csv_path = "../data/sms-call-internet-mi-2013-11-01.csv"

    grid_hour_df = build_grid_hour_table(csv_path)
    floor = choose_activity_floor(grid_hour_df)
    with_baselines = compute_baselines(grid_hour_df)
    alerts_df = apply_rules(with_baselines, floor)

    output_path = "network_alerts.csv"
    alerts_df.to_csv(output_path, index=False)
    logger.info("Wrote %d alerts to %s", len(alerts_df), output_path)

    print_operational_summary(alerts_df, grid_hour_df)
    print_baseline_limitations()

    # ---------------------------------------------------------------
    # Manual spot-check: pick one alert and print the surrounding
    # hourly rows for that grid so you can verify it by hand.
    # ---------------------------------------------------------------
    if not alerts_df.empty:
        sample_alert = alerts_df.iloc[0]
        sample_grid = sample_alert["grid_id"]
        print("\n" + "=" * 60)
        print(f"MANUAL SPOT-CHECK — grid_id={sample_grid}")
        print("=" * 60)
        print(f"Alert flagged: {sample_alert['alert_type']} at {sample_alert['timestamp']}")
        print("\nFull day of hourly activity for this grid (verify the baseline yourself):")
        check_df = with_baselines[with_baselines["grid_id"] == sample_grid][
            ["hour", "total_activity", "baseline_activity", "prev_hour_activity"]
            if "prev_hour_activity" in with_baselines.columns
            else ["hour", "total_activity", "baseline_activity"]
        ]
        print(check_df.to_string(index=False))


if __name__ == "__main__":
    main()
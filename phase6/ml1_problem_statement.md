# ML1 — Operational ML Problem Statement

## Selected training problem

Predict **high-activity risk** for a grid at the next hourly interval (`t+1`).
The prediction unit is one `grid_id` and one hourly interval. The training label
is a documented proxy: the next interval is positive when its
`total_activity` is at or above the 90th percentile of the training history.
This is a training construct, not a claim about network congestion.

Features for a prediction at `t` may use only the trailing window ending at `t`.
The future label describes `t+1`, so the target cannot be used to construct the
features. Every feature row records `feature_timestamp = t`.

## Operational action

A positive result means **an operator should investigate the grid and its
surrounding evidence**. It does not mean that the network is congested or that
capacity has been exceeded.

## Non-goals and limitations

This project does not predict or diagnose congestion, capacity, throughput,
latency, packet loss, radio utilization, or customer impact because those
measurements are not present in the supplied data. Activity values are
proportional indicators, not message counts, call counts, or megabytes.

## Leakage controls

- The current feature window ends at `t`, never at `t+1`.
- The label is computed from the following hourly row.
- Training and testing use chronological, non-overlapping periods.
- `feature_timestamp` is persisted and tested against the source rows.

"""level_history record helper.

Lightweight time-series storage of per-tank levels. advance_time
appends one entry per hour tick. Bounded to ~180 days at 1
entry/hour (4320 entries) so the data.json file stays small.

Each entry is a flat dict for easy JSON serialization:
    {
        "run_hour": float,            # sim run-hour at the tick
        "iso":      str,              # wall-clock ISO datetime of recording
        "tanks":    {tank_name: lbs}  # snapshot of every tank's level
    }

Why a ring buffer instead of full history: without bounded retention
the file grows linearly forever. 180 days is enough for any
recommendation / bias-detection logic that operates on a 6-month
window; older data goes to whatever the technical team's PostgreSQL
migration uses (MIGRATION_GUIDE § 3).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict


# 180 days at 1 entry/hour. Configurable per-customer in a future
# pass; today this is the demo's hard limit.
LEVEL_HISTORY_MAX_ENTRIES = 4320


def record_level_snapshot(data: Dict[str, Any], run_hour: float) -> None:
    """Append a snapshot of every tank's current_level_lbs to
    data['level_history'], truncating to the most recent
    LEVEL_HISTORY_MAX_ENTRIES.

    Idempotent for the same run_hour: if the most recent entry is
    already at this run_hour, the snapshot is replaced (not duplicated).
    advance_time calls this once per tick, so duplicates only happen
    if the operator manually re-runs the same hour.

    Both data dict + PlantState shape supported via the duck-typed
    dict accessor.
    """
    history = data.get("level_history", [])
    if history is None:
        history = []
    snapshot = {
        "run_hour": float(run_hour),
        "iso":      datetime.now().isoformat(),
        "tanks":    {
            name: float(info["current_level_lbs"])
            for name, info in data.get("tanks", {}).items()
        },
    }
    if history and abs(history[-1].get("run_hour", -1) - run_hour) < 0.01:
        # Same run_hour as last entry → replace (idempotent)
        history[-1] = snapshot
    else:
        history.append(snapshot)
    # Truncate from the front to maintain the bound
    if len(history) > LEVEL_HISTORY_MAX_ENTRIES:
        history = history[-LEVEL_HISTORY_MAX_ENTRIES:]
    data["level_history"] = history


def downsample_for_chart(history, max_points: int = 720):
    """Decimate a level_history list to at most `max_points` entries.

    Plotly handles ~5k points fine but UI feels sluggish past ~1k.
    180 days * 24h = 4320 entries → decimate to 720 (~6× downsample,
    one point every 6 hours).

    Strategy: stride-pick. Preserves the first and last entries
    explicitly so the chart's domain is correct."""
    if not history or max_points <= 0:
        return list(history)
    if len(history) <= max_points:
        return list(history)
    stride = max(1, len(history) // max_points)
    decimated = history[::stride]
    # Always include the very latest entry so the chart's right edge
    # reflects the current sim time.
    if decimated[-1] is not history[-1]:
        decimated.append(history[-1])
    return decimated

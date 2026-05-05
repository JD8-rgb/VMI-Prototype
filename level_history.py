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

    `iso` is the SIMULATION time corresponding to run_hour (epoch +
    run_hour). Was wall-clock-now in an earlier version, which made
    "Generate demo history" (which runs in seconds wall-clock-wise)
    produce a chart spanning seconds instead of weeks of sim time.
    """
    history = data.get("level_history", [])
    if history is None:
        history = []
    # Compute sim time from run_hour. Falls back to wall-clock if the
    # data dict doesn't carry simulation_epoch (defensive — any caller
    # passing a state dict will have it).
    epoch_iso = data.get("simulation_epoch")
    if epoch_iso:
        try:
            epoch = datetime.fromisoformat(epoch_iso)
            from datetime import timedelta as _td
            sim_iso = (epoch + _td(hours=float(run_hour))).isoformat()
        except (ValueError, TypeError):
            sim_iso = datetime.now().isoformat()
    else:
        sim_iso = datetime.now().isoformat()
    snapshot = {
        "run_hour": float(run_hour),
        "iso":      sim_iso,
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


def inventory_age_hours(
    tank_name: str,
    history: list,
    current_run_hour: float,
    threshold_lbs: float = 2000.0,
):
    """Return how many hours it has been since the tank last dipped below
    `threshold_lbs` (proxy for "near-empty = material turnover").

    Returns:
        (age_hours: float, capped: bool)

        age_hours  – hours since the last sub-threshold entry.
        capped     – True when the oldest available history entry was
                     already above the threshold; the true age is at
                     least `age_hours` but the ring buffer doesn't go
                     back far enough to find the actual dip.

    Edge cases:
        - Empty history or tank absent from history → (0.0, True).
        - Tank always below threshold in history  → (0.0, False).
    """
    if not history:
        return (0.0, True)

    # Scan newest → oldest for the most recent sub-threshold entry
    last_dip_run_hour: float | None = None
    for entry in reversed(history):
        tanks = entry.get("tanks", {})
        if tank_name not in tanks:
            continue
        if tanks[tank_name] < threshold_lbs:
            last_dip_run_hour = float(entry["run_hour"])
            break

    if last_dip_run_hour is not None:
        return (max(0.0, current_run_hour - last_dip_run_hour), False)

    # Tank was never below threshold in the available history.
    # Age is at least (current_run_hour - oldest_run_hour).
    # Find the oldest entry that contains this tank.
    oldest_run_hour: float | None = None
    for entry in history:
        if tank_name in entry.get("tanks", {}):
            oldest_run_hour = float(entry["run_hour"])
            break

    if oldest_run_hour is None:
        return (0.0, True)

    return (max(0.0, current_run_hour - oldest_run_hour), True)


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

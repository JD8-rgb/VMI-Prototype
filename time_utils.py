"""
Time conversion helpers for the VMI prototype.

The simulation tracks time as a single float: current_run_hour, where
1 run-hour = 1 wall-clock hour. The simulation epoch is stored in
data.json as an ISO datetime string ("2026-04-14T00:00:00") and represents
the wall-clock moment when run-hour 0 occurs.

All datetimes are naive local (Eastern). No timezone math.
"""

from datetime import datetime, timedelta

DISPLAY_FORMAT = "%a %Y-%m-%d %H:%M"  # e.g. "Mon 2026-04-20 08:00"


def _epoch_str(data_or_state):
    """Pull the simulation_epoch ISO string from either a raw dict or a
    PlantState dataclass. Polymorphic so the same time helpers work
    during the dataclass migration without requiring every caller to
    convert at the call site."""
    if hasattr(data_or_state, "simulation_epoch"):
        return data_or_state.simulation_epoch
    return data_or_state["simulation_epoch"]


def get_epoch(data):
    """Return the simulation epoch as a datetime object."""
    return datetime.fromisoformat(_epoch_str(data))


def run_hour_to_dt(data, run_hour):
    """Convert a run-hour (float) to a naive datetime."""
    return get_epoch(data) + timedelta(hours=run_hour)


def dt_to_run_hour(data, dt):
    """Convert a naive datetime to a run-hour (float)."""
    delta = dt - get_epoch(data)
    return delta.total_seconds() / 3600.0


def format_run_hour(data, run_hour):
    """Return a human display string: 'Mon 2026-04-20 08:00'."""
    dt = run_hour_to_dt(data, run_hour)
    return dt.strftime(DISPLAY_FORMAT)


def distribute_window_across_days(state, window):
    """Split a run-window across the calendar days it touches.

    Yields ``(week_monday_iso, weekday_int, hours)`` tuples — one per
    calendar day the window spans — so multi-day and overnight-spanning
    windows attribute hours to each day they actually run, not just the
    start day. Total hours across yielded tuples equals the window's
    duration.

    Used by `forecast.py:_bucket_run_schedule_by_week` (per-weekday
    seasonal model) and `anomaly.py:_weekly_run_hours_history` (per-week
    totals — week boundary is the relevant split there). Single source
    of truth so the two callers can't drift.

    Empty windows (start_hour == end_hour) yield nothing.
    """
    start_dt = run_hour_to_dt(state, window.start_hour)
    end_dt   = run_hour_to_dt(state, window.end_hour)
    cur = start_dt
    while cur < end_dt:
        next_midnight = (cur + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        slice_end = min(next_midnight, end_dt)
        hours = (slice_end - cur).total_seconds() / 3600.0
        if hours > 0:
            monday_iso = (
                cur - timedelta(days=cur.weekday())
            ).date().isoformat()
            yield (monday_iso, cur.weekday(), hours)
        cur = slice_end


# Implausible run-hour magnitude. The simulation runs in hours; anything
# above ~5 years (43,800 hours) is almost certainly a date typo without
# separators rather than a real duration. Reject so we don't silently
# advance the clock 2,000+ years.
_MAX_REASONABLE_RUN_HOURS = 50_000


def parse_time_input(data, text):
    """
    Accept either a run-hour number ('168' or '168.5') or a datetime
    string ('2026-04-20 08:00' or '2026-04-20T08:00'). Return a run-hour
    float in either case.

    Auto-detect: if the string parses as a float AND is within a sane
    magnitude, treat as run-hour. Otherwise try to parse as ISO datetime.
    The magnitude check catches '20260415' typos (a date pasted without
    separators succeeds as float() but means 2,300 years in the future).
    """
    text = text.strip()
    # Try float first, but reject implausible magnitudes (almost certainly
    # a date typed without separators).
    try:
        v = float(text)
        if abs(v) > _MAX_REASONABLE_RUN_HOURS:
            raise ValueError(
                f"'{text}' parses as {v:,.0f} run-hours which is >{_MAX_REASONABLE_RUN_HOURS:,} "
                f"(~{_MAX_REASONABLE_RUN_HOURS/8760:.0f} years). Did you mean a "
                f"date? Use 'YYYY-MM-DD HH:MM' format."
            )
        return v
    except ValueError as e:
        # Re-raise the magnitude-check error directly; only suppress
        # plain "could not convert" so we can try ISO parsing next.
        if "run-hours" in str(e):
            raise
    # Try datetime — accept space or T as separator
    normalized = text.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        raise ValueError(
            f"Could not parse '{text}' as a run-hour or datetime. "
            f"Examples: 168  |  168.5  |  2026-04-20 08:00"
        )
    return dt_to_run_hour(data, dt)
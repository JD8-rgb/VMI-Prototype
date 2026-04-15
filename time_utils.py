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


def get_epoch(data):
    """Return the simulation epoch as a datetime object."""
    return datetime.fromisoformat(data["simulation_epoch"])


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


def parse_time_input(data, text):
    """
    Accept either a run-hour number ('168' or '168.5') or a datetime
    string ('2026-04-20 08:00' or '2026-04-20T08:00'). Return a run-hour
    float in either case.

    Auto-detect: if the string parses as a float, treat as run-hour.
    Otherwise try to parse as ISO datetime.
    """
    text = text.strip()
    # Try float first
    try:
        return float(text)
    except ValueError:
        pass
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
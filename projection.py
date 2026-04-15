"""
projection.py
-------------
Compute per-tank level history over time for chart rendering.
Pure function — does not modify data or write files.

Reuses simulation logic from alerts.py without triggering alert side-effects.
"""

import copy
from alerts import (
    simulate_consume,
    simulate_delivery_no_alert,
    is_running_at,
)
from time_utils import run_hour_to_dt, format_run_hour

PROJECTION_HOURS = 240   # 10 days


def compute_level_history(data, hours=PROJECTION_HOURS):
    """
    Walk forward hour by hour from current_run_hour, applying consumption
    (only during scheduled run windows) and truck deliveries.
    Tracks the level of every individual tank at each step.

    Parameters
    ----------
    data  : full data dict (not mutated)
    hours : how many hours forward to project (default 240 = 10 days)

    Returns
    -------
    {
      "run_hours"    : [float, ...],           # one entry per hour step
      "datetimes"    : [str, ...],             # formatted display strings
      "tanks"        : {                       # per-tank level at each step
          "U-Tank1"  : [float, ...],
          "U-Tank2"  : [float, ...],
          "M-Tank1"  : [float, ...],
          "M-Tank2"  : [float, ...],
      },
      "truck_events" : [                       # one entry per delivery
          {
              "run_hour" : float,
              "datetime" : str,
              "sap"      : str,
              "product"  : str,
              "qty"      : int,
          },
          ...
      ],
      "run_windows"  : [                       # clipped to projection window
          {"start_hour": float, "end_hour": float, "label": str},
          ...
      ],
    }
    """
    tanks   = copy.deepcopy(data["tanks"])
    rates   = data["consumption_rates"]
    current = data["current_run_hour"]
    end     = current + hours

    # Trucks that arrive within the projection window
    pending = sorted(
        [t for t in data["scheduled_trucks"] if current < t["arrival_run_hour"] <= end],
        key=lambda t: t["arrival_run_hour"],
    )
    truck_idx = 0

    run_hours  = []
    datetimes  = []
    tank_names = list(tanks.keys())
    tank_hist  = {name: [] for name in tank_names}
    truck_events = []

    hour = current
    while hour <= end:
        # Record state BEFORE this hour's consumption (so the initial point is visible)
        run_hours.append(hour)
        datetimes.append(format_run_hour(data, hour))
        for name in tank_names:
            tank_hist[name].append(round(tanks[name]["current_level_lbs"], 1))

        if hour == end:
            break

        next_hour = hour + 1

        # Consume if running
        if is_running_at(data, hour):
            for product, rate_info in rates.items():
                simulate_consume(tanks, product, rate_info["lbs_per_hour"])

        # Deliver trucks that arrive before or at next_hour
        while truck_idx < len(pending) and pending[truck_idx]["arrival_run_hour"] <= next_hour:
            truck = pending[truck_idx]
            simulate_delivery_no_alert(tanks, truck)
            truck_events.append({
                "run_hour": truck["arrival_run_hour"],
                "datetime": format_run_hour(data, truck["arrival_run_hour"]),
                "sap":      truck["sap_order"],
                "product":  truck["product"],
                "qty":      truck["quantity_lbs"],
            })
            truck_idx += 1

        hour = next_hour

    # Clip run windows to the projection window for chart shading
    clipped_windows = []
    for w in data["run_schedule"]:
        ws = max(w["start_hour"], current)
        we = min(w["end_hour"], end)
        if we > ws:
            clipped_windows.append({
                "start_hour": ws,
                "end_hour":   we,
                "label":      w.get("label", ""),
            })

    return {
        "run_hours":    run_hours,
        "datetimes":    datetimes,
        "tanks":        tank_hist,
        "truck_events": truck_events,
        "run_windows":  clipped_windows,
    }

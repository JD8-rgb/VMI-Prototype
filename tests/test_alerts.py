"""Behavior contracts for alerts.py.

These guard the parts the demo depends on:
  - safety_stock alerts fire when projected level falls below threshold
  - lead_time fires when supply can't cover the next 48h of run-time
  - late_truck fires inclusively at the documented 3.0h threshold
  - schedule_alerts fire at Friday 11am+ / 3pm+ deadlines
  - plant_state_mismatch fires after the documented hour threshold
  - get_all_alerts produces identical output for dict and PlantState shapes
"""

from __future__ import annotations

import copy

import pytest

from alerts import (
    check_lead_time,
    check_late_trucks,
    check_plant_state_mismatch,
    check_schedule_alerts,
    get_all_alerts,
    is_running_at,
    run_projection,
)
from config import PlantConfig, DEFAULT_CONFIG
from state import PlantState

from .conftest import make_drained_state


# ── lead-time ─────────────────────────────────────────────────────────────────

def test_lead_time_no_alert_at_defaults(defaults_dict, as_shape):
    """At defaults, Product U has 19,000 usable lbs and 32 run-hours
    in the next 48h (Mon 6-22 + Tue 30-46 = 32). 32 * 583.3 = 18,666
    demand — supply > demand, no warning."""
    assert check_lead_time(as_shape(defaults_dict), "Product U") is None
    assert check_lead_time(as_shape(defaults_dict), "Product M") is None


def test_lead_time_fires_when_drained(defaults_dict, as_shape):
    drained = make_drained_state(defaults_dict, "Product U", 1500)
    alert = check_lead_time(as_shape(drained), "Product U")
    assert alert is not None
    assert alert["type"] == "lead_time"
    assert alert["severity"] == "warning"
    assert alert["product"] == "Product U"
    assert "Product U" in alert["text"]
    assert "Order another truck." in alert["text"]


def test_lead_time_returns_none_when_no_run_in_horizon(defaults_dict, as_shape):
    """If there's no scheduled run in the lead-time window, demand is
    zero and no alert can fire."""
    d = copy.deepcopy(defaults_dict)
    d["run_schedule"] = []
    drained = copy.deepcopy(d)
    for info in drained["tanks"].values():
        if info["product"] == "Product U":
            info["current_level_lbs"] = 0
    assert check_lead_time(as_shape(drained), "Product U") is None


# ── late-truck ────────────────────────────────────────────────────────────────

def test_late_truck_inclusive_threshold(defaults_dict, as_shape):
    """The handoff calls out that >= cfg.late_truck_hours is the contract.
    A truck exactly 3.0h overdue MUST fire."""
    d = copy.deepcopy(defaults_dict)
    d["current_run_hour"] = 10.0
    d["scheduled_trucks"] = [{
        "sap_order": "SAP90001",
        "product": "Product U",
        "quantity_lbs": 33000,
        "arrival_run_hour": 7.0,  # exactly 3 hours overdue
    }]
    alerts = check_late_trucks(as_shape(d))
    assert len(alerts) == 1
    assert alerts[0]["type"] == "late_truck"
    assert "LATE TRUCK" in alerts[0]["text"]


def test_late_truck_below_threshold_silent(defaults_dict, as_shape):
    d = copy.deepcopy(defaults_dict)
    d["current_run_hour"] = 9.99  # 2.99h overdue
    d["scheduled_trucks"] = [{
        "sap_order": "SAP90001",
        "product": "Product U",
        "quantity_lbs": 33000,
        "arrival_run_hour": 7.0,
    }]
    assert check_late_trucks(as_shape(d)) == []


# ── plant-state-mismatch ──────────────────────────────────────────────────────

def test_plant_state_mismatch_silent_without_override(defaults_dict, as_shape):
    assert check_plant_state_mismatch(as_shape(defaults_dict)) == []


def test_plant_state_mismatch_fires_after_threshold(defaults_dict, as_shape):
    d = copy.deepcopy(defaults_dict)
    # Schedule says "running" (Mon 6-22) at hour 10. Override says
    # "down" since hour 6 → 4h discrepancy at threshold of 3h.
    d["current_run_hour"] = 10.0
    d["plant_state_override"] = {"actual": "down", "since_hour": 6.0}
    alerts = check_plant_state_mismatch(as_shape(d))
    assert len(alerts) == 1
    assert alerts[0]["type"] == "plant_state"
    assert alerts[0]["severity"] == "red_flag"


def test_plant_state_mismatch_silent_when_states_agree(defaults_dict, as_shape):
    d = copy.deepcopy(defaults_dict)
    d["current_run_hour"] = 10.0
    # Schedule says running at hour 10 (Mon 6-22); override agrees.
    d["plant_state_override"] = {"actual": "running", "since_hour": 6.0}
    assert check_plant_state_mismatch(as_shape(d)) == []


# ── schedule alerts ───────────────────────────────────────────────────────────

def _friday_at(defaults_dict, hour_of_day):
    """Set sim clock to Friday 2026-04-17 at the given hour-of-day."""
    d = copy.deepcopy(defaults_dict)
    # Epoch is Mon 2026-04-13 00:00 → Friday at hour 96 + hour_of_day
    d["current_run_hour"] = 96 + hour_of_day
    d["schedule_received_for_week"] = None
    return d


def test_schedule_alerts_silent_outside_friday(defaults_dict, as_shape):
    # Mon 0:00 — no schedule pressure yet
    assert check_schedule_alerts(as_shape(defaults_dict)) == []


def test_schedule_alerts_warning_friday_11am(defaults_dict, as_shape):
    d = _friday_at(defaults_dict, 11)
    alerts = check_schedule_alerts(as_shape(d))
    assert any(a["type"] == "schedule_deadline" and a["severity"] == "warning"
               for a in alerts)


def test_schedule_alerts_red_flag_friday_3pm(defaults_dict, as_shape):
    d = _friday_at(defaults_dict, 15)
    alerts = check_schedule_alerts(as_shape(d))
    assert any(a["type"] == "schedule_deadline" and a["severity"] == "red_flag"
               for a in alerts)


def test_schedule_alerts_silent_when_received(defaults_dict, as_shape):
    d = _friday_at(defaults_dict, 15)
    # Friday 3pm but next Mon's schedule is in
    d["schedule_received_for_week"] = "2026-04-20"
    schedule_dl = [a for a in check_schedule_alerts(as_shape(d))
                   if a["type"] == "schedule_deadline"]
    assert schedule_dl == []


def test_schedule_parse_warning_when_unparseable(defaults_dict, as_shape):
    d = copy.deepcopy(defaults_dict)
    d["schedule_parse_issue"] = "low confidence"
    alerts = check_schedule_alerts(as_shape(d))
    assert any(a["type"] == "schedule_parse" for a in alerts)


# ── projection ────────────────────────────────────────────────────────────────

def test_run_projection_emits_safety_stock_at_defaults(defaults_dict, as_shape):
    """Defaults: 80 run-hours over the next 168h, ~46.7k lbs demand per
    product, only ~19-25k lbs usable on hand. Both products MUST hit
    safety stock somewhere in the window."""
    alerts = run_projection(as_shape(defaults_dict))
    products_with_alert = {a["product"] for a in alerts
                            if a["type"] == "safety_stock"}
    assert "Product U" in products_with_alert
    assert "Product M" in products_with_alert


def test_run_projection_safety_stock_dedup_per_product(defaults_dict, as_shape):
    """Each product fires at most ONE safety_stock alert per projection."""
    alerts = run_projection(as_shape(defaults_dict))
    counts = {}
    for a in alerts:
        if a["type"] == "safety_stock":
            counts[a["product"]] = counts.get(a["product"], 0) + 1
    for product, count in counts.items():
        assert count == 1, f"{product} fired {count} safety_stock alerts"


def test_run_projection_silent_with_full_tanks_no_run(defaults_dict, as_shape):
    """No run schedule and full tanks → projection emits nothing."""
    d = copy.deepcopy(defaults_dict)
    d["run_schedule"] = []
    for info in d["tanks"].values():
        info["current_level_lbs"] = info["max_capacity_lbs"]
    assert run_projection(as_shape(d)) == []


# ── is_running_at ─────────────────────────────────────────────────────────────

def test_is_running_at_window_boundaries(defaults_dict, as_shape):
    """Per-window: [start, end). Start inclusive, end exclusive."""
    s = as_shape(defaults_dict)
    # Mon window 6-22
    assert is_running_at(s, 5.99) is False
    assert is_running_at(s, 6.0)  is True
    assert is_running_at(s, 21.99) is True
    assert is_running_at(s, 22.0)  is False  # half-open


# ── polymorphism: dict vs PlantState parity ──────────────────────────────────

def test_get_all_alerts_dict_state_parity(defaults_dict):
    """Same input, two shapes — alerts must match exactly. Same indexing,
    same text, same severity. Drift here means a function is reading
    different fields off dict vs state somewhere."""
    via_dict  = get_all_alerts(copy.deepcopy(defaults_dict))
    via_state = get_all_alerts(PlantState.from_dict(copy.deepcopy(defaults_dict)))
    assert len(via_dict) == len(via_state)
    for a, b in zip(via_dict, via_state):
        assert a["text"]     == b["text"]
        assert a["type"]     == b["type"]
        assert a["severity"] == b["severity"]
        assert a["product"]  == b["product"]


def test_get_all_alerts_with_override_cfg(defaults_dict, as_shape):
    """A custom PlantConfig threads through every check. With a high
    safety-stock threshold the same defaults must fire safety_stock
    alerts immediately (run_projection sees the new threshold)."""
    high_safety = PlantConfig(safety_stock_lbs=50_000)
    alerts = get_all_alerts(as_shape(defaults_dict), cfg=high_safety)
    assert any(a["type"] == "safety_stock" for a in alerts)

"""VMI on/off toggle behavior + Friday off-alert.

When the operator turns VMI automation off via the Streamlit "VMI
Controls" panel:
  - PlantState.vmi_automation_enabled becomes False
  - plan_for_product stops proposing trucks
  - alerts.check_vmi_off fires every Friday 09:00+ (weekly RED reminder)
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta

import pytest

from alerts import check_vmi_off, get_all_alerts
from plan_orders import (
    plan_for_product,
    get_target_week_bounds,
    get_target_for_week,
    get_run_hours_in_window,
)


def _set_sim_now(d: dict, dt: datetime) -> dict:
    """Set current_run_hour so sim_now == dt."""
    d = copy.deepcopy(d)
    epoch = datetime.fromisoformat(d["simulation_epoch"])
    delta = (dt - epoch).total_seconds() / 3600.0
    d["current_run_hour"] = float(delta)
    return d


# ── check_vmi_off ────────────────────────────────────────────────────────────

def test_no_alert_when_vmi_enabled(defaults_dict):
    """Default state has vmi_automation_enabled=True; no alert."""
    fri_10am = datetime(2026, 4, 17, 10, 0)
    d = _set_sim_now(defaults_dict, fri_10am)
    assert check_vmi_off(d) == []


def test_no_alert_outside_friday_even_when_off(defaults_dict):
    """The reminder is Friday-only."""
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    # Mon 10 AM — not Friday
    mon_10am = datetime(2026, 4, 13, 10, 0)
    d = _set_sim_now(d, mon_10am)
    assert check_vmi_off(d) == []


def test_no_alert_friday_before_9am(defaults_dict):
    """The reminder fires at 09:00, not before."""
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    fri_8am = datetime(2026, 4, 17, 8, 30)
    d = _set_sim_now(d, fri_8am)
    assert check_vmi_off(d) == []


def test_red_alert_friday_at_9am_when_off(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    fri_9am = datetime(2026, 4, 17, 9, 0)
    d = _set_sim_now(d, fri_9am)
    alerts = check_vmi_off(d)
    assert len(alerts) == 1
    assert alerts[0]["type"] == "vmi_off"
    assert alerts[0]["severity"] == "red_flag"
    assert "VMI automation is OFF" in alerts[0]["text"]


def test_red_alert_friday_at_3pm_when_off(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    fri_3pm = datetime(2026, 4, 17, 15, 0)
    d = _set_sim_now(d, fri_3pm)
    alerts = check_vmi_off(d)
    assert len(alerts) == 1


def test_alert_text_includes_target_week(defaults_dict):
    """The alert text includes the upcoming Monday's date so weekly
    re-firings produce different hashes (the alerted_hashes dedup
    won't suppress next week's reminder)."""
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    fri_w1 = datetime(2026, 4, 17, 10, 0)
    fri_w2 = datetime(2026, 4, 24, 10, 0)
    a1 = check_vmi_off(_set_sim_now(d, fri_w1))[0]
    a2 = check_vmi_off(_set_sim_now(d, fri_w2))[0]
    # Different week → different text (dedup hash will differ)
    assert a1["text"] != a2["text"]
    assert "2026-04-20" in a1["text"]
    assert "2026-04-27" in a2["text"]


def test_check_vmi_off_wired_into_get_all_alerts(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    fri_10am = datetime(2026, 4, 17, 10, 0)
    d = _set_sim_now(d, fri_10am)
    alerts = get_all_alerts(d)
    assert any(a["type"] == "vmi_off" for a in alerts)


# ── plan_for_product respects the toggle ─────────────────────────────────────

def test_planner_returns_empty_when_vmi_disabled(defaults_dict, caplog):
    """plan_for_product short-circuits to [] when vmi_automation_enabled
    is False — the operator's manual schedule_truck CLI is the only
    path that adds trucks in this mode."""
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    # Add a target-week schedule so a normal planner run WOULD propose
    week_start, week_end = get_target_week_bounds(d)
    for i in range(5):
        d["run_schedule"].append({
            "start_hour": week_start + 6 + i*24,
            "end_hour":   week_start + 22 + i*24,
            "label":      f"Day{i+1}",
        })
    target = get_target_for_week(80)
    new_trucks = plan_for_product(d, "Product U", target,
                                    week_start, week_end, [])
    assert new_trucks == []


def test_planner_proposes_normally_when_vmi_enabled(defaults_dict):
    """Sanity: with the default vmi_automation_enabled=True, the
    planner still proposes trucks as before."""
    d = copy.deepcopy(defaults_dict)
    week_start, week_end = get_target_week_bounds(d)
    for i in range(5):
        d["run_schedule"].append({
            "start_hour": week_start + 6 + i*24,
            "end_hour":   week_start + 22 + i*24,
            "label":      f"Day{i+1}",
        })
    target = get_target_for_week(80)
    new_trucks = plan_for_product(d, "Product U", target,
                                    week_start, week_end, [])
    assert len(new_trucks) >= 1

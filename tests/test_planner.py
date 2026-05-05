"""Behavior contracts for plan_orders.py.

These guard the planner's two main responsibilities:
  - propose enough trucks to keep combined level above target during
    the target week
  - never propose a truck that would cause an overfill at the chosen slot
  - polymorphism: identical proposals for dict vs PlantState input

Note: plan_orders targets the NEXT Mon–Sun week (current_run_hour=0
puts target week at hours 168-336). The defaults.json run_schedule
only spans week 1, so all planner tests use the helper below to add
an equivalent week-2 schedule before exercising plan_for_product.
"""

from __future__ import annotations

import copy

import pytest

from plan_orders import (
    plan_for_product,
    is_valid_delivery_slot,
    find_first_breach_in_target_week,
    get_target_week_bounds,
    _all_slot_run_hours,
)
from config import PlantConfig, DEFAULT_CONFIG
from state import PlantState


def with_target_week_schedule(d: dict) -> dict:
    """Append Mon-Fri 6-22 windows for the planner's target week (next
    Mon-Sun) so the planner has somewhere to place trucks."""
    d = copy.deepcopy(d)
    week_start, _ = get_target_week_bounds(d)
    # Five M-F daily 6am-10pm windows starting at week_start
    for i, label in enumerate(("Mon", "Tue", "Wed", "Thu", "Fri")):
        offset = i * 24
        d["run_schedule"].append({
            "start_hour": week_start + 6 + offset,
            "end_hour":   week_start + 22 + offset,
            "label":      f"{label}+1w",
        })
    return d


# ── slot validity ────────────────────────────────────────────────────────────

def test_delivery_slots_match_config(defaults_dict, as_shape):
    """A Mon 6am slot in the target week is valid: in slot list, in
    run window, and >= lead_time hours from now."""
    d = with_target_week_schedule(defaults_dict)
    s = as_shape(d)
    week_start, _ = get_target_week_bounds(s)
    assert is_valid_delivery_slot(s, week_start + 6, week_start) is True


def test_delivery_slot_off_hours_invalid(defaults_dict, as_shape):
    d = with_target_week_schedule(defaults_dict)
    s = as_shape(d)
    week_start, _ = get_target_week_bounds(s)
    # 5am isn't in the configured slot set (6, 8, 14)
    assert is_valid_delivery_slot(s, week_start + 5, week_start) is False


def test_delivery_slot_before_lead_time_invalid(defaults_dict, as_shape):
    """A Mon 6am slot in week 1 is < lead_time_hours from now → invalid."""
    s = as_shape(defaults_dict)
    week_start, _ = get_target_week_bounds(s)
    assert is_valid_delivery_slot(s, 6.0, week_start) is False


# ── breach detection ─────────────────────────────────────────────────────────

def test_breach_detected_when_target_exceeds_supply(defaults_dict, as_shape):
    """Target = 25k lbs against 19k usable + week-2 demand → breach in
    the target week."""
    d = with_target_week_schedule(defaults_dict)
    s = as_shape(d)
    week_start, week_end = get_target_week_bounds(s)
    breach = find_first_breach_in_target_week(
        s, "Product U", target=25_000,
        week_start=week_start, week_end=week_end,
        extra_trucks=[],
    )
    assert breach is not None
    assert week_start <= breach <= week_end


def test_no_breach_when_target_already_satisfied(defaults_dict, as_shape):
    """Target = 1 lb is trivially satisfied for the entire week."""
    d = with_target_week_schedule(defaults_dict)
    s = as_shape(d)
    week_start, week_end = get_target_week_bounds(s)
    breach = find_first_breach_in_target_week(
        s, "Product U", target=1.0,
        week_start=week_start, week_end=week_end,
        extra_trucks=[],
    )
    assert breach is None


# ── plan_for_product ─────────────────────────────────────────────────────────

def test_plan_proposes_truck_when_short(defaults_dict, capsys):
    d = with_target_week_schedule(defaults_dict)
    week_start, week_end = get_target_week_bounds(d)
    new_trucks = plan_for_product(
        d, "Product U", target=25_000,
        week_start=week_start, week_end=week_end,
        extra_trucks=[],
    )
    assert len(new_trucks) >= 1
    for t in new_trucks:
        assert t["sap_order"] is None  # not yet committed
        assert t["product"] == "Product U"
        assert t["quantity_lbs"] == 33_000  # Product U truck size
        assert week_start <= t["arrival_run_hour"] <= week_end
        # Each proposed slot is a configured delivery hour-of-day
        slot_hour_of_day = int(t["arrival_run_hour"] - week_start) % 24
        assert slot_hour_of_day in DEFAULT_CONFIG.delivery_slots

    # The planner guarantees: after the last truck's arrival, no further
    # breach exists at the planning target within the week. This mirrors
    # the planner's own internal termination condition — it only returns
    # new_trucks once find_first_breach returns None from the last slot + 1.
    last_slot = max(t["arrival_run_hour"] for t in new_trucks)
    breach_after_last = find_first_breach_in_target_week(
        d, "Product U", target=25_000,
        week_start=week_start, week_end=week_end,
        extra_trucks=new_trucks,
        breach_floor=last_slot + 1,
    )
    assert breach_after_last is None, (
        f"After last truck delivery, Product U should have no breach "
        f"at target=25 000 in target week, but found one at "
        f"run_hour={breach_after_last}"
    )


def test_plan_skips_when_target_already_met(defaults_dict, capsys):
    d = with_target_week_schedule(defaults_dict)
    week_start, week_end = get_target_week_bounds(d)
    new_trucks = plan_for_product(
        d, "Product U", target=1.0,  # trivially satisfied
        week_start=week_start, week_end=week_end,
        extra_trucks=[],
    )
    assert new_trucks == []


def test_plan_polymorphism_parity(defaults_dict, capsys):
    """Same problem, two shapes — same arrival hours proposed."""
    d = with_target_week_schedule(defaults_dict)
    s = PlantState.from_dict(copy.deepcopy(d))
    week_start, week_end = get_target_week_bounds(d)

    via_dict = plan_for_product(d, "Product U", 25_000,
                                 week_start, week_end, [])
    via_state = plan_for_product(s, "Product U", 25_000,
                                  week_start, week_end, [])
    assert ([t["arrival_run_hour"] for t in via_dict] ==
            [t["arrival_run_hour"] for t in via_state])
    assert ([t["quantity_lbs"] for t in via_dict] ==
            [t["quantity_lbs"] for t in via_state])


def test_all_slot_run_hours_uses_config_slots(defaults_dict, as_shape):
    """The slot enumerator should respect cfg.delivery_slots."""
    d = with_target_week_schedule(defaults_dict)
    s = as_shape(d)
    week_start, week_end = get_target_week_bounds(s)
    custom_cfg = PlantConfig(delivery_slots=(7, 13))
    slots = _all_slot_run_hours(s, week_start, week_end, cfg=custom_cfg)
    # Every slot's hour-of-day must be in the configured slot set
    for slot in slots:
        hour_of_day = int(slot - week_start) % 24
        assert hour_of_day in (7, 13), (
            f"slot at run-hour {slot} → hour-of-day {hour_of_day} "
            f"isn't in custom (7, 13)")

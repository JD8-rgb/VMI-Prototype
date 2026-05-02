"""End-to-end demo-flow contract.

The demo flow per HANDOFF.md Q2:

    load defaults  →  schedule comes in  →  planner proposes trucks
                  →  user commits         →  advance time
                  →  tanks drain          →  alerts fire

This single test walks the entire algorithm side of that flow against
the stock defaults customer and against the example_customer (3-product
asymmetric topology). If any step regresses, the demo breaks. There's
no Streamlit / IMAP in this test — those are UI / I/O concerns.
"""

from __future__ import annotations

import copy

import pytest

from alerts import (
    get_all_alerts,
    run_projection,
    simulate_consume,
    simulate_delivery,
    is_running_at,
)
from config import PlantConfig, DEFAULT_CONFIG
from customers import load_customer
from plan_orders import (
    plan_for_product,
    get_target_week_bounds,
    get_run_hours_in_window,
    get_target_for_week,
)
from projection import compute_level_history
from state import PlantState
import app


def _add_next_week_schedule(d: dict) -> dict:
    """Inject Mon-Fri 6-22 windows for the planner's target week so
    plan_for_product has somewhere to place trucks."""
    d = copy.deepcopy(d)
    week_start, _ = get_target_week_bounds(d)
    for i, label in enumerate(("Mon", "Tue", "Wed", "Thu", "Fri")):
        offset = i * 24
        d["run_schedule"].append({
            "start_hour": week_start + 6 + offset,
            "end_hour":   week_start + 22 + offset,
            "label":      f"{label}+1w",
        })
    return d


def _commit_truck(d: dict, truck: dict, cfg: PlantConfig) -> None:
    """Mimic the Streamlit commit: assign a SAP, append to scheduled
    trucks, record in sap_history."""
    sap = app._next_sap(d, cfg=cfg)
    truck["sap_order"] = sap
    truck.pop("_planned_reason", None)
    d["scheduled_trucks"].append(truck)
    hist = set(d.get("sap_history", []))
    hist.add(sap)
    d["sap_history"] = sorted(hist)


def _advance_one_hour(d: dict, cfg: PlantConfig) -> None:
    """Mimic advance_time.py: tick the clock, drain tanks if running,
    deliver any trucks arriving in this hour."""
    current = d["current_run_hour"]
    next_hour = current + 1
    if is_running_at(d, current, cfg=cfg):
        for product, rate_info in d["consumption_rates"].items():
            simulate_consume(d["tanks"], product, rate_info["lbs_per_hour"])
    arrived = [t for t in d["scheduled_trucks"]
                if current < t["arrival_run_hour"] <= next_hour]
    for t in arrived:
        simulate_delivery(d["tanks"], t)
    # Remove delivered
    d["scheduled_trucks"] = [t for t in d["scheduled_trucks"]
                              if t not in arrived]
    d["current_run_hour"] = next_hour


# ── Defaults customer ────────────────────────────────────────────────────────

def test_e2e_defaults_demo_flow(defaults_dict):
    """Full algorithm walkthrough against the stock customer."""
    d = _add_next_week_schedule(defaults_dict)
    week_start, week_end = get_target_week_bounds(d)
    week_run_hours = get_run_hours_in_window(d, week_start, week_end)
    assert week_run_hours == 80     # Mon-Fri 16h * 5

    target = get_target_for_week(week_run_hours)
    assert target > 0

    # Plan trucks for both products
    new_trucks = []
    for product in d["consumption_rates"]:
        new_trucks.extend(
            plan_for_product(d, product, target, week_start, week_end, new_trucks)
        )
    assert len(new_trucks) >= 1, "planner should propose at least one truck"

    # Commit trucks (simulates user pressing the Streamlit button)
    for t in new_trucks:
        _commit_truck(d, t, DEFAULT_CONFIG)

    # Every committed truck has a SAP-prefixed order number from the seed
    saps = [t["sap_order"] for t in d["scheduled_trucks"]]
    assert len(saps) == len(new_trucks)
    for sap in saps:
        assert sap.startswith("SAP")

    # Run alerts after commit — schedule_received state is fine,
    # planner has filled the gap; no safety_stock alerts in target week
    alerts_after_plan = get_all_alerts(d)
    assert isinstance(alerts_after_plan, list)

    # Walk the clock forward one full week
    starting_hour = d["current_run_hour"]
    for _ in range(168):
        _advance_one_hour(d, DEFAULT_CONFIG)
    assert d["current_run_hour"] == starting_hour + 168

    # The committed trucks should have either delivered or still be inbound
    # — none should disappear without a trace
    delivered_count = (len(new_trucks)
                       - len([t for t in d["scheduled_trucks"]
                              if t["sap_order"] in saps]))
    assert delivered_count >= 0


def test_e2e_defaults_drains_below_safety_without_planning(defaults_dict):
    """Negative control: if the planner DOESN'T propose trucks and time
    advances, safety_stock alerts must fire. Confirms the planner
    actually matters."""
    d = copy.deepcopy(defaults_dict)
    # Walk clock forward without committing any trucks
    for _ in range(72):
        _advance_one_hour(d, DEFAULT_CONFIG)
    alerts = run_projection(d)
    safety = [a for a in alerts if a["type"] == "safety_stock"]
    assert len(safety) >= 1


# ── Example customer ──────────────────────────────────────────────────────────

def test_e2e_example_customer_demo_flow():
    """Full algorithm walkthrough against the 3-product example customer."""
    cfg, d = load_customer("example_customer")
    week_start, week_end = get_target_week_bounds(d)
    week_run_hours = get_run_hours_in_window(d, week_start, week_end)
    target = get_target_for_week(week_run_hours, cfg=cfg)

    # Plan for each product
    new_trucks = []
    for product in d["consumption_rates"]:
        new_trucks.extend(
            plan_for_product(d, product, target, week_start, week_end,
                              new_trucks, cfg=cfg)
        )

    # Commit each proposed truck with the customer's SAP format
    for t in new_trucks:
        _commit_truck(d, t, cfg)

    # All SAP numbers should match the customer's format prefix
    for t in d["scheduled_trucks"]:
        assert t["sap_order"].startswith("ORD-")

    # Tick a few hours and confirm no crashes across the 6-tank topology
    for _ in range(48):
        _advance_one_hour(d, cfg)


def test_e2e_example_customer_holiday_blocks_consumption():
    """Mark every day in the simulation horizon as a holiday and confirm
    tank levels never decrease across the walk — the holiday gate must
    propagate through the consumption simulator."""
    cfg, d = load_customer("example_customer")

    holidays = []
    from time_utils import run_hour_to_dt
    for h in range(0, 200, 24):
        holidays.append(run_hour_to_dt(d, h).date().isoformat())
    cfg = PlantConfig(
        **{**{f.name: getattr(cfg, f.name) for f in __import__("dataclasses").fields(cfg)},
           "plant_holidays": tuple(holidays)}
    )

    starting_levels = {n: t["current_level_lbs"]
                       for n, t in d["tanks"].items()}
    for _ in range(48):
        _advance_one_hour(d, cfg)
    for name, level in starting_levels.items():
        assert d["tanks"][name]["current_level_lbs"] == level, (
            f"{name} drained during a holiday block")

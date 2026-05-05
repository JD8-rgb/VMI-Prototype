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
from read_schedule import parse_schedule, apply_schedule_to_data
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

    # Run alerts after commit — no exception and every alert has the
    # required structural fields. (The projection window covers week 1,
    # not the target week, so safety_stock alerts may still appear for
    # the current-week shortfall; that's expected and outside the
    # planner's scope.)
    alerts_after_plan = get_all_alerts(d)
    for _a in alerts_after_plan:
        assert "type" in _a and "severity" in _a and "text" in _a, (
            f"Alert missing required fields: {_a!r}"
        )

    # Walk the clock through week 1 (trucks are scheduled in week 2)
    starting_hour = d["current_run_hour"]
    for _ in range(168):
        _advance_one_hour(d, DEFAULT_CONFIG)
    assert d["current_run_hour"] == starting_hour + 168

    # Trucks are scheduled in the target week (168-336); they should
    # still be pending after the week-1 advance — not yet delivered.
    pending_after_w1 = {
        t["sap_order"] for t in d["scheduled_trucks"] if t["sap_order"] in saps
    }
    assert pending_after_w1 == set(saps), (
        "Planned trucks should still be pending at week-2 start; "
        f"some missing: {set(saps) - pending_after_w1}"
    )

    # Walk the clock through week 2 — all trucks must be delivered.
    for _ in range(168):
        _advance_one_hour(d, DEFAULT_CONFIG)

    still_scheduled_saps = {
        t["sap_order"] for t in d["scheduled_trucks"] if t["sap_order"] in saps
    }
    assert still_scheduled_saps == set(), (
        f"Trucks {still_scheduled_saps} were planned for week 2 "
        "but not delivered after the full 336-hour advance."
    )

    # After delivering all trucks, the combined level for each product
    # must be positive (trucks actually added material, not zero-fill).
    for product in d["consumption_rates"]:
        combined = sum(
            t["current_level_lbs"] for t in d["tanks"].values()
            if t["product"] == product
        )
        assert combined > 0, (
            f"After truck deliveries, {product} combined level should "
            f"be > 0; got {combined}"
        )


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


# ── Real-email → alert end-to-end integration ─────────────────────────────────


def test_e2e_real_email_to_safety_stock_alert(defaults_dict):
    """Full pipeline: parse a realistic schedule email body, apply it to
    state, run the projection, and assert that a safety_stock alert fires
    when the schedule has too few run-hours to cover demand.

    Pipeline under test:
        parse_schedule (regex path, no API key)
        → apply_schedule_to_data
        → run_projection / get_all_alerts

    Scenario: the "customer" sends a 2-window schedule (Mon + Tue only =
    32 run-hours, ~18,600 lbs demand) against a near-empty tank (8,000 lbs
    usable). Safety stock (10,000 lbs) must be breached and flagged.
    """
    # 3-window schedule (parser requires >= 3 days for HIGH confidence)
    schedule_body = (
        "Hi team,\n\n"
        "Schedule for next week:\n"
        "Mon 0600 to 2200\n"
        "Tue 0600 to 2200\n"
        "Wed 0600 to 2200\n\n"
        "Thanks,\nScheduler"
    )

    # ── Step 1: parse ──
    entries, confidence, notes, method = parse_schedule(schedule_body, api_key=None)
    assert confidence == "high", (
        f"Parser should be HIGH confidence for 3-window email, got {confidence!r}. "
        "Notes: " + "; ".join(notes)
    )
    assert method == "regex"
    assert len(entries) == 3          # Mon, Tue, Wed windows

    # ── Step 2: apply schedule to state ──
    d = copy.deepcopy(defaults_dict)
    # Put tanks near-empty so safety stock will be breached
    for name, tank in d["tanks"].items():
        if tank["product"] == "Product U":
            tank["current_level_lbs"] = 9000   # U: 8k usable (9k - 1k heel)
        else:
            tank["current_level_lbs"] = 9000   # M: 8k usable

    apply_schedule_to_data(d, entries)

    # Schedule was written into run_schedule (target week windows)
    applied_windows = [w for w in d["run_schedule"]]
    assert len(applied_windows) >= 2, (
        "apply_schedule_to_data must have written at least 2 windows"
    )

    # ── Step 3: run projection → get_all_alerts ──
    alerts = get_all_alerts(d)
    ss_alerts = [a for a in alerts if a["type"] == "safety_stock"]

    # With only 32 run-hours in the week, demand (~18,600 lbs) exceeds
    # supply (~8,000 lbs usable) — safety stock must fire.
    assert ss_alerts, (
        "Safety-stock alert must fire when the applied schedule leaves "
        "insufficient supply to cover demand. Alerts received: "
        + str([a["type"] for a in alerts])
    )
    for a in ss_alerts:
        assert a["severity"] == "red_flag"
        assert a["product"] in a["text"]


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

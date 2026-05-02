"""Scalability: arbitrary tank topology (1, 2, 3+ tanks per product).

The default customer in defaults.json has 2 tanks per product. These
tests prove the algorithm layer also works for the 1-tank and 3+-tank
cases — the per-customer scalability requirement from HANDOFF.md P1#5.
"""

from __future__ import annotations

import copy

import pytest

from alerts import (
    simulate_consume,
    simulate_delivery,
    simulate_delivery_no_alert,
    find_lowest_in,
    find_others_in,
    get_combined_level_from_tanks,
    run_projection,
    _refresh_draw_status,
)


# ── Builders for non-default topologies ──────────────────────────────────────

def _one_tank_state(defaults_dict, product="Product U"):
    """Replace all tanks for `product` with a single tank; keep the
    other product unchanged."""
    d = copy.deepcopy(defaults_dict)
    new_tanks = {}
    seen = False
    for name, info in d["tanks"].items():
        if info["product"] == product:
            if not seen:
                # Keep one, rename to convey the 1-tank topology
                new_tanks[f"{product[-1]}-Solo"] = {
                    "product": product,
                    "current_level_lbs": 25_000,
                    "max_capacity_lbs": 60_000,  # bigger so a 33k truck fits
                    "heel_lbs": 1000,
                    "status": "draw",
                }
                seen = True
            # else: drop this tank
        else:
            new_tanks[name] = info
    d["tanks"] = new_tanks
    return d


def _three_tank_state(defaults_dict, product="Product U"):
    """Make `product` have three tanks; keep the other product unchanged."""
    d = copy.deepcopy(defaults_dict)
    new_tanks = {}
    for name, info in d["tanks"].items():
        if info["product"] != product:
            new_tanks[name] = info
    new_tanks[f"{product[-1]}-Tank1"] = {
        "product": product, "current_level_lbs": 5_000,
        "max_capacity_lbs": 35_000, "heel_lbs": 1000, "status": "draw",
    }
    new_tanks[f"{product[-1]}-Tank2"] = {
        "product": product, "current_level_lbs": 15_000,
        "max_capacity_lbs": 35_000, "heel_lbs": 1000, "status": "standby",
    }
    new_tanks[f"{product[-1]}-Tank3"] = {
        "product": product, "current_level_lbs": 25_000,
        "max_capacity_lbs": 35_000, "heel_lbs": 1000, "status": "standby",
    }
    d["tanks"] = new_tanks
    return d


# ── find_others_in ───────────────────────────────────────────────────────────

def test_find_others_in_returns_lowest_first(defaults_dict):
    d = _three_tank_state(defaults_dict, "Product U")
    target = find_lowest_in(d["tanks"], "Product U")
    others = find_others_in(d["tanks"], "Product U", target)
    # Ordering should be ascending by current_level_lbs
    levels = [d["tanks"][n]["current_level_lbs"] for n in others]
    assert levels == sorted(levels)


def test_find_others_in_empty_when_one_tank(defaults_dict):
    d = _one_tank_state(defaults_dict, "Product U")
    target = find_lowest_in(d["tanks"], "Product U")
    assert find_others_in(d["tanks"], "Product U", target) == []


# ── 1-tank topology ──────────────────────────────────────────────────────────

def test_simulate_consume_drains_single_tank(defaults_dict):
    d = _one_tank_state(defaults_dict, "Product U")
    starting = next(t for t in d["tanks"].values()
                    if t["product"] == "Product U")["current_level_lbs"]
    simulate_consume(d["tanks"], "Product U", 5_000)
    after = next(t for t in d["tanks"].values()
                 if t["product"] == "Product U")["current_level_lbs"]
    assert after == starting - 5_000


def test_simulate_delivery_overfill_alert_with_one_tank(defaults_dict):
    """With one 60k-capacity tank already at 25k, a 37k truck has only
    35k of headroom and must trigger the overfill alert."""
    d = _one_tank_state(defaults_dict, "Product M")
    # Force Product M tank to start at 30k so 37k truck doesn't fit
    for t in d["tanks"].values():
        if t["product"] == "Product M":
            t["current_level_lbs"] = 30_000
            t["max_capacity_lbs"] = 60_000
    truck = {"sap_order": "SAP1", "product": "Product M",
              "quantity_lbs": 37_000, "arrival_run_hour": 8.0}
    alert = simulate_delivery(d["tanks"], truck)
    assert alert is not None
    assert alert["type"] == "overfill"


def test_simulate_delivery_no_alert_one_tank_pours_until_full(defaults_dict):
    d = _one_tank_state(defaults_dict, "Product U")
    # Set tank to almost full; oversize truck — overflow has nowhere to go
    tank_name = next(n for n, t in d["tanks"].items()
                     if t["product"] == "Product U")
    d["tanks"][tank_name]["current_level_lbs"] = 55_000  # 5k headroom
    truck = {"sap_order": None, "product": "Product U",
              "quantity_lbs": 33_000, "arrival_run_hour": 8.0}
    simulate_delivery_no_alert(d["tanks"], truck)
    # Pour fills exactly to capacity; nothing escapes
    assert d["tanks"][tank_name]["current_level_lbs"] == 60_000


# ── 3-tank topology ──────────────────────────────────────────────────────────

def test_simulate_consume_three_tanks_drains_lowest_first(defaults_dict):
    d = _three_tank_state(defaults_dict, "Product U")
    # Tank1 starts at 5k (lowest above heel). Tank2 at 15k. Tank3 at 25k.
    simulate_consume(d["tanks"], "Product U", 3_000)
    # All consumption from Tank1 (5k → 2k; below heel? 1k heel, so 2k is
    # still above heel). Tank2/Tank3 untouched.
    assert d["tanks"]["U-Tank1"]["current_level_lbs"] == 2_000
    assert d["tanks"]["U-Tank2"]["current_level_lbs"] == 15_000
    assert d["tanks"]["U-Tank3"]["current_level_lbs"] == 25_000


def test_simulate_consume_three_tanks_switches_at_heel(defaults_dict):
    d = _three_tank_state(defaults_dict, "Product U")
    # Drain enough to drop Tank1 to heel and continue from Tank2.
    # Tank1 has 5k, heel 1k → 4k drawable.
    # Consume 6k: 4k from Tank1, 2k from Tank2.
    simulate_consume(d["tanks"], "Product U", 6_000)
    assert d["tanks"]["U-Tank1"]["current_level_lbs"] == 1_000  # heel
    assert d["tanks"]["U-Tank2"]["current_level_lbs"] == 13_000
    assert d["tanks"]["U-Tank3"]["current_level_lbs"] == 25_000


def test_simulate_delivery_three_tanks_cascades_overflow(defaults_dict):
    d = _three_tank_state(defaults_dict, "Product U")
    # Tank1=5k (lowest), Tank2=15k, Tank3=25k. Each cap 35k → space:
    # Tank1 30k, Tank2 20k, Tank3 10k. Total space = 60k.
    truck = {"sap_order": None, "product": "Product U",
              "quantity_lbs": 40_000,  # > 30k single-tank usable (34k)
              "arrival_run_hour": 8.0}
    simulate_delivery_no_alert(d["tanks"], truck)
    # 40k poured: Tank1 takes 30k (full), Tank2 takes 10k (next-lowest).
    # Tank3 untouched.
    assert d["tanks"]["U-Tank1"]["current_level_lbs"] == 35_000
    assert d["tanks"]["U-Tank2"]["current_level_lbs"] == 25_000
    assert d["tanks"]["U-Tank3"]["current_level_lbs"] == 25_000


def test_simulate_delivery_three_tanks_overfill_alert_uses_total_capacity(defaults_dict):
    """Three tanks, all near full. A truck that exceeds total combined
    space must alert."""
    d = _three_tank_state(defaults_dict, "Product M")
    # Set Product M to three near-full tanks
    new_tanks = {n: t for n, t in d["tanks"].items()
                  if t["product"] != "Product M"}
    new_tanks["M-T1"] = {"product": "Product M", "current_level_lbs": 33_000,
                         "max_capacity_lbs": 35_000, "heel_lbs": 1000,
                         "status": "draw"}
    new_tanks["M-T2"] = {"product": "Product M", "current_level_lbs": 33_000,
                         "max_capacity_lbs": 35_000, "heel_lbs": 1000,
                         "status": "standby"}
    new_tanks["M-T3"] = {"product": "Product M", "current_level_lbs": 33_000,
                         "max_capacity_lbs": 35_000, "heel_lbs": 1000,
                         "status": "standby"}
    d["tanks"] = new_tanks
    # Total space across all three = 6k. 37k truck doesn't fit.
    truck = {"sap_order": "SAP1", "product": "Product M",
              "quantity_lbs": 37_000, "arrival_run_hour": 8.0}
    alert = simulate_delivery(d["tanks"], truck)
    assert alert is not None
    assert alert["type"] == "overfill"
    # Alert text should reference all three tanks (not "no other tank")
    assert "M-T1" in alert["text"]
    assert "M-T2" in alert["text"]
    assert "M-T3" in alert["text"]


def test_run_projection_three_tank_topology_no_crash(defaults_dict):
    """Smoke: projection over a 3-tank-per-product state runs and
    returns a list (no assumptions about tank count)."""
    d = _three_tank_state(defaults_dict, "Product U")
    alerts = run_projection(d)
    assert isinstance(alerts, list)


# ── _refresh_draw_status ──────────────────────────────────────────────────────

def test_refresh_draw_status_three_tanks_picks_lowest_above_heel(defaults_dict):
    d = _three_tank_state(defaults_dict, "Product U")
    _refresh_draw_status(d["tanks"], "Product U")
    statuses = {n: t["status"] for n, t in d["tanks"].items()
                if t["product"] == "Product U"}
    assert statuses["U-Tank1"] == "draw"  # lowest above heel
    assert statuses["U-Tank2"] == "standby"
    assert statuses["U-Tank3"] == "standby"

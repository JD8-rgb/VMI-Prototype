"""Multi-customer scalability smoke test (HANDOFF.md Q15).

Loads customers/example_customer.json — 3 products with asymmetric
tank topology, custom delivery slots, custom SAP format, custom
holiday calendar — and exercises every public algorithm path. The
algorithms must not depend on any demo-specific assumption (2 products,
2-tanks-per-product, "Product U" / "Product M" naming, default
slots/lead time/safety stock).

If any of these tests fail, the prototype's claim of multi-customer
scalability is broken.
"""

from __future__ import annotations

import copy

import pytest

from customers import load_customer
from alerts import (
    get_all_alerts,
    check_lead_time,
    check_late_trucks,
    run_projection,
    is_running_at,
)
from plan_orders import (
    plan_for_product,
    get_target_week_bounds,
    get_run_hours_in_window,
    get_target_for_week,
    is_valid_delivery_slot,
)
from projection import compute_level_history
from state import PlantState
import app


@pytest.fixture
def customer():
    cfg, state_dict = load_customer("example_customer")
    return cfg, state_dict


# ── Loader ────────────────────────────────────────────────────────────────────

def test_loader_returns_cfg_and_state(customer):
    cfg, state_dict = customer
    # Cfg overrides applied
    assert cfg.lead_time_hours == 72
    assert cfg.safety_stock_lbs == 8000
    assert cfg.delivery_slots == (5, 11, 17)
    assert cfg.sap_order_format == "ORD-{n:08d}"
    assert cfg.sap_order_seed == 100_000
    assert "2026-12-25" in cfg.plant_holidays
    # State sanity — schema_version reflects whatever CURRENT is today;
    # the loader auto-migrates older customer files forward.
    from data_io import CURRENT_SCHEMA_VERSION
    assert state_dict["schema_version"] == CURRENT_SCHEMA_VERSION
    assert len(state_dict["tanks"]) == 6      # 3 + 2 + 1


def test_state_round_trips_via_plant_state(customer):
    _, state_dict = customer
    rt = PlantState.from_dict(state_dict).to_dict()
    # Lossless on every product/tank
    for name in state_dict["tanks"]:
        assert rt["tanks"][name] == state_dict["tanks"][name]


# ── alerts ────────────────────────────────────────────────────────────────────

def test_get_all_alerts_runs_against_three_product_customer(customer):
    cfg, state_dict = customer
    alerts = get_all_alerts(state_dict, cfg=cfg)
    assert isinstance(alerts, list)


def test_check_lead_time_for_each_product(customer):
    cfg, state_dict = customer
    for product in state_dict["consumption_rates"].keys():
        # Should not crash and should return None or a properly-typed alert dict
        result = check_lead_time(state_dict, product, cfg=cfg)
        assert result is None or result["type"] == "lead_time"
        if result is not None:
            assert result["product"] == product


def test_check_late_trucks_with_third_product(customer):
    cfg, state_dict = customer
    d = copy.deepcopy(state_dict)
    d["current_run_hour"] = 10.0
    d["scheduled_trucks"] = [{
        "sap_order": "ORD-00100001",
        "product": "Product Catalyst",  # third product, never touched in defaults
        "quantity_lbs": 22000,
        "arrival_run_hour": 5.0,  # 5h overdue
    }]
    alerts = check_late_trucks(d, cfg=cfg)
    assert len(alerts) == 1
    assert alerts[0]["product"] == "Product Catalyst"
    assert alerts[0]["type"] == "late_truck"


def test_run_projection_handles_three_products_and_holidays(customer):
    cfg, state_dict = customer
    alerts = run_projection(state_dict, cfg=cfg)
    # Each safety alert should reference a product, no crashes
    for a in alerts:
        if a["type"] == "safety_stock":
            assert a["product"] in state_dict["consumption_rates"]


def test_is_running_at_respects_customer_holidays(customer):
    cfg, state_dict = customer
    # Christmas 2026 falls into the planner's near-term horizon when
    # current_run_hour=0 and epoch=2026-04-13. Compute the run-hour
    # for a holiday and confirm it's not running.
    from time_utils import dt_to_run_hour
    from datetime import datetime
    holiday = datetime.fromisoformat("2026-05-25T10:00:00")
    rh = dt_to_run_hour(state_dict, holiday)
    assert is_running_at(state_dict, rh, cfg=cfg) is False


# ── planner ───────────────────────────────────────────────────────────────────

def test_planner_proposes_for_third_product(customer, capsys):
    cfg, state_dict = customer
    week_start, week_end = get_target_week_bounds(state_dict)
    week_run_hours = get_run_hours_in_window(state_dict, week_start, week_end)
    target = get_target_for_week(week_run_hours, cfg=cfg)

    # Catalyst starts at 9k, draws 120 lbs/hr * 80 weekly run hours = 9.6k → breach
    new = plan_for_product(state_dict, "Product Catalyst", target,
                           week_start, week_end, [], cfg=cfg)
    # Smoke: no crash. Each truck should have the customer's quantity and a
    # slot in the customer's delivery_slot set.
    for t in new:
        assert t["product"] == "Product Catalyst"
        assert t["quantity_lbs"] == 22000
        slot_hod = int(t["arrival_run_hour"] - week_start) % 24
        assert slot_hod in cfg.delivery_slots


def test_valid_delivery_slot_uses_customer_slots(customer):
    cfg, state_dict = customer
    week_start, _ = get_target_week_bounds(state_dict)
    # 11 IS a configured slot for this customer → valid (assuming run window
    # covers it and >= lead_time)
    assert is_valid_delivery_slot(state_dict, week_start + 11, week_start, cfg=cfg) is True
    # 8 is the default slot but NOT in this customer's slot set → invalid
    assert is_valid_delivery_slot(state_dict, week_start + 8, week_start, cfg=cfg) is False


# ── projection ────────────────────────────────────────────────────────────────

def test_projection_returns_history_for_each_tank(customer):
    cfg, state_dict = customer
    out = compute_level_history(state_dict, hours=48, cfg=cfg)
    assert set(out["tanks"].keys()) == set(state_dict["tanks"].keys())
    for hist in out["tanks"].values():
        assert len(hist) == 48 + 1


# ── SAP numbering ─────────────────────────────────────────────────────────────

def test_sap_format_threads_through_app_next_sap(customer):
    cfg, state_dict = customer
    # Empty history → seed
    fresh = dict(state_dict)
    fresh["scheduled_trucks"] = []
    fresh["sap_history"] = []
    assert app._next_sap(fresh, cfg=cfg) == "ORD-00100000"


def test_sap_increments_above_existing_history(customer):
    cfg, state_dict = customer
    fresh = dict(state_dict)
    fresh["scheduled_trucks"] = []
    fresh["sap_history"] = ["ORD-00100050", "ORD-00100051"]
    assert app._next_sap(fresh, cfg=cfg) == "ORD-00100052"

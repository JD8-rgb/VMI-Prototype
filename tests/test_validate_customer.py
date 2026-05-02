"""Coverage for validate_customer linter.

Each test crafts a deliberately-broken customer file in tmp_path, points
the loader at it via monkeypatch, and asserts the linter surfaces the
right error/warning."""

from __future__ import annotations

import copy
import json

import pytest

import customers as customers_mod
import validate_customer


def _write_customer(tmp_path, customer_id: str, doc: dict) -> None:
    (tmp_path / f"{customer_id}.json").write_text(json.dumps(doc))


@pytest.fixture
def tmp_customers(tmp_path, monkeypatch):
    """Redirect the customers/ dir to tmp_path for the duration of one test."""
    monkeypatch.setattr(customers_mod, "CUSTOMERS_DIR", str(tmp_path))
    monkeypatch.setattr(validate_customer, "CUSTOMERS_DIR", str(tmp_path),
                          raising=False)
    return tmp_path


def _minimal_valid_state(overrides: dict | None = None) -> dict:
    state = {
        "schema_version": 1,
        "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0,
        "tanks": {
            "T1": {
                "product": "X",
                "current_level_lbs": 5000,
                "max_capacity_lbs": 30000,
                "heel_lbs": 1000,
                "status": "draw",
            },
        },
        "consumption_rates": {"X": {"lbs_per_hour": 100.0}},
        "truck_quantities": {"X": 25000},
        "scheduled_trucks": [],
        "run_schedule": [],
    }
    if overrides:
        state.update(overrides)
    return state


def test_clean_customer_passes(tmp_customers):
    _write_customer(tmp_customers, "good", {
        "config_overrides": {},
        "state": _minimal_valid_state(),
    })
    findings = validate_customer.validate_customer("good")
    assert findings == []


def test_missing_truck_quantity_is_error(tmp_customers):
    state = _minimal_valid_state()
    state["truck_quantities"] = {}     # X missing
    _write_customer(tmp_customers, "no_qty", {
        "config_overrides": {},
        "state": state,
    })
    findings = validate_customer.validate_customer("no_qty")
    assert any(f.severity == "error" and f.field == "truck_quantities"
                for f in findings)


def test_orphan_tank_warns(tmp_customers):
    state = _minimal_valid_state()
    state["tanks"]["TankOrphan"] = {
        "product": "Y",   # no rate for Y
        "current_level_lbs": 0, "max_capacity_lbs": 10000,
        "heel_lbs": 0, "status": "standby",
    }
    _write_customer(tmp_customers, "orphan", {
        "config_overrides": {},
        "state": state,
    })
    findings = validate_customer.validate_customer("orphan")
    assert any(f.severity == "warning" and "orphan" in f.message.lower()
                for f in findings)


def test_heel_above_capacity_is_error(tmp_customers):
    state = _minimal_valid_state()
    state["tanks"]["T1"]["heel_lbs"] = 999_999
    _write_customer(tmp_customers, "bad_heel", {
        "config_overrides": {},
        "state": state,
    })
    findings = validate_customer.validate_customer("bad_heel")
    assert any(f.severity == "error" and "heel_lbs" in f.message
                for f in findings)


def test_negative_current_level_warns(tmp_customers):
    state = _minimal_valid_state()
    state["tanks"]["T1"]["current_level_lbs"] = -100
    _write_customer(tmp_customers, "neg_level", {
        "config_overrides": {},
        "state": state,
    })
    findings = validate_customer.validate_customer("neg_level")
    assert any(f.severity == "warning" and "negative" in f.message.lower()
                for f in findings)


def test_unknown_truck_product_errors(tmp_customers):
    state = _minimal_valid_state()
    state["scheduled_trucks"] = [{
        "sap_order": "SAP1", "product": "Mystery",
        "quantity_lbs": 1000, "arrival_run_hour": 8.0,
    }]
    _write_customer(tmp_customers, "bad_truck", {
        "config_overrides": {},
        "state": state,
    })
    findings = validate_customer.validate_customer("bad_truck")
    assert any(f.severity == "error"
                and f.field.startswith("scheduled_trucks")
                for f in findings)


def test_invalid_delivery_slot_hour_errors(tmp_customers):
    _write_customer(tmp_customers, "bad_slot", {
        "config_overrides": {"delivery_slots": [25, 99]},
        "state": _minimal_valid_state(),
    })
    findings = validate_customer.validate_customer("bad_slot")
    assert any(f.severity == "error" and f.field == "delivery_slots"
                for f in findings)


def test_invalid_holiday_iso_warns(tmp_customers):
    _write_customer(tmp_customers, "bad_holiday", {
        "config_overrides": {"plant_holidays": ["not-a-date", "2026-12-25"]},
        "state": _minimal_valid_state(),
    })
    findings = validate_customer.validate_customer("bad_holiday")
    assert any(f.severity == "warning" and f.field == "plant_holidays"
                for f in findings)


def test_zero_lead_time_errors(tmp_customers):
    _write_customer(tmp_customers, "zero_lead", {
        "config_overrides": {"lead_time_hours": 0},
        "state": _minimal_valid_state(),
    })
    findings = validate_customer.validate_customer("zero_lead")
    assert any(f.severity == "error" and f.field == "lead_time_hours"
                for f in findings)


def test_load_failure_surfaces_as_error(tmp_customers):
    """A file with an unknown PlantConfig field fails at load_customer
    (TypeError). The linter should report a single load error."""
    _write_customer(tmp_customers, "load_fail", {
        "config_overrides": {"this_does_not_exist": 42},
        "state": _minimal_valid_state(),
    })
    findings = validate_customer.validate_customer("load_fail")
    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert findings[0].field == "<load>"


def test_missing_file_surfaces_as_error(tmp_customers):
    findings = validate_customer.validate_customer("nonexistent")
    assert len(findings) == 1
    assert findings[0].severity == "error"
    assert findings[0].field == "<file>"


# ── main entrypoint ──────────────────────────────────────────────────────────

def test_main_returns_zero_on_clean(tmp_customers, capsys):
    _write_customer(tmp_customers, "clean", {
        "config_overrides": {},
        "state": _minimal_valid_state(),
    })
    rc = validate_customer.main(["clean"])
    assert rc == 0


def test_main_returns_one_on_error(tmp_customers, capsys):
    _write_customer(tmp_customers, "broken", {
        "config_overrides": {"lead_time_hours": -5},
        "state": _minimal_valid_state(),
    })
    rc = validate_customer.main(["broken"])
    assert rc == 1


def test_main_strict_mode_treats_warnings_as_errors(tmp_customers, capsys):
    state = _minimal_valid_state()
    state["tanks"]["T1"]["current_level_lbs"] = -1   # warning only
    _write_customer(tmp_customers, "warn_only", {
        "config_overrides": {},
        "state": state,
    })
    assert validate_customer.main(["warn_only"]) == 0
    assert validate_customer.main(["warn_only", "--strict"]) == 1


def test_main_no_args_iterates_directory(tmp_customers, capsys):
    """With no args, main should pick up every *.json in the dir."""
    _write_customer(tmp_customers, "a", {"config_overrides": {},
                                            "state": _minimal_valid_state()})
    _write_customer(tmp_customers, "b", {"config_overrides": {},
                                            "state": _minimal_valid_state()})
    rc = validate_customer.main([])
    captured = capsys.readouterr().out
    assert "a" in captured and "b" in captured
    assert "Checked 2 customer(s)" in captured
    assert rc == 0


def test_real_example_customer_passes_lint():
    """The real example_customer.json file in customers/ must pass
    the linter — regression guard for the canonical reference."""
    findings = validate_customer.validate_customer("example_customer")
    errors = [f for f in findings if f.severity == "error"]
    assert errors == []

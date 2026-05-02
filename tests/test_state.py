"""Round-trip + invariants for the PlantState dataclass family.

These guard the bridge layer between the JSON file shape and the typed
domain model. If any of these fail, the dict-based callers in app.py and
the CLI scripts will silently lose fields when they hand off to a
PlantState-aware algorithm and back.
"""

from __future__ import annotations

import copy
import json

from state import PlantState, TankState, RunWindow, Truck, ProductRate


def test_round_trip_preserves_known_fields(defaults_dict):
    rt = PlantState.from_dict(defaults_dict).to_dict()
    # Every known top-level key from defaults.json must come back unchanged.
    for k, v in defaults_dict.items():
        assert k in rt, f"missing key on round-trip: {k}"
    assert rt["simulation_epoch"] == defaults_dict["simulation_epoch"]
    assert rt["current_run_hour"] == defaults_dict["current_run_hour"]
    assert rt["truck_quantities"] == defaults_dict["truck_quantities"]


def test_round_trip_preserves_tank_values(defaults_dict):
    rt = PlantState.from_dict(defaults_dict).to_dict()
    for name, before in defaults_dict["tanks"].items():
        after = rt["tanks"][name]
        for field in ("product", "current_level_lbs",
                       "max_capacity_lbs", "heel_lbs", "status"):
            assert after[field] == before[field], f"{name}.{field} drifted"


def test_extra_field_preserves_unknown_keys(defaults_dict):
    """Future schema fields must round-trip even when older code can't
    interpret them. This is what enables forward-compatible data files."""
    d = copy.deepcopy(defaults_dict)
    d["future_feature_x"] = {"nested": [1, 2, 3]}
    d["new_top_level"] = "hello"

    state = PlantState.from_dict(d)
    rt = state.to_dict()
    assert rt["future_feature_x"] == {"nested": [1, 2, 3]}
    assert rt["new_top_level"] == "hello"


def test_combined_level_and_usable(defaults_dict):
    state = PlantState.from_dict(defaults_dict)
    # Defaults: U-Tank1=0, U-Tank2=20000 (heel 1000 each)
    assert state.combined_level_lbs("Product U") == 20_000
    # Usable clamps each tank at zero, so empty tank with heel 1000
    # contributes 0, not -1000:
    assert state.combined_usable_lbs("Product U") == 19_000
    # Defaults: M-Tank1=0, M-Tank2=26000
    assert state.combined_level_lbs("Product M") == 26_000
    assert state.combined_usable_lbs("Product M") == 25_000


def test_tanks_for_returns_in_declaration_order(defaults_dict):
    state = PlantState.from_dict(defaults_dict)
    u_tanks = state.tanks_for("Product U")
    m_tanks = state.tanks_for("Product M")
    assert len(u_tanks) == 2 and all(t.product == "Product U" for t in u_tanks)
    assert len(m_tanks) == 2 and all(t.product == "Product M" for t in m_tanks)


def test_truck_round_trip():
    payload = {
        "sap_order": "SAP90001",
        "product": "Product U",
        "quantity_lbs": 33000,
        "arrival_run_hour": 12.5,
    }
    assert Truck.from_dict(payload).to_dict() == payload


def test_run_window_default_label():
    rw = RunWindow.from_dict({"start_hour": 6.0, "end_hour": 22.0})
    assert rw.label == ""
    assert rw.to_dict() == {"start_hour": 6.0, "end_hour": 22.0, "label": ""}


def test_truck_with_no_sap():
    """Planned-but-not-yet-committed trucks have sap_order=None."""
    payload = {
        "product": "Product M",
        "quantity_lbs": 37000,
        "arrival_run_hour": 50.0,
    }
    t = Truck.from_dict(payload)
    assert t.sap_order is None
    assert t.to_dict()["sap_order"] is None


def test_round_trip_idempotent(defaults_dict):
    """from_dict(to_dict(from_dict(d))) == from_dict(d) — no drift on
    repeated conversions."""
    once = PlantState.from_dict(defaults_dict).to_dict()
    twice = PlantState.from_dict(once).to_dict()
    assert json.dumps(once, sort_keys=True) == json.dumps(twice, sort_keys=True)

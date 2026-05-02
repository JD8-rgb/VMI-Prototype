"""Shared fixtures for the algorithm test suites.

The prototype's algorithm modules (alerts, plan_orders, projection) all
accept either a legacy `data` dict OR a typed `PlantState`. Every test
that walks a public API path is parametrized over `shape` so both call
shapes stay in lockstep — if a future change drifts one path from the
other, these tests fail.
"""

from __future__ import annotations

import copy
import json
import os
import sys
from pathlib import Path

import pytest

# Make the project root importable when pytest is invoked from any cwd.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from state import PlantState  # noqa: E402


@pytest.fixture
def defaults_dict():
    """Pristine `data` dict loaded from defaults.json (deep-copied per test)."""
    with open(ROOT / "defaults.json") as f:
        return json.load(f)


@pytest.fixture
def default_state(defaults_dict):
    """Same content as defaults_dict, as a typed PlantState."""
    return PlantState.from_dict(defaults_dict)


@pytest.fixture(params=["dict", "state"])
def shape(request):
    """Polymorphism gate: every test using this fixture runs twice,
    once with a dict and once with a PlantState. Both call shapes must
    produce identical results in the algorithm modules."""
    return request.param


@pytest.fixture
def as_shape(shape):
    """Helper that converts a dict to either dict or PlantState per the
    parametrized shape. Tests build a dict, then pass `as_shape(d)` into
    the function under test."""
    def _convert(data: dict):
        if shape == "dict":
            return copy.deepcopy(data)
        return PlantState.from_dict(copy.deepcopy(data))
    return _convert


def make_drained_state(defaults_dict: dict, product: str, level_lbs: float) -> dict:
    """Return a fresh dict with every tank for `product` set to `level_lbs`.
    Useful for forcing safety-stock / lead-time alerts deterministically."""
    d = copy.deepcopy(defaults_dict)
    for info in d["tanks"].values():
        if info["product"] == product:
            info["current_level_lbs"] = level_lbs
    return d

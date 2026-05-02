"""Per-customer config + state loader.

Each `customers/<id>.json` is a single document bundling a PlantConfig
overrides dict and a data.json-shaped state dict. `load_customer(id)`
returns both halves, with schema migration applied to the state half so
older customer files keep working through schema bumps."""

from __future__ import annotations

import json
import os
from typing import Tuple

from config import PlantConfig
from data_io import _migrate


CUSTOMERS_DIR = os.path.dirname(__file__)


def load_customer(customer_id: str) -> Tuple[PlantConfig, dict]:
    """Read customers/<customer_id>.json. Return (cfg, state_dict).

    `cfg` is built by PlantConfig(**config_overrides). `state_dict`
    is the data.json-shaped half, run through _migrate so older files
    are upgraded to the current schema before any algorithm sees them.
    """
    path = os.path.join(CUSTOMERS_DIR, f"{customer_id}.json")
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    overrides = doc.get("config_overrides", {})
    # PlantConfig stores tuples for some fields (delivery_slots,
    # plant_holidays). JSON gives us lists; coerce so the dataclass
    # remains hashable / frozen.
    if "delivery_slots" in overrides:
        overrides["delivery_slots"] = tuple(overrides["delivery_slots"])
    if "plant_holidays" in overrides:
        overrides["plant_holidays"] = tuple(overrides["plant_holidays"])
    cfg = PlantConfig(**overrides)
    state_dict = _migrate(dict(doc.get("state", {})))
    return cfg, state_dict

"""Per-customer config + state loader.

Each `customers/<id>.json` is a single document bundling a PlantConfig
overrides dict and a data.json-shaped state dict. `load_customer(id)`
returns both halves, with schema migration applied to the state half so
older customer files keep working through schema bumps."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Tuple

from config import PlantConfig
from data_io import _migrate


CUSTOMERS_DIR = os.path.dirname(__file__)

# Customer IDs must be filesystem-safe and not allow path traversal.
# Letters, digits, underscore, hyphen only — same charset as a typical
# database tenant slug. Length cap defense-in-depth against pathological
# input.
_VALID_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


def _validate_id(customer_id: str) -> None:
    if not isinstance(customer_id, str) or not _VALID_ID_RE.match(customer_id):
        raise ValueError(
            f"invalid customer_id {customer_id!r}: must match "
            f"[A-Za-z0-9_-]{{1,64}}. Path traversal attempts are blocked."
        )


def load_customer(customer_id: str) -> Tuple[PlantConfig, Dict[str, Any]]:
    """Read customers/<customer_id>.json. Return (cfg, state_dict).

    `cfg` is built by PlantConfig(**config_overrides). `state_dict`
    is the data.json-shaped half, run through _migrate so older files
    are upgraded to the current schema before any algorithm sees them.

    Raises ValueError on invalid customer_id (anything that isn't
    [A-Za-z0-9_-]{1,64}). Raises FileNotFoundError if the file doesn't
    exist; raises TypeError if config_overrides contains a key that
    PlantConfig doesn't recognize (fail fast on misconfigurations).
    """
    _validate_id(customer_id)
    path = os.path.join(CUSTOMERS_DIR, f"{customer_id}.json")
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    overrides = dict(doc.get("config_overrides", {}))
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


def list_customers() -> List[str]:
    """Return every customer_id with a JSON file in this directory.

    Convenience for app.py / CLI scripts that need to populate a tenant
    selector. Filters out non-JSON files (README, __init__) so the list
    is exactly customer ids."""
    out = []
    for name in os.listdir(CUSTOMERS_DIR):
        if name.endswith(".json"):
            out.append(name[:-5])
    return sorted(out)

# Migration Guide

> Recipes for turning the prototype into a production system. Read
> [`ARCHITECTURE.md`](ARCHITECTURE.md) first. Each section here can be
> picked up independently. Order doesn't matter except where noted.

The prototype's **algorithm core** is intentionally portable: it has no
file I/O, no network, no clock. Everything below is about replacing the
edges (storage, email, UI) without touching the core. If a migration
recipe ever requires editing `alerts.py` / `plan_orders.py` /
`projection.py`, that's a smell — read the polymorphic shim section in
`ARCHITECTURE.md` and look for the bridge points first.

---

## 1. Add a new customer

Two-step:

1. Drop a new `customers/<customer_id>.json` with two top-level keys:
   * `config_overrides` — kwargs for `PlantConfig(**)`. Anything you
     don't override inherits the prototype default. List values in
     JSON (e.g. `delivery_slots`, `plant_holidays`) are coerced to
     tuples by the loader so the dataclass stays frozen.
   * `state` — same shape as `data.json` / `defaults.json`. Tank
     topology, consumption rates, truck quantities, run schedule,
     in-flight orders.
2. Confirm with `tests/test_example_customer.py` as a template: copy
   it, point it at your `customer_id`, run the smoke battery.

The algorithm core is provably customer-agnostic across:
* Number of tanks per product (1, 2, 3+, asymmetric).
* Product naming (no hardcoded "Product U" / "Product M" in algorithm code).
* Number of products (≥ 1).
* Custom `delivery_slots`, `lead_time_hours`, `safety_stock_lbs`,
  target curve corners.
* Custom `sap_order_format` (any Python format string with `{n}`).
* Custom `plant_holidays`.

The Streamlit UI (`app.py`) is **not** customer-agnostic and is the
main rewrite target — see § 7 below.

---

## 2. Add a new schema version (data.json shape change)

```python
# In data_io.py
def _migrate_v1_to_v2(data: dict) -> None:
    """Describe what changed and why."""
    # ... mutate data in place ...

_MIGRATIONS[1] = _migrate_v1_to_v2
CURRENT_SCHEMA_VERSION = 2
```

That's it. Every `load_data` call will pick up the new migrator. No
call sites change. Older files (v0 unstamped, v1 stamped) auto-upgrade
on read. Files written by newer code (v3+) pass through unchanged
because `PlantState._extra` preserves unknown fields.

Add tests in `tests/test_data_io.py` modeled on the existing migration
test pattern.

---

## 3. Replace `data_io` with PostgreSQL

The seam is `data_io.load_data` / `save_data` (and the typed wrappers
`load_state` / `save_state`). Replace the body, keep the signature.

```python
# data_io.py (postgres edition)
from sqlalchemy import create_engine, ...

def load_data(plant_id: str = "default") -> dict:
    with engine.connect() as conn:
        row = conn.execute(text("SELECT data FROM plant_state WHERE plant_id=:id"),
                           {"id": plant_id}).first()
    if row is None:
        with open(DEFAULTS_PATH) as f:
            return _migrate(json.load(f))
    return _migrate(row.data)   # row.data is JSONB

def save_data(data: dict, plant_id: str = "default") -> None:
    data.setdefault("schema_version", CURRENT_SCHEMA_VERSION)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO plant_state (plant_id, data, updated_at)
            VALUES (:id, :data, now())
            ON CONFLICT (plant_id) DO UPDATE
            SET data = EXCLUDED.data, updated_at = now()
        """), {"id": plant_id, "data": Json(data)})
```

Keep `_migrate` exactly as-is — it operates on the dict, not on the
storage layer. `PlantState.from_dict` / `.to_dict` work identically
whether the source is a file or a JSONB column.

Concurrency note: the prototype's atomic-write strategy assumes a
single writer. PostgreSQL gives you proper transactions for free —
delete the tempfile / `os.replace` ceremony.

---

## 4. Replace IMAP/SMTP with Microsoft Graph

The seam is `email_client.py`. The functions
`send_mail(subject, body, recipients)` and `fetch_recent_emails(...)`
are the contract; rewrite their bodies against Graph delta queries
and change notifications.

`email_hooks.py` shouldn't need to change — it calls
`send_mail` / `fetch_recent_emails` and doesn't know how the bytes get
on the wire.

For change notifications (web hooks), wire the Graph subscription
into a small endpoint that, on incoming notification, calls the same
read path the IMAP poller currently uses (`read_schedule.fetch_and_apply`
or equivalent). The schedule parser doesn't care where the email body
came from.

---

## 5. Migrate `simulate_*` to pure functions

The four mutators (`simulate_consume`, `simulate_delivery`,
`simulate_delivery_no_alert`, `_refresh_draw_status`) currently
mutate a `tanks` dict-of-dicts in place. The four bridge points
(see `ARCHITECTURE.md`) deepcopy `state.to_dict()["tanks"]` to
satisfy the in-place contract.

Pure-function recipe:

```python
# new shape
def simulate_consume(tanks: Dict[str, TankState], product: str, lbs: float
                      ) -> Dict[str, TankState]:
    """Return a NEW dict with consumption applied. tanks unchanged."""
    new_tanks = {n: dataclasses.replace(t) for n, t in tanks.items()}
    # ... apply consumption to new_tanks ...
    return new_tanks
```

Touch every caller (~30 sites). The bridge points collapse:

```python
# before
tanks = copy.deepcopy(state.to_dict()["tanks"])
simulate_consume(tanks, product, rate)
# ... continue using mutated tanks dict ...

# after
new_tanks = simulate_consume(state.tanks, product, rate)
# state.tanks unchanged; new_tanks is the projection result
```

Algorithm tests survive without modification because they exercise the
public functions (`get_all_alerts`, `plan_for_product`,
`compute_level_history`), which take care of threading the new return
values through internally.

Risk: low but non-zero. Worth ~2 sessions of focused work.

---

## 6. Add proper file locking (or skip and use a database)

Currently a concurrent CLI + Streamlit can lose updates. The right
fix is § 3 (move to a database with proper transactions). If you must
stay on a file: wrap `save_data` with `filelock`:

```python
from filelock import FileLock

def save_data(data: dict, path: str = DATA_PATH) -> None:
    with FileLock(path + ".lock"):
        # ... existing tempfile + os.replace ceremony ...
```

Don't try to write a hand-rolled cross-platform mutex. Filelock
already handles the Windows / POSIX corner cases.

---

## 7. Generalize the Streamlit UI

`app.py` is 1620 LOC and assumes the demo's exact shape:
* 4 hardcoded tank colors, 4 hardcoded `_tank_info` calls
  (`app.py:65, 1100, 1106`).
* Two hardcoded chart calls per product (`app.py:1098, 1104`).
* Form labels and quantity defaults coded to "Product U" / "Product M"
  (`app.py:1388`).
* Y-axis range hardcoded at 37000 (`app.py:508`).

Recommended: build an UI shell that walks `state.tanks` and
`state.consumption_rates` to render whatever topology the loaded
customer has. Color palette becomes a function of tank name (hash →
HSL palette). Charts iterate over products. Forms enumerate products
from `state.truck_quantities`.

This work is independent of the algorithm core — touch only `app.py`,
add a `customer_id` selector at the top, route through
`customers.load_customer`. Or split `app.py` into `ui/` modules per
the deferred work item Q11.

---

## 8. Replace IMAP-style polling with web hooks (orders / acks)

The prototype models orders as a `scheduled_trucks` list with a
free-form `sap_history` ledger. Production needs a state machine:

```
proposed → approved → submitted → acknowledged → scheduled →
shipped → arrived → reconciled
                                              ↘ exception
```

Recipe:
1. Add a `lifecycle_state` field to the truck dict shape; bump
   schema_version (§ 2).
2. Write a `transition_truck(truck, new_state, evidence_id)` helper
   that validates the transition (only `proposed → approved`, etc.)
   and records the event in an audit log.
3. Web-hook endpoints (`/sap/ack`, `/edi/856`, etc.) call
   `transition_truck` and persist via `data_io.save_data` or whatever
   the production storage layer is.

The algorithm core never reads `lifecycle_state` — `plan_for_product`
and the alert checks treat any truck in `scheduled_trucks` as inbound
supply, regardless of state. If you want planner behavior to depend on
state ("don't count an unack'd truck as supply"), filter at the call
site:

```python
state = state.with_only_acknowledged_trucks()
get_all_alerts(state)
```

…rather than threading lifecycle awareness into the algorithm.

---

## 9. Replace `print()` with structured logging (already in scope)

160-ish `print()` call sites across the codebase. The right shape is:

```python
import logging
logger = logging.getLogger(__name__)
logger.info("Placed %s truck at %s", product, format_run_hour(...))
```

Production attaches a JSON formatter for Splunk / Datadog ingestion
without touching algorithm code. The CLI entry-point `print()`s
(usage messages, summary banners) should stay as user-facing UI —
those aren't logs.

This is partially done in this autonomous run; see the logging commit.

---

## 10. Replace naive datetime with tz-aware (DST safety)

`time_utils.py` uses naive `datetime` throughout. For a real plant
operating in a region with DST, the simulation epoch needs a `tzinfo`
and the `run_hour ↔ datetime` conversion needs to honor it. The shape
of the change is small — add `tz=ZoneInfo("America/New_York")` (or
whatever) at the epoch construction site, propagate through
`run_hour_to_dt`. Holiday-date comparison (`is_running_at` /
`_all_slot_run_hours`) becomes tz-aware automatically because it works
off `.date()`.

Test the spring-forward / fall-back cases explicitly.

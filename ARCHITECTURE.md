# Architecture

> Read this before reading code. It explains *why* the modules are
> shaped this way and where the seams are. The migration recipes for
> turning the prototype into a production system live in
> [`MIGRATION_GUIDE.md`](MIGRATION_GUIDE.md).

The prototype is intentionally split into a **pure algorithm core**, a
**typed domain model**, a **serialization layer**, and a **Streamlit
UI / CLI shell**. The algorithm core has no file I/O, no network calls,
and no wall-clock reads — it accepts a clock value and mutable state as
inputs and mutates state in place. Everything that touches a file, an
SMTP server, or a real clock is at the edge.

```
                         ┌──────────────────────────────┐
                         │        Streamlit UI          │
                         │   (app.py — ~3130 LOC)       │
                         └──────────────┬───────────────┘
   ┌──────────────────────┐             │
   │       CLI scripts    │─────────────┤
   │  advance_time.py     │             │
   │  tank_status.py      │             │
   │  plan_orders.main    │             │
   │  schedule_run.py     │             │
   │  schedule_truck.py   │             │
   │  check_reminder.py   │             │
   └──────────────────────┘             │
                                        │
                                        ▼
                  ┌─────────────────────────────────────┐
                  │   data_io.py  (atomic JSON I/O)     │
                  │     load_data / save_data           │
                  │     _migrate (schema versioning)    │
                  └─────────────────┬───────────────────┘
                                    │
                                    ▼
                  ┌─────────────────────────────────────┐
                  │   state.py    PlantState dataclass  │
                  │   config.py   PlantConfig dataclass │
                  │   customers/  per-customer overrides│
                  └─────────────────┬───────────────────┘
                                    │
                                    ▼
            ┌────────────────────────────────────────────┐
            │              ALGORITHM CORE                │
            │                                            │
            │   alerts.py        get_all_alerts          │
            │   plan_orders.py   plan_for_product        │
            │   projection.py    compute_level_history   │
            │   read_schedule.py parse_schedule          │
            │                                            │
            │   No file I/O. No clock. No network.       │
            └────────────────────────────────────────────┘
```

## Modules

| Module | LOC | Responsibility |
|---|---:|---|
| `alerts.py`         |  ~550 | All alert checks (safety-stock, overfill, lead-time, late-truck, schedule-deadline, plant-state-mismatch). Forward simulation engine (`simulate_consume`, `simulate_delivery`, `simulate_delivery_no_alert`). Tank routing helpers (`find_lowest_in`, `find_others_in`, `_refresh_draw_status`). |
| `plan_orders.py`    |  ~520 | Per-product truck planner. Slot enumeration / validation, breach detection, overfill avoidance, iterative planning loop. CLI wrapper. |
| `projection.py`     |  ~135 | Tank-level history for chart rendering. Wraps the same simulation engine without alert side-effects. |
| `read_schedule.py`  | ~2588 | Schedule-email parser. Most fragile module in the prototype. Heavily hardened by the stress harness in `test_schedule_parser.py` (1466 generated + 87 must-pass cases). |
| `state.py`          |  ~240 | Typed dataclasses (`PlantState`, `TankState`, `Truck`, `RunWindow`, `ProductRate`) with round-trip `from_dict` / `to_dict`. Unknown future fields preserved via `_extra`. |
| `config.py`         |  ~115 | `PlantConfig` (frozen dataclass) with every plant-specific business constant. `target_for_week` reorder curve. |
| `data_io.py`        |  ~115 | Atomic JSON read/write via tempfile + `os.replace`. Schema migration via `_MIGRATIONS` chain. UTF-8 explicit. |
| `customers/`        |  +    | Per-customer config + state bundles. `load_customer(id)` returns `(PlantConfig, state_dict)`. |
| `time_utils.py`     |  ~90  | Naive datetime helpers. Run-hour ↔ datetime conversion. |
| `email_client.py`   |  ~250 | IMAP/SMTP wrapper. Production target: MS Graph. |
| `email_hooks.py`    |  ~220 | Wires alert/customer-success/reminder emails to `send_mail`. |
| `pdf_generator.py`  |  ~140 | Load-entry PDF via reportlab. |
| `app.py`            | ~3130 | Streamlit dashboard + automation loop. UI is heavily coupled to the 2-product 4-tank demo shape — this is fine for a prototype but is the main module the technical team will rewrite. |
| CLI scripts         | small | Thin wrappers over algorithm functions. `advance_time.py 1` is the canonical "tick the clock" command. |

## Polymorphic call shape (the rule)

Every public function in `alerts.py`, `plan_orders.py`, and
`projection.py` accepts **either**:

```python
data = json.load(open("data.json"))
get_all_alerts(data)                 # legacy dict shape
```

**or:**

```python
state = PlantState.from_dict(data)
get_all_alerts(state)                # typed shape
```

The shim is `_as_state(data_or_state)` defined in `alerts.py:34` and
re-imported by `plan_orders.py` and `projection.py`. Inside the
function body everything uses `state.attribute` access. Don't break
this contract — every existing dict caller must keep working without
changes.

## Bridge points (where dict↔state conversion still happens)

The mutators (`simulate_consume`, `simulate_delivery`,
`simulate_delivery_no_alert`, `_refresh_draw_status`) still operate
on dict-of-dicts tank shape. The four call sites that bridge:

1. `alerts.run_projection`                  (~ line 372)
2. `plan_orders._project_tanks_to_hour`     (~ line 162)
3. `plan_orders.find_first_breach_in_target_week` (~ line 326)
4. `projection.compute_level_history`       (~ line 64)

Each does `tanks = copy.deepcopy(state.to_dict()["tanks"])`. This is
the work for the deferred mutation refactor (see
`MIGRATION_GUIDE.md` § "Migrate `simulate_*` to pure functions").

## Per-customer scalability

Three layers of overridability today:

1. **`PlantConfig`** owns every plant-specific business constant:
   lead time, safety stock, late-truck threshold, delivery slots,
   plant-max-hours sanity cap, target curve corners, SAP order
   format + seed, plant holidays. Customers override via
   `customers/<id>.json:config_overrides`.
2. **`PlantState`** owns customer-specific *data*: tank topology
   (any number of tanks per product, any product naming), per-product
   consumption rates, per-product truck quantities, run schedule.
   Customers override via `customers/<id>.json:state`.
3. **`customers.load_customer(id)`** combines the two halves and runs
   schema migration on the state half so older customer files keep
   working through schema bumps.

The smoke test in `tests/test_example_customer.py` proves the
algorithm core is genuinely customer-agnostic:
3 products, asymmetric tank topology (3+2+1), custom delivery slots,
custom SAP format, custom holidays — every algorithm path passes.

The **UI** in `app.py` is *not* customer-agnostic — it's coded for
the demo's two products and four tanks (color palette, chart
selectors, form labels). Generalizing the UI is its own pass and is
expected to be the technical team's first job (see
`MIGRATION_GUIDE.md` § "Generalize the Streamlit UI").

## Schema versioning

`data_io.CURRENT_SCHEMA_VERSION = 1`. Every load runs `_migrate`
through the `_MIGRATIONS` chain. Files written before this field
existed are treated as version 0. To bump the schema:

1. Write the structural change in `_migrate_vN_to_v{N+1}`
2. Register: `_MIGRATIONS[N] = _migrate_vN_to_v{N+1}`
3. Bump `CURRENT_SCHEMA_VERSION = N+1`

Call sites don't change. `PlantState._extra` round-trips unknown
future fields verbatim, so older code reads newer files without
losing data.

## Demo flow (canonical, must not break)

```
load defaults  →  schedule email parses  →  Streamlit shows projection
              →  planner proposes trucks →  user commits via UI
              →  advance_time / clock tick  →  tanks drain
              →  alerts fire on threshold breach  →  email send/receive
```

Per HANDOFF.md Q1/Q2: never push without explicit approval, and email
send/receive (IMAP/SMTP) is part of the demo path that mustn't break.
The schedule parser stress harness must stay PASS before every commit.

## Hard-won lessons (from prior session — keep in mind)

1. The polymorphic `_as_state` shim is the right pattern; don't try
   to do "big-bang typed everywhere".
2. Defaults must be preserved. Every commit verifies that
   `DEFAULT_CONFIG` produces byte-identical alerts to the prior
   version.
3. Streamlit reruns the entire script on every interaction. State
   persistence happens at the END of each rerun via
   `_save_data_state(...)`. mtime poll at the TOP detects external
   changes. Don't break this loop or the Streamlit↔CLI bridge breaks.
4. The schedule parser is the most fragile module — and the most
   over-tested. Any parser change MUST add a `must_*` case in
   `test_schedule_parser.py`.
5. Atomic writes are real. Don't write to `data.json` any other way
   than `data_io.save_data`.
6. The alert text strings are user-facing and have been demoed.
   Don't rephrase casually.
7. `PlantState._extra` preserves unknown future fields. Don't lose
   this property.

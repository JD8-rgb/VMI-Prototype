# Per-customer configuration files

Each `<customer_id>.json` in this directory bundles two things:

1. `config_overrides` — kwargs passed to `PlantConfig(**config_overrides)`.
   These override the defaults in `config.py:PlantConfig` for this
   customer. Anything left unset inherits the prototype default.
2. `state` — the same shape as `data.json` / `defaults.json`. The
   customer's tank topology, consumption rates, truck quantities,
   run schedule, and any in-flight order log live here.

Use the `customers.load_customer(id)` helper (see `customers/__init__.py`)
to load both halves at once. The function runs the standard schema
migration on the state half before returning.

## Why combined?

Per HANDOFF.md Q3 (`customers/<id>.json combined`): the prototype's
algorithms read state and consult cfg in lockstep on every operation.
Splitting them across two files invites version drift — a
config-override that assumes a tank that no longer exists in state, or
a state that lists a product the cfg doesn't know about. One file per
customer keeps the two halves cocommitted in version control.

Once the technical team migrates state to a relational database, the
state half moves to PostgreSQL while config_overrides stays in this
directory (or a per-tenant config table). Same algorithm code; only
the loaders change.

## Scalability proof: example_customer.json

The example customer demonstrates every per-customer dimension that
the prototype currently supports:

* **Three products** instead of the demo's two ("Acid", "Base",
  "Catalyst") — proves product names aren't hardcoded in algorithm code.
* **Asymmetric tank topology** (3 / 2 / 1 tanks per product) — proves
  the planner / alerts / projection chain handles arbitrary tank counts.
* **Custom delivery slots** (5am, 11am, 16:30 → only 5 and 11 align with
  the slot-hour integer convention; 16:30 demonstrates that non-default
  slot hours work).
* **Custom truck quantities** per product.
* **Custom lead-time hours** (72 instead of 48).
* **Custom SAP order format** (`ORD-{n:08d}` with seed 100,000).
* **Custom plant holidays** (a sample US-style holiday calendar).
* **Custom safety-stock and reorder-target curve** so the planner
  numerics differ from the demo.

The smoke test in `tests/test_example_customer.py` exercises every
algorithm path against this customer to prove they don't depend on
any demo-specific assumption.

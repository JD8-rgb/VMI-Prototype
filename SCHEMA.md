# `data.json` schema reference

> Authoritative reference for the on-disk shape of `data.json` /
> `defaults.json` / the `state` half of `customers/<id>.json`. Use this
> when migrating to PostgreSQL or any other persistence layer
> (`MIGRATION_GUIDE.md` § 3).

Current schema version: **2** (`data_io.CURRENT_SCHEMA_VERSION`).

## Top-level keys

| Key | Type | Required | Notes |
|---|---|---|---|
| `schema_version`             | int                   | yes (auto-stamped on save) | Migrator gate. See `data_io._migrate`. |
| `simulation_epoch`           | str (ISO datetime)    | yes | Naive local. Run-hour 0 = this instant. Format `YYYY-MM-DDTHH:MM:SS`. |
| `current_run_hour`           | float                 | yes | Hours elapsed since `simulation_epoch`. Always non-negative. |
| `tanks`                      | object (str → tank)   | yes | Tank name → tank object (see below). Tank names are arbitrary strings; algorithm code does not parse them. |
| `consumption_rates`          | object (str → rate)   | yes | Product name → `{ "lbs_per_hour": float }`. |
| `truck_quantities`           | object (str → int)    | yes | Product name → standard truck size in lbs. |
| `run_schedule`               | array of windows      | yes | List of `{start_hour, end_hour, label}` objects in run-hour space. |
| `scheduled_trucks`           | array of trucks       | yes | Inbound trucks not yet delivered. Empty list is fine. |
| `schedule_received_for_week` | str (ISO date) or null| yes | Mon-of-week the most-recent applied schedule covers (e.g. `2026-04-20`). |
| `schedule_email_id`          | str or null           | optional | RFC822 message-id of the schedule email applied. |
| `schedule_parse_issue`       | str or null           | optional | Truthy ⇒ a schedule email was found but couldn't be parsed (low confidence). Triggers a warning alert. |
| `schedule_unreadable_alert_id` | str or null         | optional | Dedup key — the email-id we last alerted on for unreadable schedule. Prevents repeated alerts for the same problem email. |
| `schedule_alerted_ids`       | array of str          | optional | Set-as-list of email-ids we've already emitted a low-confidence alert for. |
| `alerted_hashes`             | array of str          | optional | Dedup hashes for the alert log so the same alert doesn't email repeatedly. |
| `alert_log`                  | array of objects      | optional | Persistent record of every alert fired. Each entry has the alert dict + `logged_at_iso`. |
| `sap_history`                | array of str          | optional | Append-only ledger of every SAP order number ever issued. Prevents reuse after delivery. |
| `plant_state_override`       | object or null        | optional | Mocks the plant historian for testing the plant-state-mismatch alert. Shape: `{actual: "running" \| "down", since_hour: float}`. |
| `level_history`              | array of snapshots    | optional | Ring buffer of per-tank level snapshots (added in v2). Each entry: `{run_hour, iso, tanks: {name: lbs}}`. Capped at 4320 entries (~180 days at 1/hr). |
| `last_parse_method`          | str or null           | optional | `"regex"` or `"llm"` — which parser produced the last applied schedule. Drives the LLM-parse warning alert. |
| `safety_stock_lbs`           | int                   | optional | Per-customer safety-stock floor in lbs. Defaults to `PlantConfig.safety_stock_lbs` (10 000) if absent. Developer-editable; not exposed in the operator UI. |

Any key not listed above is preserved verbatim through the
`PlantState` round-trip via the `_extra` field (see `state.py`). New
schema versions can therefore add fields without breaking existing
code, and older code reading newer files won't lose data.

## Tank object

```json
{
  "product":           "Product U",
  "current_level_lbs": 20000,
  "max_capacity_lbs":  35000,
  "heel_lbs":          1000,
  "status":            "draw" | "standby" | "receiving"
}
```

| Field | Notes |
|---|---|
| `product`           | Must match a key in `consumption_rates` and `truck_quantities`. |
| `current_level_lbs` | May be 0; below `heel_lbs` is treated as drawable=0 (clamped). |
| `max_capacity_lbs`  | Used by overfill alert. |
| `heel_lbs`          | Per-tank dead inventory floor; consumption stops here. |
| `status`            | `draw` = currently being drained; `standby` = not draining; `receiving` = currently filling (not heavily exercised). |

Tank topology is **arbitrary**: any number of tanks per product (1, 2,
3+, asymmetric across products). The algorithm core does not assume
the demo's 2-tank-per-product shape.

## Truck object

```json
{
  "sap_order":        "SAP90001" | null,
  "product":          "Product U",
  "quantity_lbs":     33000,
  "arrival_run_hour": 168.0
}
```

| Field | Notes |
|---|---|
| `sap_order`        | `null` for planner-proposed trucks; populated when committed. Prefix/format from `PlantConfig.sap_order_format`. |
| `product`          | Must match a `consumption_rates` key. |
| `quantity_lbs`     | Standard customer truck size. |
| `arrival_run_hour` | When the truck arrives in run-hour space. Validated against `delivery_slots`, `lead_time_hours`, and run-window membership at planning time. |

Trucks **may** carry a `_planned_reason` field (string) when the
planner just proposed them — `plan_for_product` strips it when the
user commits, so it doesn't end up persisted.

## Run window object

```json
{
  "start_hour": 6.0,
  "end_hour":   22.0,
  "label":      "Mon"
}
```

Half-open interval: `start_hour` inclusive, `end_hour` exclusive
(`is_running_at` semantics). Labels are display-only — the algorithm
walks `start_hour` / `end_hour` directly.

## Alert log entry

```json
{
  "text":           "RED FLAG: ...",
  "type":           "safety_stock" | "overfill" | "lead_time" | "late_truck" | "schedule_parse" | "schedule_deadline" | "plant_state",
  "severity":       "red_flag" | "warning",
  "direction":      "too_low" | "too_full" | "other",
  "product":        "Product U" | null,
  "tank":           "U-Tank1" | null,
  "level_lbs":      float | null,
  "logged_at_iso":  "2026-04-15T08:23:00",
  "hash":           "..."
}
```

The first 7 fields come from `alerts._alert(...)`. `logged_at_iso` and
`hash` are added by `email_hooks.send_alert_emails_if_new` when the
alert is persisted to the log.

## PostgreSQL DDL sketch

For the technical team migrating to a relational store
(`MIGRATION_GUIDE.md` § 3):

```sql
-- Single JSONB column keeps the migration trivial and preserves
-- forward-compatibility. Schema-version-aware migrators run on read;
-- algorithm code doesn't see the shape change.
CREATE TABLE plant_state (
    plant_id    text PRIMARY KEY,
    data        jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Optional split if the alert log grows large enough to want its own
-- table for indexed querying. Mirror the alert-entry shape above.
CREATE TABLE alert_log (
    plant_id      text NOT NULL,
    logged_at     timestamptz NOT NULL,
    hash          text NOT NULL,
    type          text NOT NULL,
    severity      text NOT NULL,
    product       text,
    tank          text,
    text          text NOT NULL,
    PRIMARY KEY (plant_id, hash)
);
CREATE INDEX ON alert_log (plant_id, logged_at DESC);
```

The single-JSONB approach is recommended for the first migration —
keeping shape-preservation simple. Splitting `alert_log` into its own
table is a follow-up once you actually need analytical queries on it.

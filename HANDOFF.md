# Autonomous Run — Handoff

> **For the next Claude instance picking this up.** Read this top-to-bottom
> before doing anything. The user is going to put you on auto. Your job is
> to keep extending this prototype toward enterprise-ready handoff status
> WITHOUT breaking the demo.

---

## Mission

**Primary:** Move the prototype's "autonomous enterprise readiness" score
meaningfully above its current ~1.5/10 by closing as many gaps as you can
that fit the prototype's scope. Goal is *handoff-ready for a technical
team*, not *production-deployed*. They will own infrastructure (real DB,
durable state, EDI integration, monitoring, RBAC, secrets management).
**You** prepare the algorithm layer so they can wrap it rather than rewrite it.

**Secondary:** Make the prototype scalable to other customers without
algorithm changes. That means tank topology, run-schedule semantics, truck
sizes, lead times, delivery-slot conventions, reorder targets, holidays,
and product naming all need to live in configuration — not in module
constants or hardcoded dict shapes.

**Constraint:** Don't break the demo. Specifically:
- Streamlit dashboard must still load and render.
- The "apply schedule → plan trucks → advance time → see alerts" flow must work.
- The schedule-parser stress harness must stay at PASS.
- All existing dict-based callers (`app.py`, `advance_time.py`, `tank_status.py`, the CLI scripts) must keep working without their signatures changing.

---

## Working rules — DO NOT VIOLATE

1. **Branch.** Work on `claude-autonomous-run`. The session that created this branch left `main` at `1388d23` and forked from there. Do NOT touch `main`. Do NOT push.
2. **Push policy.** Do NOT push to GitHub without explicit user approval. Commit locally on `claude-autonomous-run` is fine — pushes require an ask.
3. **Scope.** Stay inside `C:\Users\jonat\Documents\vmi-prototype`. Do not modify files outside this directory. Do not modify the product sheet PDF (`assets/product_sheet.pdf`) — the user authors it externally.
4. **Verify before commit.** After every meaningful change, run:
   - `py -3 -X utf8 test_schedule_parser.py --regex-only` — must end with `OVERALL: PASS`
   - At least one focused script confirming the changed behavior end-to-end
5. **One concept per commit.** Don't bundle unrelated changes. Each commit message should explain *why* in the body.
6. **Add must-pass cases for parser changes.** Anything new in the parser flow needs a `must_*` case in `test_schedule_parser.py` so future changes can't regress it.
7. **Polymorphism preserved.** Algorithm functions today accept either `dict` (legacy) OR `PlantState` (typed). Every change you make to those functions must keep both call shapes working.
8. **Don't risk the demo.** If a change is "interesting refactor but might break something," log it under "TODO — defer, ask user" and move on.

---

## Current state (forked from `main` at `1388d23`)

### What's done

| Layer | State | Notes |
|---|---|---|
| Schedule parser | Hardened | 1428/1428 generated + 83/83 must-pass. Stale-context, range-day-off subtraction, exception detector, half-hour minute detector, Unicode dash + RTL marker normalization, single-letter day lists, bare-numeric range heuristics, set-based completeness gating, product-prefix detector, "changed from X to Y" recovery. |
| Alerts | Polymorphic dict/state | All read-only checks (`check_lead_time`, `check_late_trucks`, `check_plant_state_mismatch`, `check_schedule_alerts`, `run_projection`, `get_all_alerts`) accept dict or PlantState via `_as_state()` shim. |
| Planner | Polymorphic dict/state | `plan_for_product`, slot finders, `_would_overfill`, `find_first_breach_in_target_week` migrated. |
| Projection | Polymorphic dict/state | `compute_level_history` migrated. |
| Config | `PlantConfig` dataclass | All business constants (lead time, safety stock, late-truck threshold, delivery slots, projection window, plant max hours, target curve) live in `config.py:PlantConfig`. Module-level globals are back-compat re-exports. |
| State | `PlantState` dataclass | `state.py` has typed dataclasses (TankState, Truck, RunWindow, ProductRate, PlantState) with round-trip `from_dict` / `to_dict`. Unknown future fields preserved via `_extra`. |
| Persistence | Atomic + bridged | `data_io.save_data` writes via tempfile + `os.replace`. `data_io.load_state` / `save_state` typed wrappers. Streamlit ↔ data.json bridged via mtime poll. |
| SAP numbers | Append-only history | `data['sap_history']` prevents reuse after delivery. |

### What's NOT done — by deliberate scope choice

| Item | Why deferred |
|---|---|
| Mutation refactor of `simulate_consume` / `simulate_delivery` / `simulate_delivery_no_alert` / `_refresh_draw_status` | Pure-function migration ("return new tanks") is mechanical but touches every caller. Currently bridged via `state.to_dict()['tanks']` at four sites (`run_projection`, `_project_tanks_to_hour`, `find_first_breach_in_target_week`, `compute_level_history`). |
| Module decomposition | `app.py` is 1,597 LOC, `read_schedule.py` is 2,289 LOC. Splitting along service boundaries (`core/` / `adapters/` / `ui/`) is a multi-session refactor. |
| File locking | Concurrent CLI + Streamlit can lose updates. Real DB is enterprise-team scope. |
| TZ awareness / DST | `time_utils` is naive datetime throughout. Comment-documented today. Production needs `tzinfo`. |
| Microsoft Graph migration | IMAP/SMTP today via `email_client.py`. Production wants Graph + delta queries + change notifications. |
| Order lifecycle state machine | Today: just `scheduled_trucks` list + `sap_history`. Production needs `proposed → approved → submitted → acknowledged → scheduled → shipped → arrived → reconciled` with exception states. |
| Multi-tenant config loading | Single `email_config.json`, single `data.json`. Multi-customer needs per-tenant config + state. |
| Structured logging | 160 `print()` calls, 0 `logger.*`. Production wants `logging` with JSON formatter for Splunk/Datadog ingestion. |

---

## Priority TODO — pick from the top

### P0 — In-scope demo wins, no risk

1. **Add proper unit tests for alerts / planner / projection.** Today only the parser has a stress harness (1444 generated cases). The rest has zero. Even a 50-case smoke suite per module would unblock safe refactoring for the technical team. Use `pytest` if not already present, or follow the parser harness's standalone-runner pattern. Place in `tests/test_alerts.py` etc.
2. **Add a `schema_version` field to `data.json` / `defaults.json` and make `data_io.load_data` / `load_state` migration-aware.** Currently no version → enterprise team can't safely add fields. Start at `"schema_version": 1`. Add a `_migrate(data)` function in `data_io.py` that applies version-specific transformations.
3. **Stale-context strip false positives** (M2/M3 from red-team review): "Old plan was X **but** Y" eats Y; "Was supposed to send Friday but: Mon-Fri 6am-4pm" loses the schedule. Pattern: `was supposed to … but …` and `old plan was … but …` both strip through the wrong clause boundary. Add must-pass cases first, then narrow the strip.
4. **Replace 160 `print()` calls with `logging` module.** Module-level `logger = logging.getLogger(__name__)`. Keep the same human-readable strings but route through `logger.info` / `logger.warning` / `logger.error`. The technical team can then attach handlers (file, Splunk, JSON formatter) without touching algorithm code. Don't remove the `print()` calls in CLI entry points — those are user-facing UI.

### P1 — Scalability wins

5. **Make tank topology arbitrary.** Today defaults.json has exactly 4 tanks (`U-Tank1`, `U-Tank2`, `M-Tank1`, `M-Tank2`) hardcoded by product letter prefix. The `_next_sap` / planner / Streamlit display all assume this shape. Verify nothing breaks if a customer has 1 tank or 3 tanks per product. Add a defaults variant in `tests/fixtures/customer_three_tanks.json` and run all algorithms against it.
6. **Make `truck_quantities` per-product flexibility explicit.** Today `data["truck_quantities"]` is `{"Product U": 33000, "Product M": 37000}`. Verify the planner handles a third product (`{"Product X": 25000}` added). The hardcoded `default_value` arg in the Streamlit form quantity input (`app.py:1348` defaults to `33000`) is one risk.
7. **Audit hardcoded "Product U" / "Product M" strings.** Run a grep. They should only appear in `defaults.json` (data) and Streamlit display labels. If they're in algorithm code, that's a scalability bug.
8. **Plant calendar / holiday support.** Add `plant_holidays: List[date]` to `PlantConfig`. `is_running_at` should return False on holidays even if the run window covers them. `_all_slot_run_hours` should skip holiday days.

### P2 — Enterprise-handoff polish

9. **Type hints across all algorithm functions.** Many already have `cfg: PlantConfig = DEFAULT_CONFIG`. Extend to all signatures. Run `py -3 -m mypy --strict alerts.py plan_orders.py projection.py state.py config.py` and fix the easy errors.
10. **Module docstring inventory.** Every module-level public function should have a docstring with `Parameters` and `Returns` sections. The parser is well-commented; alerts/planner are partial; the smaller CLI scripts are bare.
11. **Write `ARCHITECTURE.md`** describing the current module layout, the dataclass migration status, the bridge points where dict↔state conversion happens, and the recommended next steps for the technical team. This is the document they will read first.
12. **Write a `MIGRATION_GUIDE.md`** for the technical team explaining: how to swap in PostgreSQL for `data_io`, how to add a per-customer `PlantConfig`, how to replace IMAP with Graph, how to migrate `simulate_*` to pure functions.

### Deferred — ask user before doing

| Item | Why ambiguous |
|---|---|
| Mutation refactor of `simulate_*` | Mechanical but high-touch. Worth ~2 sessions of focused work. Asks under "Questions for the user" item Q10. |
| Module split (`app.py`, `read_schedule.py`) | Same. Q11. |
| File locking | Real fix is durable DB. Local fix (filelock library) might be too "production-y" for a prototype. Q12. |
| `plan_orders.main()` interactive `input()` block | Blocks unattended automation, but plan_orders is mainly a CLI dev tool — Streamlit is the real planner UI. Q13. |
| Structured logging right now (vs. defer) | Big touch (160 prints) with no immediate demo benefit. Could be a "before handoff" finishing pass. Q14. |

---

## Things to ASK the user at the start of every session

The user explicitly said to ask before starting. Lead with these.

### Q1 — Branch and commit policy
"I'm on `claude-autonomous-run`. Confirm: I should commit autonomously on this branch but never push without explicit approval per change?"

### Q2 — Demo flow
"What's the canonical demo flow I must not break? My current understanding: load defaults → schedule parses → planner proposes trucks → user commits → time advances → tanks drain → alerts fire. Anything else?"

### Q3 — Multi-customer config shape
"For multi-customer scalability, do you want: (a) one JSON config file per customer in `customers/<id>.json` containing both `PlantConfig` overrides and tank topology; (b) a relational shape with separate `config.json` and `state.json` per customer; (c) something else? My default plan is (a) since it matches how the prototype already loads `defaults.json`."

### Q4 — Tank topology flexibility
"Current default has exactly 2 tanks per product. Should the prototype support arbitrary tank counts (1 tank, 3+ tanks, shared tanks across products)? If yes, are there constraints I should preserve (e.g., each product must have at least 2 tanks for draw/standby switching)?"

### Q5 — SAP order-number format
"Current is `SAP90001` with auto-incrementing integer suffix. Real SAP order numbers are typically 10-digit numerics. Should the prefix/width be configurable per-customer in `PlantConfig`? Should I store an `sap_order_format` field with a Python format string?"

### Q6 — Test framework
"Do you want me to set up `pytest` properly for the algorithm modules, or stick with the current standalone-runner pattern (like `test_schedule_parser.py`)? The technical team likely expects pytest. Adding it adds a dependency but unlocks `pytest --cov`, parametrized tests, fixtures."

### Q7 — Python version target
"What Python version will the technical team be on? Affects: type-hint syntax (`dict[str, X]` is 3.9+, `from __future__ import annotations` works on 3.8+), match statement (3.10+), `Self` type (3.11+). I'm currently writing 3.10-compatible. Confirm or specify."

### Q8 — Logging
"Do you want me to introduce proper Python `logging` (P0 item 4 above) in this autonomous run, or defer to the technical team? It's 160 print() call sites. No demo regression risk if done carefully, but it's a lot of touched lines."

### Q9 — Architecture/migration documentation
"Should I write `ARCHITECTURE.md` and `MIGRATION_GUIDE.md` for the technical team during this run, or just keep the HANDOFF.md updated and let them ask?"

### Q10 — Mutation refactor of `simulate_*`
"The simulate_consume / simulate_delivery functions still mutate a `tanks` dict in place. Migrating to 'return new tanks' would: (a) drop ~15 LOC of bridge plumbing; (b) make algorithms unit-testable without fixtures; (c) touch every caller. Risk: low but non-zero. Worth doing in this autonomous run, or defer?"

### Q11 — Module split
"`app.py` is 1,597 LOC and `read_schedule.py` is 2,289 LOC. Splitting along service boundaries (`core/`, `adapters/`, `ui/`) is the cleanest enterprise-handoff prep but touches every import. Do this autonomously, or wait?"

### Q12 — File locking
"Concurrent CLI + Streamlit can lose updates. Should I add `filelock` library (extra dep), or leave as-is and document for the technical team?"

### Q13 — `plan_orders.main()` interactive `input()`
"Today the CLI planner asks for a starting SAP number interactively, blocking unattended automation. Streamlit doesn't have this issue. Should I add a `--sap-start=` flag so cron jobs can call plan_orders.py headless?"

### Q14 — Structured logging during this run vs. defer
"Same as Q8 — clarifying split: do logging now if the answer to Q8 is yes."

### Q15 — Customer prototype: should I produce a sample
"To demonstrate scalability, should I produce a `customers/example_customer.json` with non-default tank topology + lead time + delivery slots, plus a small smoke test that runs all algorithms against it? Useful proof point for the technical team."

### Q16 — Anything in `data.json` worth preserving
"Current local `data.json` may have state from your testing (alert log, sap_history, schedule_received_for_week). Am I free to regenerate from defaults.json if I need a clean test environment, or should I preserve it?"

### Q17 — What to do with discoveries during the run
"If I find new bugs during this autonomous run that weren't on the prior red-team list, should I: (a) fix small ones inline if low-risk, log everything else; (b) log everything and never fix without ask; (c) something else?"

**Default behavior if user says nothing:** Q1=yes commit on this branch don't push. Q2=I'll work from my current understanding. Q3=(a). Q4=allow arbitrary count, require min 1 tank. Q5=add sap_order_format to PlantConfig. Q6=pytest. Q7=3.10. Q8=yes do logging, but late in the session. Q9=write both. Q10=defer. Q11=defer. Q12=document only. Q13=add --sap-start flag. Q14=tied to Q8. Q15=yes produce sample. Q16=preserve. Q17=(a) fix small low-risk inline.

---

## Continual self-evaluation prompt — RE-RUN AT THE END OF EVERY MAJOR CHANGE

Verbatim, what to ask yourself periodically:

> Red-team this prototype hard. Break it down piece by piece — parsing, projection, alerts, safeguards, the way the architecture is set up to hand off to a technical team eventually. Find bugs everywhere I can. Test it as many ways as I can. Rate it on a 1-10 scale: how close is it to being autonomous-enterprise-production-ready, assuming infrastructure (DB, EDI, Graph, RBAC, monitoring) would be done by more technical teams. Rate it on a 1-10 scale: how scalable is it across customers — could I take this and adapt it to a different plant with different tanks, sizes, products, and run-schedule logic, without algorithm changes?

Re-run that prompt:
- After each P0 item completes
- After each module migration
- Before adding the discovery to the TODO list

The goal is to track whether the score is moving up. Current baseline:
- **Autonomous enterprise readiness: 1.5/10** (per the most recent red-team)
- **Multi-customer scalability: ~3/10** (PlantConfig exists, but tank topology and product names are still hardcoded in defaults; no per-customer adapter pattern)

Target by end of autonomous run:
- **Autonomous enterprise readiness: 4-5/10** (algorithm layer ready for handoff, infrastructure still missing — but that's the technical team's job)
- **Multi-customer scalability: 6-7/10** (everything algorithm-relevant is in PlantConfig + per-customer state; no algorithm grep returns hardcoded customer names)

---

## Test commands you must know

From `C:\Users\jonat\Documents\vmi-prototype`:

```bash
# Schedule parser stress + must-pass — REQUIRED PASS BEFORE EVERY COMMIT
py -3 -X utf8 test_schedule_parser.py --regex-only

# Read-only smoke test — confirm tank_status.py CLI still works
py -3 -X utf8 tank_status.py

# CLI clock advance smoke test — backup data.json first if needed
py -3 -X utf8 advance_time.py 1

# Streamlit visual smoke — only if you need to verify dashboard hasn't broken
py -3 -m streamlit run app.py
```

If you add new test files, mirror the parser harness pattern (a `__main__` runner that prints `OVERALL: PASS` on success and exits non-zero on fail).

---

## Architecture map (where things live)

```
vmi-prototype/
├── app.py                  Streamlit UI + automation loop (1597 LOC — too big)
├── advance_time.py         CLI: advance sim clock, send alerts
├── alerts.py               Read-only alert checks + simulate_* mutators
│                           ALL READ-ONLY CHECKS NOW POLYMORPHIC (dict/PlantState)
├── plan_orders.py          Truck planner + slot finders + CLI main
│                           ALL POLYMORPHIC except main() which is dict-based
├── projection.py           Chart-data simulator
│                           POLYMORPHIC
├── read_schedule.py        Schedule email parser + IMAP fetch + apply (2289 LOC)
│                           Parser is mature; apply path is partly polymorphic via PlantConfig
├── email_client.py         IMAP/SMTP wrapper. Production target = MS Graph.
├── email_hooks.py          Wires alert/CS/reminder emails to send_mail
├── pdf_generator.py        Load-entry PDF builder
│
├── config.py               PlantConfig dataclass + DEFAULT_CONFIG (Shape change 2)
├── state.py                TankState/Truck/RunWindow/ProductRate/PlantState
│                           dataclasses with from_dict/to_dict round-trip (Shape change 3)
├── data_io.py              Atomic save_data + load_data + load_state/save_state
├── time_utils.py           Naive datetime helpers (no DST today)
│
├── tank_status.py          Read-only CLI status report
├── check_reminder.py       Friday schedule reminder CLI
├── mark_schedule_received.py
├── schedule_run.py         Manual run-window add CLI
├── schedule_truck.py       Manual truck add CLI
├── test_schedule_parser.py 1428 generated + 83 must-pass cases
│
├── defaults.json           Default plant state (epoch, tanks, rates, etc.)
├── data.json               Live mutable state (atomic-written, gitignored)
├── email_config.json       SMTP/IMAP creds (gitignored)
│
├── HANDOFF.md              ← THIS FILE
├── README.md               User-facing
└── assets/
    ├── product_sheet.pdf   Author externally; do not modify
    └── screenshot.png
```

### Polymorphic call shape — the rule across alerts/planner/projection

Every public function in `alerts.py`, `plan_orders.py`, `projection.py` accepts EITHER:

```python
data_dict = json.load(open("data.json"))
get_all_alerts(data_dict, cfg=DEFAULT_CONFIG)   # legacy
```

OR:

```python
state = PlantState.from_dict(data_dict)
get_all_alerts(state, cfg=DEFAULT_CONFIG)        # typed
```

The shim is `_as_state(arg)` defined in `alerts.py`. Reused by `plan_orders.py` and `projection.py` via import. **Don't break this contract** — every existing dict caller must keep working without changes.

### Bridge points — where state.tanks → dict-of-dicts conversion still happens

The mutators (`simulate_consume`, `simulate_delivery`, `simulate_delivery_no_alert`, `_refresh_draw_status`) still take dict-of-dicts. The four call sites that bridge:

1. `alerts.run_projection` — line ~370
2. `plan_orders._project_tanks_to_hour` — line ~155
3. `plan_orders.find_first_breach_in_target_week` — line ~309
4. `projection.compute_level_history` — line ~63

Each does `tanks = copy.deepcopy(state.to_dict()["tanks"])`. This is the work for the deferred mutation refactor (Q10).

---

## Hard-won lessons from this session — don't relearn them

1. **The polymorphic shim approach worked.** Migrating each function to accept `dict OR PlantState` via `_as_state()` at the entry kept all 30+ call sites working without changes. Don't try to do "big-bang typed everywhere" — the shim is the right pattern.
2. **Defaults must be preserved.** Every commit verified that `DEFAULT_CONFIG` produces byte-identical alerts to the prior version. The technical team will trust the migration only if behavior is provably unchanged.
3. **Streamlit reruns the entire script on every interaction.** State persistence happens at the END of each rerun via `_save_data_state(st.session_state.data, _DATA_FILE)`. mtime poll at the TOP detects external changes. Don't break this loop or the Streamlit↔CLI bridge breaks.
4. **The schedule parser is the most fragile module — and the most over-tested.** 1428 generated cases + 83 must-pass. Any parser change MUST add a must-pass case.
5. **Atomic writes are real.** `data_io.save_data` writes to tempfile + os.replace. Don't write to `data.json` any other way.
6. **`PLANT_MAX_HOURS` is referenced from `test_schedule_parser.py:1105`.** Keep it back-compat exported from `read_schedule.py`.
7. **The alert text strings are user-facing.** Don't rephrase casually. The user has demoed specific phrases ("RED FLAG: Product U projected to drop to..."). Keep them stable.
8. **The `_extra` field on PlantState preserves unknown future fields.** When you add `schema_version`, `_extra` will round-trip it for older code paths that don't know about it. Don't lose this property.
9. **Don't trust your own test scripts.** The earlier red-team session found 2 real bugs and 2 test-author errors (mine). Always re-derive expected values from first principles, not from "what the function returned last time."

---

## What success looks like at the end of the autonomous run

A summary commit (or final HANDOFF.md update) that includes:

- The final autonomous-enterprise-readiness rating (target: 4-5/10) with justification
- The final multi-customer-scalability rating (target: 6-7/10) with justification
- A list of every commit made on `claude-autonomous-run`
- A list of every TODO that's still open with severity tags
- A list of every "I wasn't sure" question with the assumption I made
- A clean parser sweep (`OVERALL: PASS`)
- All CLI smoke tests still passing
- A list of files touched and why

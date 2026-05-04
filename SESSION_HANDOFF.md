# VMI Prototype — Session Handoff
**Last updated:** 2026-05-04
**Latest commit:** `37ee602` (Sprint 9, on `claude-autonomous-run`)
**Main branch:** `eb9b28c` (one fast-forward behind — say `push` to promote)
**Tests:** 466 passing, 1 skipped

This doc captures session state so a new Claude Code chat can pick up cleanly. Includes the red-team prompt, the latest review + fixes, and a complete inventory of features added in the last 5 days for the product data sheet.

---

## 1. Red-Team Prompt

Use this prompt at the end of any sprint to find bugs before the user does.

> **Run the full red-team prompt.**
> 
> Conduct an adversarial review of the most recent code changes. Specifically:
> 
> 1. Read the relevant code paths for the latest sprint(s).
> 2. Construct edge-case test inputs targeting:
>    - Boundary conditions (empty / zero / max values)
>    - Type assumptions (None, missing fields, malformed config)
>    - State mutation / side effects
>    - Operator typos / fat-finger inputs
>    - Multi-customer config (non-default product/tank topology)
>    - Holiday / off-day combinations
>    - Backwards compatibility
> 3. Run those probes and look for: crashes, silent failures, wrong outputs, fail-open paths, regressions.
> 4. Categorize findings as 🔴 Critical / 🟠 Real / 🟡 Robustness / 🟢 Verified safe.
> 5. Fix the 🔴 and 🟠 items inline, with regression tests where applicable.
> 6. Run the full test suite to confirm no regressions.
> 7. Report findings + fixes as a structured red-team report.

---

## 2. Latest Red-Team Findings (Sprint 9, applied 2026-05-04)

External review against `0e6c144` flagged the following classes. All confirmed bugs are now **fixed in `37ee602`**.

| # | Class | Verdict | Fix |
|---|---|---|---|
| 1 | Split-shift parsing fail-open (`Mon 6am-10am and 2pm-6pm` → HIGH) | ✅ Confirmed | `_SPLIT_SHIFT_RE` runs on full cleaned text → forces LOW with note |
| 2 | Product-alias flatten (`U resin Mon-Wed; M resin Thu-Fri` → HIGH) | ✅ Confirmed | `_PRODUCT_ALIAS_RE` (resin/feed/grade/P-NNN) → forces LOW |
| 3 | Hardcoded `"U-"/"M-"` tank prefix in 4 sites | ✅ Confirmed | `tank["product"]` membership everywhere; `compute_level_history` exposes `tank_product` map; combined chart renders dynamic per-product loop |
| 4 | Windows test path `__file__.rsplit("/", 1)` | ✅ Confirmed | `Path(__file__).parent` |
| 5 | Forecast collisions (30-day chart, example_customer 36 trucks) | ❌ Already fixed by Sprint 7 | Bug 3 fix removes the latent class |
| 6 | Streamlit auth wall | ❌ Dismissed | User confirmed incognito works |

Plus 9 regression tests in `tests/test_review_fixes.py`.

### Readiness scorecard (post-Sprint 9 estimate)

| Tier | % ready |
|---|---|
| Personal portfolio / demo | 9 / 10 |
| Controlled internal pilot with human approval | 60–70% |
| Autonomous enterprise order placement | 25–35% |
| Enterprise production application overall | 45–55% |

Framing: **"This prototype demonstrates the operating model with hardened, test-backed core algorithms that could become the decision engine inside a governed production workflow."** Don't claim "production-ready core" — claim "production-shaped prototype."

### Open robustness gaps (noted, not fixed)

- SAP ID collision possible if state has a manually-named `FORECAST-NNN` truck.
- Audit log lacks RBAC, before/after payloads, immutable storage.
- JSON persistence has no optimistic concurrency / version checks.
- UI products/tanks rendered dynamically per Sprint 9, but planner-side enums + email templates still assume Acme topology in places.
- Tentative / conditional language ("Tentative: …", "if material available") still parses HIGH; should force LOW.

---

## 3. Feature Inventory (Last 5 Days)

129 commits, 9,049 lines added across 58 files. Use this list to update the product data sheet — pick what's strategic, drop what's noise.

### 🧠 Algorithms

- **Weighted seasonal forecaster** — 4-week lookback, 40/30/20/10 recency weights, week-level outlier exclusion, holiday gating, per-product truck-count suggestion.
- **5 anomaly detection checks** — yellow warnings, not blockers:
  - Run-hours unusual (>2σ outlier vs. recent weeks)
  - Day-shape unusual (novel weekday in this week's schedule)
  - Holiday-in-run-window (parsed window covers a configured holiday)
  - Truck-cadence unusual (forecaster ±35%, ±2 truck floor)
  - Schedule-arrival unusual (email arrived on unusual day-of-week)
- **12-day integrated projection chart** — solid line for operator-parsed period, dotted forecast beyond cutoff, "forecast →" seam marker.
- **Prospective forecast-truck simulator** — walks forward by hour, schedules trucks at next valid delivery slot to keep combined product above `safety_stock + lead × rate`. Mirrors the live planner's logic.
- **Reorder target curve** — linear interpolation between (low_run_hours, low_lbs) and (high_run_hours, high_lbs); operator-tunable per customer.
- **Severity-based alert escalation** — yellow goes to primary contact, red goes to distribution group.

### 📅 Schedule parsing (the parser is now its own product)

- **Hybrid regex-first / LLM-rescue parser** — regex extracts windows with confidence; LLM only fires on demoted-confidence text.
- **Quote-stripping** — Gmail/Outlook reply chains, forwarded headers stripped at first separator before parsing.
- **Date-token resolution** — `5/11 0600-1600` → Monday's run window if 5/11 is in the target week.
- **Stale-context stripping** — `"Last week we ran ..."` → removed.
- **Multi-day continuous shifts** — `Mon 6am to Sat 4am` → single 118-hour window.
- **Off-day subtraction** — `Mon-Fri 6-22; Wed off` removes Wednesday cleanly.
- **Full-week fallback** — `"Run all week"` / `"24/5"` → LOW-confidence Mon 6am → Sat 4am template.
- **Half-hour rounding warning** — operator typed `Mon 6:30am` → editor surfaces a caption noting the truncation.
- **Split-shift detection** *(Sprint 9)* — `Mon 6-10 and 2-6` → forces LOW with a note.
- **Product-alias detection** *(Sprint 9)* — `U resin Mon-Wed; M resin Thu-Fri` → forces LOW.
- **Editable per-row parser table** — operator types `Mon 6am` / `Thu 4pm` text strings instead of raw hour offsets.
- **Confirm-and-apply LOW panel** — operator reviews + edits before the schedule applies.
- **HIGH-parse review panel** — always shows the original email body alongside the parsed windows, with "Acknowledge / Dismiss" actions logged to the audit trail.
- **Per-customer LLM rescue enrichment** — after 10 parser misses, the LLM rescue prompt gets enriched with that customer's phrasing patterns.
- **Parser-miss log + triage CLI** — appends every miss to a JSONL log; `triage_parser_misses.py` summarizes for engineering review.
- **Email-shape regression suite** — 16 tests covering Gmail single quote, reply-with-quoted-block, Outlook headers, signatures, sign-offs.

### 🏗️ State + persistence

- **Typed `PlantState` / `PlantConfig`** dataclasses; algorithms accept dict OR PlantState polymorphically via `_as_state` shim.
- **Per-customer JSON config bundles** — `customers/<id>.json` lets the team support multi-tenant without forking.
- **Schema migration** v0→v1→v2 in `data_io.py` (idempotent).
- **Atomic JSON persistence** — temp file + `os.replace` so crashes can't corrupt state.
- **Operator audit log** — 1,000-entry ring buffer with action, details, timestamp.
- **Alert log** — every alert fire logged with severity, type, dedup hash.
- **Schedule arrival history** — 9-week rolling window for the schedule-arrival anomaly check.
- **Re-anchor sim clock on session start** — opening the demo any day re-anchors the epoch to the most-recent Monday so the demo always shows "this week" runs.
- **`level_history` ring buffer** — 4,320 entries (180 days at 1/hour) for the historical tank chart.
- **Validation CLI** (`validate_customer.py`) — checks a customer config bundle against the schema.

### 🎮 Simulation

- **Hour-by-hour tank simulator** — consumption (during run windows only), tank switching at heel, truck deliveries with overflow cascade, holiday gating.
- **`Generate demo history` button** — backfills 4 weeks of synthetic past by walking forward through alternating shift patterns + auto-inserting trucks. Realistic chart without advancing the sim clock.
- **🧪 Simulate HIGH parse / Simulate LOW parse buttons** — pre-loads canonical email examples so operators can demo both confidence paths without typing.
- **What-if simulator** — skip days, weekend runs, extra truck, target-slider what-ifs.
- **Plant state mismatch detection** — alerts when "running" telemetry contradicts schedule for >3 hours.

### 📦 Planner

- **Constraint-aware planner** — lead time, allowed delivery slots, active run windows, no same-slot collisions, holiday skips, overfill avoidance.
- **Forecast-driven prospective trucks** — projection chart shows where the planner *would* place trucks if the operator ran it for the forecast week.
- **Planner output** rendered as a table with per-truck reasoning + estimated impact.
- **CS Load Entry PDF generator** — emits a printable load-entry artifact.

### 🎨 UI / Design

- **Customer roster landing page** — multi-tenant view; click "Acme" → live demo dashboard.
- **Tank cards** with animated SVG fluid fill, semantic fill colors (red <20%, amber <50%, blue ≥50%), draw/standby chips.
- **12-day projection chart** with prospective truck markers, safety-stock floor, run-window vrects.
- **Combined-product tank-levels chart** — one line per product (rendered dynamically; works for any topology).
- **VMI Health Dashboard** — 6-month overfill / safety-stock counts + alert-bias indicator (overfill % vs. safety %).
- **VMI Controls panel** — operator-tunable low/high target sliders + automation on/off toggle (red Friday alert when off).
- **Customer notes scratchpad** — free-text per-customer context.
- **Single-blue design system** — `#1E40AF` for brand AND primary actions; 3px blue left-border on every h3 (the signature gesture); Inter for UI, JetBrains Mono for numerics with tabular figures.
- **Sim-time pill** in the brand header.
- **Status chips** (draw / standby / receiving / success / warning / danger / info) and **alert banners** with 4px semantic left-borders.
- **Compact next-week forecast strip** — single-line caption ("Mon 16h · Tue 16h · Wed 8h …") with refresh button.
- **Alert Rules Reference + Anomaly Detection table** — bottom-of-page expander documenting every alert / anomaly trigger.
- **Friday weekly reminder + 3pm escalation** — yellow at 11am, red at 3pm; alert text simplified per operator feedback.
- **`Apply Schedule` unified button** — works for both HIGH (replace + mark week received) and LOW (additive merge).
- **Roster status indicators** — Acme card shows live alert count + bias.

### 📧 Email integration

- **Email schedule fetch** with confidence-based application semantics.
- **Email log** tracking every send + receive event.
- **Severity-based escalation chain** — yellow → primary, red → distribution group.

### 🧪 Testing

- **466 pytest cases** across:
  - 71 alert + planner + forecast tests
  - 30 anomaly tests
  - 100+ schedule-parser stress tests (`test_schedule_parser.py` harness)
  - 16 email-shape regressions (Gmail/Outlook quotes, signatures)
  - 14 forecast-truck regressions (Sprint 8)
  - 9 review-fixes regressions (Sprint 9 — split-shift, product-alias, U-/M- prefix)
  - audit-log, customer-validation, data-io, e2e demo flow, time-utils, topology, state-roundtrip, target-overrides, VMI-off escalation, parser-misses, parser-learning, friday-reminder, dashboard-summary

### 🛠️ Operator tooling

- **`🎬 Generate demo history` button** (4-week backfill).
- **`🧪 Simulate HIGH / LOW parse` buttons** in Schedule Parser.
- **`↻ Refresh forecast` button**.
- **Audit log display** — operator can see their last N actions.
- **Reset Demo CLI** — restores defaults.
- **Half-hour rounding warning** in parser editor (Sprint 8).

### 📚 Documentation

- `ARCHITECTURE.md` — module map + data flow.
- `HANDOFF.md` — engineering handoff for the next implementer.
- `MIGRATION_GUIDE.md` — JSON → PostgreSQL migration plan.
- `SCHEMA.md` — JSON schema reference.
- `DESIGN_HANDOFF.md` — for Claude Design redesign pass.
- `customers/README.md` — how to add a customer.
- `design-system-handoff/` — full design kit (CSS tokens, React reference, screenshots).

### 🧰 Infrastructure

- `theme.py` — design system as a Python module (CSS injected at app start).
- `.mcp.json` — Playwright MCP server config (project-scoped).
- Streamlit Cloud deployment at `vmi-prototype.streamlit.app`.

### ⚠️ Features that may not belong on the data sheet

These exist but might be noise for the strategic narrative:

- "Generate demo history" is internal demo polish, not a customer-facing feature.
- "🧪 Simulate HIGH / LOW parse" buttons are demo accelerators.
- Re-anchor sim clock is demo polish.
- The roster placeholder customers (4 decorative cards) are visual scale signaling, not real multi-tenant.
- `triage_parser_misses.py` CLI is engineering ops, not customer-visible.
- Half-hour rounding warning is defensive UX, not a marketed feature.
- Tank-card SVG animation is cosmetic.
- Customer notes scratchpad is operator-quality-of-life.

---

## 4. Open Workflow Notes

- **Push promotion is paused** — every commit goes to `claude-autonomous-run`; user explicitly says `push` to fast-forward `main`. Don't auto-promote.
- **Playwright MCP is committed** but not yet activated in user's local Claude Code session. Three activation paths in last assistant turn — restart-with-cwd, `claude mcp add` CLI, or direct edit of `~/.claude.json`.
- **Streamlit deployment** at `vmi-prototype.streamlit.app` runs from `main`; user reboots via "Manage app" after each push.
- **Tests must stay at 466 passing** — that's the floor.

---

## 5. Files Worth Knowing

```
app.py                 ~3,000 lines — the Streamlit dashboard
forecast.py            weighted seasonal + forecast-truck simulator
read_schedule.py       hybrid regex/LLM parser
plan_orders.py         constraint-aware planner
alerts.py              alert-eval + simulation primitives
anomaly.py             5 anomaly checks
state.py               PlantState / Tank / Truck / RunWindow dataclasses
config.py              PlantConfig (per-customer business constants)
projection.py          compute_level_history (chart data)
demo_history.py        synthetic past backfill
level_history.py       ring buffer + downsampler
audit_log.py           1k-entry operator log
data_io.py             atomic load/save + schema migration
email_hooks.py         alert-fire → email triggers
parser_misses.py       miss log + triage
parser_learning.py     per-customer LLM prompt enrichment
theme.py               design system tokens + CSS
customers/             per-tenant JSON configs
tests/                 466 pytest cases
```

---

## 6. Quickstart for the Next Chat

Open Claude Code in `/home/user/VMI-Prototype` (or your local clone). To resume work:

1. `git fetch && git status` — confirm working branch + remote sync.
2. `python -m pytest tests/ -q` — confirm 466 passing.
3. Check `git log --oneline -5` for the last few commits to orient.
4. Read this file's section 2 (latest red-team) + section 4 (workflow notes).
5. If user mentions Playwright, see section 4.
6. Default policy: push to `claude-autonomous-run`, wait for explicit `push` before fast-forwarding main.

Recent open threads to close (or ignore — user's call):
- Playwright MCP activation on user's Windows machine.
- Tentative/conditional schedule language → LOW (review-flagged but unfixed).
- UI generalization: planner-side strings + email templates still assume Acme topology.
- Audit log RBAC + immutability (production-readiness gap).
- JSON persistence concurrency / versioning (multi-session safety).

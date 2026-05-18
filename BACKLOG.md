# Backlog

Tracked items deferred from prior sessions. Survives across Claude
sessions so we don't lose context. Add to this list when a worth-doing
item gets explicitly deferred.

---

## Pending

### B3 — Multi-customer text passes
**Priority:** Low — single-customer prototype.

Hardcoded "Customer 1 — Acme" and similar UI text remains. Do a sweep
when the multi-customer roster goes live; until then, low value.

### B4 — Per-customer projection-window config
**Priority:** Very low — speculative.

`PROJECTION_HOURS = 240` is a module constant. If different customers
need different chart horizons, lift to `PlantConfig`. No customer is
asking for this today.

---

## Production-readiness (from red-team audit `fd0066b`)

Items raised by the audit that were verified legitimate but
intentionally deferred (separate scope from quick prototype hardening).

### B-PROD-1 — anna_email sender gating
**Priority:** P0 for production / pilot; not needed for demo.

The schedule-email ingestion currently accepts emails from any sender —
intentional for demo convenience (reviewer can email from any account).
Production must filter `fetch_and_apply_schedule` to the configured
`anna_email` (single authoritative sender), or, if multiple senders at
one customer are valid, to `@{customer_domain}`. Code already loads
`anna_email` and a comment in `read_schedule.py` flags the demo-vs-
production distinction.

### B-PROD-2 — Order lifecycle states
**Priority:** P0 for production.

Today `Commit Trucks` appends a truck record with a SAP-style ID and
that's the end of the lifecycle. Real operations need states:
proposed → approved → submitted → acknowledged → rejected → scheduled
→ shipped → arrived → reconciled → cancelled. Plus a per-order event
log. Foundational for the multi-system integration story (Graph / SAP /
EDI).

### B-PROD-3 — PM handoff doc bundle
**Priority:** P1 for work-sample polish.

Audit recommends creating `PRODUCT_BRIEF.md`, `HANDOFF.md`,
`PILOT_RUNBOOK.md`, plus a PowerApps screen map and a Dataverse /
Azure SQL entity model. These translate the prototype into a
PM-grade engineering handoff package.

### B-PROD-4 — Reset confirmation + role separation
**Priority:** P2.

Reset is broad and immediate. Production needs a confirmation step
and a split between operator path (no reset access) and admin / demo
path (reset, advance, test email, etc.).

### B-PROD-5 — Streamlit slider min/max/step warning
**Priority:** P3.

Console warning observed in audit. Clean up before any polished
external demo.

### B-PROD-6 — "Same as usual" and other prior-schedule references
**Priority:** P2.

The full-week shorthand fix (`24/5`, `Run all week`, `24/7`) routes
vague-but-plausible schedules to a LOW-confidence operator review
with a pre-filled best-guess template. The audit's probe table
flagged another vague-but-plausible class that the prototype does
NOT yet handle:

- `next week same as usual`
- `regular schedule`
- `no changes`
- `same as last week`

These imply "carry over the most-recent applied schedule." That's a
different rescue: the system would need to look up the most-recent
HIGH-confidence apply, copy its windows to next week, and present
them at LOW confidence for confirmation. Distinct from the
`_FULL_WEEK_TEMPLATE` fallback (which uses Acme's hardcoded shift
template, not the actual recent schedule).

Plan: add a separate "carry-prior-schedule" pattern + handler. Same
operator-review flow at the end: vague-but-plausible → LOW with a
best-guess → approve / edit / dismiss.

---

## Completed (recent reference)

- **B2 — `_as_state()` shim consolidation**: canonical `as_state()` now
  lives in `state.py` (public, no leading underscore). `alerts.py` and
  `forecast.py` keep one-line back-compat aliases (`_as_state = as_state`)
  so existing in-module callers don't need touch-ups; `anomaly.py`,
  `projection.py`, `plan_orders.py` import via the public name. The
  Streamlit Cloud underscore-import bug that originally forced the
  duplication no longer fires through this path. (Round-6 commit
  `775cd5d` Audit round 6.)
- **Audit Round 2** (commit `4bc0d15`): widened `_PRODUCT_ALIAS_RE`,
  promoted `_DAY_OFF_RE` to single source of truth, added
  `_validate_llm_windows()`, fixed Makefile path + removed root-level
  test harness duplicate, polished tank-card text colors.
- **Sprint A audit** (commit `dafea45`): forecast-cutoff boundary
  bug fix, `alert_log` default, 16 unit tests.
- **Demo polish + planner forecast trucks** (commit `4e29482`):
  replaced forecast simulator with `plan_for_product()` calls.
- **Forecast cutoff + overlap on apply** (commit `1d9ac8c`).
- **First-run onboarding** (commit `38b17d6`): defensive
  `inventory_age_hours` import, auto-history bootstrap, 3-step demo.

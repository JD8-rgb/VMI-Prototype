# Backlog

Tracked items deferred from prior sessions. Survives across Claude
sessions so we don't lose context. Add to this list when a worth-doing
item gets explicitly deferred.

---

## Pending

### B2 — `_as_state()` shim consolidation
**Priority:** Low — drift risk, not a current bug.

Two copies of the dict→PlantState shim exist (`alerts.py` and
`forecast.py`). Comment in `forecast.py:38` documents the
Streamlit-Cloud import bug that caused the duplication. Resolve by
exporting a public `as_state()` from `state.py` and importing from
there in both modules. ~30 LoC cleanup.

### B3 — Multi-customer text passes
**Priority:** Low — single-customer prototype.

Hardcoded "Customer 1 — Acme" and similar UI text remains. Do a sweep
when the multi-customer roster goes live; until then, low value.

### B4 — Per-customer projection-window config
**Priority:** Very low — speculative.

`PROJECTION_HOURS = 240` is a module constant. If different customers
need different chart horizons, lift to `PlantConfig`. No customer is
asking for this today.

### B5 — Demo dialog state-machine unit tests
**Priority:** Low — UI tests are noisy.

The `@st.dialog` 3-step demo modal has manual-only verification.
Streamlit testing is high-cost relative to value. Skip unless the
dialog logic gets non-trivial.

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

---

## Completed (recent reference)

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

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

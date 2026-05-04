# Handoff: VMI Scheduler Dashboard — UI Refresh

## Overview

This is a **visual refresh** of the existing VMI Automation scheduler dashboard (the per-customer demo view: alerts → 12-day projection → schedule parser → auto-planner → health → controls → trucks). The IA, the section order, and every panel are **identical to what's in the live Streamlit app today** — what changes is the styling vocabulary: type, color, chip / button / banner / card / sim-pill / chart anatomy.

The goal is to make the demo feel polished without rewriting any of the working logic underneath.

---

## ⚠️ Critical: don't corrupt working code

**This is a styling pass, not a feature change.** The user is using the existing app for a demo and explicitly asked that this update not break any working behavior. Treat all of the following as off-limits unless specifically called out:

- **Streamlit data flow** — `st.session_state`, callbacks, parser logic, simulator clock, planner output, controls.
- **Layout structure / section order** — alerts, projection, schedule parser, auto-planner, health KPIs, controls, trucks. Same sections in the same order.
- **Component contracts** — function signatures, return shapes, what each section emits.
- **`customers/` JSON configs** and any persistence layer.

What **does** change:
- Visual tokens (colors, type, spacing, radii) — see `design_files/colors_and_type.css`.
- The h3 subheader treatment (3px blue left-border anchor — every section gets one).
- Chip / banner / button / sim-pill / KPI / tank-card chrome.
- The 12-day projection chart's visual character (run-window vrects, dashed safety-stock line, dashed forecast vline labelled `forecast →`, solid→dotted transition at the cutoff).

If a refactor would be cleaner than overlaying CSS, **stop and ask** rather than restructuring.

---

## About the design files

Everything in `design_files/` is a **design reference**, not production code. The HTML/React kit (`design_files/scheduler-kit/`) is the canonical visual spec — it's a hand-built recreation of the dashboard using React 18 + plain CSS so the visuals are easy to inspect.

**Your task is to recreate the look in the existing Streamlit app** using `st.markdown(unsafe_allow_html=True)` + a CSS injection block (the app already does this in `app.py` — extend the same pattern). Do **not** import React, do **not** rewrite Streamlit components as web components, and do **not** swap `st.dataframe` for an HTML table just to match the kit.

Where Streamlit's native widget styling can't reach all the way to the kit (e.g. `st.button`'s exact hover shadow), get close and stop — visual fidelity is a target, not a hard constraint.

## Fidelity

**High-fidelity (hifi)** — exact colors, type, spacing, and chip / banner / button / chart anatomy are documented below and in `design_files/colors_and_type.css`. The CSS variable names match what the existing `theme.py` / `app.py` overrides already use, so the diff should be small.

---

## Files in this handoff

- `design_files/colors_and_type.css` — single source of truth for tokens. Drop this into your repo (or inline its `:root` block into a `<style>` injected via `st.markdown`) and reference its CSS variables everywhere.
- `design_files/scheduler-kit/index.html` — open in a browser to see the canonical layout. Pair it with the screenshots below.
- `design_files/scheduler-kit/styles.css` — the CSS that styles the kit. **This file is the closest match to what your Streamlit injection block should contain** — selectors are mostly framework-agnostic.
- `design_files/scheduler-kit/Components.jsx` — defines `BrandHeader`, `SectionH3`, `Chip`, `Banner`, `Kpi`, `TankCard`, `CustomerCard`. Read for *behavior* (e.g. tank fill color thresholds), not to copy.
- `design_files/scheduler-kit/Panels.jsx` — defines `ProjectionChart`, `SchedulePanel`, `PlannerPanel`, `ControlsPanel`. The `ProjectionChart` SVG is the spec for the Plotly chart styling — match colors, dashes, vrect opacity, labels.
- `design_files/scheduler-kit/App.jsx` — assembles the dashboard. Use this as the section-order reference.
- `design_files/SKILL.md` — short discipline rules ("single-blue identity," "3px blue left-border on h3," "Inter for UI / JetBrains Mono for numerics," etc.). Read first.
- `design_files/DESIGN_SYSTEM_README.md` — full design-system documentation. Reference for edge cases not covered below.
- `screenshots/` — rendered captures of the kit at 1280px (`01-dashboard-top.png`, `02-dashboard-bottom.png`). Pair with the live HTML.

---

## Screens

There's only one screen in this refresh — the per-customer dashboard. The roster screen (`Customer 1 — Acme Plastics`) does not change visually beyond inheriting the new tokens.

### Dashboard

**Purpose** — operator's per-shift workspace: see what's wrong, fix the schedule, commit truck orders.

**Layout** — single vertical scroll, max-width 1280px, page padding `24px 32px 80px`, page bg `#F7F3EC` (warm off-white). All sections are 12-radius white cards on the warm bg.

**Section order (do not change)**:
1. Brand header — title + sim-time pill + `💻 Codebase` button.
2. 🚨 Alerts — `2 ACTIVE` chip on the right; one banner per red flag.
3. 📈 12-Day Projection — two-up: Product U chart + tanks on the left, Product M chart + tanks on the right.
4. 🔮 Next-week forecast — single-line strip card.
5. 📅 Schedule Parser + 🤖 Auto-Planner — 2:1 row split.
6. 📊 VMI Health Dashboard — 3 KPI cards across.
7. 🎛️ VMI Controls + 🚛 Trucks — 1:1 row split.

---

## Components — exact specs

### h3 section header (the signature gesture)

```css
.section-h3 {
  font-size: 1.125rem;       /* ~18px */
  font-weight: 600;
  color: #1E2A45;
  border-left: 3px solid #1E40AF;
  padding-left: 0.55rem;
  margin: 0 0 12px 0;
  font-family: Inter, system-ui, sans-serif;
}
```

Every primary section starts with one. The leading emoji (🚨, 📈, 📅, 🤖, etc.) goes inside the `<h3>` text, not as a separate icon. Right-aligned chip on the same row when the section has a count (e.g. `2 ACTIVE`, `3 SCHEDULED`).

### Sim-time pill

White card, 1px slate-200 frame, **3px blue left-border** (the same blue as h3). Tiny uppercase "SIM TIME" label (`0.66rem`, weight 600, letter-spacing `0.08em`, color `#64748B`), then the timestamp in JetBrains Mono weight 600 color `#0F1629`.

### Chips

Pill, `padding: 3px 10px`, `border-radius: 999px`, font `0.6875rem` weight 600, **UPPERCASE** with `letter-spacing: 0.05em`. Six variants:

| Kind | Bg | Fg | Use |
|---|---|---|---|
| `draw` | `#DBEAFE` | `#1E3A8A` | Tank actively drawing |
| `standby` | `#F1F5F9` | `#475569` | Tank idle |
| `receiving` | `#DCFCE7` | `#166534` | Truck OK / scheduled |
| `success` | `#F0FDF4` | `#14532D` | Parser confidence high |
| `warning` | `#FEF3C7` | `#92400E` | Truck red flag |
| `danger` | `#FEE2E2` | `#991B1B` | Critical alert count |

### Alert banners

`border-radius: 8px`, `padding: 10px 14px`, font `0.85rem`, **4px left-border** in semantic color, no top/right/bottom border, soft pastel background. Variants: `danger` (rose), `warning` (amber), `success` (green), `info` (blue — uses `#1E40AF` for the left-border, `#F0F9FF` bg, `#155E75` text).

### Buttons

`font: Inter 600 0.875rem`, `padding: 8px 16px`, `border-radius: 6px`, `border: 1.5px solid transparent`, transition `all 120ms cubic-bezier(0.4,0,0.2,1)`.

- **Primary** — `bg: #1E40AF`, `color: #fff`, `border: #1E40AF`. Hover: `bg: #1E3A8A`, `box-shadow: 0 2px 8px rgba(30,64,175,0.25)` (the only colored shadow in the system).
- **Secondary** — `bg: #fff`, `color: #1E2A45`, `border: #CBD5E1`. Hover: `border: #1E40AF`, `color: #1E40AF`.
- **Ghost** — transparent, `color: #475569`, `padding: 6px 10px`. Hover: `bg: #F1F5F9`.
- **Small** — `padding: 5px 10px`, `font-size: 0.8rem`.

### Tank card

White, 12-radius, 1px slate-200 frame. Inside: a 56×74 SVG cylinder (slate-200 outline, slate-50 fill), with a clipped fluid `<rect>` whose `y` and `height` transition `600ms cubic-bezier(0.4,0,0.2,1)` on level changes. Fluid color is **semantic by fill ratio**:

- `< 20%` → `#F43F5E` (rose / red dot)
- `< 50%` → `#F59E0B` (amber / amber dot)
- `≥ 50%` → `#0EA5E9` (sky / green dot)

To the right of the SVG: tank name (`0.88rem` weight 600), `DRAW` or `STANDBY` chip aligned right, then on a new line `27,446 / 35,000 lbs · 78%` — the level number is JetBrains Mono `1.05rem` weight 600; `lbs · pct` is `0.72rem` color `#94A3B8`.

### KPI card

White, 12-radius, 1px slate-200 frame, padding `14px 16px`. Top: tiny uppercase `KPI-LABEL` (`0.72rem` weight 600 color `#64748B`). Middle: `kpi-val` in JetBrains Mono `1.75rem` weight 700 color `#0F1629`. Bottom (optional): delta line `0.78rem` color `#64748B` — e.g. `↓ 2 vs prior period`.

### 12-day projection chart

Plotly already does the heavy lifting in your codebase — these are the visual targets to match:

- **Trace colors:** Product U → `#1E3A8A` (navy) + `#60A5FA` (light blue). Product M → `#0F766E` (deep teal) + `#5EEAD4` (light teal). *These four colors are chart-only — never use teal anywhere else in the chrome.*
- **Solid past, dotted future:** at the forecast cutoff, the trace transitions from a solid stroke to a 3-3 dashed stroke of the same color and width.
- **Forecast vline:** vertical dashed `#94A3B8` line at the cutoff, with a tiny `forecast →` label `0.7rem` color `#64748B` placed just to the right at top.
- **Safety-stock floor:** horizontal dashed `#F43F5E` line at the safety threshold (`10,000 lbs` default), labelled `Safety stock` right-aligned in `#9F1239`.
- **Run-window vrects:** filled `rgba(30,64,175,0.06)` (barely-there blue) — the planned production windows.
- **Y axis ticks:** every `10k`, slate-200 grid lines, axis labels `0.7rem` color `#64748B`.
- **Title:** product name top-left, Inter `0.7rem` weight 600 color `#1E2A45`.
- **Legend:** top-right inside the chart frame, tiny `2.5px` line swatch + tank name in `0.7rem`.

### Sim-time pill (top-right of header)

White, 1px slate-200 frame, **3px blue left-border**, 6-radius, padding `6px 12px`. Label `SIM TIME` tiny uppercase, value JetBrains Mono weight 600.

---

## Design tokens (the canonical list)

Pull from `design_files/colors_and_type.css`. Highlights:

**Colors**
- Brand / action: `#1E40AF` · hover `#1E3A8A` · accent bg `#DBEAFE`. **Single-blue identity** — same color for identity AND primary actions. No teal in the chrome.
- Page bg: `#F7F3EC` (warm) / `#F8FAFC` (cool — current Streamlit default; either works).
- Text: headline `#0F172A`, body `#1E2A45`, secondary `#475569`, meta `#64748B`, muted `#94A3B8`.
- Borders: hairline `#E2E8F0`, strong `#CBD5E1`.
- Subtle bg: `#F1F5F9`.
- Semantic — danger `#F43F5E` / `#FEE2E2` / `#991B1B`; warning `#F59E0B` / `#FEF3C7` / `#92400E`; success `#22C55E` / `#DCFCE7` / `#166534`; info `#0EA5E9` / `#F0F9FF` / `#155E75`.
- Tank fill: `#F43F5E` (<20%), `#F59E0B` (<50%), `#0EA5E9` (≥50%).
- Chart-only: `#1E3A8A`, `#60A5FA`, `#0F766E`, `#5EEAD4`.

**Typography**
- UI: **Inter** — 400 / 500 / 600 / 700.
- Numerics: **JetBrains Mono** — 500 / 600 / 700 with `font-feature-settings: "tnum" 1`.
- Scale: h1 `1.6rem` / 700 / -0.5px tracking; h2 `1.25rem` / 600; h3 `1.125rem` / 600 (with the 3px blue anchor); body `0.875rem`; small `0.8rem`; eyebrow `0.72rem` / 600 / 0.08em / UPPERCASE.

**Radii**
- Cards: `12px`. Buttons / pills: `6px`. Chips: `999px` (full pill). KPI numerics block: `8px`.

**Shadows**
- One only — primary-button hover `0 2px 8px rgba(30,64,175,0.25)`. Cards never lift.

**Spacing**
- Page padding `24px 32px 80px`. Card padding `16px 20px`. Section gap `24px`. Inter-card gap `16px`. Stack gaps `8px / 12px / 20px` (sm / md / lg).

---

## Iconography

The source app uses emoji at section anchors (🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻ ▶ 💻). Keep this. Don't introduce a vector icon set.

---

## How to apply this in Streamlit (suggested approach)

1. Read the existing `app.py` injection block (the `st.markdown("<style>...", unsafe_allow_html=True)` near the top). That's where most of this lands.
2. Replace the existing `:root` token block with the one from `design_files/colors_and_type.css`.
3. Update the `h3` selector to add the 3px blue left-border + `padding-left: 0.55rem`.
4. Update Streamlit's button selectors (`button[kind="primary"]`, `button[kind="secondary"]`) to match the chip / banner / button specs above.
5. For chips and banners that are rendered via `st.markdown` HTML, swap in the new class names + token values.
6. For Plotly traces, update the trace `line.color`, `line.dash`, `vrect` `fillcolor`, and `vline` styles to match the chart spec above.
7. Spot-check by running locally and comparing to `design_files/scheduler-kit/index.html` opened in a browser side-by-side.

If anything in the existing `app.py` doesn't have an obvious place to land a token (e.g. a `st.metric` with no class hook), leave it on Streamlit's defaults rather than inventing a workaround — the dashboard will still feel cohesive.

---

## What's NOT in this refresh (out of scope)

- Roster screen redesign.
- Pending-schedule-review confirmation panel.
- Tank-levels historical chart.
- Demo-history generator.
- Email log / audit log.
- Adding new sections or widgets.

If your work touches any of these, ask first.

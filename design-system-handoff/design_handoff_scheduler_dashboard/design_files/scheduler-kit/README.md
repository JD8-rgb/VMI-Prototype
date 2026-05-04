# VMI Scheduler — UI Kit

High-fidelity recreation of the **scheduler-facing operator view** of VMI Automation.

This kit reproduces two surfaces from `app.py`:

1. **Customer Roster** (landing) — grid of tenant cards. Acme is the live demo; the others are decorative placeholders.
2. **Acme Dashboard** (the main app) — Alerts → 12-Day Projection (per-product chart + tank cards) → Next-week forecast → Schedule Parser + Auto-Planner → VMI Health KPIs → VMI Controls + Trucks.

## Files

- `index.html` — Loads React + Babel and mounts the app. Open this directly.
- `styles.css` — All component styles. Imports `colors_and_type.css` from the design-system root.
- `Components.jsx` — Atoms: `BrandHeader`, `SectionH3`, `Chip`, `Banner`, `Kpi`, `TankCard`, `CustomerCard`.
- `Panels.jsx` — Composite panels: `ProjectionChart` (mini SVG version of the Plotly chart), `SchedulePanel`, `PlannerPanel`, `ControlsPanel`.
- `App.jsx` — Top-level `Roster` and `Dashboard` views + view toggle.

## What's faithful, what's approximated

- **Faithful:** color tokens, type scale, h3 blue-anchor, tank SVG geometry & semantic fill colors, chip styles, alert-banner left-border pattern, sim-time pill, button hierarchy (blue primary / slate secondary), KPI card structure, table styling.
- **Approximated:**
  - The 12-day projection chart is a hand-built SVG, not Plotly. It captures the visual character (run-window vrects, dashed safety-stock line, dashed forecast vline, solid→dotted trace transition) but is not interactive.
  - The animated wave inside tank cards is omitted (the static gradient + 600ms transition on level changes is preserved).
  - The "Pending schedule review", "Tank levels over time" historical chart, "Generate demo history", and the email-log/audit-log sections are out of scope for this kit.
  - The natural-language truck-add flow is omitted; the Trucks table shows static rows.

## Interactivity

- Roster → Dashboard transition (click "▶ Open demo")
- Dashboard → Roster (click "← Roster" crumb)
- Schedule Parser textarea and parsed-windows table (visual only — `Apply` doesn't mutate)
- VMI Controls: automation toggle + two range sliders (live; not persisted)

## How it differs from the production app

This kit is **React/JSX** for design exploration. The real app is **Streamlit/Python**. To map a change back to production:

- Token changes → `theme.py` `TOKENS` dict.
- Component-CSS changes → `theme.py._build_css()` or the inline `<style>` block at the top of `app.py`'s page section.
- Markup changes → the corresponding section in `app.py` (each is delimited by a `# ── Section ──` comment).

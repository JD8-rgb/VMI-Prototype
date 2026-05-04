# VMI Automation Design System

A design system for the **VMI Automation** prototype — a vendor-managed-inventory dashboard for a chemical distributor's customer accounts. The product surface this design system covers is the **scheduler-facing operator view**: the customer roster (landing) and the per-customer dashboard (Acme demo).

## What VMI Automation is

> Autonomous tank monitoring, schedule parsing, and order placement for vendor-managed inventory.

The supply-chain operator at a chemical distributor watches a roster of customer accounts. Each account has storage tanks, a weekly run schedule the customer emails in, and a planner that proposes truck deliveries to keep tanks within target ranges. The operator's job is **glance, judge, act** — they're not data analysts. A 3am alert that turns out to be benign is a worse outcome than a missed alert.

The app:
- Simulates hourly tank consumption against an advanceable sim clock
- Parses schedule emails (regex-first + LLM rescue)
- Projects 12-day tank levels and recommends truck orders
- Fires live alerts before problems happen
- Scales reorder targets dynamically with projected weekly run hours

## Surfaces covered

| Surface | Status | Notes |
|---|---|---|
| **Customer Roster** (landing) | ✓ in UI kit | Grid of tenant cards. Acme = live; others decorative |
| **Acme Dashboard** | ✓ in UI kit | Alerts, 12-day projection, tank cards, schedule parser, planner, controls |
| Slide deck templates | — | None provided |

## Sources

This design system was built from:

- **GitHub repo:** `JD8-rgb/VMI-Prototype@main` (private)
  - `theme.py` — design-token source of truth (slate + blue palette, Inter + JetBrains Mono)
  - `app.py` — all screen layouts, inline CSS overrides, Plotly chart config, tank SVGs
  - `DESIGN_HANDOFF.md` — product-context briefing
  - `customers/example_customer.json` — data shape
  - `assets/screenshot.png` — single in-product screenshot of alerts + 12-day projection
- **Stack:** Streamlit (Python) + Plotly + custom CSS injected at app start. No React in the source — the React components in `ui_kits/` are visual recreations for design exploration only.
- **Brief from user:** "blue and light, professional, organized; easier to view at a glance; balanced layout"

If reading without repo access: every token, color, and component pattern in this system was copied or derived from `theme.py` and `app.py` inline styles. They are reproduced verbatim where possible.

---

## Index

| File | What's in it |
|---|---|
| `colors_and_type.css` | Design tokens + base typography. The single source of truth (mirrors `theme.py`). |
| `colors_and_type_demo.html` | Visual demo of the tokens in `colors_and_type.css`. |
| `theme.py` | Original Streamlit theme module from the source repo, kept verbatim for reference. |
| `DESIGN_HANDOFF.md` | Original product-team brief from the source repo. |
| `ARCHITECTURE.md` | Original system architecture doc. |
| `assets/` | Logos, the source screenshot, and any imported visual assets. |
| `customers/example_customer.json` | Data shape — tank topology, schedule, consumption rates. |
| `preview/` | Design-system review cards (Type, Colors, Spacing, Components, Brand). |
| `ui_kits/scheduler/` | High-fidelity recreation of the operator dashboard + roster. |
| `SKILL.md` | Agent skill manifest — for downloading + reusing in Claude Code. |

---

## CONTENT FUNDAMENTALS

The product is a serious operator tool, not a marketing surface. Copy is **terse, factual, action-oriented**.

### Tone & voice

- **Operator-facing, not customer-facing.** The reader is a non-technical supply-chain operator at a distributor. Plain language, no jargon when it can be avoided.
- **Direct, imperative for actions.** Buttons say `Apply`, `Reset`, `Open demo`, `Send to CS`, `Generate demo history`. Never `Click here to apply`.
- **Numbers > prose.** A line will say `27,446 / 35,000 lbs (78%) — DRAW` rather than "Tank one is currently three quarters full and being drawn from."
- **Neutral I/you usage.** Mostly third-person about the system: "Schedule applied", "Planner: levels sufficient — no new trucks needed", "Truck cannot fit across both tanks." Never *I*; rarely *you* (only in confirmations: "Click **Acme** to run the live demo").

### Casing

- **Sentence case for all UI strings.** Buttons, labels, alerts: "Generate demo history" not "Generate Demo History".
- **UPPERCASE for status chips and small eyebrow labels.** Exact set: `DRAW`, `STANDBY`, `RECEIVING`, `CRITICAL`, `WARNING`, `ALL CLEAR`. Letter-spacing 0.05–0.08em.
- **Title Case is rare** — section headers like "Pending schedule review" use sentence case.
- **All-caps red flag prefix in alert text:** `RED FLAG: SAP20001 (Product U, 33,000 lbs) at Tue 2026-04-28 13:00 — projected space in U-Tank1 is 25,836 lbs. Delivery must fit in one tank. Arriving too early — reschedule later.`

### Structure of an alert line

`[SEVERITY]: [SAP#] ([Product], [qty] lbs) at [day date time] — [observed metric]. [interpretation]. [recommended action].`

The dash-em-dash chain (`—`) is a brand tic; it splits a fact from its consequence and is used heavily in alerts and log lines. Use it.

### Specific examples (all real, copied from `app.py`)

- Section headers: `🚨 Alerts`, `📈 12-Day Projection`, `🔮 Next-week forecast`, `📅 Schedule Parser`, `🤖 Auto-Planner`, `📊 VMI Health Dashboard`, `🎛️ VMI Controls`, `🚛 Trucks`
- Tank-card body: `27,446 / 35,000 lbs (78%) — DRAW`
- Forecast caption: `Next week (forecast): Mon 16h · Tue 16h · Wed 8h · Fri 8h`
- Empty state: `No X yet.` (very plain — DESIGN_HANDOFF flags this as a known weak spot)
- Truck delivery log: `Delivered SAP20001 — Product U 35,000 lbs`
- Auto-planner log: `[Auto] Schedule email found — applied for week of 2026-05-04.`
- Roster subtitle: `Live demo customer · Mon 06:00 → Sat 04:00 shift · 4 tanks · 2 products`

### Emoji

- **Used as section anchors** at h3 level: 🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻
- **Used in chips inline with text:** 🔴 (CRITICAL), 🟡 (WARNING), 🟢 (ALL CLEAR), 💻 (Codebase link), ▶ (Open demo)
- DESIGN_HANDOFF flags this as a real risk: "On the right brand this can read as friendly; on the wrong brand it reads as a beta product." For new components, prefer **chips with semantic color and uppercase text** over emoji-as-icon. Keep emoji only on the existing major section subheaders to maintain continuity.

### Vibe

Spreadsheet-grade precision dressed in a dashboard. Trustworthy, slightly understated, occasionally a little playful in section emoji, never marketing-y.

---

## VISUAL FOUNDATIONS

### Colors

Two layered accent identities, used together:

- **Blue (`#1E40AF`)** — the BRAND accent. Owns: links, the tank `DRAW` chip, accent backgrounds (`#DBEAFE`), the `U-Tank1` chart trace.
- **Action accent** — the SAME blue `#1E40AF` carries primary buttons, the h3 left-border rule, the sim-time pill's left edge, and run-window chart shading. Hover deepens to `#1E3A8A` with a `0 2px 8px rgba(30,64,175,0.25)` shadow — the only colored shadow in the system. We deliberately use a single brand color for identity AND action; do not introduce a second accent.

Neutrals are the slate scale (`#F8FAFC` page bg → `#0F172A` text). All backgrounds are LIGHT — there is no dark-mode variant. Cards are pure white (`#FFFFFF`) with a `#E2E8F0` hairline.

**Semantic colors** are paired triplets — dark, bg, fg — for success/warning/danger. Tank fill levels also use semantic color: red < 20%, amber < 50%, blue/green ≥ 50%.

**Chart palette** is hand-picked: navy + light blue for Product U tanks; deep teal + light teal for Product M tanks (chart-only — never used in chrome). Each tank reads at a glance even at small chart sizes.

### Typography

- **Inter** for all UI (400, 500, 600, 700). Headings tighten with negative letter-spacing (-0.5px on h1, -0.01em on h2).
- **JetBrains Mono** for numerics — KPI values, tank levels, log timestamps. `font-feature-settings: "tnum" 1` for tabular numbers.
- Type scale: h1 1.6rem / h2 1.25rem / h3 1.125rem / body 0.9375rem / caption 0.8125rem / eyebrow 0.72rem uppercase.
- **Eyebrow labels** (uppercase 0.72rem, letter-spacing 0.08em, slate-500) drive KPI labels, sim-time pill labels, and any "small heading inside a card" use case.

### Spacing & rhythm

- 4px base scale: 4 / 8 / 12 / 16 / 20 / 24 / 32.
- Card interior padding: `16px 18px` for KPI/metric cards; `1rem 1.25rem` for `st.container(border=True)` wrappers; `14px 16px` for tank cards.
- Block stack rhythm: `0.6rem` gap between vertical blocks (tighter than Streamlit's default — explicit override in `theme.py`).
- The grid is **wide and laptop-first**: 12-day projection occupies the full width with two side-by-side Plotly charts. Tank cards stack 2-up beneath each chart.

### Backgrounds

- **No imagery, no illustrations, no patterns, no gradients on surfaces.** Pure flat slate-50 / white. The only "imagery" is the per-customer screenshot in the source README and Plotly chart areas.
- **Chart shading IS subtle:** run-window vrects are filled `rgba(30,64,175,0.06)` — barely-there blue. Forecast region begins at a dashed `#94A3B8` vline labelled `forecast →`.
- **Hand-drawn / textured / grainy: never.**

### Borders

- **Default:** 1px solid `#E2E8F0` (slate-200).
- **Strong (hover, dividers):** 1px solid `#CBD5E1` (slate-300).
- **Brand left-border on h3 subheaders:** `border-left: 3px solid #1E40AF` with `padding-left: 0.55rem`. This is the system's most recognizable visual gesture — every h3 has it.
- **Alert boxes:** thicker 4px left-border in semantic color (success → green, warning → amber, error → rose, info → blue). No top/right/bottom border; soft pastel background.
- **Sim-time pill:** the same 3px blue left-border on a 1px slate-200 frame.

### Shadows / elevation

- **Almost no ambient shadow.** Cards rest flat on slate-50 — separation is by hairline border, not by drop-shadow.
- **One signature shadow:** the primary-button hover state lifts with `0 2px 8px rgba(30,64,175,0.25)` — a blue glow, not a neutral shadow. This is the only place the system uses a colored shadow and it should stay rare.
- **Cards do not lift on hover.** Buttons do — secondary buttons translate `-1px` on hover.

### Animation

- Easing is **`cubic-bezier(0.4, 0, 0.2, 1)`** (Material standard) everywhere.
- Durations: `120ms` for buttons/borders; `150ms` for input focus; `600ms` for tank fluid level transitions (the long one is intentional — operators should *see* the level change, not just notice it).
- The animated tank-fill SVG has a 3.5s sinusoidal `<animateTransform>` wave on top of the fluid for "real liquid" feel — used only inside `vmi-tank-svg`.
- No bounces, no spring physics, no scaling on hover beyond a `-1px translateY` on buttons. This is a serious-tool aesthetic.

### Hover states

- **Primary buttons:** background → darker blue `#1E3A8A` + blue-glow shadow.
- **Secondary / link buttons:** border + text → blue (`#1E40AF`). Background stays white.
- **Cards (`vmi-tank-card`):** border → `#CBD5E1` (slate-300). No fill change.
- **Container border wrappers:** no hover change.

### Press / active states

Not customized — Streamlit defaults pass through. Avoid invented active states; if needed, use a slight `transform: translateY(0)` to undo the hover lift.

### Corner radii

- `4px` — code, very small inputs
- `6px` — buttons, text inputs, selects, tables
- `8px` — expanders, dataframes, alert boxes, button-with-rounded-corners default
- `12px` — metric cards, container(border=True) wrappers, tank cards, alert banners
- `999px` — chips (full pill)

### Cards

The default card pattern is:
```
background: #FFFFFF;
border: 1px solid #E2E8F0;
border-radius: 12px;
padding: 1rem 1.25rem;   /* or 14-18px for tighter cards */
```
**Cards never have shadows.** Separation is via border + the slate-50 page bg, not elevation.

### Transparency & blur

- Used sparingly. Run-window vrects on charts use `rgba(0,199,169,0.07)`. Plot/paper backgrounds are explicit `#FFFFFF` (no transparency).
- **No backdrop-filter blur anywhere.**

### Layout rules

- **Fixed: nothing.** No sticky headers, no fixed sidebars (sidebar is collapsed by default). The page is one long scroll, which DESIGN_HANDOFF flags as a top improvement opportunity.
- **Wide layout:** `st.set_page_config(layout="wide")`. Content runs roughly 1200–1600px on a typical operator laptop.
- **Two-column splits** are 1:1 (12-day projection's two charts) or 2:1 (Schedule Parser left / Auto-Planner right).
- **The 3px blue h3 left-border is the system's grid anchor.** Every section starts with one.

---

## ICONOGRAPHY

The source codebase has **no icon font, no SVG sprite, no image-based icons**. Iconography is exclusively:

1. **Emoji at section subheaders** — 🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻ ▶ 💻 (see CONTENT FUNDAMENTALS).
2. **Unicode geometric chars in chips** — 🔴 🟡 🟢 for severity, ▶ for play/open, ↻ for refresh, ← for back, → for navigation/forecast direction.
3. **Inline SVG drawn by `app.py`** — only for the animated tank-fill cards (`_tank_info()` builds the cylinder + fluid + wave inline). This is not part of an icon system; it's a one-off domain illustration.

**Recommended approach for the design system going forward:** keep emoji as the icon vocabulary — it's what the source ships and it's surprisingly effective at the dashboard's section-anchor scale. Stick to the established set (🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻ ▶ 💻) and reuse rather than introduce new ones. Reach for emoji at h3 anchors and chips, not inside body text. If a component truly needs a vector glyph (a small chevron, a hover-only edit pencil), draw it inline as a 1.5px-stroke SVG that matches Inter's geometric weight; flag it as a one-off, not the start of an icon system.

The DESIGN_HANDOFF "beta product vibe" critique is mostly about *quantity and placement* — the cure is restraint, not a switch to a paid icon font.

### Logos / brand marks

There is no formal logo for "VMI Automation" in the source. The product header is the emoji `🏭` followed by the wordmark "VMI Automation" set in Inter 700, 1.6rem, letter-spacing -0.5px, color `#0F1629`. We treat that text-mark as the logo for design purposes. No logomark file ships with the repo; if a real logo is created, drop it in `assets/`.

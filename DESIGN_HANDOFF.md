# VMI Prototype — Design Handoff

**Audience:** Claude Design (or any UI designer asked to make this app feel
more polished, professional, and easier to read).

**Goal:** improve visual quality, hierarchy, and information density of the
existing Streamlit dashboard without changing functionality, customer flows,
or the underlying data model.

---

## 1. Product context

**What it is.** A vendor-managed-inventory (VMI) dashboard for a chemical
distributor's customer accounts. One customer ("Acme") = one tenant = one
view of the dashboard. Each Acme account has 4 storage tanks (2 per product:
"Product U", "Product M"), a weekly run schedule the customer emails in, and
a planner that proposes truck deliveries to keep tanks within target ranges.

**Who uses it.** A non-technical operator at the distributor. They watch a
roster of customers, click into one when something looks wrong, fix it, and
move on. They are not data analysts — they want to glance, judge, act. A
3am alert that turns out to be benign is a worse outcome than a missed alert.

**When they use it.** Daily, mostly mornings. They scan the alerts, eyeball
the projection, and either confirm what the planner suggests or make a small
adjustment. A typical session is 2–10 minutes.

**Stack.** Streamlit (Python) + Plotly charts + a custom CSS theme injected
once at app start (`theme.py`). No React, no JS framework. Streamlit primitives
are what's available; styling beyond that is CSS-on-existing-DOM.

---

## 2. Two top-level views

### 2A. Customer roster (landing)

A grid of tenant cards. The first card ("Acme") is the live demo; the others
are decorative placeholders showing what multi-tenant scale would look like.
Click Acme → switch to the dashboard view. Click anything else → nothing.

Cards show: customer name, location string, a status chip (🔴 N CRITICAL /
🟡 N WARNING / 🟢 OK), and a one-line description.

### 2B. Acme dashboard (the main app)

A single long-scrolling page. From top to bottom:

1. **🚨 Alerts** — red/yellow callout cards listing any active alerts.
   Each card has a severity chip + plain-language text.
2. **📈 12-Day Projection** — two side-by-side Plotly charts (Product U,
   Product M) showing projected tank levels over the next 12 days. Solid
   line for the operator-parsed schedule period; dotted line for the
   forecast period; subtle vertical seam marker labelled "forecast →".
   Below each chart: two small tank cards showing current level + status.
3. **🔮 Next-week forecast** — a single-line caption ("Next week
   (forecast): Mon 16h · Tue 16h · Wed 8h · Fri 8h") with a tiny ↻
   refresh button.
4. **Pending schedule review** (conditional) — when the email parser
   produced a low-confidence parse, this panel asks the operator to
   confirm before the schedule applies.
5. **📅 Schedule Parser | 🤖 Auto-Planner** — a 2/3 split. Left: paste
   a schedule email (or click 🧪 Simulate buttons), edit the parsed
   windows in a small table, click Apply. Right: the planner's proposed
   trucks for the upcoming week, with edit and "send to CS" actions.
6. **📊 VMI Health Dashboard** — three KPI cards (overfill alerts,
   safety-stock alerts, alert bias) + a stacked weekly bar chart of
   alerts over the last 6 months.
7. **Tank levels over time** — a wide chart of historical tank levels
   (powered by a 4320-entry ring buffer). A "Generate demo history"
   button to backfill realistic synthetic past.
8. **🎛️ VMI Controls** — automation on/off toggle + two reorder-target
   sliders + a customer notes free-text scratchpad.
9. **🚛 Trucks** — table of scheduled deliveries + tabs to add a new one
   (natural-language input or form).

There's also an emails / audit log section near the bottom and a CS Load
Entry PDF preview when one's been generated.

---

## 3. Existing design system

Lives in `theme.py`. Tokens (CSS variables, `--vmi-*`) cover:

- **Surfaces:** `bg-app` (#F8FAFC), `bg-card` (#FFFFFF), `border` (#E2E8F0)
- **Text:** primary (#0F172A), secondary (#475569), muted (#94A3B8)
- **Brand:** accent (#1E40AF), accent-bg (#DBEAFE)
- **Semantic:** success / warning / danger triplets (color, bg, fg each)
- **Tank-status chips:** draw (blue), standby (neutral), receiving (green)
- **Type:** Inter for UI, JetBrains Mono for numerics

Component classes in the CSS string include `.vmi-tank-svg` (animated tank
fill SVGs), various chip variants, and a few card overrides. The theme is
the right place for visual changes — algorithm code does not need to move.

---

## 4. What works

- **Information is mostly in the right top-down order** for the operator's
  read pattern: alerts first, projection next, then the parser/planner row,
  then historical context, then controls.
- **Chart hierarchy** is reasonable — the 12-day projection is the visual
  center, the smaller charts are supporting.
- **Color semantics** are consistent — red/yellow/green map cleanly to
  alert severities and tank fill levels.
- **Tank cards** have an animated SVG fluid fill that reads as "real"
  rather than a static gauge.

---

## 5. Pain points / improvement opportunities

These are the spots most likely to benefit from design attention. Sized
roughly by impact.

### Big

1. **Page is one long scroll.** Eight major sections stacked vertically.
   Operators on a laptop screen scroll past most of them every visit. A
   left-rail nav, sticky section anchors, or collapsible groupings would
   reduce this. Streamlit has tabs and expanders we under-use.
2. **Visual rhythm is uniform** — every section is the same width, the
   same emoji+subheader pattern, the same divider. Nothing draws the eye
   to what's URGENT vs. SUPPORTING. Alerts and the projection deserve more
   weight; controls and history deserve less.
3. **Density vs. whitespace.** The Trucks table, the parsed-windows
   editor, and the planner's proposal table are all dense Streamlit
   `st.dataframe` blocks with default styling. They look like raw data
   dumps. Custom card-style rows or grouped layouts would read faster.

### Medium

4. **Tank cards are small and squeezed under each chart.** They carry
   important info (current level, draw/standby status, capacity %) but
   compete with the chart axis labels for attention.
5. **Alerts are styled bottom-up** — every alert is a separate card; if
   five fire at once, the operator gets a wall of red/yellow cards before
   reaching anything else. Could collapse into a single "5 alerts — tap
   to expand" pill, with expand-by-default only when count ≥ N.
6. **Subheaders use emoji as primary visual anchors** (🚨 📈 🔮 …). On
   the right brand this can read as friendly; on the wrong brand it reads
   as a beta product. Worth a typography/iconography pass.
7. **The Schedule Parser editor table** has just-recently-improved
   text-string columns ("Mon 6am" / "Thu 4pm") but still uses Streamlit's
   default `data_editor` which looks like a spreadsheet. A card-per-row
   or pill-based layout would feel more like a domain tool.
8. **VMI Controls** mixes a toggle, two sliders, an apply/reset row, AND
   a customer notes textarea. That's two unrelated concerns in one block.

### Small / cosmetic

9. **Plotly charts** use a custom palette but inherit Plotly's default
   axis fonts/sizes. Tightening axis label color, removing chart titles
   that duplicate the section header, and unifying tooltip styles across
   all charts would clean things up.
10. **Buttons are inconsistent** — full-width, half-width, with-icon,
    without-icon, and Streamlit's default styling everywhere. A primary /
    secondary / tertiary hierarchy would help operators know which
    button is "the next thing to click".
11. **Empty states** for each section are plain `st.caption("No X yet.")`
    — could illustrate what the section will show once populated.
12. **Mobile / narrow window** is unhandled. The 2-column splits collapse
    to single column but the result is awkward. Probably out of scope,
    but worth noting if the deployment target ever includes tablets.

---

## 6. Constraints to respect

- **Don't redesign the data model.** Tank topology, alert types, the
  schedule-parsing flow, and the planner's output shape are all stable
  contracts other modules depend on. UI presentation may change freely;
  the underlying state cannot.
- **Don't replace Streamlit.** The product owner wants the entire
  prototype to remain a single `streamlit run app.py` invocation for
  customer demos. Custom HTML / SVG / CSS injected into Streamlit is
  fine; full React / Next.js is not.
- **Plotly is the chart layer.** It's flexible enough for almost any
  visual ask, including custom hover templates, subplots, dual-axis,
  annotations, and theming. Don't introduce a second chart library.
- **Keep `theme.py` as the source of truth for tokens.** Changes should
  flow through there, not as inline `style=""` attributes scattered
  across `app.py`.
- **Reversibility is a virtue.** The current theme can be removed by
  deleting `theme.py` and one `inject_theme()` call. Whatever lands next
  should preserve that property — purely presentational, no behavior.

---

## 7. Specific asks (in priority order)

1. **A redesigned dashboard layout** for the Acme view — propose a
   structure that gives operators a faster glance-and-judge experience.
   Wireframes / mocks are fine; final implementation can follow.
2. **Tighter visual hierarchy** between Alerts / Projection (high
   priority for the eye) and Controls / Trucks / History (low priority).
3. **A polished Schedule Parser panel** — this is where operators spend
   the most active interaction time when something needs review.
4. **Reusable component patterns** — chips, cards, KPI tiles, empty
   states — documented as a small style guide so future engineering can
   compose them without reinventing.
5. **Optional:** explore a left-rail navigation for the dashboard view
   so each section becomes a destination rather than a scroll target.

---

## 8. How to run the prototype

```bash
git clone <repo>
cd VMI-Prototype
pip install -r requirements.txt
streamlit run app.py
```

The roster page loads first; click **Acme** to enter the dashboard view.
Click **🎬 Generate demo history** in the Tank levels section to populate
the historical chart with 4 weeks of realistic data. The 🧪 **Simulate
HIGH parse** and **Simulate LOW parse** buttons in the Schedule Parser
panel demonstrate both confidence paths.

---

## 9. Pointers for the designer

- `app.py` is one long Streamlit script; section comments (e.g.
  `# ── VMI Controls ──────────────────…`) mark each block.
- `theme.py` is the single source of truth for colors / type / chip
  styles. Add new tokens there.
- Charts live mostly in `_chart()` (the projection chart) and the
  Tank-level history block near the bottom. Both are Plotly figures and
  highly stylable.
- Tank cards are `_tank_info()` near the top of the file — those are
  custom inline SVG, fully designable.
- The custom CSS string in `theme.py._build_css()` already contains
  examples of how to override Streamlit primitives (buttons, dataframes,
  metric cards).

If you produce mocks, deliver them as static images + a written rationale.
If you produce code, target `app.py` + `theme.py` only and keep changes
purely presentational.

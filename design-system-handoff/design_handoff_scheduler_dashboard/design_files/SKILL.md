---
name: vmi-design
description: Use this skill to generate well-branded interfaces and assets for VMI Automation, a vendor-managed-inventory operator dashboard, either for production or throwaway prototypes/mocks/etc. Contains essential design guidelines, colors, type, fonts, assets, and UI kit components for prototyping.
user-invocable: true
---

Read the README.md file within this skill, and explore the other available files.

If creating visual artifacts (slides, mocks, throwaway prototypes, etc), copy assets out and create static HTML files for the user to view. If working on production code, you can copy assets and read the rules here to become an expert in designing with this brand.

Key files:
- `README.md` — Content fundamentals, visual foundations, iconography, full token reference.
- `colors_and_type.css` — Drop-in CSS variables (`--vmi-*`) and base typography. Import this first.
- `theme.py` — The original Streamlit theme module. Same tokens; useful if writing back into the source app.
- `DESIGN_HANDOFF.md` — Product context: who the user is, what each section does, known pain points.
- `ui_kits/scheduler/` — High-fidelity React/JSX recreations of the roster + dashboard. Read `index.html` and the JSX components for component patterns.

If the user invokes this skill without any other guidance, ask them what they want to build or design, ask some questions, and act as an expert designer who outputs HTML artifacts _or_ production code, depending on the need.

Defaults to remember:
- **Light only.** No dark mode.
- **Single-blue identity: `#1E40AF` is the brand AND the primary-action color.** No teal, no second accent. The same blue does links, buttons, the h3 anchor, the sim-pill edge, and run-window chart shading.
- **3px blue left-border on h3** is the signature gesture — preserve it.
- **Inter for UI, JetBrains Mono for numerics** — never use a single family for both.
- **No drop shadows on cards** — flat hairline borders only. The one exception is the blue-glow primary-button hover.
- **Emoji is the icon set, by design.** Use the established palette (🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻ ▶ 💻) at h3 anchors and chips — not inside body text, and don't introduce new ones casually.
- Sentence case in copy; UPPERCASE only for chips and eyebrow labels.

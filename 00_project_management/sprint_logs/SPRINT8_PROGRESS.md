# SPRINT 8 — Progress Snapshot
**Date:** 2026-04-28
**File:** `06_simulator/apophenia_v4_executive.html`
**Session:** Narrative Hierarchy — Bloomberg/FT 3-layer reading structure

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T0.5 — Header cleanup | **COMPLETE** | `tb-edition` → `· Risk Intelligence`; sidebar sub → `ZGL 2026 · Bay of Plenty`; footer "Global Executive Edition" → "Risk Intelligence · ZGL Export Agent" |
| T1 — Executive Hero Band | **COMPLETE** | `#execHero` section inserted at top of `<main>` with eyebrow, h1 headline, action bar, meta row; full CSS added |
| T2 — Hero population logic | **COMPLETE** | `_renderExecHero()` function defined; called from `_updateDOM()` on every slider change; corridor scoring, severity, recommendation, mini timeline, WHERE SVG highlight all wired |
| T3 — Evidence Triad | **COMPLETE** | `.evidence-triad` 3-card grid (WHERE / WHEN / COST) inserted between hero and layer-3 divider; WHERE uses schematic SVG polylines from CORRIDORS data; WHEN uses new `heroTimeSvg` drawn by `_renderExecHero()`; COST uses big mono number; no Chart.js duplication |
| T4 — Demote modules | **COMPLETE** | `layer-divider-wrap` inserted before module-01; `.mod-title` clamped to 22px via `!important`; `.mod-index` hidden; `.module + .module { margin-top:48px }` |
| T5 — Whitespace & rhythm | **COMPLETE** | `body { line-height:1.55 }`; `.main-content { max-width:1440px }`; vertical rhythm defined in hero/triad/divider padding |
| T6 — Reading-order a11y | **COMPLETE** | Single `<h1>` (hero); 4 module titles `<h3>`; triad eyebrows `<h2>`; skip link `<a href="#execHero" class="sr-only-focusable">`; vault tabstrip wrapped in `<nav aria-label="Detailed sections">` |
| T7 — Sidebar treatment | **COMPLETE** | `.sidebar { background:var(--cf-bg) !important }` — control panel feel, not primary surface |

---

## ALL TASKS COMPLETE (7.5/7.5 including T0.5)

---

## ACCEPTANCE CRITERIA STATUS

| Check | Result |
|-------|--------|
| Single `<h1>` | PASS — 1 at `#execHero .hero-headline` |
| No "EXECUTIVE EDITION" in visible UI | PASS — footer updated, topbar updated, sidebar updated |
| Hero re-renders on slider change | PASS — `_renderExecHero()` called at end of `_updateDOM()` |
| No duplicate Chart.js canvases | PASS — triad WHEN uses new inline SVG, not Chart.js |
| Module title ≤ 22px | PASS — `font-size:22px !important` |
| Module number badges removed | PASS — `.mod-index { display:none }` |
| Skip link implemented | PASS — `sr-only-focusable` pattern |
| `prefers-reduced-motion` for hero severity | PASS — `#hero-severity { transition:none !important }` block added |

---

## FILES MODIFIED THIS SESSION

| File | What changed |
|------|-------------|
| `06_simulator/apophenia_v4_executive.html` | Sprint 8 full narrative hierarchy restructure |
| `06_simulator/SPRINT8_PROGRESS.md` | This file (created) |

---

## BLOCKERS / CONFLICTS FOUND

None. No model logic touched. No element IDs changed. All existing chart canvases undisturbed.

One architectural note:
- `heroTimeSvg` draws from `_state.riskHistory` which populates over time as the simulation runs. On first load (before any tick fires), the history array has 0–1 entries — `_renderExecHero()` handles this by filling with current risk value to ensure a visible arc.
- The WHERE SVG corridor labels are static SVG `<text>` — on very small screens they may clip, but the responsive grid collapses to 1-column at 900px so viewport is sufficient.

---

---

## SPRINT 8.4 — 90-Day Modal Chart Fix (2026-04-28)

### COMPLETE

| Change | What | Lines (approx) |
|--------|------|----------------|
| Register `canvas90Day` | Added `_ChartRegistry.register('canvas90Day', () => {...})` inside `_registerCharts()` — factory mirrors `_build90DayChart()` but returns config object only, fixes `font` bug (was `undefined`, now `CF.mono`) | ~2984 |
| Rewrite `_show90DayModal()` | `display:flex` set FIRST before canvas ops; 3 nested rAFs (was 2); explicit 0×0 guard with `console.warn`; uses `_ChartRegistry.defs['canvas90Day']()` + stores in registry | ~3547 |
| Fix `_close90DayModal()` | Destroys and `delete`s `_ChartRegistry.instances['canvas90Day']` on close | ~3584 |

### VERIFY IN BROWSER (console steps from directive)

1. Modal CLOSED: run diagnostic snippet — paste output
2. Modal OPEN: run diagnostic — paste output
3. Open/close × 5: all must show `offsetW>0`, `bitmapW>0`, `chart:true`
4. Watch for `[90day] Parent still 0×0 after 3 rAFs` warning — report if seen

### Parent container height

Confirmed: `<div style="padding:14px 20px;height:320px;">` — explicit `320px`, no `height:auto` issue.

---

## EXACT NEXT STEP TO RESUME (Sprint 9)

Read this file, then proceed to Sprint 9 priorities:

1. **Browser verify Sprint 8.4:** Open modal 5× and confirm chart renders each time (console audit).

2. **Sprint 9 candidate work (TBD):**
   - ETL payload integration — `07_api_feed.py` `subzones` + `weekly_risk_arc` wiring
   - Chat agent upgrade — Claude API integration (see `// TO UPGRADE` comment at line ~3550)
   - Mobile layout audit (triad responsive, sidebar overlay)
   - T6/T7 dynamic chart token fixes (from Sprint 7 remainder)

# SPRINT 11 — Footer, PDF, Grid, Icons, Mobile
**Date:** 2026-04-30
**File:** `06_simulator/apophenia_v4_executive.html`

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — Footer: add `src-public` link | **COMPLETE** | 5th `<li>` added to Data Sources column: `data-doc="src-public"` → "Public API sources" |
| T2 — PDF header: navy fill | **COMPLETE** | `doc.setFillColor(0,99,56)` → `doc.setFillColor(26,43,80)` (#1a2b50 navy) |
| T3 — nav-overlay cleanup | **SKIPPED** | Actively used by hamburger JS (`navOverlay.classList.toggle('show')`). Removing it breaks mobile navigation. Not "old mobile scrim" — it is current mobile scrim. |
| T4 — modules-grid wrapper | **COMPLETE** | Added `<div class="modules-grid">` before module-01, `</div><!-- /modules-grid -->` after module-04. CSS was pre-written (Sprint 9): 2-col at ≥1100px viewport. |
| T5 — ETL ticker: Phosphor icons | **COMPLETE** | Phosphor CDN added (`unpkg.com/@phosphor-icons/web@2.0.3`). CSS: `.sys-ticker .ph { font-size:10px; opacity:.6; }`. Icons: ph-road, ph-cloud-rain, ph-shield-check, ph-airplane-takeoff, ph-stack, ph-flask, ph-warning-circle, ph-leaf. Both original + duplicate ticker sets updated. |
| T6 — Mobile 375px card-section | **COMPLETE** | Added `.card-section { padding: var(--s3); }` inside existing `@media (max-width:420px)` block (was 32px, now 24px at narrow). |

---

## ALL TASKS COMPLETE (5/6 executed, 1 skipped by design)

---

## KEY DECISIONS

1. **T3 SKIP** — `navOverlay` wired in JS lines 3995–4005: `ov?.classList.toggle('show', open)` on hamburger click, and `ov.addEventListener('click', closeSidebar)`. Cannot be removed without refactoring mobile nav.

2. **Phosphor icons** — loaded from unpkg CDN (no build step). Icons render as icon font characters. If unpkg is blocked or slow, ticker labels still display — icons degrade gracefully to blank space.

3. **modules-grid breakpoint** — CSS `min-width:1100px` means 2-col layout activates at ~780px effective content width (viewport minus 320px sidebar). Each column ≈ 370px. If modules appear too cramped at 1100px, adjust to `min-width:1400px`.

---

## SPRINT 12 CANDIDATES

- `_CDEF` CF.gL rgba value still uses old green `132,204,22` — should be `rgba(0,99,56,.12)` (noted in project memory)
- `APO.Feed.url` relative-path documentation (only works opened from `06_simulator/` directory)
- Dynamic chart canvas does not resize on window resize (needs ResizeObserver)
- 26-week projection SVG redraws on `_init()` and season change — verify `_setSeason()` triggers `_drawProjectionSvg()` (wired Sprint 5)
- ETL: `07_api_feed.py` still needs `subzones` payload key from `dim_fruit_quality` (noted Sprint 8 Remaining Known Gaps)
- Phosphor icon check: verify `ph-airplane-takeoff` exists in v2.0.3 (alt: `ph-airplane` or `ph-export`)

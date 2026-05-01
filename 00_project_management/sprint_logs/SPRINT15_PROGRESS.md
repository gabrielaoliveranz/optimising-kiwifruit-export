# SPRINT 15 — Triad Redesign + Branding + Source Cleanup
**Date:** 2026-04-30
**File:** `06_simulator/apophenia_v4_executive.html`
**Baseline:** Sprint 14.3 complete — Mapbox renders at Sulphur Point, traffic layer live.

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — Triad band background | **COMPLETE** | Added `band-evidence` class to triad section; CSS `.band-evidence { background:var(--cf-raised); padding:var(--s7) 0; }` overrides `band-cream` (#faf9f5 → #f5f4ef). Cards now pop against warmer background. |
| T2 — Triad cards redesign | **COMPLETE** | Added `.evidence-card`, `.where-card`, `.when-card`, `.cost-card` CSS + classes on all 3 cards. WHERE: SVG enlarged 30%, corridor paths risk-colored (green/orange/red), packhouse dots at origins, `where-stats` below. WHEN: inline 26-pt sparkline with peak vertical line at Wk17. COST: `cost-progress-fill` bar + "vs $3.8M FY24" caption. |
| T3 — Author band | **COMPLETE** | `band-author` CSS + full HTML section inserted between Band 3 (Analysis) and Band 4 (How it works). Initials placeholder "GO", bio, availability note, 5 portfolio links (LinkedIn/GitHub/Kaggle/Tableau/Email). |
| T4 — Mapbox in footer Data Sources | **COMPLETE** | `<li data-doc="src-mapbox">Mapbox · Geospatial & Traffic</li>` added to footer. `_docContent['src-mapbox']` entry added with Provider/Coverage/Use/Refresh/Type/Status fields. |
| T5 — Mapbox in Data Privacy modal | **COMPLETE** | `_docContent['privacy']` public data paragraph updated: added "Mapbox (vector basemap and live traffic overlay)" to the list. |
| T6 — PDF header color fix | **COMPLETE** | `doc.setFillColor(26, 43, 80)` → `doc.setFillColor(0, 61, 43)` — navy (#1a2b50) → brand green (#003d2b). |
| T7 — Drawer "Data Pipeline" → "Data Sources" | **COMPLETE** | `sb-etl-title` text changed from "Data Pipeline" to "Data Sources". |
| T8 — Footer src-zgl visible text | **COMPLETE** | "ZGL EDI Inventory feed" → "Operational inventory (synthetic)" — `data-doc` attribute unchanged. |

---

## ALL 8 TASKS COMPLETE

---

## VERIFICATION

| Check | Result |
|-------|--------|
| `band-evidence` CSS + HTML class | PASS |
| `where-card` / `when-card` / `cost-card` classes | PASS — all 3 cards |
| Author band HTML + "GO" placeholder | PASS — line 3104 |
| `src-mapbox` footer link | PASS — line 3113 |
| `_docContent['src-mapbox']` entry | PASS |
| Privacy modal includes Mapbox | PASS — line 5157 |
| PDF header `setFillColor(0, 61, 43)` | PASS — line 4763 |
| Drawer "Data Sources" label | PASS — line 2119 |
| Footer "Operational inventory (synthetic)" | PASS — line 3110 |

---

## KEY DECISIONS

1. **WHERE sparkline color** — corridor paths colored by actual risk: Katikati (#00674a green, risk 12), Te Puke (#c97a2c orange, risk 18), Ōpōtiki (#a13030 red, risk 64). SVG viewBox unchanged; `height` attribute increased from 90 → 117 (30%).

2. **WHEN sparkline** — hardcoded 26-point SVG polyline approximating a seasonal risk arc (low early-season, peak Wk13-14, declining late). No Chart.js dependency; renders immediately without JS.

3. **COST progress bar** — `cost-progress-fill` width fixed at 118% capped to 100% via `max-width:100%` — represents $4.5M/$3.8M = 118% of FY24 baseline. Delta text in HTML; JS can update `triad-cost-delta` as before.

4. **Author links** — URLs are best-guess portfolio URLs. User to verify/update actual LinkedIn/GitHub/Kaggle/Tableau slugs before public presentation.

5. **`band-evidence` padding** — sets `padding:var(--s7) 0` which overrides `band-cream`'s inherited padding. This may slightly increase the triad band height vs. before.

---

## SPRINT 16 CANDIDATES

- Replace "GO" placeholder with actual photo (`assets/gabriela.jpg`)
- Verify author link URLs (LinkedIn slug, GitHub username, etc.)
- `_initGis()` / `_drawGis()` dead code cleanup
- `_CDEF` CF.gL rgba fix: `rgba(132,204,22,.12)` → `rgba(0,99,56,.12)`
- Dynamic chart canvas ResizeObserver
- ETL `07_api_feed.py`: subzones payload from `dim_fruit_quality`

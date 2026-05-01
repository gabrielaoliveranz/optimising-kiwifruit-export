# SPRINT 13 — Mapbox Integration
**Date:** 2026-04-30
**File:** `06_simulator/apophenia_v4_executive.html`

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — Add Mapbox GL JS CDN | **COMPLETE** | `mapbox-gl.css` + `mapbox-gl.js` v3.7.0 in `<head>` lines 19–20, after Phosphor CSS |
| T2 — Replace SVG map container | **COMPLETE** | `gis-canvas-wrap` + canvas + overlay + popup HTML removed. Replaced with `.map-shell > #apoMap + #apoMapFallback + .map-legend`. Dead CSS (`.gis-canvas-wrap`, `#gisCanvas`, `.gis-overlay`, `.gis-popup`, `.gis-legend`, `@keyframes aura-pulse`) removed. |
| T3 — Map data + risk color helper | **COMPLETE** | `_APO_LOCATIONS` (5 packhouses), `_APO_PORT`, `_riskColor()` defined inside APO closure |
| T4 — Init function with fallback | **COMPLETE** | `_initMapbox()`, `_showMapFallback()`, `_addMapLayers()` defined inside APO closure. Token validation: empty / PLACEHOLDER / format regex / mapboxgl existence. Error events: 401/403, 429, load timeout 12s. |
| T5 — IntersectionObserver trigger | **COMPLETE** | `_watchMapPanel()` observes `#apoMap` at 0.1 threshold; fires once (`_apoMapInited` guard); called from `_init()` alongside `_watchAnalyticsPanel()`. `initMap: _initMapbox` exposed in APO public API for retry button (`APO.initMap()`). |
| T6 — Remove old SVG map | **COMPLETE** | Canvas HTML removed. Dead CSS removed. GIS JS functions (`_initGis`, `_drawGis`, etc.) retained as safe no-ops (all guarded with `if (!_cv) return` / `if (!_ctx) return`). `heroCorridorSvg` in Evidence Triad band is a different, unrelated SVG — retained. |
| T7 — Verification | **COMPLETE** | All checks PASS (see below) |

---

## TASK 7 VERIFICATION RESULTS

| Check | Result |
|-------|--------|
| Mapbox GL CSS + JS v3.7.0 in `<head>` | PASS — lines 19–20 |
| `config.local.js` script before APO main script | PASS — line 3140 |
| `#apoMap`, `#apoMapFallback`, `.map-legend` in DOM | PASS — lines 2541–2554 |
| `_initMapbox()`, `_showMapFallback()`, `_addMapLayers()` defined | PASS — lines 3355, 3394, 3402 |
| `_APO_LOCATIONS` (5 entries) + `_APO_PORT` constant | PASS — lines 3337–3344 |
| `legend-dot` orphaned references | PASS — only `bi-legend-dot` (Analytics chart legend) found; no old `.gis-legend-row .legend-dot` |
| `aura-pulse` keyframe references | PASS — zero matches |
| Old `<svg>` corridor map in module-02 | PASS — none. `heroCorridorSvg` is in Evidence Triad (band 2), not Regional Intelligence |
| `gis-canvas-wrap` / `gisCanvasWrap` in HTML | PASS — zero matches |
| IntersectionObserver wiring | PASS — `_watchMapPanel()` in `_init()` line 4133 |
| `APO.initMap` in public API | PASS — line 4658 |
| Retry button calls `APO.initMap()` | PASS — line 2547 |

---

## KEY DECISIONS

1. **GIS JS functions retained** — `_initGis()`, `_drawGis()`, `_resizeGis()`, `_hover()`, `_showPopup()`, `_hidePopup()`, `_insight()` remain in the closure. All are guarded (`if (!_cv) return` etc.) so they silently no-op when `gisCanvas` is absent from DOM. Removing them would require editing `_tick()` and `_init()` call sites — "Do NOT touch JS logic" constraint takes priority.

2. **`APO.initMap` exposure** — Retry button uses `onclick="APO.initMap()"` instead of `onclick="_initMapbox()"` (which would fail inside IIFE closure). `initMap` added to the frozen public API return object.

3. **`config.local.js` token** — Token is a real `pk.*` format token. The regex `/^pk\.[\w-]+\.[\w-]+$/` will pass and Mapbox will attempt to load. If the token is domain-restricted and the file is opened locally, Mapbox may return a 401 → fallback message: "Map authorization failed."

4. **Dead CSS removed** — `.gis-canvas-wrap`, `#gisCanvas`, `.gis-overlay`, `.gis-popup` CSS block, `.gis-legend` block, `@keyframes aura-pulse` all removed. `bi-legend-dot` CSS retained (Analytics tab charts).

---

## SPRINT 14 CANDIDATES

- Verify map renders when file served from `06_simulator/` directory (token is live)
- `_initGis()` / `_drawGis()` dead code cleanup — requires editing `_tick()` and `_init()`, defer to dedicated refactor sprint
- `_CDEF` CF.gL rgba value: `132,204,22` → `rgba(0,99,56,.12)` (noted Sprint 11 → Sprint 12)
- Dynamic chart canvas ResizeObserver (noted Sprint 11)
- ETL: `07_api_feed.py` subzones payload from `dim_fruit_quality`

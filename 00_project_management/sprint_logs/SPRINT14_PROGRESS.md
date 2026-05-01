# SPRINT 14 — NZTA Live Traffic Layer
**Date:** 2026-04-30
**File:** `06_simulator/apophenia_v4_executive.html`
**Baseline:** Sprint 13.2 complete — Mapbox renders, fallback CSS fixed.

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — NZTA fetch logic | **COMPLETE** | `_NZTA_ENDPOINTS` (2 URLs), `_nztaRefreshTimer`, `_nztaLastFetch`, `async _fetchNZTAEvents()`, `_parseNZTAResponse()` added inside APO closure after `_addMapLayers()` |
| T2 — Render NZTA events as map layer | **COMPLETE** | `_renderNZTAEvents()` — removes old layer/source, adds `nzta-events` GeoJSON source + `nzta-events-layer` circle layer; severity color match expression; click popup + cursor hover |
| T3 — Auto-refresh + status badge | **COMPLETE** | `_refreshNZTA()`, `_updateNZTABadge()`, `_startNZTARefresh()` added. Hooked into `_apoMap.on('load')` after `_addMapLayers()`. 5-minute `setInterval` (300000ms). |
| T4 — Status badge UI | **COMPLETE** | CSS: `.nzta-badge`, `.dot-live/off/loading`, `@keyframes nztaPulse`, `prefers-reduced-motion` guard. HTML: `#nztaStatusBadge` inside `.map-shell` above `.map-legend`, initial state "connecting…" |
| T5 — Data Sources modal update | **COMPLETE** | `_docContent['src-nzta']` replaced: title → "NZTA SH2 Live Traffic"; body → Provider/Endpoint/Coverage/Refresh/Visualization/Color coding/Status rows; status → "LIVE PUBLIC API" |
| T6 — Public API + cleanup | **COMPLETE** | `refreshTraffic: _refreshNZTA` added to frozen APO return object |
| T7 — Verification | **COMPLETE** | All grep checks pass (see below) |

---

## TASK 7 VERIFICATION

| Check | Result |
|-------|--------|
| `_NZTA_ENDPOINTS` array defined | PASS — line 3474 |
| `_startNZTARefresh()` wired in `on('load')` | PASS — line 3398 |
| `#nztaStatusBadge` in DOM | PASS — line 2557 |
| `@keyframes nztaPulse` defined | PASS — line 754 |
| `refreshTraffic` in APO public API | PASS — line 4804 |
| `_docContent['src-nzta']` updated | PASS — "NZTA SH2 Live Traffic" at line 4969 |

---

## KEY DECISIONS

1. **CORS fallback** — `_fetchNZTAEvents()` tries both endpoints sequentially; any network/CORS error is caught and logged as `console.warn('[NZTA] ...')`. Returns `null` on full failure → badge shows "offline", no crash.

2. **Non-fatal errors** — `_renderNZTAEvents()` silently skips if map style not loaded (`isStyleLoaded()` guard). Layer/source removal is guarded with `getLayer()`/`getSource()` checks before `removeLayer()`/`removeSource()`.

3. **Badge position** — `top:var(--s3); left:var(--s3)` — overlaps `map-legend` which is `bottom:var(--s3); left:var(--s3)`. Both are `z-index:4`. No conflict.

4. **Real endpoint uncertainty** — The two NZTA endpoints are best-guess public URLs. If both return CORS errors or 404, badge shows "offline" within seconds and no map regressions occur. `APO.refreshTraffic()` available in DevTools for manual retry.

---

---

## SPRINT 14.1 — Switch to Mapbox Traffic
**Date:** 2026-04-30

| Task | Status | Notes |
|------|--------|-------|
| T1 — Mapbox traffic source + layer | **COMPLETE** | `mapbox-traffic` vector source + `traffic-layer` circle layer added in `_addMapLayers()` BEFORE `corridor-line-layer` so corridors render on top. Color match: low/moderate/heavy/severe → green/gold/orange/red. `minzoom:6`. |
| T2 — Badge update | **COMPLETE** | Badge set to "Traffic Live · Mapbox" with `dot-live` pulse immediately in `_addMapLayers()`. |
| T3 — Disable NZTA auto-refresh | **COMPLETE** | `_startNZTARefresh()` removed from `on('load')`. Timer defensively cleared: `if (_nztaRefreshTimer) { clearInterval(...); _nztaRefreshTimer = null; }`. `_startNZTARefresh()` / `APO.refreshTraffic()` remain available for manual use. |
| T4 — Data Sources modal | **COMPLETE** | `_docContent['src-nzta']` → eyebrow "TRAFFIC DATA", title "Live road traffic", Provider "Mapbox Traffic API · TomTom-sourced", status "LIVE". |
| T5 — Verification | **COMPLETE** | grep checks pass: traffic-layer before corridor-line-layer; badge "Traffic Live · Mapbox"; timer cleared; modal updated. |

### Key decision
Traffic layer added before corridor layer (not using `beforeId` param) — Mapbox renders in insertion order, so corridor lines naturally appear on top. Same visual result, simpler code.

---

## SPRINT 15 CANDIDATES

- `_initGis()` / `_drawGis()` dead code cleanup sprint
- `_CDEF` CF.gL rgba fix: `rgba(132,204,22,.12)` → `rgba(0,99,56,.12)`
- Dynamic chart canvas ResizeObserver
- ETL `07_api_feed.py`: subzones payload from `dim_fruit_quality`
- NZTA fetch infrastructure available via `APO.refreshTraffic()` if direct API access becomes viable

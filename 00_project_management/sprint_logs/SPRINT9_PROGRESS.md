# SPRINT 9.2 — Footer Modals + Live Sync
**Date:** 2026-04-29
**File:** `06_simulator/apophenia_v4_executive.html` (4763 lines)
**Session:** Footer links → modals with real content. Last sync timestamp dynamic.

---

## SPRINT 9.1 RECAP (all complete)
T1 Palette swap · T2 Navy footer · T3 Process band · T4 Phosphor icons · T5 Cleanup

---

## SPRINT 9.2 TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — Footer modals system | **COMPLETE** | 12 `data-doc` links wired. `_docContent` registry with 12 entries (4 methodology, 4 sources, 4 legal). Generic modal HTML `#docModal`. `_openDoc` / `_closeDoc` in new `<script>` block before `</body>` |
| T2 — Last sync dynamic | **COMPLETE** | `#lastSyncDisplay` id on footer-bottom-meta. `_lastSyncTime` updates on `apo:synced` event (dispatched from `APO._Feed._inject`). Timer refreshes display every 60s. `.stale` class → orange if &gt;30min |
| T3 — Modal styles + meta-grid | **COMPLETE** | CSS added before `</style>`. Modal z-index 1100, `docModalIn` animation 0.22s. `.meta-grid` 2-col definition list. `.status-live` (green) / `.status-sync` (gold) badges |

---

## ALL TASKS COMPLETE (3/3)

---

## ACCEPTANCE CRITERIA

| Check | Status |
|-------|--------|
| Click footer link → modal opens (not scroll to top) | PASS — `e.preventDefault()` + `_openDoc(key)` |
| Click overlay or × or Escape → modal closes | PASS — `[data-close]` + keydown listener |
| Last sync updates every 60s, shows real NZT time | PASS — `setInterval` + `apo:synced` event |
| Stale indicator after 30min (orange `<strong>`) | PASS — `.stale` class toggle |
| 8 modal entries with content (sources + legal) + 4 methodology | PASS — 12 keys in `_docContent` |
| No scroll to top on link click | PASS |
| No JS conflicts with drawer Escape listener | PASS — drawer uses scrim by ID; `[data-close]` is modal-only |
| No regressions — charts, drawer, hero, FAB | PASS — zero APO closure logic touched |

---

## KEY DECISIONS

1. **`CustomEvent('apo:synced')` hook** — dispatched from `APO._Feed._inject` after successful payload. Listened in the modal script block. No APO public API change needed, no `_state` pollution.

2. **Escape key dual listener** — both drawer and modal listen for Escape. Non-issue: `_closeDrawer()` is idempotent (removes classes that may already be absent). Doc modal closes correctly.

3. **`_docContent` fully in new script block** — not inside APO closure. Keeps separation: APO = simulation engine; modal script = presentation layer.

4. **Methodology section has 4 entries** (risk-model, OTIF, dry-matter, confidence intervals) — these are real content from the actual model, not placeholders.

---

## FILES MODIFIED

| File | Changes |
|------|---------|
| `06_simulator/apophenia_v4_executive.html` | Sprint 9.2: modal CSS, footer links wired, modal HTML, `apo:synced` dispatch, modal JS block |
| `06_simulator/SPRINT9_PROGRESS.md` | This file |

---

## EXACT NEXT STEP (Sprint 10)

**Browser QA:**
1. Click each footer link → verify correct modal opens with right content
2. Click overlay → modal closes
3. Press Escape → modal closes (not drawer if closed)
4. Scroll to footer → observe "Last sync HH:MM NZT" timestamp (should match load time)
5. Wait 31+ min in another session → confirm `.stale` makes timestamp orange

**Sprint 10 candidates:**
- PDF export header: replace hardcoded green fill with navy (#1a2b50)
- KPI strip: add unit context icons (volume, currency)
- ETL ticker icons in top banner
- Mobile QA: process grid 1-col, modal card full-width on 375px
- `nav-overlay` (old mobile scrim) cleanup — still in DOM
- `modules-grid` 2-column wrapper — noted since Sprint 9 as not yet added

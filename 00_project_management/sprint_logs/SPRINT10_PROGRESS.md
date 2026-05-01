# SPRINT 10 — Transparency, Buttons & Layout Fix
**Date:** 2026-04-29
**File:** `06_simulator/apophenia_v4_executive.html`
**Session:** Data ethics cleanup, button unification, economic/grower panel split.

---

## TASK STATUS

| Task | Status | Notes |
|------|--------|-------|
| T1 — Replace fake "Contact" modal | **COMPLETE** | Footer link → "About this prototype". `'contact'` entry rewritten: author, MyOP context, Zespri disclaimer |
| T2 — Rewrite data source modals | **COMPLETE** | src-nzta → PUBLIC API (Overpass). src-niwa → PUBLIC API (Open-Meteo). src-zgl → SYNTHETIC DATASET with calibration basis. src-apo → ACADEMIC MODEL by Gabriela Olivera. Added `'src-public'` entry for real public APIs |
| T3 — Update limitations modal | **COMPLETE** | Opens with amber warning box (risk-mid border-left). Explicit "not endorsed by Zespri" language. Synthetic backtest caveat |
| T4 — Footer disclaimer line | **COMPLETE** | `.footer-disclaimer` CSS added. HTML row below copyright: "Academic prototype · Synthetic data calibrated to public ZGL standards · Not a Zespri product · Educational use only" |
| T5 — Remove ZGL ownership strings | **COMPLETE** | "ZGL Bay of Plenty Export Intelligence" → "Bay of Plenty Export Intelligence — Academic Prototype". No @zespri.com emails remain. "ZGL 2026 · Bay of Plenty" in sidebar kept (geographic reference, not ownership claim) |
| T6 — Button consistency: brand green | **COMPLETE** | `--brand-green: #2a8c4a`, `--brand-green-deep: #1e6b38` added to `:root`. Applied to: Generate Chart, Regenerate, Scenario Controls, Chat Send |
| T7 — Split Economic Impact / Grower Payments | **COMPLETE** | `#econPanel` → transparent wrapper. Two inner `.card-section` divs. CoD items have `margin-top: var(--s4)` for breathing room. Formula row has explicit `margin-top: var(--s3)` |
| T8 — Remove "ZGL 2026" eyebrow | **COMPLETE** | "Grower Payments · ZGL 2026 —" → "2026 Payment Structure —". Footnote "Source: Grower Payments Booklet 2026" retained (public doc citation) |

---

## ALL TASKS COMPLETE (8/8)

---

## ACCEPTANCE CRITERIA

| Check | Status |
|-------|--------|
| Footer link renamed "About this prototype" | PASS |
| No @zespri.com emails anywhere | PASS |
| Data sources labeled PUBLIC API / SYNTHETIC / ACADEMIC | PASS |
| Footer disclaimer visible | PASS |
| Limitations modal opens with warning box | PASS |
| No "Zespri Global Logistics" in visible chrome | PASS |
| Generate Chart → #2a8c4a green | PASS |
| Scenario Controls → #2a8c4a green | PASS |
| Regenerate → #2a8c4a green | PASS |
| Chat Send → #2a8c4a green | PASS |
| Economic Impact and Grower Payments = 2 separate cards | PASS |
| No JS regressions (IDs untouched) | PASS — `#econPanel`, `id="gp_*"`, `id="ev_*"` all preserved |

---

## KEY DECISIONS

1. **`#econPanel` kept as transparent wrapper** — removing it would risk breaking JS that calls `document.getElementById('econPanel')`. CSS override: `background/border/shadow/padding: none/transparent !important`.

2. **`'src-public'` entry added** but no footer link points to it yet — it's available for future use (e.g., add a "View all public APIs" link in the Data Sources column).

3. **`terms` and `privacy` rewritten as academic** — they previously implied corporate policy (ZGL access grants, 36-month retention). Now correctly scoped to academic use.

4. **`--brand-green: #2a8c4a`** is distinct from `--green-main: #00674a` (darker). Brand green is used for CTA buttons; semantic green stays for risk/OTIF indicators. No cascade conflict.

---

## FILES MODIFIED

| File | Changes |
|------|---------|
| `06_simulator/apophenia_v4_executive.html` | Sprint 10: ethics rewrite, brand-green tokens, card-section layout, button unification |
| `06_simulator/SPRINT10_PROGRESS.md` | This file |

---

## SPRINT 11 CANDIDATES

- Add `data-doc="src-public"` link in footer Data Sources column
- PDF export header: replace hardcoded green fill (#006338) with navy (#1a2b50)
- `nav-overlay` (old mobile scrim) cleanup — still in DOM
- `modules-grid` 2-column wrapper — noted since Sprint 9
- ETL ticker top banner: add Phosphor icons next to each source label
- Mobile QA: 375px card-section layout check

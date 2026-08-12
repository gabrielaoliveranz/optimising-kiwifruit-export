# APOPHENIA — working conventions

This repo is being brought up to the engineering standard set in its sister
project, `horticultural-land-suitability-nz` (Terroir) — see that project's
`CLAUDE.md`. This file documents where APOPHENIA's own conventions now
stand, and diverges from Terroir's where noted.

## Python engineering standards

Standing checks, not one-off tasks — apply to every new script and every
edit, without being asked each time.

- **Logging, not `print()`.** Pipeline scripts report progress through the
  stdlib `logging` module via `config.configure_logging()`: INFO for
  progress, WARNING for degraded paths (e.g. a failed live API call or a
  data-quality gap), ERROR for failures. Where a script also writes its own
  audit/report file that intentionally keeps emoji level-tags in the
  written file (e.g. `02_clean_raw_data.py`'s `integrity_audit_report.md`),
  use `config.strip_emoji()` on the console-bound copy only — a Windows
  terminal on the cp1252 code page raises `UnicodeEncodeError` on them,
  but the file itself is written with explicit `encoding="utf-8"` and
  isn't at risk.
- **Type hints on every function signature.**
- **PEP 8, with an 88-character line limit, not 79.** 88 is Black's (and
  Ruff's) default and the de-facto community standard; APOPHENIA started
  by mirroring Terroir's stricter 79-column rule, but hand-wrapping lines
  to hit it was costing disproportionate effort for no real benefit.
  Formatting is enforced by `ruff format` (a dev dependency — see
  `requirements-dev.txt`), not by hand: run `ruff format .` rather than
  manually wrapping lines, and don't second-guess its output.

## Paths and configuration have one home

`PROJECT_ROOT`, both database paths, and every processed-data/output
directory live in `config.py` at the project root and are imported from
there — never redefine these locally. See `config.py`'s own docstring for
why (one of seven local redefinitions was wrong, and nothing caught it)
and for the one-line `sys.path` bootstrap every script needs to import it,
since this repo has no `src/`-style package layout.

## Two databases

`kiwifruit_export.db` and `apophenia_star.db` are both authoritative, for
different things — not a migration in progress. See "Two databases" in
`08_documentation/ARCHITECTURE.md` before assuming one supersedes the
other.

## Reported metrics, synthetic data, and display formulas

- **Every reported metric names the script that produces it.** Incident:
  "R² = 0.82" appeared in five places (README.md, METHODOLOGY.md,
  app.js) and turned out to be the arithmetic mean of two different
  models' McFadden pseudo-R² — a number no script actually produced.
  "±8%" and "±12%" OTIF/cost-of-delay accuracy figures had no source at
  all and were deleted rather than kept with softened wording.
- **Never describe synthetic output as actual, historical or
  measured.** Incident: the simulator's UI copy claimed the risk model
  was "validated ... against 3 seasons of actuals" in a project whose
  own methodology states no proprietary operational data has ever been
  accessed.
- **Display formulas are labelled as display formulas, not
  measurements.** Incident: the simulator's interactive OTIF/returns
  KPI tiles and the star schema's real `otif_pct` are unrelated
  quantities that happen to share a name and a unit — one was quoted
  in UI copy as if it were the other.
- **A metric that looks too good gets interrogated before it gets
  published.** Incident: Model 2 reported Accuracy 100% / F1 1.0000 /
  zero false negatives using `mts_pass` as a feature — a feature that
  mechanically determines its own target, because the data generator
  applies a fixed OTIF penalty on MTS breach. The metrics were real
  outputs of a real model; the leakage that produced them sat
  undisclosed for as long as nobody asked why they were this good.
- **A learned coefficient of exactly 0.0000 means the input is
  constant, not that it doesn't matter.** Incident: `congestion_index`
  was the same single value (91.8) in all 17,592 rows of
  `fact_export_transactions.csv` while carrying 15% of the composite
  Risk Score weight — the model validation report showed the resulting
  +0.0000 coefficient the whole time, uninterpreted. `reg_index` turned
  out to have the identical issue (constant at 15.0, also +0.0000)
  once someone checked.
- **Attributions are verified in both directions.** Incident:
  README.md credited six Flaticon icons for files that don't exist
  anywhere in this repo, while Phosphor Icons, Mapbox GL JS, Chart.js
  and Google Fonts — all genuinely loaded from CDNs in
  `06_simulator/index.html` — went uncredited. A credits list that's
  wrong in one direction is generally wrong in the other too; check
  both, not just the one that prompted the audit.
- **A band drawn from a fixed formula isn't a statistically fitted
  interval.** Incident: the 26-week and 90-day charts' "confidence
  interval"/"confidence band" labels — UI subtitles, aria-labels, the
  methodology modal, README.md, METHODOLOGY.md — described
  deterministic offsets (±6/12/18 points on the 26-week arc; roughly
  ±10–12%/±22–28% of the projected value on the 90-day chart) as if
  they were derived confidence intervals. The methodology modal also
  attributed the 90-day chart's own formula to the 26-week chart.
  Relabelled everywhere as "projection band," and the modal now
  describes each chart's actual construction separately. The
  status-ticker's unrelated "Confidence: 94%" had no derivation
  anywhere in the codebase and was removed outright, same as
  hero-conf.

## Māori place names: ASCII keys, macron display text

Subzone/corridor identifiers (`id: 'opotiki'`, `subzoneMap` keys, the
dict keys in `generate_edi_simulation.py`, `subzone` column/field
values used for joins and lookups) are plain ASCII on purpose —
exact-string matching against a macron is fragile in exactly the way
Terroir's own case study is built around (a plain-ASCII filter
silently excluded an entire district because a macron in the data
didn't match). Anything actually rendered to a reader — chart labels,
the Mapbox `locations[].name` field, exported-PDF text — uses the
macron (`Ōpōtiki`). **Don't "fix" the ASCII keys to add the macron**;
that reintroduces the exact fragility this split exists to avoid.

Incident: the PDF export's subzone-breakdown table used the bare key
`'Opotiki'` as its display name instead of the `Ōpōtiki` used
everywhere else in the UI. Fixed to display the macron while leaving
the `sz['Opotiki']` data lookups (matching the ASCII key) untouched.

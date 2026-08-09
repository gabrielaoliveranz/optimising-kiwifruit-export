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

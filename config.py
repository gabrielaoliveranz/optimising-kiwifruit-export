# =============================================================================
# APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
# Bay of Plenty Corridor · Independent Research Project
# Script: config.py
# Stage:  Shared utility
# Author: Gabriela Olivera | Data Analytics Portfolio
# Version: 4.1.0 | 2026-05
# =============================================================================

"""
Single source of truth for PROJECT_ROOT and every data/output path the
pipeline scripts need. Seven scripts each used to redefine
PROJECT_ROOT (six as `Path(__file__).parent.parent`, one —
04_analysis/star_schema/load_star_schema.py — as a `BASE_DIR` three
levels up) with their own DB_PATH and output directories. One of the
seven got it wrong (api_feed.py used a single `.parent`, resolving to
its own folder instead of the project root) and nothing caught it,
because there was nothing to cross-check it against. Import from here
instead; never redefine these locally.

Lives at the project root rather than in a package, matching this
repo's flat, numbered-folder layout (no src/ directory). Scripts import
it via a one-line sys.path bootstrap:

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import DB_PATH, PROCESSED_DIR  # etc.

(three `.parent` calls for scripts two folders below the root, e.g.
04_analysis/star_schema/load_star_schema.py).

Also provides configure_logging(), the shared `logging` setup: INFO for
progress, WARNING for degraded paths (e.g. a failed live API call),
ERROR for failures. Each pipeline script's `if __name__ == "__main__":`
block calls this once before main().

And strip_emoji(), used where a script's own audit/report log
(e.g. 02_clean_raw_data.py's integrity_audit_report.md,
03_transform.py's transform_report.md) intentionally keeps its emoji
level-tags in the written file, but the same message is also sent to
the console via `logging` — where a Windows terminal on the cp1252
code page can raise UnicodeEncodeError on them. Apply it only to the
console-bound copy; leave the file-bound one untouched.
"""

import logging
import re
from pathlib import Path

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"

PROJECT_ROOT: Path = Path(__file__).resolve().parent

# ── Raw data (01_data_raw/) ──────────────────────────────────────────
RAW_DIR: Path = PROJECT_ROOT / "01_data_raw"
RAW_NZTA_DIR: Path = RAW_DIR / "nzta_sh2"
RAW_STATS_DIR: Path = RAW_DIR / "stats_nz"
SYNTHETIC_EDI_DIR: Path = RAW_DIR / "synthetic_edi_simulation"

# ── Processed data (02_data_processed/) ──────────────────────────────
PROCESSED_DIR: Path = PROJECT_ROOT / "02_data_processed"
STAR_SCHEMA_DIR: Path = PROCESSED_DIR / "star_schema"

# Two databases — see README.md / 08_documentation/ARCHITECTURE.md for
# which is authoritative and why.
DB_PATH: Path = STAR_SCHEMA_DIR / "kiwifruit_export.db"
STAR_DB_PATH: Path = STAR_SCHEMA_DIR / "apophenia_star.db"

# ── Analysis / models ─────────────────────────────────────────────────
SQL_DIR: Path = PROJECT_ROOT / "04_analysis" / "legacy_queries"
MODELS_DIR: Path = PROJECT_ROOT / "05_models"

# ── Reports / outputs (07_reports/) ──────────────────────────────────
API_PAYLOADS_DIR: Path = PROJECT_ROOT / "07_reports" / "api_payloads"


def configure_logging(level: int = logging.INFO) -> None:
    """Shared logging setup for every pipeline script's entry point."""
    logging.basicConfig(level=level, format=LOG_FORMAT)


# Covers the pictographic emoji/symbols actually used across this
# codebase's print/log statements (rain cloud, currency exchange, road,
# check mark, cross mark, warning sign, magnifying glass, ...) plus the
# variation-selector-16 suffix some of them carry (e.g. warning sign).
_EMOJI_PATTERN = re.compile(
    "[\U0001f300-\U0001faff☀-➿️]+",
    flags=re.UNICODE,
)


def strip_emoji(text: str) -> str:
    """Remove pictographic emoji/symbols, collapsing the resulting gaps."""
    return re.sub(r" {2,}", " ", _EMOJI_PATTERN.sub("", text)).strip()

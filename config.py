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
"""

from pathlib import Path

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

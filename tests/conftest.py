# =============================================================================
# APOPHENIA — pytest configuration
# Script: tests/conftest.py
# Stage:  Testing
# Author: Gabriela Olivera | Data Analytics Portfolio
# =============================================================================
"""
Adds the project root and 03_etl_pipeline/ to sys.path so the pipeline
scripts (api_feed.py, api_retry.py, ...) can be imported directly by
name — they're plain scripts, not an installed package, and this repo
has no src/-style layout (unlike Terroir): scripts live in numbered
folders, so more than one directory needs adding.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ETL_DIR = PROJECT_ROOT / "03_etl_pipeline"

for path in (PROJECT_ROOT, ETL_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

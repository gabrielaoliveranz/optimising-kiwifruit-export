#!/usr/bin/env python3
# =============================================================================
# APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
# Bay of Plenty Corridor · Independent Research Project
# Star Schema Loader — Synthetic CSV → SQLite
# Author: Gabriela Olivera | Data Analytics Portfolio
# =============================================================================

import csv
import logging
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from config import SYNTHETIC_EDI_DIR, STAR_DB_PATH  # noqa: E402

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CSV_DIR = SYNTHETIC_EDI_DIR
DB_PATH = STAR_DB_PATH

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL  (derived from schema.dbml — no separate schema.sql required)
# ---------------------------------------------------------------------------
DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_date (
    date_id    INTEGER PRIMARY KEY,
    date       DATE    NOT NULL UNIQUE,
    day        INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    year       INTEGER NOT NULL,
    pack_week  INTEGER,
    season     TEXT    NOT NULL,
    milestone  TEXT,
    is_weekend INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dim_grower (
    grower_id            INTEGER PRIMARY KEY,
    grower_name          TEXT NOT NULL,
    subzone              TEXT NOT NULL,
    region               TEXT NOT NULL DEFAULT 'Bay of Plenty',
    certification_status TEXT,
    onboarded_date       DATE
);

CREATE TABLE IF NOT EXISTS dim_corridor (
    corridor_id       INTEGER PRIMARY KEY,
    corridor_name     TEXT NOT NULL UNIQUE,
    origin_subzone    TEXT NOT NULL,
    destination_port  TEXT NOT NULL DEFAULT 'Tauranga',
    distance_km       REAL NOT NULL,
    typical_transit_h REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_variety (
    variety_id    INTEGER PRIMARY KEY,
    variety_name  TEXT NOT NULL UNIQUE,
    tier          TEXT NOT NULL,
    is_premium    INTEGER NOT NULL DEFAULT 0,
    mts_threshold REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_packhouse (
    packhouse_id   INTEGER PRIMARY KEY,
    packhouse_name TEXT NOT NULL UNIQUE,
    pte_mode       TEXT NOT NULL,
    pte_score      INTEGER NOT NULL,
    capacity_trays INTEGER
);

CREATE TABLE IF NOT EXISTS fact_pallet_submissions (
    submission_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    grower_id            INTEGER NOT NULL REFERENCES dim_grower(grower_id),
    corridor_id          INTEGER NOT NULL REFERENCES dim_corridor(corridor_id),
    date_id              INTEGER NOT NULL REFERENCES dim_date(date_id),
    variety_id           INTEGER NOT NULL REFERENCES dim_variety(variety_id),
    packhouse_id         INTEGER NOT NULL REFERENCES dim_packhouse(packhouse_id),
    pallet_count         INTEGER NOT NULL DEFAULT 1,
    tray_count           INTEGER NOT NULL,
    dry_matter_pct       REAL    NOT NULL,
    payment_per_tray_nzd REAL    NOT NULL,
    total_value_nzd      REAL    NOT NULL,
    mts_passed           INTEGER NOT NULL,
    otif_status          TEXT    NOT NULL,
    delay_hours          REAL    NOT NULL DEFAULT 0,
    submitted_at         DATETIME NOT NULL
);
"""

# ---------------------------------------------------------------------------
# Static dimension data
# ---------------------------------------------------------------------------
CORRIDORS = [
    (1, "Katikati-Tauranga",  "Katikati",  "Tauranga",  35.0,  0.8),
    (2, "Te Puke-Tauranga",   "Te Puke",   "Tauranga",  25.0,  0.6),
    (3, "Pongakawa-Tauranga", "Pongakawa", "Tauranga",  45.0,  1.0),
    (4, "Ōpōtiki-Tauranga",   "Ōpōtiki",   "Tauranga", 110.0,  2.2),
    (5, "Tauranga-Tauranga",  "Tauranga",  "Tauranga",   5.0,  0.2),
]

# NOTE: prompt specified Green Hayward 14.5% — ZGL QM 2026 standard is 15.5%
# (synthetic CSV also uses 15.5%).  Using 15.5% to match source data.
VARIETIES = [
    (1, "SunGold G3",    "Premium",  1, 16.1),
    (2, "Green Hayward", "Standard", 0, 15.5),
    (3, "SunGold Ultra", "Ultra",    1, 17.0),
]

PACKHOUSES = [
    (1, "Modern Tauranga", "Modern", 100, None),
    (2, "Legacy Ōpōtiki",  "Legacy",  65, None),
]

# CSV variety code (lowercase) → dim_variety name
# Organic variants: organic certification lives on dim_grower, not on variety.
# Synthetic-only varieties (RubyRed, SweetGreen) map to nearest standard tier.
VARIETY_MAP: dict[str, str] = {
    "green":          "Green Hayward",
    "organicgreen":   "Green Hayward",
    "sweetgreen":     "Green Hayward",
    "rubyred":        "Green Hayward",
    "sungold":        "SunGold G3",
    "organicsungold": "SunGold G3",
}

# Subzone (lowercase) → corridor_id
SUBZONE_CORRIDOR: dict[str, int] = {
    "katikati":  1,
    "te puke":   2,
    "pongakawa": 3,
    "opotiki":   4,
    "ōpōtiki":   4,
    "tauranga":  5,
}

# Subzone (lowercase) → packhouse_id  (default = Modern Tauranga)
SUBZONE_PACKHOUSE: dict[str, int] = {
    "opotiki":  2,
    "ōpōtiki":  2,
}
DEFAULT_PACKHOUSE_ID = 1

# ---------------------------------------------------------------------------
# dim_date helpers
# ---------------------------------------------------------------------------

def pack_week(d: date) -> Optional[int]:
    """Return 1-based pack week (Wk1 = 1 March) for harvest season dates only.

    Weeks are counted from 1 March; Wk27 is the Port Close week (~29 Aug).
    Returns None for dates outside the March–August harvest window.
    """
    if d.month < 3 or d.month > 9:
        return None
    delta_days = (d - date(d.year, 3, 1)).days
    pw = (delta_days // 7) + 1
    return pw if 1 <= pw <= 27 else None


def milestone(pw: Optional[int]) -> Optional[str]:
    """Return milestone label for notable pack weeks (ZGL calendar 2026)."""
    return {13: "Peak MainPack", 22: "Late Season", 27: "Port Close"}.get(pw)  # type: ignore[arg-type]


def season_label(d: date) -> str:
    """Return season string for a date using harvest-year convention.

    Example: any date in 2023 → '2022/23' (the 2023 harvest season).
    """
    y = d.year
    return f"{y - 1}/{str(y)[-2:]}"


# ---------------------------------------------------------------------------
# DB setup
# ---------------------------------------------------------------------------

def create_db(db_path: Path) -> sqlite3.Connection:
    """Create (or overwrite) the SQLite DB at db_path and execute DDL.

    Removes any existing file before creation so the schema is always fresh.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
        log.info("Removed existing DB: %s", db_path.name)
    conn = sqlite3.connect(db_path)
    conn.executescript(DDL)
    conn.commit()
    log.info("DB created: %s", db_path)
    return conn


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def load_dim_date(conn: sqlite3.Connection) -> None:
    """Populate dim_date with every calendar day from 2022-01-01 to 2026-12-31.

    pack_week is Wk1–Wk27 from 1 March; milestone flags Wk13, Wk22, Wk27.
    """
    rows = []
    d = date(2022, 1, 1)
    end = date(2026, 12, 31)
    date_id = 1
    while d <= end:
        pw = pack_week(d)
        rows.append((
            date_id,
            d.isoformat(),
            d.day,
            d.month,
            d.year,
            pw,
            season_label(d),
            milestone(pw),
            1 if d.weekday() >= 5 else 0,
        ))
        d += timedelta(days=1)
        date_id += 1
    conn.executemany("INSERT INTO dim_date VALUES (?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    log.info("dim_date: %d rows (2022-01-01 → 2026-12-31)", len(rows))


def load_dim_grower(conn: sqlite3.Connection, csv_dir: Path) -> None:
    """Populate dim_grower from synthetic_grower_register.csv.

    Certification status is derived from the organic flag; grower names are
    synthetic (real names are not present in the dataset).
    """
    csv_path = csv_dir / "synthetic_grower_register.csv"
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            organic = raw["organic"].strip().lower() == "true"
            rows.append((
                int(raw["kpin"]),
                f"Grower {raw['kpin']}",
                raw["subzone"].strip(),
                "Bay of Plenty",
                "Organic" if organic else "Standard",
                None,
            ))
    conn.executemany("INSERT INTO dim_grower VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    log.info("dim_grower: %d rows", len(rows))


def load_dim_corridor(conn: sqlite3.Connection) -> None:
    """Populate dim_corridor with hardcoded Bay of Plenty corridor data."""
    conn.executemany("INSERT INTO dim_corridor VALUES (?,?,?,?,?,?)", CORRIDORS)
    conn.commit()
    log.info("dim_corridor: %d rows", len(CORRIDORS))


def load_dim_variety(conn: sqlite3.Connection) -> None:
    """Populate dim_variety with ZGL QM 2026 MTS thresholds."""
    conn.executemany("INSERT INTO dim_variety VALUES (?,?,?,?,?)", VARIETIES)
    conn.commit()
    log.info("dim_variety: %d rows", len(VARIETIES))


def load_dim_packhouse(conn: sqlite3.Connection) -> None:
    """Populate dim_packhouse with hardcoded packhouse PTE classifications."""
    conn.executemany("INSERT INTO dim_packhouse VALUES (?,?,?,?,?)", PACKHOUSES)
    conn.commit()
    log.info("dim_packhouse: %d rows", len(PACKHOUSES))


# ---------------------------------------------------------------------------
# Fact loader
# ---------------------------------------------------------------------------

def _build_date_lut(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {iso_date_str: date_id} for all rows in dim_date."""
    return dict(conn.execute("SELECT date, date_id FROM dim_date").fetchall())


def _build_variety_lut(conn: sqlite3.Connection) -> dict[str, int]:
    """Return {lower_variety_name: variety_id} for all rows in dim_variety."""
    return dict(
        conn.execute("SELECT LOWER(variety_name), variety_id FROM dim_variety").fetchall()
    )


def _build_grower_id_set(conn: sqlite3.Connection) -> set[int]:
    """Return the set of all grower_ids present in dim_grower."""
    return {r[0] for r in conn.execute("SELECT grower_id FROM dim_grower").fetchall()}


def _otif_status(impact: float) -> str:
    """Derive OTIF status from numeric otif_impact field."""
    if impact > 0:
        return "Late"
    if impact < 0:
        return "Cancelled"
    return "OnTime"


def load_fact_pallet_submissions(conn: sqlite3.Connection, csv_dir: Path) -> None:
    """Populate fact_pallet_submissions from synthetic_pallet_submissions.csv.

    Joins to dims via:
      - submission_date → dim_date (date_id lookup)
      - kpin            → dim_grower FK validation
      - subzone         → dim_corridor / dim_packhouse (static maps)
      - variety         → dim_variety (via VARIETY_MAP consolidation)

    Synthetic variety variants (OrganicGreen, RubyRed, SweetGreen,
    OrganicSunGold) are consolidated to their nearest dim_variety entry;
    see VARIETY_MAP for the mapping rationale.
    """
    csv_path  = csv_dir / "synthetic_pallet_submissions.csv"
    date_lut  = _build_date_lut(conn)
    var_lut   = _build_variety_lut(conn)
    grower_ids = _build_grower_id_set(conn)

    rows: list[tuple] = []
    skipped = 0

    with open(csv_path, newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            try:
                grower_id = int(raw["kpin"])
                sub_date  = raw["submission_date"].strip()
                var_key   = raw["variety"].strip().lower()

                if grower_id not in grower_ids:
                    log.warning("kpin %d not in dim_grower — row skipped", grower_id)
                    skipped += 1
                    continue

                date_id = date_lut.get(sub_date)
                if date_id is None:
                    log.warning("date '%s' not in dim_date — row skipped", sub_date)
                    skipped += 1
                    continue

                variety_name = VARIETY_MAP.get(var_key)
                if variety_name is None:
                    log.warning("unknown variety '%s' — row skipped", raw["variety"])
                    skipped += 1
                    continue
                variety_id = var_lut[variety_name.lower()]

                subzone      = raw["subzone"].strip().lower()
                corridor_id  = SUBZONE_CORRIDOR.get(subzone, 2)
                packhouse_id = SUBZONE_PACKHOUSE.get(subzone, DEFAULT_PACKHOUSE_ID)

                trays        = int(raw["trays_submitted"])
                sub_payment  = float(raw["submit_payment_nzd"])
                total_return = float(raw["total_return_nzd"])
                otif_impact  = float(raw["otif_impact"])
                mts_passed   = 1 if raw["mts_pass"].strip().lower() == "true" else 0
                pay_per_tray = round(sub_payment / trays, 4) if trays else 0.0

                rows.append((
                    grower_id,
                    corridor_id,
                    date_id,
                    variety_id,
                    packhouse_id,
                    1,
                    trays,
                    float(raw["dm_pct_avg"]),
                    pay_per_tray,
                    total_return,
                    mts_passed,
                    _otif_status(otif_impact),
                    max(0.0, otif_impact),
                    f"{sub_date} 00:00:00",
                ))

            except (KeyError, ValueError) as exc:
                log.warning("Malformed row — %s: %s", exc, raw)
                skipped += 1

    conn.executemany(
        """INSERT INTO fact_pallet_submissions
           (grower_id, corridor_id, date_id, variety_id, packhouse_id,
            pallet_count, tray_count, dry_matter_pct, payment_per_tray_nzd,
            total_value_nzd, mts_passed, otif_status, delay_hours, submitted_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    log.info("fact_pallet_submissions: %d rows loaded, %d skipped", len(rows), skipped)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(conn: sqlite3.Connection) -> None:
    """Print row counts, check FK integrity, and verify no NULLs in NOT NULL columns."""
    tables = [
        "dim_date",
        "dim_grower",
        "dim_corridor",
        "dim_variety",
        "dim_packhouse",
        "fact_pallet_submissions",
    ]

    print("\n── Row counts ─────────────────────────────────────")
    for t in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<36} {n:>8,}")

    print("\n── FK integrity ───────────────────────────────────")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        print(f"  ✗  {len(violations)} FK violation(s):")
        for v in violations:
            print(f"     {v}")
    else:
        print("  ✓  0 FK violations")

    print("\n── NOT NULL columns (fact) ────────────────────────")
    not_null_cols = [
        "grower_id", "corridor_id", "date_id", "variety_id", "packhouse_id",
        "tray_count", "dry_matter_pct", "total_value_nzd",
        "mts_passed", "otif_status", "submitted_at",
    ]
    any_nulls = False
    for col in not_null_cols:
        n = conn.execute(
            f"SELECT COUNT(*) FROM fact_pallet_submissions WHERE {col} IS NULL"
        ).fetchone()[0]
        if n:
            print(f"  ✗  {col}: {n} NULL(s)")
            any_nulls = True
    if not any_nulls:
        print("  ✓  0 NULLs in NOT NULL columns")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Orchestrate full star schema load from synthetic CSVs into SQLite."""
    log.info("=== APOPHENIA Star Schema Loader — Start ===")
    t0 = datetime.now()

    try:
        conn = create_db(DB_PATH)

        load_dim_date(conn)
        load_dim_grower(conn, CSV_DIR)
        load_dim_corridor(conn)
        load_dim_variety(conn)
        load_dim_packhouse(conn)
        load_fact_pallet_submissions(conn, CSV_DIR)

        validate(conn)
        conn.close()

    except Exception as exc:
        log.error("Load failed: %s", exc, exc_info=True)
        raise

    elapsed = (datetime.now() - t0).total_seconds()
    log.info("=== Done in %.2fs ===", elapsed)


if __name__ == "__main__":
    main()

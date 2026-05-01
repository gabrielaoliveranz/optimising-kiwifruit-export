"""
=============================================================================
OPTIMISING KIWIFRUIT EXPORT PERFORMANCE — ZGL 2026
Script: 04_load.py
Stage: ETL Phase 3 — Load Star Schema into SQLite Database
Author: Gabriela Olivera | Data Analytics Portfolio
=============================================================================

WHAT THIS SCRIPT DOES:
  Loads the 5 Star Schema CSVs into a SQLite database with:
  - Typed columns (INTEGER, REAL, TEXT, DATE)
  - Primary key constraints on every dimension
  - Foreign key constraints from Fact → all Dimensions
  - Indexes on high-frequency query columns
  - Row count and constraint validation after load

  Output: 02_data_processed/star_schema/kiwifruit_export.db

WHY SQLITE:
  SQLite is a self-contained database engine — no server needed.
  The .db file is portable, shareable, and queryable from Python,
  DB Browser for SQLite, or any SQL tool. It produces real SQL
  execution plans and query results identical to PostgreSQL/MySQL
  for analytical workloads of this size.

HOW TO RUN:
  python 03_etl_pipeline/04_load.py
  Run from project root: G:\\My Drive\\optimising-kiwifruit-export\\

SCHEMA CREATED:
  dim_time              (date_key PK)
  dim_corridor          (corridor_key PK)
  dim_fruit_quality     (fruit_key PK)
  dim_grower            (grower_key PK)
  fact_export_transactions (export_id PK, 4 FKs → all dims)
=============================================================================
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
STAR         = PROJECT_ROOT / "02_data_processed" / "star_schema"
DB_PATH      = STAR / "kiwifruit_export.db"

log_lines = []

def log(msg: str, level: str = "INFO"):
    tag  = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "FIND": "🔍"}.get(level, "•")
    line = f"  {tag} {msg}"
    print(line)
    log_lines.append(line)


# =============================================================================
# DDL — CREATE TABLE STATEMENTS
# =============================================================================

DDL = {

"dim_time": """
CREATE TABLE IF NOT EXISTS dim_time (
    date_key        INTEGER PRIMARY KEY,   -- YYYYMMDD
    date            TEXT    NOT NULL,
    iso_week        INTEGER,
    iso_year        INTEGER,
    month           INTEGER,
    year            INTEGER,
    pack_week       INTEGER,
    season_year     TEXT,                  -- e.g. '2025/26'
    season_phase    TEXT,                  -- KiwiStart | MainPack | Late
    is_pack_season  INTEGER                -- 0 | 1 boolean
)
""",

"dim_corridor": """
CREATE TABLE IF NOT EXISTS dim_corridor (
    corridor_key                INTEGER PRIMARY KEY,
    subzone                     TEXT    NOT NULL,
    highway                     TEXT,
    distance_port_km            REAL,
    congestion_index_avg        REAL,
    base_risk_weight            REAL,
    psa_incidence_historical    REAL
)
""",

"dim_fruit_quality": """
CREATE TABLE IF NOT EXISTS dim_fruit_quality (
    fruit_key           INTEGER PRIMARY KEY,
    kpin                INTEGER NOT NULL,
    season              TEXT    NOT NULL,
    subzone             TEXT,
    variety             TEXT,
    growing_method      TEXT,              -- conventional | organic
    pack_week           INTEGER,
    reading_date        TEXT,
    dm_pct              REAL    NOT NULL,
    mts_threshold       REAL    NOT NULL,
    mts_status          TEXT    NOT NULL,  -- PASS | FAIL
    tzg_score           REAL,
    tzg_grade           TEXT,              -- A+ | A | B | C | D | F
    pest_indicator      INTEGER,           -- 0 | 1
    maturity_area       TEXT,
    sample_size         INTEGER
)
""",

"dim_grower": """
CREATE TABLE IF NOT EXISTS dim_grower (
    grower_key          INTEGER PRIMARY KEY,
    kpin                INTEGER NOT NULL UNIQUE,
    subzone             TEXT,
    primary_variety     TEXT,
    organic             INTEGER,           -- 0 | 1
    orchard_ha          REAL,
    distance_port_km    REAL,
    psa_history         INTEGER,           -- 0 | 1
    base_risk_weight    REAL,
    dm_std_modifier     REAL,
    corridor_key        INTEGER,
    FOREIGN KEY (corridor_key) REFERENCES dim_corridor(corridor_key)
)
""",

"fact_export_transactions": """
CREATE TABLE IF NOT EXISTS fact_export_transactions (
    export_id               INTEGER PRIMARY KEY,
    -- Foreign keys
    date_key                INTEGER,
    corridor_key            INTEGER,
    fruit_key               INTEGER,
    grower_key              INTEGER,
    -- Context
    season                  TEXT,
    subzone                 TEXT,
    variety                 TEXT,
    pack_week               INTEGER,
    season_phase            TEXT,
    -- Volume measures
    trays_submitted         INTEGER,
    trays_exported          INTEGER,
    trays_lost              INTEGER,
    loss_pct                REAL,
    -- Quality measures
    dm_pct_avg              REAL,
    tzg_score               REAL,
    mts_pass                INTEGER,       -- 0 | 1
    -- Financial measures (NZD)
    submit_payment_nzd      REAL,
    taste_payment_nzd       REAL,
    total_return_nzd        REAL,
    freight_cost_nzd        REAL,
    margin_erosion_pct      REAL,
    -- Operational measures
    otif_pct                REAL,
    risk_score              INTEGER,
    -- Context variables (for simulator feed)
    congestion_index        REAL,
    rainfall_mm_7d          REAL,
    reg_index               REAL,
    vol_index               REAL,
    -- Foreign key constraints
    FOREIGN KEY (date_key)    REFERENCES dim_time(date_key),
    FOREIGN KEY (corridor_key) REFERENCES dim_corridor(corridor_key),
    FOREIGN KEY (fruit_key)   REFERENCES dim_fruit_quality(fruit_key),
    FOREIGN KEY (grower_key)  REFERENCES dim_grower(grower_key)
)
"""
}

# =============================================================================
# INDEXES — query performance
# =============================================================================

INDEXES = [
    # Fact table — most queried columns
    "CREATE INDEX IF NOT EXISTS idx_fact_season    ON fact_export_transactions(season)",
    "CREATE INDEX IF NOT EXISTS idx_fact_subzone   ON fact_export_transactions(subzone)",
    "CREATE INDEX IF NOT EXISTS idx_fact_variety   ON fact_export_transactions(variety)",
    "CREATE INDEX IF NOT EXISTS idx_fact_pack_week ON fact_export_transactions(pack_week)",
    "CREATE INDEX IF NOT EXISTS idx_fact_mts_pass  ON fact_export_transactions(mts_pass)",
    "CREATE INDEX IF NOT EXISTS idx_fact_risk      ON fact_export_transactions(risk_score)",
    "CREATE INDEX IF NOT EXISTS idx_fact_date      ON fact_export_transactions(date_key)",

    # Dimension table indexes
    "CREATE INDEX IF NOT EXISTS idx_dim_fq_kpin    ON dim_fruit_quality(kpin)",
    "CREATE INDEX IF NOT EXISTS idx_dim_fq_season  ON dim_fruit_quality(season)",
    "CREATE INDEX IF NOT EXISTS idx_dim_fq_dm      ON dim_fruit_quality(dm_pct)",
    "CREATE INDEX IF NOT EXISTS idx_dim_fq_mts     ON dim_fruit_quality(mts_status)",
    "CREATE INDEX IF NOT EXISTS idx_dim_gr_subzone ON dim_grower(subzone)",
    "CREATE INDEX IF NOT EXISTS idx_dim_time_pw    ON dim_time(pack_week)",
    "CREATE INDEX IF NOT EXISTS idx_dim_time_sy    ON dim_time(season_year)",
]

# =============================================================================
# COLUMN MAPPING
# CSVs may have extra or differently-named columns.
# Map CSV column names → DB column names where needed.
# =============================================================================

COLUMN_MAPS = {
    "dim_grower": {
        "distance_port_km_x": "distance_port_km",
    }
}


# =============================================================================
# LOAD FUNCTION
# =============================================================================

def load_table(conn: sqlite3.Connection, table: str, csv_path: Path):
    """
    Load a single CSV into a SQLite table.
    Steps:
    1. Read CSV
    2. Apply column renames if needed
    3. Keep only columns that exist in the DB schema
    4. Convert booleans to 0/1 integers (SQLite has no BOOLEAN type)
    5. Insert with pandas to_sql
    6. Verify row count matches
    """
    if not csv_path.exists():
        log(f"{csv_path.name} not found — skipping {table}", "ERROR")
        return 0

    df = pd.read_csv(csv_path, low_memory=False)

    # Apply column renames
    renames = COLUMN_MAPS.get(table, {})
    df = df.rename(columns=renames)

    # Convert boolean columns to int (True→1, False→0)
    bool_cols = df.select_dtypes(include="bool").columns
    for col in bool_cols:
        df[col] = df[col].astype(int)

    # Get columns defined in the DB schema for this table
    cursor = conn.execute(f"PRAGMA table_info({table})")
    db_cols = [row[1] for row in cursor.fetchall()]

    # Keep only CSV columns that exist in schema, in schema order
    valid_cols = [c for c in db_cols if c in df.columns]
    missing    = [c for c in db_cols if c not in df.columns]
    extra      = [c for c in df.columns if c not in db_cols]

    if missing:
        log(f"{table}: {len(missing)} schema columns not in CSV — "
            f"will be NULL: {missing}", "WARN")
    if extra:
        log(f"{table}: {len(extra)} CSV columns not in schema — "
            f"dropped: {extra[:5]}{'...' if len(extra)>5 else ''}", "WARN")

    df_load = df[valid_cols]

    # Load into SQLite — replace if table already exists
    conn.execute(f"DELETE FROM {table}")
    df_load.to_sql(table, conn, if_exists="append", index=False,
                   method="multi", chunksize=1000)

    # Verify
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    log(f"{table:<35} {len(df):>7,} CSV rows → {count:>7,} DB rows  ✓")
    return count


# =============================================================================
# VALIDATION QUERIES
# Run after load to confirm FK integrity and data quality
# =============================================================================

VALIDATION_QUERIES = {
    "MTS fail rate overall": """
        SELECT
            ROUND(100.0 * SUM(CASE WHEN mts_pass = 0 THEN 1 ELSE 0 END) / COUNT(*), 2)
            AS mts_fail_pct
        FROM fact_export_transactions
    """,

    "Returns by season (NZD M)": """
        SELECT
            season,
            ROUND(SUM(total_return_nzd) / 1000000.0, 2) AS return_nzd_m,
            COUNT(*) AS submissions
        FROM fact_export_transactions
        GROUP BY season
        ORDER BY season
    """,

    "Avg DM by subzone": """
        SELECT
            subzone,
            ROUND(AVG(dm_pct), 3)  AS dm_mean,
            ROUND(AVG(tzg_score), 3) AS tzg_mean,
            COUNT(*)               AS readings
        FROM dim_fruit_quality
        GROUP BY subzone
        ORDER BY dm_mean DESC
    """,

    "Risk score distribution": """
        SELECT
            CASE
                WHEN risk_score < 30  THEN 'LOW (0-29)'
                WHEN risk_score < 55  THEN 'ELEVATED (30-54)'
                WHEN risk_score < 70  THEN 'HIGH (55-69)'
                ELSE                       'CRITICAL (70+)'
            END AS risk_band,
            COUNT(*) AS submissions,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_export_transactions), 1) AS pct
        FROM fact_export_transactions
        GROUP BY risk_band
        ORDER BY MIN(risk_score)
    """,

    "FK integrity — orphan fact rows": """
        SELECT COUNT(*) AS orphan_rows
        FROM fact_export_transactions f
        LEFT JOIN dim_fruit_quality q ON f.fruit_key = q.fruit_key
        WHERE q.fruit_key IS NULL
    """,

    "Avg OTIF by season": """
        SELECT
            season,
            ROUND(AVG(otif_pct), 2)   AS avg_otif,
            ROUND(MIN(otif_pct), 2)   AS min_otif,
            COUNT(*) AS submissions
        FROM fact_export_transactions
        GROUP BY season
        ORDER BY season
    """,
}


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("  OPTIMISING KIWIFRUIT EXPORT — ETL Phase 3: Load to SQLite")
    print("  ZGL 2026 | Gabriela Olivera | Data Analytics Portfolio")
    print("=" * 70)
    print()

    # Remove existing DB to start fresh
    if DB_PATH.exists():
        DB_PATH.unlink()
        log(f"Existing database removed — fresh load")

    conn = sqlite3.connect(DB_PATH)

    # Enable foreign key enforcement
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")    # better write performance
    conn.execute("PRAGMA synchronous = NORMAL")

    # ── CREATE TABLES ──────────────────────────────────────────────
    print("\n── CREATE TABLES ────────────────────────────────────────────────")
    for table, ddl in DDL.items():
        conn.execute(ddl)
        log(f"CREATE TABLE {table}")
    conn.commit()

    # ── LOAD DATA ──────────────────────────────────────────────────
    print("\n── LOAD DATA ────────────────────────────────────────────────────")

    load_order = [
        ("dim_time",              STAR / "dim_time.csv"),
        ("dim_corridor",          STAR / "dim_corridor.csv"),
        ("dim_fruit_quality",     STAR / "dim_fruit_quality.csv"),
        ("dim_grower",            STAR / "dim_grower.csv"),
        ("fact_export_transactions", STAR / "fact_export_transactions.csv"),
    ]

    total_rows = 0
    for table, path in load_order:
        rows = load_table(conn, table, path)
        total_rows += rows

    conn.commit()

    # ── CREATE INDEXES ─────────────────────────────────────────────
    print("\n── CREATE INDEXES ───────────────────────────────────────────────")
    for idx_sql in INDEXES:
        conn.execute(idx_sql)
        idx_name = idx_sql.split("IF NOT EXISTS ")[1].split(" ON")[0]
        log(f"INDEX {idx_name}")
    conn.commit()

    # ── VALIDATION ─────────────────────────────────────────────────
    print("\n── VALIDATION QUERIES ───────────────────────────────────────────")

    for title, query in VALIDATION_QUERIES.items():
        print(f"\n  [{title}]")
        try:
            cursor = conn.execute(query)
            cols   = [d[0] for d in cursor.description]
            rows   = cursor.fetchall()
            # Print as simple table
            col_w = [max(len(c), max((len(str(r[i])) for r in rows), default=0))
                     for i, c in enumerate(cols)]
            header = "  " + "  ".join(c.ljust(col_w[i]) for i, c in enumerate(cols))
            sep    = "  " + "  ".join("-" * w for w in col_w)
            print(header)
            print(sep)
            for row in rows:
                print("  " + "  ".join(str(v).ljust(col_w[i]) for i, v in enumerate(row)))
        except Exception as e:
            log(f"Query failed: {e}", "ERROR")

    # ── DB SUMMARY ─────────────────────────────────────────────────
    print("\n── DATABASE SUMMARY ─────────────────────────────────────────────")
    db_size_kb = DB_PATH.stat().st_size / 1024
    log(f"Database: {DB_PATH.name}")
    log(f"Size: {db_size_kb:,.1f} KB ({db_size_kb/1024:.1f} MB)")
    log(f"Total rows loaded: {total_rows:,}")
    log(f"Tables: {len(DDL)}")
    log(f"Indexes: {len(INDEXES)}")

    # Table sizes
    for table in DDL:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log(f"  {table:<35} {n:>8,} rows", "FIND")

    conn.close()

    print("\n" + "=" * 70)
    print("  ETL Phase 3 COMPLETE")
    print(f"  DB ready: {DB_PATH}")
    print()
    print("  Next step: run SQL queries in 04_analysis/sql_queries/")
    print("  Or open with DB Browser for SQLite to explore interactively.")
    print("=" * 70)


if __name__ == "__main__":
    main()
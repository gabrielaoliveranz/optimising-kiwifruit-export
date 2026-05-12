-- =============================================================================
-- APOPHENIA — HORTICULTURAL EXPORT RISK INTELLIGENCE AGENT
-- Bay of Plenty Corridor · Star Schema DDL
-- Extracted from: 04_analysis/star_schema/load_star_schema.py
-- Database: 02_data_processed/star_schema/apophenia_star.db
-- =============================================================================
--
-- Tables: 5 dimensions + 1 fact
--   dim_date                 Calendar spine (2022-01-01 – 2026-12-31), pack weeks, milestones
--   dim_grower               Grower register (kpin = grower_id), subzone, certification
--   dim_corridor             BOP road corridors — Katikati, Te Puke, Pongakawa, Ōpōtiki, Tauranga
--   dim_variety              Kiwifruit varieties with ZGL QM 2026 MTS thresholds
--   dim_packhouse            Packhouse PTE mode and score
--   fact_pallet_submissions  Pallet-level submissions: DM%, payment, MTS pass/fail, OTIF status
-- =============================================================================

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

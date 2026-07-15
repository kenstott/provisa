-- Materialized Views DDL (Phase P)
-- Extracted from provisa/core/schema.sql for MV-specific documentation.

CREATE TABLE IF NOT EXISTS materialized_views (
    id              TEXT PRIMARY KEY,
    source_tables   TEXT[] NOT NULL,
    target_catalog  TEXT NOT NULL,
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    refresh_interval INTEGER NOT NULL DEFAULT 300,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    join_pattern    JSONB,          -- {left_table, left_column, right_table, right_column, join_type}
    custom_sql      TEXT,           -- custom SELECT for the MV
    expose_in_sdl   BOOLEAN NOT NULL DEFAULT FALSE,
    sdl_config      JSONB,          -- {domain_id, governance, columns}
    status          TEXT NOT NULL DEFAULT 'stale'
                    CHECK (status IN ('fresh', 'stale', 'refreshing', 'disabled')),
    last_refresh_at TIMESTAMPTZ,
    row_count       INTEGER,
    last_error      TEXT,
    -- REQ-879: authoritative SHARED refresh-coordination state for a load-balanced fleet.
    -- writer owns the in-flight refresh; lease_until is when its claim expires (crash reclaim).
    -- The version stamps are the REQ-862 dedup key for the atomic claim (skip when already current).
    writer          TEXT,
    lease_until     TIMESTAMPTZ,
    materialized_definition_version TEXT,
    materialized_input_version      TEXT,
    snapshot_id     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mv_refresh_log (
    id          SERIAL PRIMARY KEY,
    mv_id       TEXT NOT NULL REFERENCES materialized_views(id) ON DELETE CASCADE,
    status      TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    row_count   INTEGER,
    duration_ms INTEGER,
    error       TEXT,
    definition_version TEXT,
    input_version      TEXT,
    input_version_kind TEXT,
    trace_id           TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REQ-877: opt-in per-MV ROW-LEVEL delta ledger — append-only row-level companion to mv_refresh_log.
-- One event per changed key per refresh version: change_type, row_key, row hashes, and row VALUES
-- (value-delta tier enabling REQ-878 full-content point-in-time reconstruction, both fold directions).
CREATE TABLE IF NOT EXISTS mv_delta_ledger (
    id                 SERIAL PRIMARY KEY,
    mv_id              TEXT NOT NULL REFERENCES materialized_views(id) ON DELETE CASCADE,
    refresh_version    INTEGER NOT NULL,
    definition_version TEXT,
    trace_id           TEXT,
    change_type        TEXT NOT NULL CHECK (change_type IN ('insert', 'update', 'delete')),
    row_key            TEXT NOT NULL,
    old_hash           TEXT,
    new_hash           TEXT,
    old_values         JSONB,
    new_values         JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

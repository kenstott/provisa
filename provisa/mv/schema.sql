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
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS mv_refresh_log (
    id          SERIAL PRIMARY KEY,
    mv_id       TEXT NOT NULL REFERENCES materialized_views(id) ON DELETE CASCADE,
    status      TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    row_count   INTEGER,
    duration_ms INTEGER,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

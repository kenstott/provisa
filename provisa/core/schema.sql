-- Provisa config DB schema. V1: no migrations, this file is source of truth.

CREATE TABLE IF NOT EXISTS sources (
    id            TEXT PRIMARY KEY,
    type          TEXT NOT NULL,
    host          TEXT NOT NULL DEFAULT '',
    port          INTEGER NOT NULL DEFAULT 0,
    database      TEXT NOT NULL DEFAULT '',
    username      TEXT NOT NULL DEFAULT '',
    dialect       TEXT NOT NULL DEFAULT '',
    cache_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    cache_ttl     INTEGER,
    naming_convention TEXT,
    path          TEXT  -- file path or URL for file-based sources (csv, parquet, sqlite)
    -- password never stored; resolved at runtime via secrets provider
);

CREATE TABLE IF NOT EXISTS domains (
    id          TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

-- Seed default (no-domain) row so domain_id='' is always a valid FK target
INSERT INTO domains (id, description) VALUES ('', 'No domain')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS naming_rules (
    id          SERIAL PRIMARY KEY,
    pattern     TEXT NOT NULL,
    replacement TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS registered_tables (
    id          SERIAL PRIMARY KEY,
    source_id   TEXT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    domain_id   TEXT NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    schema_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    governance  TEXT NOT NULL CHECK (governance IN ('pre-approved', 'registry-required')),
    alias       TEXT,
    description TEXT,
    cache_ttl   INTEGER,
    naming_convention TEXT,
    UNIQUE (source_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS table_columns (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    visible_to  TEXT[] NOT NULL DEFAULT '{}',
    alias       TEXT,
    description TEXT,
    path        TEXT,
    data_type   TEXT,
    writable_by  TEXT[] NOT NULL DEFAULT '{}',
    unmasked_to  TEXT[] NOT NULL DEFAULT '{}',
    mask_type    TEXT CHECK (mask_type IN ('regex', 'constant', 'truncate')),
    mask_pattern TEXT,
    mask_replace TEXT,
    mask_value   TEXT,
    mask_precision TEXT,
    UNIQUE (table_id, column_name)
);

-- Migration: add alias/description/writable_by columns if missing
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS writable_by TEXT[] NOT NULL DEFAULT '{}';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS unmasked_to TEXT[] NOT NULL DEFAULT '{}';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_pattern TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_replace TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_value TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_precision TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS native_filter_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS data_type TEXT;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_enabled BOOLEAN NOT NULL DEFAULT TRUE;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS watermark_column TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS visible_to TEXT[] NOT NULL DEFAULT '{}';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS relationships (
    id               TEXT PRIMARY KEY,
    source_table_id  INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    target_table_id  INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    source_column    TEXT NOT NULL,
    target_column    TEXT NOT NULL,
    cardinality      TEXT NOT NULL CHECK (cardinality IN ('many-to-one', 'one-to-many')),
    materialize      BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_interval INTEGER NOT NULL DEFAULT 300
);

-- Migration: add materialize columns if missing
DO $$ BEGIN
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS materialize BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS refresh_interval INTEGER NOT NULL DEFAULT 300;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS target_function_name TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS function_arg TEXT;
    ALTER TABLE relationships ALTER COLUMN target_table_id DROP NOT NULL;
    ALTER TABLE relationships ALTER COLUMN target_column DROP NOT NULL;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS roles (
    id              TEXT PRIMARY KEY,
    capabilities    TEXT[] NOT NULL DEFAULT '{}',
    domain_access   TEXT[] NOT NULL DEFAULT '{}',
    parent_role_id  TEXT REFERENCES roles(id)
);

-- Migration: add parent_role_id if missing
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS parent_role_id TEXT REFERENCES roles(id);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS rls_rules (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    filter_expr TEXT NOT NULL,
    UNIQUE (table_id, role_id)
);

-- Materialized Views (Phase P)
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

-- Column-Level Masking (Phase Q) — masking rules are inline on table_columns.

-- Persisted Query Registry (Phase H)
CREATE TABLE IF NOT EXISTS persisted_queries (
    id              SERIAL PRIMARY KEY,
    stable_id       TEXT UNIQUE,        -- assigned on approval (REQ-023)
    query_text      TEXT NOT NULL,
    compiled_sql    TEXT NOT NULL,
    target_tables   INTEGER[] NOT NULL, -- registered_tables IDs
    parameter_schema JSONB,             -- JSON Schema for variables
    permitted_outputs TEXT[] NOT NULL DEFAULT '{json}',
    developer_id    TEXT NOT NULL,       -- who submitted (REQ-022)
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'approved', 'deprecated', 'flagged')),
    approved_by     TEXT,               -- who approved (REQ-024)
    approved_at     TIMESTAMPTZ,
    routing_hint    TEXT,               -- steward override: 'direct' or 'trino'
    cache_ttl       INTEGER,            -- steward-specified cache TTL in seconds (Phase O)
    model_version   INTEGER NOT NULL DEFAULT 1,  -- registration model version
    deprecated_by   TEXT,               -- replacement stable_id (REQ-026)
    sink_topic      TEXT,               -- Kafka sink topic (REQ-176)
    sink_trigger    TEXT CHECK (sink_trigger IN ('change_event', 'schedule', 'manual')),
    sink_key_column TEXT,               -- message key column
    -- Submission metadata
    business_purpose TEXT,              -- why this query is needed
    use_cases       TEXT,               -- expected consumers/dashboards/reports
    data_sensitivity TEXT CHECK (data_sensitivity IN ('public', 'internal', 'confidential', 'restricted')),
    refresh_frequency TEXT,             -- how often results are needed (e.g., "real-time", "hourly", "daily")
    expected_row_count TEXT,            -- estimated result size (e.g., "<1K", "1K-100K", "100K+")
    owner_team      TEXT,               -- team responsible for this query
    expiry_date     DATE,               -- optional: when this query should be reviewed/retired
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Migration: add sink and metadata columns
DO $$ BEGIN
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS sink_topic TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS sink_trigger TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS sink_key_column TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS business_purpose TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS use_cases TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS data_sensitivity TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS refresh_frequency TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS expected_row_count TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS owner_team TEXT;
    ALTER TABLE persisted_queries ADD COLUMN IF NOT EXISTS expiry_date DATE;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS approval_log (
    id              SERIAL PRIMARY KEY,
    query_id        INTEGER NOT NULL REFERENCES persisted_queries(id) ON DELETE CASCADE,
    action          TEXT NOT NULL CHECK (action IN ('submitted', 'approved', 'rejected', 'deprecated', 'flagged')),
    actor_id        TEXT NOT NULL,
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Relationship Discovery Candidates (Phase R)
CREATE TABLE IF NOT EXISTS relationship_candidates (
    id              SERIAL PRIMARY KEY,
    source_table_id INTEGER NOT NULL REFERENCES registered_tables(id),
    target_table_id INTEGER NOT NULL REFERENCES registered_tables(id),
    source_column   TEXT NOT NULL,
    target_column   TEXT NOT NULL,
    cardinality     TEXT NOT NULL,
    confidence      REAL NOT NULL,
    reasoning       TEXT,
    suggested_name  TEXT,
    status          TEXT NOT NULL DEFAULT 'suggested'
                    CHECK (status IN ('suggested', 'accepted', 'rejected', 'expired')),
    scope           TEXT NOT NULL,
    rejection_reason TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_table_id, source_column, target_table_id, target_column)
);

-- Kafka Sources (Phase V)
CREATE TABLE IF NOT EXISTS kafka_sources (
    id                  TEXT PRIMARY KEY,
    bootstrap_servers   TEXT NOT NULL,
    schema_registry_url TEXT,
    auth_type           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kafka_topics (
    id              SERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES kafka_sources(id) ON DELETE CASCADE,
    topic           TEXT NOT NULL,
    table_name      TEXT NOT NULL UNIQUE,
    schema_source   TEXT NOT NULL DEFAULT 'registry'
                    CHECK (schema_source IN ('registry', 'manual', 'sample')),
    value_format    TEXT NOT NULL DEFAULT 'json'
                    CHECK (value_format IN ('json', 'avro', 'protobuf')),
    columns         JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, topic)
);

CREATE TABLE IF NOT EXISTS kafka_sinks (
    id              SERIAL PRIMARY KEY,
    query_stable_id TEXT NOT NULL,
    topic           TEXT NOT NULL,
    key_column      TEXT,
    value_format    TEXT NOT NULL DEFAULT 'json',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (query_stable_id)
);

-- API Sources (Phase U)
CREATE TABLE IF NOT EXISTS api_sources (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL CHECK (type IN ('openapi', 'graphql_api', 'grpc_api')),
    base_url    TEXT NOT NULL,
    spec_url    TEXT,
    auth        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_endpoints (
    id              SERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES api_sources(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    method          TEXT NOT NULL DEFAULT 'GET',
    table_name      TEXT NOT NULL UNIQUE,
    columns         JSONB NOT NULL,
    ttl             INTEGER NOT NULL DEFAULT 300,
    response_root   TEXT,
    pagination      JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_endpoint_candidates (
    id              SERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES api_sources(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    method          TEXT NOT NULL DEFAULT 'GET',
    table_name      TEXT,
    columns         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'discovered'
                    CHECK (status IN ('discovered', 'registered', 'rejected')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, path, method)
);

-- Live Query Engine watermark state (Phase AM)
CREATE TABLE IF NOT EXISTS live_query_state (
    query_id    TEXT PRIMARY KEY,       -- stable_id of the approved persisted query
    watermark   TEXT,                   -- last-seen watermark column value (serialized as text)
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tracked DB functions exposed as GraphQL mutations (REQ-205)
CREATE TABLE IF NOT EXISTS tracked_functions (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    source_id     TEXT NOT NULL DEFAULT '',
    schema_name   TEXT NOT NULL DEFAULT 'public',
    function_name TEXT NOT NULL DEFAULT '',
    returns       TEXT NOT NULL DEFAULT '',
    arguments     JSONB NOT NULL DEFAULT '[]',
    visible_to    TEXT[] NOT NULL DEFAULT '{}',
    writable_by   TEXT[] NOT NULL DEFAULT '{}',
    domain_id     TEXT NOT NULL DEFAULT '',
    description   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tracked webhooks exposed as GraphQL mutations (REQ-211)
CREATE TABLE IF NOT EXISTS tracked_webhooks (
    id                 SERIAL PRIMARY KEY,
    name               TEXT NOT NULL UNIQUE,
    url                TEXT NOT NULL DEFAULT '',
    method             TEXT NOT NULL DEFAULT 'POST',
    timeout_ms         INTEGER NOT NULL DEFAULT 5000,
    returns            TEXT,
    inline_return_type JSONB NOT NULL DEFAULT '[]',
    arguments          JSONB NOT NULL DEFAULT '[]',
    visible_to         TEXT[] NOT NULL DEFAULT '{}',
    domain_id          TEXT NOT NULL DEFAULT '',
    description        TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Migration: add kind column to tracked_functions and tracked_webhooks
DO $$ BEGIN
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'mutation';
    ALTER TABLE tracked_webhooks ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'mutation';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Migration: add return_schema to tracked_functions (custom shape for non-table returns)
DO $$ BEGIN
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS return_schema JSONB;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- No auth tables needed — auth is config-driven, not DB-driven

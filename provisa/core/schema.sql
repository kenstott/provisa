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
    prefer_materialized BOOLEAN NOT NULL DEFAULT FALSE,  -- force MATERIALIZED federation for this source's tables
    change_signal TEXT NOT NULL DEFAULT 'ttl',  -- REQ-929: source default change signal (ttl|probe|ttl_probe|native|debezium|kafka)
    gql_naming_convention TEXT,
    path          TEXT  -- file path or URL for file-based sources (csv, parquet, sqlite)
    -- password never stored; resolved at runtime via secrets provider
);

CREATE TABLE IF NOT EXISTS domains (
    id            TEXT PRIMARY KEY,
    description   TEXT NOT NULL DEFAULT '',
    graphql_alias TEXT
);
ALTER TABLE domains ADD COLUMN IF NOT EXISTS graphql_alias TEXT;

-- Seed default (no-domain) row so domain_id='' is always a valid FK target
INSERT INTO domains (id, description) VALUES ('', 'No domain')
ON CONFLICT (id) DO NOTHING;

-- Seed built-in system metadata domain
INSERT INTO domains (id, description) VALUES ('meta', 'System metadata')
ON CONFLICT (id) DO NOTHING;

-- Seed built-in operational telemetry domain
INSERT INTO domains (id, description) VALUES ('ops', 'Operational telemetry')
ON CONFLICT (id) DO NOTHING;


-- Seed demo shelter domain
INSERT INTO domains (id, description) VALUES ('shelter', 'Animal shelter staff and breed management')
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
    governance  TEXT NOT NULL DEFAULT 'pre-approved',
    alias       TEXT,
    description TEXT,
    cache_ttl   INTEGER,
    prefer_materialized BOOLEAN,  -- NULL = inherit source; overrides federation strategy to MATERIALIZED
    gql_naming_convention TEXT,
    watermark_column TEXT,
    change_signal TEXT,  -- REQ-929: override source change signal; NULL = inherit
    probe_query TEXT,    -- REQ-929: source-native freshness probe for change_signal in {probe, ttl_probe}
    probe_type TEXT,     -- REQ-982: input-probe method; NULL = resolve per source class
    mv_debounce_quiet     DOUBLE PRECISION NOT NULL DEFAULT 0,  -- MV NRT debounce quiet window (s)
    mv_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5,  -- MV NRT debounce max delay (s)
    mv_consistency TEXT NOT NULL DEFAULT 'shared',  -- REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    UNIQUE (source_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS table_columns (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    visible_to  JSONB NOT NULL DEFAULT '[]',
    alias       TEXT,
    description TEXT,
    path        TEXT,
    data_type   TEXT,
    writable_by  JSONB NOT NULL DEFAULT '[]',
    unmasked_to  JSONB NOT NULL DEFAULT '[]',
    mask_type    TEXT CHECK (mask_type IN ('regex', 'constant', 'truncate')),
    mask_pattern TEXT,
    mask_replace TEXT,
    mask_value   TEXT,
    mask_precision TEXT,
    is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (table_id, column_name)
);

-- Migration: add alias/description/writable_by columns if missing
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS writable_by JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS unmasked_to JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_pattern TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_replace TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_value TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS mask_precision TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS native_filter_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS data_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_primary_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_foreign_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_alternate_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS object_fields JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'domain';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_enabled BOOLEAN NOT NULL DEFAULT TRUE;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS prefer_materialized BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS allowed_domains JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS mapping JSONB NOT NULL DEFAULT '{}';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cdc JSONB;  -- REQ-824: source-level CDC transport
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS change_signal TEXT NOT NULL DEFAULT 'ttl';  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS prefer_materialized BOOLEAN;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS watermark_column TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS change_signal TEXT;  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS probe_query TEXT;  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS probe_type TEXT;  -- REQ-982
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS column_presets JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS view_sql TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS data_product BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS materialize BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_refresh_interval INTEGER NOT NULL DEFAULT 300;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_debounce_quiet DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_consistency TEXT NOT NULL DEFAULT 'shared';  -- REQ-879
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS enable_aggregates BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS enable_group_by BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS live JSONB;
    ALTER TABLE registered_tables DROP CONSTRAINT IF EXISTS registered_tables_governance_check;
    ALTER TABLE registered_tables ALTER COLUMN governance SET DEFAULT 'pre-approved';
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
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS graphql_alias TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS disable_cypher BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS source_json_key TEXT;
    -- REQ-020: ownership, versioning, and re-review flag for join-field schema changes.
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS owner TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS needs_review BOOLEAN NOT NULL DEFAULT FALSE;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Uniqueness constraint: one alias per (source_table, alias) pair
DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_source_alias_unique
        UNIQUE (source_table_id, alias);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS roles (
    id              TEXT PRIMARY KEY,
    capabilities    JSONB NOT NULL DEFAULT '[]',
    domain_access   JSONB NOT NULL DEFAULT '[]',
    parent_role_id  TEXT REFERENCES roles(id)
);

-- Migration: add parent_role_id if missing
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS parent_role_id TEXT REFERENCES roles(id);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS rls_rules (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER REFERENCES registered_tables(id) ON DELETE CASCADE,
    domain_id   TEXT REFERENCES domains(id) ON DELETE CASCADE,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    filter_expr BYTEA NOT NULL,  -- REQ-686: encrypted at rest via EncryptionService
    UNIQUE (table_id, role_id)
);

-- Migration: add domain_id and make table_id nullable for domain-level RLS rules
DO $$ BEGIN
    ALTER TABLE rls_rules ADD COLUMN IF NOT EXISTS domain_id TEXT REFERENCES domains(id) ON DELETE CASCADE;
    ALTER TABLE rls_rules ALTER COLUMN table_id DROP NOT NULL;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_domain_role_key UNIQUE (domain_id, role_id);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Materialized Views (Phase P)
CREATE TABLE IF NOT EXISTS materialized_views (
    id              TEXT PRIMARY KEY,
    source_tables   JSONB NOT NULL,
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
    -- REQ-961/962: temporal-processing declaration (calendar-bounded windows + freshness contract).
    calendar          TEXT,
    grain             TEXT,
    allowed_lateness  INTEGER NOT NULL DEFAULT 0,
    expected_events   JSONB,          -- freshness-contract inputs; NULL = all SQL-lineage inputs
    business_day_grain BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REQ-962: named, shared, VERSIONED calendars — the temporal-window boundary source. Holiday/
-- business-day set is captured per version so a replay reproduces the same window existence.
CREATE TABLE IF NOT EXISTS calendars (
    name                TEXT NOT NULL,
    version             TEXT NOT NULL,
    base_system         TEXT NOT NULL DEFAULT 'gregorian'
                        CHECK (base_system IN ('gregorian', 'fiscal', 'retail_445')),
    tz                  TEXT NOT NULL DEFAULT 'UTC',
    fiscal_anchor_month INTEGER NOT NULL DEFAULT 1,
    fiscal_anchor_day   INTEGER NOT NULL DEFAULT 1,
    retail_anchor       DATE,
    week_start          INTEGER NOT NULL DEFAULT 0,
    holidays            JSONB NOT NULL DEFAULT '[]',
    weekend             JSONB NOT NULL DEFAULT '[5, 6]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version)
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

-- Column-Level Masking (Phase Q) — masking rules are inline on table_columns.

-- Approved-query / GPQ registry removed (REQ-001/003) — access is governed solely by
-- table/view + relationship rights. The persisted_queries and approval_log tables are
-- deprecated and no longer created. (Apollo APQ — REQ-288-291 — is separate, lives in
-- Redis, and is unaffected.)

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
    auth        BYTEA,  -- REQ-686: API auth (keys/tokens) encrypted at rest
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
    error_path      TEXT,
    pk_column       TEXT,
    pagination      JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$ BEGIN
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS response_root TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS error_path TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS pk_column TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS pagination JSONB;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS max_concurrency INTEGER;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS default_params JSONB;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS promotions JSONB NOT NULL DEFAULT '[]';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-434/063: creation-request queue. A governed create (view, relationship)
-- attempted by a user lacking the authority becomes a persisted request that a
-- rights-holder executes or rejects (with an actionable reason).
CREATE TABLE IF NOT EXISTS creation_requests (
    id               SERIAL PRIMARY KEY,
    request_type     TEXT NOT NULL,        -- 'view' | 'relationship' | 'webhook_registration'
    capability       TEXT NOT NULL,        -- capability required to execute
    payload          JSONB NOT NULL,       -- original create input
    requested_by     TEXT,                 -- user_id of the requester
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'executed', 'rejected')),
    rejection_reason TEXT,
    resolved_by      TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ
);
ALTER TABLE creation_requests ADD COLUMN IF NOT EXISTS approvals JSONB NOT NULL DEFAULT '[]';
ALTER TABLE creation_requests ADD COLUMN IF NOT EXISTS required_approvals INT NOT NULL DEFAULT 1;

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

-- Live Query Engine watermark state (Phase AM, Phase AY)
CREATE TABLE IF NOT EXISTS live_query_state (
    source          TEXT NOT NULL,
    output_type     TEXT NOT NULL,
    last_watermark  TEXT,
    last_polled_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT NOT NULL DEFAULT 'active',
    PRIMARY KEY (source, output_type)
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
    visible_to    JSONB NOT NULL DEFAULT '[]',
    writable_by   JSONB NOT NULL DEFAULT '[]',
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
    visible_to         JSONB NOT NULL DEFAULT '[]',
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

-- REQ-885: implementation-kind dimension + swappable binding (transport+location),
-- decoupled from addressing (name/function_name). materialize selects DEFINER vs INVOKER.
DO $$ BEGIN
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS impl_kind TEXT NOT NULL DEFAULT 'source_procedure';
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS binding JSONB NOT NULL DEFAULT '{}';
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS materialize BOOLEAN NOT NULL DEFAULT FALSE;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Bridge table: every registered user table → its meta:registered_tables row
-- Populated automatically on register_table; enables REGISTERED_AS Cypher edges
CREATE TABLE IF NOT EXISTS table_meta_links (
    source_table_id INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    target_table_id INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    PRIMARY KEY (source_table_id)
);

-- File source staleness tracking — mtime per registered SQLite/CSV/Parquet table
CREATE TABLE IF NOT EXISTS file_source_mtimes (
    table_id     INTEGER PRIMARY KEY REFERENCES registered_tables(id) ON DELETE CASCADE,
    source_mtime DOUBLE PRECISION NOT NULL,
    synced_at    DOUBLE PRECISION NOT NULL
);

-- Orgs, users, memberships, invites are the PLATFORM control plane's global
-- registry (see provisa/core/schema_admin.py). They are NOT created per-org
-- here; they live in a separate control-plane database (init_registry_schema).
-- The org_id columns below therefore hold a plain org identifier — no FK to
-- orgs, which lives in a different physical database.

-- Add org_id to domains (nullable; existing rows stamped to 'root')
DO $$ BEGIN
    ALTER TABLE domains ADD COLUMN IF NOT EXISTS org_id TEXT;
    UPDATE domains SET org_id = 'root' WHERE org_id IS NULL;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Add org_id to roles (nullable = system role: admin, superadmin)
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS org_id TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- User role:domain assignment pairs.
-- domain_id = '*' means the user has this role across all domains.
-- For external IdP users, user_id matches the IdP subject claim.
CREATE TABLE IF NOT EXISTS user_role_assignments (
    id          SERIAL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    domain_id   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, role_id, domain_id)
);

-- tenant_id isolation for _META_TABLES (SaaS multi-tenancy)
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE domains ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE rls_rules ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS tenant_id UUID;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Org invite tokens live in the platform control plane (schema_admin.org_invites),
-- not per-org here.

-- Louvain cluster assignments as computed attributes on registered_tables
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS l1_cluster INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS l2_cluster INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS l3_cluster INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS clusters_computed_at TIMESTAMPTZ;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Stable integer node identity registry.
-- Each unique graph node (label + primary key) gets a BIGSERIAL id on first sight.
-- Properties are merged on update so the registry always reflects the latest attributes.
CREATE TABLE IF NOT EXISTS node_ids (
    id           BIGSERIAL PRIMARY KEY,
    composite_id TEXT UNIQUE NOT NULL,   -- "Label|pkValue" — internal lookup key
    label        TEXT NOT NULL,
    properties   JSONB NOT NULL DEFAULT '{}'
);

-- Each unique graph relationship gets a BIGSERIAL id on first sight, mirroring node_ids.
-- composite_id is the edge identity string, e.g. "SUBMITTED_BY:1-1".
CREATE TABLE IF NOT EXISTS rel_ids (
    id           BIGSERIAL PRIMARY KEY,
    composite_id TEXT UNIQUE NOT NULL,   -- edge identity "Type:startPk-endPk"
    rel_type     TEXT NOT NULL,
    properties   JSONB NOT NULL DEFAULT '{}'
);

-- Per-node freshness state (REQ-981/982): content hash of the last land (output gate)
-- and the last probe token (input probe baseline). One row per node; upserted on each
-- successful land. Mirrors provisa.core.schema_org.node_freshness_state.
CREATE TABLE IF NOT EXISTS node_freshness_state (
    node         TEXT PRIMARY KEY,       -- the source-table / MV node key
    content_hash TEXT,                   -- REQ-981: hash of the last landed replace-shaped content
    probe_token  TEXT,                   -- REQ-982: last probe token (watermark/hash/count baseline)
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);


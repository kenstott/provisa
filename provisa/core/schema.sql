-- Provisa config DB schema. V1: no migrations, this file is source of truth.

-- REQ-1921/1922: the stores an org keeps its data in, per region (a store is a connection URL,
-- a secret reference), and the platform regions the org selects with the store for each kind of
-- data there.
CREATE TABLE IF NOT EXISTS stores (
    id      TEXT PRIMARY KEY,
    url     TEXT NOT NULL,
    kind    TEXT,                           -- REQ-1922: the engine kind, on a region's engine store
    binding TEXT NOT NULL DEFAULT 'own' CHECK (binding IN ('own', 'copied', 'unbound'))  -- REQ-1491, REQ-1942
);

CREATE TABLE IF NOT EXISTS org_regions (
    id        TEXT PRIMARY KEY,
    engine    TEXT NOT NULL,
    replicas  TEXT NOT NULL,
    views     TEXT NOT NULL,
    cache     TEXT NOT NULL,
    state     TEXT NOT NULL,
    record    TEXT NOT NULL
);

-- REQ-1919: a configuration file seeds this model store once. The row is written when the seed
-- is applied (at the first start into an empty store, or by an admin's explicit apply); while it
-- exists, a restart, redeploy or reload never applies the file again.
CREATE TABLE IF NOT EXISTS model_seed (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    seeded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
    replicate   INTEGER,  -- REQ-826: when this source's tables are served from replicas; NULL = global threshold, -1 never, N hot, 0 always
    region      TEXT,     -- REQ-1921: the region the admin form starts its new tables in
    load_protected BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-1141: scheduled-refresh-only; query path never pulls the source
    off_peak_window TEXT,  -- REQ-1141: "HH:MM-HH:MM" maintenance window for the scheduler; NULL = no window gate
    off_peak_tz   TEXT NOT NULL DEFAULT 'UTC',  -- REQ-1141: IANA zone the off_peak_window is evaluated in
    max_live_concurrency INTEGER,  -- REQ-1909: cap on concurrent LIVE reads of this source; NULL = no cap
    sentinel_path TEXT,  -- REQ-1148: freshness marker URL (file/ftp/sftp/http(s))
    download_from TEXT,  -- REQ-2113: where a CSV or Parquet source's file is downloaded from (mount path, https, s3)
    freshness_gate BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-860: gate reads on the source's freshness decision
    change_signal TEXT NOT NULL DEFAULT 'ttl',  -- REQ-929/1149: source default change signal (ttl|probe|ttl_probe|native|debezium|kafka|signal)
    gql_naming_convention TEXT,
    federation_hints JSONB NOT NULL DEFAULT '{}',  -- connection extras the typed columns can't carry
    path          TEXT,  -- file path or URL for file-based sources (csv, parquet, sqlite)
    -- REQ-1919: the rest of a source's settings. After the seed the model store alone owns the
    -- model, so everything a configuration can say about a source is held here.
    base_url        TEXT,  -- OpenAPI sources
    pool_min        INTEGER NOT NULL DEFAULT 1,
    pool_max        INTEGER NOT NULL DEFAULT 5,
    use_pgbouncer   BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-053
    pgbouncer_port  INTEGER NOT NULL DEFAULT 6432,  -- REQ-053
    producer_command JSONB,  -- REQ-861
    cache_catalog   TEXT,
    cache_schema    TEXT NOT NULL DEFAULT 'api_cache',
    approval_hook   BOOLEAN NOT NULL DEFAULT FALSE  -- REQ-204/247
    -- the password itself is never stored; password_ref below carries the reference that names it
);

-- REQ-1695: the source's password as a SECRET REFERENCE. A literal typed into the Sources form is
-- put into the org vault and what lands here is the ${secret:NAME} naming it; a reference the
-- operator wrote themselves is stored verbatim. Empty means the source needs no password.
ALTER TABLE sources ADD COLUMN IF NOT EXISTS password_ref TEXT NOT NULL DEFAULT '';

-- REQ-1491, REQ-1942: how this environment reaches -- a connection given in it (own), one copied from its parent
-- (copied), or nothing (unbound) -- the source's connection values. A copy between environments carries the
-- row and never the binding, and an empty value is not an absent one, so an unbound source is
-- MARKED rather than blanked and the query path refuses to dial whatever is local to the node.
-- ``synthetic``: bound to the environment's synthetic store by generating its model -- type and
-- connection; the ``synthetic`` column names the store schema, the tables generated there and the
-- type the model gives the source ({schema, tables, model_type}).
ALTER TABLE sources ADD COLUMN IF NOT EXISTS binding TEXT NOT NULL DEFAULT 'own' CHECK (binding IN ('own', 'copied', 'unbound', 'synthetic'));
-- REQ-1942: a sources table that predates ``synthetic`` kept the three-binding check (ADD COLUMN IF
-- NOT EXISTS leaves an existing column's constraint as it was), and generating a model refused it.
DO $$ BEGIN
    ALTER TABLE sources DROP CONSTRAINT IF EXISTS sources_binding_check;
    ALTER TABLE sources ADD CONSTRAINT sources_binding_check
        CHECK (binding IN ('own', 'copied', 'unbound', 'synthetic'));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS synthetic JSONB;

CREATE TABLE IF NOT EXISTS domains (
    id            TEXT PRIMARY KEY,
    description   TEXT NOT NULL DEFAULT '',
    steward       TEXT,
    graphql_alias TEXT,
    -- REQ-1363: the domain this one sits under; NULL = no parent. No foreign key (see
    -- schema_org.domains): the model store holds the tree.
    parent_domain_id TEXT
);
ALTER TABLE domains ADD COLUMN IF NOT EXISTS graphql_alias TEXT;
-- REQ-609: designated steward; NULL = pending (the domain may not serve governed data).
ALTER TABLE domains ADD COLUMN IF NOT EXISTS steward TEXT;

-- Seed default (no-domain) row so domain_id='' is always a valid FK target
INSERT INTO domains (id, description) VALUES ('', 'No domain')
ON CONFLICT (id) DO NOTHING;

-- Seed built-in system metadata domain
INSERT INTO domains (id, description) VALUES ('meta', 'System metadata')
ON CONFLICT (id) DO NOTHING;

-- Seed built-in operational telemetry domain (REQ-1386: org_admin is its steward)
INSERT INTO domains (id, description, steward) VALUES ('ops', 'Operational telemetry', 'org_admin')
ON CONFLICT (id) DO NOTHING;


-- REQ-1634: a data product groups tables within a single domain for governed publication.
-- REQ-1660: fields aligned to the ODPS (Open Data Product Standard) metamodel where Provisa
-- already has the underlying source of truth -- team_role/owner_role resolve to individuals
-- via the existing role machinery (OwnerResolutionInline), tags/authoritativeDefinitions/
-- inputPorts/outputPorts are derived from existing tag assignments/glossary/lineage rather
-- than stored here, and sla/support stay prose because a product spans multiple tables and a
-- structured SLA can't unambiguously attribute which member it describes.
CREATE TABLE IF NOT EXISTS data_products (
    id            TEXT PRIMARY KEY,
    domain_id     TEXT NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    owner_role    TEXT,
    team_role     TEXT,
    purpose       TEXT NOT NULL DEFAULT '',
    limitations   TEXT NOT NULL DEFAULT '',
    usage         TEXT NOT NULL DEFAULT '',
    version       TEXT,
    status        TEXT,
    sla           TEXT,
    support       TEXT,
    support_contact TEXT,
    publish       BOOLEAN NOT NULL DEFAULT FALSE,
    custom_properties JSONB NOT NULL DEFAULT '{}'
);

-- REQ-1660: added to a table that predates them.
DO $$ BEGIN
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS team_role TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT '';
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS limitations TEXT NOT NULL DEFAULT '';
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS usage TEXT NOT NULL DEFAULT '';
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS version TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS status TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS sla TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS support TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS support_contact TEXT;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS publish BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE data_products ADD COLUMN IF NOT EXISTS custom_properties JSONB NOT NULL DEFAULT '{}';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

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
    alias       TEXT,
    description TEXT,
    cache_ttl   INTEGER,
    role_ttl    JSONB NOT NULL DEFAULT '{}',  -- REQ-1907: role -> TTL seconds; effective = max(cache_ttl, role_ttl)
    replicate   INTEGER,  -- REQ-826: NULL = inherit source; -1 never, N > 0 once it passes N statements per interval, 0 always
    region      TEXT,     -- REQ-1921: the org region its data lives in; NULL = no region
    -- REQ-1921: out of service while set — read and written nowhere, offered in no schema,
    -- copied nowhere; a table or view registered through the admin starts so.
    draft       BOOLEAN NOT NULL DEFAULT FALSE,
    load_protected BOOLEAN,  -- REQ-1141: NULL = inherit source; overrides scheduled-refresh-only load protection
    off_peak_window TEXT,    -- REQ-1141: per-table "HH:MM-HH:MM" window override; NULL = inherit source
    off_peak_tz TEXT,        -- REQ-1141: per-table window zone override; NULL = inherit source
    pagination  JSONB,       -- REQ-318: how the table is read page by page; NULL = not paged here
    arguments   JSONB,       -- REQ-2061: its source's parameters fixed for every read; NULL = none
    landing_filter TEXT,     -- REQ-2062: predicate over its own columns, applied when its replica lands
    gql_naming_convention TEXT,
    watermark_column TEXT,
    change_signal TEXT,  -- REQ-929: override source change signal; NULL = inherit
    probe_query TEXT,    -- REQ-929: source-native freshness probe for change_signal in {probe, ttl_probe}
    probe_type TEXT,     -- REQ-982: input-probe method; NULL = resolve per source class
    mv_debounce_quiet     DOUBLE PRECISION NOT NULL DEFAULT 0,  -- MV NRT debounce quiet window (s)
    mv_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5,  -- MV NRT debounce max delay (s)
    push_debounce_quiet     DOUBLE PRECISION NOT NULL DEFAULT 0,  -- REQ-1733: CDC push-source debounce quiet window (s)
    push_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5,  -- REQ-1733: CDC push-source debounce max delay (s)
    mv_consistency TEXT NOT NULL DEFAULT 'shared',  -- REQ-879: shared (fleet-coordinated) | distributed (per-instance)
    mv_preprocess TEXT,  -- REQ-957: inline preprocess(rows, ctx) hook source; NULL = identity
    business_key JSONB,  -- REQ-2109: columns identifying the same thing across versions
    valid_time TEXT,  -- REQ-2109: the column holding the date the business gives the record
    recorded_time TEXT,  -- REQ-2109: the column holding when this version was entered or changed
    mv_persist TEXT NOT NULL DEFAULT 'replace',  -- REQ-965: replace | append | upsert
    mv_primary_key JSONB,  -- REQ-970: row identity (required for upsert / incremental)
    mv_incremental BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-969: incremental maintenance
    mv_calendar TEXT,  -- REQ-962: periodic-snapshot calendar name (NULL = not periodic)
    mv_grain TEXT,  -- REQ-962/1168: nesting grain ('daily'..'annual') or nth-weekday ('3WE'/'LFR')
    mv_allowed_lateness DOUBLE PRECISION NOT NULL DEFAULT 0,  -- REQ-961: seal-deadline slack (s)
    mv_expected_events JSONB,  -- REQ-961: preflight freshness contract (NULL = all lineage inputs)
    mv_business_day_grain BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-962: gate windows to business days
    unique_constraints JSONB NOT NULL DEFAULT '[]',  -- REQ-1093: table-level UNIQUE constraints
    modeling_role TEXT,     -- REQ-1320: 'dimension' | 'fact' | NULL (entity/fact lowering metadata)
    modeling_history TEXT,  -- REQ-1320: originating entity history mode ('scd2' | 'snapshot' | NULL)
    view_metrics JSONB,     -- REQ-1318: declarative metric-view spec {metrics, dimensions, filters}; view_sql holds the generated SELECT
    -- REQ-1919: the rest of a table's settings, held here so the store alone owns the model.
    approval_hook BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-204/247
    hot           BOOLEAN,  -- NULL = auto-detect
    promotions    JSONB NOT NULL DEFAULT '[]',  -- REQ-119
    query_template TEXT,  -- REQ-1668/1683: neo4j / sparql tables
    relay_pagination BOOLEAN,  -- NULL = inherit
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
    masks        JSONB NOT NULL DEFAULT '[]',  -- REQ-2059: the ordered mask list
    is_primary_key BOOLEAN NOT NULL DEFAULT FALSE,
    epoch_unit   TEXT,  -- REQ-1908: epoch-number storage unit of a temporal column
    -- REQ-1965: the hidden attributes of a natively array/struct/map column, from its source
    array_depth  INTEGER NOT NULL DEFAULT 0,
    element_type TEXT,
    native_type  TEXT,
    native_platform TEXT,  -- whose spelling native_type is
    fake         TEXT,  -- REQ-1494: the column's kind of fake, as declared
    fake_stable  BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-1494
    fake_stable_version INTEGER,  -- REQ-1494: the portable definition version a stable fake is pinned to
    synthetic_rule TEXT,  -- REQ-1494, REQ-1939: laid over the fake, for synthetic generation
    fake_why JSONB,  -- REQ-2094: why the column's fake and synthetic rule are what they are
    embedding    BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-421, REQ-1919
    embedding_model TEXT,  -- REQ-421
    embedding_source_column TEXT,  -- REQ-421
    encrypted    BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-1919: surfaced as @encrypted
    UNIQUE (table_id, column_name)
);

-- Migration: add alias/description columns if missing
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS alias TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS description TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS native_filter_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS native_filter_required BOOLEAN;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS required_by_steward BOOLEAN NOT NULL DEFAULT FALSE;  -- REQ-1986
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS data_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_primary_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_foreign_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS is_alternate_key BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS object_fields JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'domain';
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS gql_selection TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS gql_object_type TEXT;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS gql_is_list BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS domain_id TEXT REFERENCES domains(id) ON DELETE CASCADE;
    -- Backfill existing rows from the owning table's domain_id (registration now sets it going forward).
    UPDATE table_columns tc SET domain_id = rt.domain_id
        FROM registered_tables rt WHERE rt.id = tc.table_id AND tc.domain_id IS NULL;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_enabled BOOLEAN NOT NULL DEFAULT TRUE;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS path TEXT;
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS allowed_domains JSONB NOT NULL DEFAULT '[]';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS mapping JSONB NOT NULL DEFAULT '{}';
    -- Connection extras the typed columns cannot carry (warehouse account/http_path, remote-schema
    -- override, Exasol server-certificate fingerprint) — Source.federation_hints.
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS federation_hints JSONB NOT NULL DEFAULT '{}';
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS cdc JSONB;  -- REQ-824: source-level CDC transport
    ALTER TABLE sources ADD COLUMN IF NOT EXISTS change_signal TEXT NOT NULL DEFAULT 'ttl';  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS cache_ttl INTEGER;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS watermark_column TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS change_signal TEXT;  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS probe_query TEXT;  -- REQ-929
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS probe_type TEXT;  -- REQ-982
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS unique_constraints JSONB NOT NULL DEFAULT '[]';  -- REQ-1093
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS view_sql TEXT;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS dq_contract TEXT;  -- REQ-1443
    -- REQ-1443 clause 10: the contract's checks, parsed at registration, for the meta dq_rules.
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS dq_rules JSONB;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS profiler_source_id TEXT;  -- REQ-1934
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS product_id TEXT REFERENCES data_products(id) ON DELETE SET NULL;  -- REQ-1634
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS materialize BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS row_materialize BOOLEAN NOT NULL DEFAULT FALSE;  -- REQ-1865
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS file_glob TEXT;  -- REQ-788
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS source_file_column TEXT;  -- REQ-788
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS table_format TEXT;  -- REQ-1981
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS import_set TEXT;  -- REQ-1954
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS delta JSONB;  -- REQ-874
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_refresh_interval INTEGER NOT NULL DEFAULT 300;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_debounce_quiet DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS push_debounce_quiet DOUBLE PRECISION NOT NULL DEFAULT 0;  -- REQ-1733
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS push_debounce_max_delay DOUBLE PRECISION NOT NULL DEFAULT 5;  -- REQ-1733
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_consistency TEXT NOT NULL DEFAULT 'shared';  -- REQ-879
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_preprocess TEXT;  -- REQ-957
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS business_key JSONB;  -- REQ-2109
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS valid_time TEXT;  -- REQ-2109
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS recorded_time TEXT;  -- REQ-2109
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_persist TEXT NOT NULL DEFAULT 'replace';  -- REQ-965
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_primary_key JSONB;  -- REQ-970
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_incremental BOOLEAN NOT NULL DEFAULT FALSE;  -- REQ-969
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_calendar TEXT;  -- REQ-962
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_grain TEXT;  -- REQ-962/1168
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_allowed_lateness DOUBLE PRECISION NOT NULL DEFAULT 0;  -- REQ-961
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_expected_events JSONB;  -- REQ-961
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS mv_business_day_grain BOOLEAN NOT NULL DEFAULT FALSE;  -- REQ-962
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS modeling_role TEXT;  -- REQ-1320
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS modeling_history TEXT;  -- REQ-1320
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS view_metrics JSONB;  -- REQ-1318
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS enable_aggregates BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS enable_group_by BOOLEAN NOT NULL DEFAULT FALSE;
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS live JSONB;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS relationships (
    id               TEXT PRIMARY KEY,
    source_table_id  INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    target_table_id  INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    source_column    TEXT NOT NULL,
    target_column    TEXT NOT NULL,
    cardinality      TEXT NOT NULL CHECK (cardinality IN ('many-to-one', 'one-to-many', 'one-to-one')),
    materialize      BOOLEAN NOT NULL DEFAULT FALSE,
    refresh_interval INTEGER NOT NULL DEFAULT 300,
    -- REQ-1132: opt this edge OUT of discovery — its target table's metadata is not exposed to a role
    -- that reaches the target only via this relationship (a sensitive target behind an innocuous edge).
    hide_target_meta BOOLEAN NOT NULL DEFAULT FALSE,
    -- REQ-2141: the rows this edge joins are the same entity (both tables grounded in one class
    -- term, joined one-to-one on a key of each; repositories/identity.py holds the rule).
    same_entity      BOOLEAN NOT NULL DEFAULT FALSE,
    -- The Cypher type name a steward pinned on this edge; also what the 'fixed' junction
    -- nomination below reads, which is why it is declared here rather than added by the
    -- ALTER block that follows.
    alias            TEXT,
    -- REQ-1586: junction declaration. via_table_id names an associative table this edge traverses
    -- through; its two foreign keys are via_source_column (joined to source_column) and
    -- via_target_column (joined to target_column). Each is a comma-separated ordered column list,
    -- so a composite key is mapped by listing its columns in order and pairing them positionally. via_type_column/via_type_value discriminate a
    -- junction that carries several relationship types — one row declares one exposed type.
    -- All five null = an ordinary single-hop edge.
    via_table_id      INTEGER REFERENCES registered_tables(id) ON DELETE CASCADE,
    via_source_column TEXT,
    via_target_column TEXT,
    via_type_column   TEXT,
    via_type_value    TEXT,
    -- REQ-1586: where this edge's Cypher type name comes from. 'column' = the discriminator value
    -- in via_type_value; 'table' = the junction table's own name; 'fixed' = the alias pinned on
    -- this row. All three are upper-snake-cased before they are exposed, so a junction edge names
    -- itself the same way every other relationship type does.
    via_label_source  TEXT CHECK (via_label_source IN ('column', 'table', 'fixed')),
    -- REQ-2099: the target table's parameters this edge feeds, {parameter: source column}. A join
    -- to a parameterized target (a code table) over this edge calls it with the source's values.
    parameters        JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT relationships_via_complete CHECK (
        (via_table_id IS NULL AND via_source_column IS NULL AND via_target_column IS NULL
         AND via_label_source IS NULL)
        OR (via_table_id IS NOT NULL AND via_source_column IS NOT NULL AND via_target_column IS NOT NULL
            AND via_label_source IS NOT NULL)
    ),
    -- REQ-1586: each nomination needs the thing it names. 'column' reads via_type_value, 'fixed'
    -- reads the pinned alias; 'table' needs neither because the junction table is already named.
    CONSTRAINT relationships_via_label_source_complete CHECK (
        via_label_source IS NULL
        OR (via_label_source = 'column' AND via_type_value IS NOT NULL)
        OR (via_label_source = 'fixed' AND alias IS NOT NULL)
        OR via_label_source = 'table'
    ),
    CONSTRAINT relationships_via_type_pairs CHECK (
        (via_type_column IS NULL AND via_type_value IS NULL)
        OR (via_type_column IS NOT NULL AND via_type_value IS NOT NULL AND via_table_id IS NOT NULL)
    )
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
    -- REQ-1586: the junction declaration reaches a relationships table that predates it.
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_table_id INTEGER
        REFERENCES registered_tables(id) ON DELETE CASCADE;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_source_column TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_target_column TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_type_column TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_type_value TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS via_label_source TEXT;
    -- REQ-2141: the same-entity flag reaches a relationships table that predates it.
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS same_entity BOOLEAN NOT NULL DEFAULT FALSE;
    -- REQ-351: the effective names a relationship is served under, derived once and stored
    -- (core.relationship_names), so the meta domain reads what the admin API and Cypher use.
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS cypher_type TEXT;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS graphql_field TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1586: the junction CHECKs the CREATE TABLE above declares, for a table that predates them.
-- One block each: a constraint already present must not skip the ones after it.
DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_via_label_source_check
        CHECK (via_label_source IN ('column', 'table', 'fixed'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_via_complete CHECK (
        (via_table_id IS NULL AND via_source_column IS NULL AND via_target_column IS NULL
         AND via_label_source IS NULL)
        OR (via_table_id IS NOT NULL AND via_source_column IS NOT NULL AND via_target_column IS NOT NULL
            AND via_label_source IS NOT NULL)
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_via_label_source_complete CHECK (
        via_label_source IS NULL
        OR (via_label_source = 'column' AND via_type_value IS NOT NULL)
        OR (via_label_source = 'fixed' AND alias IS NOT NULL)
        OR via_label_source = 'table'
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_via_type_pairs CHECK (
        (via_type_column IS NULL AND via_type_value IS NULL)
        OR (via_type_column IS NOT NULL AND via_type_value IS NOT NULL AND via_table_id IS NOT NULL)
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Uniqueness constraint: one alias per (source_table, alias) pair
DO $$ BEGIN
    ALTER TABLE relationships ADD CONSTRAINT relationships_source_alias_unique
        UNIQUE (source_table_id, alias);
EXCEPTION WHEN duplicate_table THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS roles (
    id              TEXT PRIMARY KEY,
    -- REQ-2080: what the role is for; required on every write. A standard role's is written by
    -- db.assert_standard_roles (REQ-2155).
    description     TEXT NOT NULL DEFAULT '',
    capabilities    JSONB NOT NULL DEFAULT '[]',
    -- REQ-1602: rights this role does NOT hold, whose surfaces are shown to it anyway -- disabled,
    -- and badged with what they are. The sandbox (REQ-2155) demonstrates what org_admin holds and
    -- it does not, and a feature that is simply absent demonstrates nothing.
    -- A right may never appear in both lists; the holder of a right needs no explanation of it.
    demonstrated    JSONB NOT NULL DEFAULT '[]',
    domain_access   JSONB NOT NULL DEFAULT '[]',
    -- REQ-1921: with data_residency, the region values the grant covers (regions, "no_region").
    residency_values JSONB NOT NULL DEFAULT '[]',
    -- REQ-1682: the role's constant session values {name: value}, what
    -- current_setting('provisa.<name>') reads when the request binds nothing of that name.
    session_vars    JSONB NOT NULL DEFAULT '{}',
    rate_limit      JSONB,  -- REQ-1174: per-role rate + burst; mirrors schema_org.roles.rate_limit
    -- REQ-2155: the roles this one inherits from, in declared order; its capabilities are the
    -- union of its own and every ancestor's (security.inheritance). Mirrors schema_org.roles.parents.
    parents         JSONB NOT NULL DEFAULT '[]',
    max_rows        INTEGER,  -- REQ-005: the role's result-size ceiling; NULL = none of its own
    -- A grant of this role unused for this many days is removed (auth.role_grants); NULL = never.
    inactivity_days INTEGER,
    relationship_guard BOOLEAN NOT NULL DEFAULT TRUE  -- REQ-1919: held so the store owns it
);

-- REQ-2080: the seed below writes each seeded role's description before add_missing_columns runs,
-- so a roles table made before the column existed needs it here first.
ALTER TABLE roles ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '';
ALTER TABLE roles ADD COLUMN IF NOT EXISTS inactivity_days INTEGER;

CREATE TABLE IF NOT EXISTS rls_rules (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER REFERENCES registered_tables(id) ON DELETE CASCADE,
    domain_id   TEXT REFERENCES domains(id) ON DELETE CASCADE,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    filter_expr BYTEA NOT NULL,  -- REQ-686: encrypted at rest via EncryptionService
    description TEXT NOT NULL DEFAULT '',  -- REQ-2081: what the rule is for; required on write
    -- REQ-1679: a rule may target a tracked function or webhook by name instead of a table.
    action_name TEXT,
    -- REQ-1384: a rule may target a user tag; it then applies to every table carrying the tag.
    -- The reference to tags(id) is declared after that table, below.
    tag_id      TEXT,
    why         JSONB,  -- REQ-2094: why the rule is what it is
    UNIQUE (table_id, role_id),
    UNIQUE (action_name, role_id),
    CONSTRAINT rls_rules_tag_role_key UNIQUE (tag_id, role_id)
);

-- REQ-2074: what each role may write to each table, per operation. rows_expr / check_expr are row
-- predicates like rls_rules.filter_expr and are encrypted at rest the same way (REQ-686); NULL is
-- the role's read row filter on the table.
CREATE TABLE IF NOT EXISTS table_write_grants (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    operation   TEXT NOT NULL CHECK (operation IN ('insert', 'update', 'delete')),
    columns     JSONB NOT NULL DEFAULT '[]',
    rows_expr   BYTEA,
    check_expr  BYTEA,
    presets     JSONB NOT NULL DEFAULT '[]',
    why         JSONB,  -- REQ-2094: why the grant is what it is
    tenant_id   UUID,
    UNIQUE (table_id, role_id, operation)
);

ALTER TABLE rls_rules ADD COLUMN IF NOT EXISTS tag_id TEXT;  -- REQ-1384

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

-- REQ-1373/REQ-1375: org-level tag registry — USER tags only. The three system tags
-- (technical/pii/deprecated) are code-defined intrinsics (provisa.core.models.SYSTEM_TAGS),
-- present in every install and never stored, so no rows are seeded here.
CREATE TABLE IF NOT EXISTS tags (
    id             TEXT PRIMARY KEY,
    description    TEXT NOT NULL DEFAULT '',
    applies_to     JSONB NOT NULL DEFAULT '[]',
    is_system      BOOLEAN NOT NULL DEFAULT FALSE,
    -- Per-tag field policy for assignments: whether the picker shows the field and
    -- whether the mutation demands it.
    reason_policy  TEXT NOT NULL DEFAULT 'optional' CHECK (reason_policy IN ('hidden', 'optional', 'required')),
    expires_policy TEXT NOT NULL DEFAULT 'optional' CHECK (expires_policy IN ('hidden', 'optional', 'required')),
    -- REQ-1467: whether assignments of this tag carry a "{tag}:{value}" parameter. No
    -- 'optional': a bare `entity` beside `entity:customer` would need a reading, and the only
    -- reading available is a guessed entity type.
    param_policy   TEXT NOT NULL DEFAULT 'none' CHECK (param_policy IN ('none', 'required')),
    -- REQ-1943: the Sensitive data option: a column carrying this tag is a sensitive column.
    sensitive      BOOLEAN NOT NULL DEFAULT FALSE
);

-- REQ-1384: a row filter on a tag refers to the tag. No ON DELETE action: a tag that carries a
-- filter is refused deletion (repositories.integrity), never taken with its filter.
DO $$ BEGIN
    ALTER TABLE rls_rules ADD CONSTRAINT rls_rules_tag_id_fkey
        FOREIGN KEY (tag_id) REFERENCES tags(id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- REQ-1467: the permitted parameter values for a parameterized tag. Its own table rather than a
-- column on `tags` because the tag that motivates it — `entity` — is a system tag, and system
-- tags are code-defined with no row in `tags` at all. The list is CLOSED: assignment refuses a
-- value that is not here, because an open list accepts `entity:custmoer` and a misspelt entity
-- type reads downstream as an entity that is simply absent from the corpus. tag_id is the BASE
-- id ('entity'), never an assigned 'entity:customer'.
CREATE TABLE IF NOT EXISTS tag_param_values (
    tag_id      TEXT NOT NULL,
    value       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tag_id, value)
);

DO $$ BEGIN
    ALTER TABLE tags ADD COLUMN IF NOT EXISTS reason_policy TEXT NOT NULL DEFAULT 'optional';
    ALTER TABLE tags ADD COLUMN IF NOT EXISTS expires_policy TEXT NOT NULL DEFAULT 'optional';
    -- REQ-1467: tags_meta unions this column in, so a database created before REQ-1467 fails
    -- every startup at the meta-view DDL until the column exists.
    ALTER TABLE tags ADD COLUMN IF NOT EXISTS param_policy TEXT NOT NULL DEFAULT 'none';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1377: tag assignments. Exactly the typed columns implied by object_type are set;
-- their FKs cascade cleanup when the tagged object is deleted. tag_id deliberately has NO
-- FK: system tags have no row to reference (user-tag assignment cleanup happens in the
-- delete mutation). object_key is the canonical dedup identity ('<type>:<id...>') because
-- a UNIQUE over nullable typed columns does not deduplicate under SQL NULL semantics.
CREATE TABLE IF NOT EXISTS tag_assignments (
    id              SERIAL PRIMARY KEY,
    tag_id          TEXT NOT NULL,
    -- REQ-1467: tag_id with the parameter stripped ('entity:customer' -> 'entity'). Stored
    -- rather than derived at read time because the uniqueness rule below is stated over it.
    base_tag_id     TEXT NOT NULL,
    object_type     TEXT NOT NULL CHECK (object_type IN ('source', 'table', 'column', 'relationship', 'command', 'product', 'term')),
    source_id       TEXT REFERENCES sources(id) ON DELETE CASCADE,
    table_id        INTEGER REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name     TEXT,
    relationship_id TEXT REFERENCES relationships(id) ON DELETE CASCADE,
    -- Commands (tracked functions/webhooks) are named, not serial-keyed; no FK — their
    -- registries split across two tables and deletion cleanup is the mutation's job.
    command_name    TEXT,
    -- REQ-1660: explicit data-product-level tags, distinct from tags inherited/rolled up
    -- from member tables' own assignments (computed, not stored here).
    product_id      TEXT REFERENCES data_products(id) ON DELETE CASCADE,
    -- REQ-2145: the glossary term a tag is on (pii on a Property term). Its reference to
    -- glossary_terms is declared after that table, below.
    term_id         INTEGER,
    object_key      TEXT NOT NULL,
    -- Why this tag is on this object; REQUIRED for 'deprecated' (system semantic).
    reason          TEXT,
    -- ISO date the assignment stops being intended: for 'deprecated' the planned removal
    -- date, queryable for management reporting (typed on purpose — never buried in prose).
    expires_on      TEXT,
    -- REQ-2094: the rest of the assignment's "why" (reason is above): what it is for, the bases
    -- it cites, a review-by date, and who decided it, when and on what profile evidence.
    purpose         TEXT,
    bases           JSONB,
    review_on       TEXT,
    decided_by      TEXT,
    decided_at      TIMESTAMPTZ,
    evidence        JSONB,
    -- REQ-1467: over the BASE id, so one column carries one entity type. A UNIQUE over the full
    -- tag_id would accept entity:customer and entity:employee on the same column, which is a
    -- contradiction — the column's values are entity names of ONE type.
    UNIQUE (base_tag_id, object_key)
);

DO $$ BEGIN
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS reason TEXT;
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS command_name TEXT;
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS expires_on TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1660: product-level tags, added to a table that predates them.
DO $$ BEGIN
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS product_id TEXT REFERENCES data_products(id) ON DELETE CASCADE;
    ALTER TABLE tag_assignments DROP CONSTRAINT IF EXISTS tag_assignments_object_type_check;
    ALTER TABLE tag_assignments ADD CONSTRAINT tag_assignments_object_type_check
        CHECK (object_type IN ('source', 'table', 'column', 'relationship', 'command', 'product', 'term'));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1467: base_tag_id on a table that predates it. Added nullable and backfilled from the
-- stored tag_id before the NOT NULL is asserted, because the column carries no default and a
-- bare NOT NULL ADD COLUMN fails outright on a table that already holds assignments. The
-- uniqueness rule is stated over the base id, so the index goes on in the same step.
DO $$ BEGIN
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS base_tag_id TEXT;
    UPDATE tag_assignments SET base_tag_id = split_part(tag_id, ':', 1) WHERE base_tag_id IS NULL;
    ALTER TABLE tag_assignments ALTER COLUMN base_tag_id SET NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS tag_assignments_base_tag_object_key
        ON tag_assignments (base_tag_id, object_key);
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1385/REQ-1389: vendor-side identity bindings captured at publish time. One row per
-- (export target, published asset): vendor_ref is the catalog's own id for the asset (Atlas guid,
-- OpenMetadata entity UUID, Collibra asset UUID, DataHub dataset URN) and physical_key is
-- the vendor-side name-key the asset was published under. On a physical re-address the
-- exporter uses the stored vendor_ref to rebind (or deprecate) the SAME catalog entity
-- instead of relying on the vendor's name-keyed upsert to find it.
CREATE TABLE IF NOT EXISTS catalog_bindings (
    id            SERIAL PRIMARY KEY,
    target        TEXT NOT NULL,
    semantic_uri  TEXT NOT NULL,
    vendor_ref    TEXT NOT NULL,
    physical_key  TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (target, semantic_uri)
);

DO $$ BEGIN
    ALTER TABLE catalog_bindings ADD COLUMN IF NOT EXISTS tenant_id UUID;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1387: business glossary. Terms are the normalized vocabulary derived from physical
-- field names (the term unit is the phrase — the full normalized field name). Rooted terms
-- hold physical refs; abstract terms hold none and connect to the rooted graph through the
-- closed edge set. Lifecycle is derived from semantic-layer membership: the table repository
-- creates/links terms as columns register and removes refs as they depart; a term losing its
-- last ref is REMOVED unless an abstract term is connected to the rooted graph through it,
-- in which case it is deprecated (kept) so no abstract term is left dangling.
CREATE TABLE IF NOT EXISTS glossary_terms (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    definition      TEXT,
    deprecated      BOOLEAN NOT NULL DEFAULT FALSE,
    -- Out of service, set only by a curator (REQ-2144). Retiring moves what the term holds --
    -- its column refs, or its table groundings with their keys -- to a successor, and keeps the
    -- term as an inert record (definition, edges, experts, the successor's REPLACES edge, the
    -- reason). No consuming surface offers it, and it conducts nothing. Never written by the
    -- derived lifecycle, so a retirement survives its old columns departing and re-registering.
    retired         BOOLEAN NOT NULL DEFAULT FALSE,
    -- Per-term opt-out from metadata export: the term (and any edge touching it) is
    -- withheld from every vendor snapshot while its curation stays intact locally.
    export_excluded BOOLEAN NOT NULL DEFAULT FALSE,
    -- REQ-2144: the parts of a term's status a curator asserts (draft and live are computed):
    -- a deprecation of a live term, with its reason and an optional planned retirement date, on
    -- which the scheduler retires it; a retirement's reason; and who asserted it, when.
    deprecation_asserted BOOLEAN NOT NULL DEFAULT FALSE,
    status_reason   TEXT,
    retire_on       DATE,
    status_by       TEXT,
    status_at       TIMESTAMPTZ,
    tenant_id       UUID,
    UNIQUE (name)
);
ALTER TABLE glossary_terms ADD COLUMN IF NOT EXISTS deprecation_asserted BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE glossary_terms ADD COLUMN IF NOT EXISTS status_reason TEXT;
ALTER TABLE glossary_terms ADD COLUMN IF NOT EXISTS retire_on DATE;
ALTER TABLE glossary_terms ADD COLUMN IF NOT EXISTS status_by TEXT;
ALTER TABLE glossary_terms ADD COLUMN IF NOT EXISTS status_at TIMESTAMPTZ;

-- REQ-2145: a tag on a glossary term, on a table that predates it. The term's deletion takes its
-- tags: a term holding a column cannot be deleted (glossary.delete_term), so none of them is a
-- column's.
DO $$ BEGIN
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS term_id INTEGER;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;
DO $$ BEGIN
    ALTER TABLE tag_assignments ADD CONSTRAINT tag_assignments_term_id_fkey
        FOREIGN KEY (term_id) REFERENCES glossary_terms(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- A physical ref binds a term to (table_id, column_name) — the stable column identity
-- (table_columns.id is NOT stable: the table upsert replaces column rows wholesale).
-- Each physical field belongs to exactly one term. Table deletion cascades the refs;
-- the glossary sweep then applies the remove-or-deprecate rule to newly refless terms.
CREATE TABLE IF NOT EXISTS glossary_term_refs (
    id          SERIAL PRIMARY KEY,
    term_id     INTEGER NOT NULL REFERENCES glossary_terms(id) ON DELETE CASCADE,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    tenant_id   UUID,
    UNIQUE (table_id, column_name)
);

-- REQ-2130: a class term grounds a whole table or view -- the table is what the term names an
-- instance of. A table has at most one class term (its class_term_id); a term may ground many.
-- Only a table with a primary key may be grounded, so each of its rows is an identifiable instance.
ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS class_term_id INTEGER
    REFERENCES glossary_terms(id) ON DELETE SET NULL;
-- REQ-2141: TRUE when sync derived the class term from the table's concept; FALSE when a
-- configuration declared it or a person grounded it. A derived grounding whose term is not
-- curated is a draft: the config export does not state it and it goes with its table.
ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS class_term_derived BOOLEAN NOT NULL DEFAULT FALSE;

-- Closed, enum-typed relationship set (REQ-1387) — free-form edge types are not permitted.
CREATE TABLE IF NOT EXISTS glossary_term_edges (
    id           SERIAL PRIMARY KEY,
    from_term_id INTEGER NOT NULL REFERENCES glossary_terms(id) ON DELETE CASCADE,
    to_term_id   INTEGER NOT NULL REFERENCES glossary_terms(id) ON DELETE CASCADE,
    rel_type     TEXT NOT NULL CONSTRAINT glossary_term_edges_rel_type_check CHECK (rel_type IN (
        'KIND_OF', 'RELATED_TO', 'PART_OF', 'SYNONYM_OF',
        'VALID_VALUE_OF', 'DERIVED_FROM', 'REPLACES', 'PREFERRED_TERM_FOR',
        'TRANSLATION_OF', 'ANTONYM_OF')),
    tenant_id    UUID,
    UNIQUE (from_term_id, to_term_id, rel_type)
);

-- REQ-1387: the edge set is core.glossary.TERM_EDGE_TYPES; a table that predates the wider set
-- carries the four-type check, which refused a config's DERIVED_FROM edge at boot.
DO $$ BEGIN
    ALTER TABLE glossary_term_edges DROP CONSTRAINT IF EXISTS glossary_term_edges_rel_type_check;
    ALTER TABLE glossary_term_edges ADD CONSTRAINT glossary_term_edges_rel_type_check
        CHECK (rel_type IN (
        'KIND_OF', 'RELATED_TO', 'PART_OF', 'SYNONYM_OF',
        'VALID_VALUE_OF', 'DERIVED_FROM', 'REPLACES', 'PREFERRED_TERM_FOR',
        'TRANSLATION_OF', 'ANTONYM_OF'));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1591: a term's DECLARED domains. A rooted term's domains are derived from its refs'
-- tables and nothing is written here while it holds a ref; rows appear for two reasons only:
-- an abstract term, which holds no refs to derive from and declares its scope on creation,
-- and the stamp written when a rooted term's last ref departs — without which a deprecated
-- term's domains become underivable and its own curators lose the right to touch it.
-- The rule everywhere: a term's domains are its refs' domains while it has refs, these
-- otherwise. Not an FK to domains(id): a domain deleted out from under a retired term must
-- not silently widen that term's scope to everyone.
CREATE TABLE IF NOT EXISTS glossary_term_domains (
    id        SERIAL PRIMARY KEY,
    term_id   INTEGER NOT NULL REFERENCES glossary_terms(id) ON DELETE CASCADE,
    domain_id TEXT NOT NULL,
    tenant_id UUID,
    UNIQUE (term_id, domain_id)
);

-- People who can answer questions about the term or who authored its definition.
CREATE TABLE IF NOT EXISTS glossary_term_experts (
    id        SERIAL PRIMARY KEY,
    term_id   INTEGER NOT NULL REFERENCES glossary_terms(id) ON DELETE CASCADE,
    user_id   TEXT NOT NULL,
    kind      TEXT NOT NULL DEFAULT 'expert' CHECK (kind IN ('expert', 'author')),
    tenant_id UUID,
    UNIQUE (term_id, user_id)
);

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
    sdl_config      JSONB,          -- {domain_id, columns}
    -- REQ-1922: the view's definition; each region's build of it is mv_build_state (state store).
    -- REQ-961/962: temporal-processing declaration (calendar-bounded windows + freshness contract).
    calendar          TEXT,
    grain             TEXT,
    allowed_lateness  INTEGER NOT NULL DEFAULT 0,
    expected_events   JSONB,          -- freshness-contract inputs; NULL = all SQL-lineage inputs
    business_day_grain BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REQ-879, REQ-1922: a region's build of a materialized view (its state store): the record of what
-- the region built (region names it) and the fleet's refresh coordination (claim, lease, the
-- REQ-862 version stamps the claim dedups on).
CREATE TABLE IF NOT EXISTS mv_build_state (
    mv_id           TEXT PRIMARY KEY,
    region          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'stale'
                    CHECK (status IN ('fresh', 'stale', 'refreshing', 'disabled')),
    last_refresh_at TIMESTAMPTZ,
    row_count       INTEGER,
    last_error      TEXT,
    writer          TEXT,
    lease_until     TIMESTAMPTZ,
    materialized_definition_version TEXT,
    materialized_input_version      TEXT,
    snapshot_id     TEXT
);

-- REQ-1317: governed metric definitions — named aggregates with query-time grain.
-- from_fact marks a metric auto-registered from a fact spec's measure (REQ-1320).
CREATE TABLE IF NOT EXISTS metrics (
    name        TEXT PRIMARY KEY,
    expression  TEXT NOT NULL,
    datatype    TEXT,
    description TEXT,
    ai_context  TEXT,  -- REQ-1319: definition text for AI consumers
    visible_to  JSONB NOT NULL DEFAULT '["*"]',
    from_fact   TEXT
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
    day_close           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version)
);

CREATE TABLE IF NOT EXISTS mv_refresh_log (
    id          SERIAL PRIMARY KEY,
    mv_id       TEXT NOT NULL,  -- REQ-1922: state store; no foreign key into the model
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

-- REQ-877: opt-in per-MV ROW-LEVEL delta ledger (append-only; store-independent, never stamped on
-- the target). One event per changed key per refresh version: change_type, row_key, the row hashes
-- (change detection over the hash-excluded projection), and the row VALUES (value-delta tier for
-- REQ-878 full-content point-in-time reconstruction). Delete carries old_values; insert new_values.
CREATE TABLE IF NOT EXISTS mv_delta_ledger (
    id                 SERIAL PRIMARY KEY,
    mv_id              TEXT NOT NULL,  -- REQ-1922: state store; no foreign key into the model
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

-- Column-Level Masking (Phase Q) — masking rules are inline on table_columns.

-- Approved-query / GPQ registry removed (REQ-001/003) — access is governed solely by
-- table/view + relationship rights. The persisted_queries and approval_log tables are
-- deprecated and no longer created. (Apollo APQ — REQ-288-291 — is separate, lives in
-- Redis, and is unaffected.)

-- REQ-2129: the model's column lineage edges, folded from its compiled views and statement
-- commands. Derived state: each model build replaces every row; nothing else writes it.
CREATE TABLE IF NOT EXISTS lineage_edges (
    id                 SERIAL PRIMARY KEY,
    source_relation    TEXT NOT NULL,
    source_column      TEXT NOT NULL,
    target_relation    TEXT NOT NULL,
    target_column      TEXT NOT NULL,
    via                TEXT NOT NULL,
    via_kind           TEXT NOT NULL CONSTRAINT lineage_edges_via_kind_check
                       CHECK (via_kind IN ('view', 'command')),
    source_table_id    INTEGER,
    source_column_name TEXT,
    target_table_id    INTEGER,
    target_column_name TEXT
);

-- REQ-2125: the governed table each checker table's contract scans, as the model build resolved
-- it. Derived state: each build replaces every row.
CREATE TABLE IF NOT EXISTS checker_targets (
    table_id         INTEGER PRIMARY KEY,
    checked_table_id INTEGER NOT NULL
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
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- REQ-1919: the source as its configuration wrote it (topics, windows, discriminators, auth;
    -- a credential stays its reference). The model store holds it once a configuration seeded it.
    spec                JSONB
);
ALTER TABLE kafka_sources ALTER COLUMN bootstrap_servers SET DEFAULT '';

-- REQ-1491, REQ-1942: how this environment reaches -- a connection given in it (own), one copied from its parent
-- (copied), or nothing (unbound) -- the cluster's bootstrap servers. A copy between environments carries the
-- row and never the binding, and an empty value is not an absent one, so an unbound kafka source is
-- MARKED rather than blanked and the query path refuses to dial whatever is local to the node.
ALTER TABLE kafka_sources ADD COLUMN IF NOT EXISTS binding TEXT NOT NULL DEFAULT 'own' CHECK (binding IN ('own', 'copied', 'unbound'));

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

-- API Sources (Phase U)
CREATE TABLE IF NOT EXISTS api_sources (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL CHECK (type IN ('openapi', 'graphql_api', 'grpc_api', 'neo4j', 'sparql')),
    base_url    TEXT NOT NULL,
    spec_url    TEXT,
    auth        BYTEA,  -- REQ-686: API auth (keys/tokens) encrypted at rest
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE api_sources ALTER COLUMN base_url SET DEFAULT '';

-- REQ-1491, REQ-1942: how this environment reaches -- a connection given in it (own), one copied from its parent
-- (copied), or nothing (unbound) -- the API's base URL and auth. A copy between environments carries the
-- row and never the binding, and an empty value is not an absent one, so an unbound API source is
-- MARKED rather than blanked and the query path refuses to dial whatever is local to the node.
ALTER TABLE api_sources ADD COLUMN IF NOT EXISTS binding TEXT NOT NULL DEFAULT 'own' CHECK (binding IN ('own', 'copied', 'unbound'));

CREATE TABLE IF NOT EXISTS api_endpoints (
    id              SERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES api_sources(id) ON DELETE CASCADE,
    path            TEXT NOT NULL,
    method          TEXT NOT NULL DEFAULT 'GET',
    -- A table is named within its source: two sources may each register a table of one name.
    table_name      TEXT NOT NULL,
    columns         JSONB NOT NULL,
    ttl             INTEGER NOT NULL DEFAULT 300,
    response_root   TEXT,
    error_path      TEXT,
    pk_column       TEXT,
    pagination      JSONB,
    -- REQ-1668: query-API endpoints (neo4j): how the request body is encoded, the query it
    -- carries, and the normalizer that flattens the response. NULL on plain REST endpoints.
    body_encoding       TEXT,
    query_template      TEXT,
    response_normalizer TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT api_endpoints_source_table_key UNIQUE (source_id, table_name)
);

DO $$ BEGIN
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS response_root TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS error_path TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS pk_column TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS pagination JSONB;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS max_concurrency INTEGER;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS default_params JSONB;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS promotions JSONB NOT NULL DEFAULT '[]';
    -- REQ-1668/REQ-1683: query-API endpoints (neo4j, sparql)
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS body_encoding TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS query_template TEXT;
    ALTER TABLE api_endpoints ADD COLUMN IF NOT EXISTS response_normalizer TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-1668/REQ-1683: the query-API source kinds joined the type check after api_sources existed;
-- an org schema created before them still carries the narrower constraint.
DO $$ BEGIN
    ALTER TABLE api_sources DROP CONSTRAINT IF EXISTS api_sources_type_check;
    ALTER TABLE api_sources ADD CONSTRAINT api_sources_type_check
        CHECK (type IN ('openapi', 'graphql_api', 'grpc_api', 'neo4j', 'sparql'));
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
    domain_id     TEXT NOT NULL DEFAULT '',
    description   TEXT,
    output_columns JSONB,  -- REQ-1159: canonical IR-typed output dataset contract [{name,type}]
    requires_approval BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-1924: each call approved by the hook
    writes_table  TEXT,  -- REQ-1924, REQ-871: "schema.table" of the registered table it writes
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
    requires_approval  BOOLEAN NOT NULL DEFAULT FALSE,  -- REQ-209, REQ-1924: each call approved by the hook
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REQ-1003, REQ-1004: the org's scheduled triggers, in its model (REQ-1919). The schema is the org:
-- a trigger runs bound to the org whose model holds it, and only prod's are scheduled.
CREATE TABLE IF NOT EXISTS scheduled_triggers (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    cron          TEXT,                   -- when: a schedule, or (REQ-2054) on_table/on_event
    on_table      TEXT,                   -- REQ-2054: "source_id.schema.table" it fires on
    on_event      TEXT,                   -- REQ-2054: change | error | quarantine
    kind          TEXT NOT NULL,          -- 'webhook' | 'sql' | 'command'
    url           TEXT,                   -- webhook: the URL it calls (a config trigger)
    webhook_name  TEXT,                   -- webhook: the org's tracked webhook, resolved at fire time
    args          JSONB NOT NULL DEFAULT '{}',
    sql           TEXT,                   -- sql: the statement (REQ-1003/1004)
    command       TEXT,                   -- command (REQ-2054): the registered command it runs
    role          TEXT,                   -- sql, command: the role of this org it runs as
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT scheduled_triggers_kind_fields CHECK (
        (kind = 'sql' AND sql IS NOT NULL AND role IS NOT NULL)
        OR (kind = 'webhook' AND (url IS NOT NULL OR webhook_name IS NOT NULL))
        OR (kind = 'command' AND command IS NOT NULL AND role IS NOT NULL)
    ),
    CONSTRAINT scheduled_triggers_when CHECK (
        (cron IS NOT NULL AND on_table IS NULL)
        OR (cron IS NULL AND on_table IS NOT NULL AND on_event IS NOT NULL)
    )
);

-- REQ-1939: an environment's synthetic datasets, their (table, profile run) pairs and their report.
CREATE TABLE IF NOT EXISTS synthetic_datasets (
    id            TEXT PRIMARY KEY,
    seed          BIGINT NOT NULL,
    scale         DOUBLE PRECISION NOT NULL,
    status        TEXT NOT NULL,          -- defined | generating | generated | failed
    store_schema  TEXT NOT NULL,
    error         TEXT,
    report_error  TEXT,             -- REQ-1942: why the report on its generated rows failed
    generated_at  TIMESTAMPTZ,
    -- REQ-1939: conditional child counts by relationship, and statements checked after generation
    fanout_conditions JSONB NOT NULL DEFAULT '[]',
    assertions    JSONB NOT NULL DEFAULT '[]',
    private_epsilon DOUBLE PRECISION,     -- REQ-1939: the privacy budget ε; NULL: not private
    -- REQ-1939, NOT TOO CLOSE TO A REAL ROW: the threshold (a share of the real rows' own typical
    -- nearest-neighbour distance) and the draws a row takes; both NULL: not checked
    closeness_threshold DOUBLE PRECISION,
    closeness_draws INTEGER,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS synthetic_dataset_tables (
    dataset_id    TEXT NOT NULL REFERENCES synthetic_datasets(id) ON DELETE CASCADE,
    table_id      INTEGER NOT NULL UNIQUE REFERENCES registered_tables(id) ON DELETE CASCADE,
    profile_env   TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    scale         DOUBLE PRECISION,
    PRIMARY KEY (dataset_id, table_id)
);

-- REQ-1942: what each source of a Test (synthetic) environment was bound to before generating
-- bound it to the synthetic store; dropping the dataset or leaving the mode restores exactly that.
-- REQ-1942: each finished whole-model generation of the environment -- the rows it generated
-- and the seconds it took -- which the next generation's time is estimated from.
CREATE TABLE IF NOT EXISTS synthetic_generations (
    finished_at   TIMESTAMPTZ PRIMARY KEY,
    generated_rows BIGINT NOT NULL,
    seconds       DOUBLE PRECISION NOT NULL
);

-- REQ-2094: named bases a judgment can cite; declaring one changed dates its judgments.
CREATE TABLE IF NOT EXISTS judgment_bases (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    description   TEXT,
    owner         TEXT,
    changed_at    TIMESTAMPTZ,
    changed_by    TEXT,
    change_note   TEXT
);

-- REQ-2094: the review queue, one row per (object, judgment, finding kind).
CREATE TABLE IF NOT EXISTS judgment_findings (
    id            SERIAL PRIMARY KEY,
    object_type   TEXT NOT NULL,
    object_key    TEXT NOT NULL,
    judgment      TEXT NOT NULL,
    category      TEXT NOT NULL,
    finding       TEXT NOT NULL,
    observed      JSONB,
    first_seen    TIMESTAMPTZ NOT NULL,
    last_seen     TIMESTAMPTZ NOT NULL,
    resolved_at   TIMESTAMPTZ,
    resolved_by   TEXT,
    resolution    TEXT,
    UNIQUE (object_type, object_key, judgment, finding)
);

-- REQ-2091: the adequacy checks Provisa runs on each sensitive column's fake and synthetic rule,
-- one row per check per run; the sensitive_coverage view reads each check's latest. A result is
-- a share or a count and a reason, never a value.
CREATE TABLE IF NOT EXISTS fake_adequacy_checks (
    table_id      INTEGER REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name   TEXT NOT NULL,
    check_name    TEXT NOT NULL,
    checked_at    TIMESTAMPTZ NOT NULL,
    status        TEXT NOT NULL,
    measure       DOUBLE PRECISION,
    detail        TEXT NOT NULL,
    PRIMARY KEY (table_id, column_name, check_name, checked_at)
);

CREATE TABLE IF NOT EXISTS synthetic_source_bindings (
    source_id     TEXT PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
    previous      JSONB NOT NULL
);

-- REQ-1934 PROPOSED CONSTRAINTS: the operator's decision on a constraint a profile proposed.
CREATE TABLE IF NOT EXISTS profiler_constraints (
    id            TEXT PRIMARY KEY,
    table_id      INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    signature     TEXT NOT NULL,
    kind          TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    other_column  TEXT,
    physical_column        TEXT NOT NULL,
    physical_other_column  TEXT,
    definition    TEXT NOT NULL,
    evidence      TEXT NOT NULL,
    share         DOUBLE PRECISION,
    sampled       BOOLEAN NOT NULL,
    status        TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (table_id, signature)
);

-- REQ-2147: a steward's decision on a classification a profile suggested for a column.
CREATE TABLE IF NOT EXISTS profiler_classifications (
    table_id         INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    physical_column  TEXT NOT NULL,
    kind             TEXT NOT NULL,
    status           TEXT NOT NULL,
    evidence         TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    decided_by       TEXT,
    decided_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_id, physical_column, kind)
);

CREATE TABLE IF NOT EXISTS synthetic_report (
    id               SERIAL PRIMARY KEY,
    dataset_id       TEXT NOT NULL REFERENCES synthetic_datasets(id) ON DELETE CASCADE,
    table_name       TEXT NOT NULL,
    column_name      TEXT,
    measure          TEXT NOT NULL,
    source_value     DOUBLE PRECISION,
    synthetic_value  DOUBLE PRECISION,
    delta            DOUBLE PRECISION,
    note             TEXT
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

-- REQ-1634: optional data-product membership for commands, mirroring registered_tables.product_id.
DO $$ BEGIN
    ALTER TABLE tracked_functions ADD COLUMN IF NOT EXISTS product_id TEXT REFERENCES data_products(id) ON DELETE SET NULL;
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
    table_id     INTEGER PRIMARY KEY,  -- REQ-1922: state store; no foreign key into the model
    source_mtime DOUBLE PRECISION NOT NULL,
    synced_at    DOUBLE PRECISION NOT NULL
);

-- REQ-1349: per-org settings overrides. The deployment config file is the BASE for every setting;
-- a row here narrows one top-level config key for the org whose schema this table lives in. Only
-- keys an org administrator may own are ever written (see _ORG_OVERRIDABLE_KEYS in
-- provisa/core/org_settings.py) — deployment-wide concerns (federation engine, cache storage,
-- encryption, auth) have no representation here and stay gated on platform_settings.
CREATE TABLE IF NOT EXISTS org_settings (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  TEXT
);

-- REQ-1914: the config stamp of this plane -- one row per kind ('model', 'settings'), advanced by
-- trigger in the same transaction as any write to a table of that kind. The rows and the triggers
-- are created by provisa/core/config_stamp.py once every table exists.
CREATE TABLE IF NOT EXISTS config_stamp (
    kind   TEXT PRIMARY KEY,
    stamp  BIGINT NOT NULL
);

-- REQ-1395: per-org secrets (e.g. the org's Anthropic API key), encrypted at rest via
-- provisa.encryption.runtime.encryption_service() — same pattern as api_sources.auth.
-- Never echoed back in reads; write-only from the admin API.
CREATE TABLE IF NOT EXISTS org_secrets (
    key         TEXT PRIMARY KEY,
    value_enc   BYTEA NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  TEXT
);

-- Orgs, users, memberships, invites are the PLATFORM control plane's global
-- registry (see provisa/core/schema_admin.py). They are NOT created per-org
-- here; they live in a separate control-plane database (init_registry_schema).
-- The org_id columns below therefore hold a plain org identifier — no FK to
-- orgs, which lives in a different physical database.

-- Add org_id to domains. REQ-1293: this script runs inside EVERY org schema, so it cannot know
-- which org it is running in — an earlier `UPDATE domains SET org_id = 'root'` here stamped every
-- org's seeded domains with another org's id, and the admin read path that trusted the stamp
-- returned nothing for any org but 'root'. The org a domain belongs to is the schema it lives in;
-- the column is left unstamped rather than filled with a value that is wrong by construction.
DO $$ BEGIN
    ALTER TABLE domains ADD COLUMN IF NOT EXISTS org_id TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Add org_id to roles (nullable = system role: admin, superadmin)
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS org_id TEXT;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Add demonstrated to roles (REQ-1602) -- pre-1602 control planes lack it, and the roles query
-- selects every column.
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS demonstrated JSONB NOT NULL DEFAULT '[]';
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Add rate_limit to roles (REQ-1174) — pre-1174 control planes lack it, so the role
-- repository's rate_limit upsert fails against an existing Postgres roles table.
DO $$ BEGIN
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS rate_limit JSONB;
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
    -- A time-limited grant: held until this instant, then absent (auth.role_grants).
    expires_at  TIMESTAMPTZ,
    -- The last day a request depended on the grant, written at most once a day; with the role's
    -- inactivity_days it decides when an unused grant is removed.
    last_used_at TIMESTAMPTZ,
    -- When the holder and the org's admins were told of the coming expiry, and of the coming
    -- removal for inactivity (cleared by a new expiry; the second is stale once the grant is used).
    expiry_notified_at   TIMESTAMPTZ,
    inactive_notified_at TIMESTAMPTZ,
    UNIQUE (user_id, role_id, domain_id)
);

-- REQ-2077: the org registry view (core/org_registry_view.py) reads expires_at at startup, before
-- add_missing_columns reaches an org schema made before these columns existed, so they are added
-- here first.
ALTER TABLE user_role_assignments ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE user_role_assignments ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
ALTER TABLE user_role_assignments ADD COLUMN IF NOT EXISTS expiry_notified_at TIMESTAMPTZ;
ALTER TABLE user_role_assignments ADD COLUMN IF NOT EXISTS inactive_notified_at TIMESTAMPTZ;

-- REQ-1439: tenant-local mirror of the platform user directory. Every ops report keys rows by
-- user_id, which is an IdP subject and unreadable to a person. The directory that maps it to a
-- name (user_profiles) lives in the PLATFORM control plane — a different engine and possibly a
-- different physical database — so a report view cannot join to it. The auth middleware writes
-- this copy into the tenant plane at the same moment it upserts the platform profile, which puts
-- the name a plain SQL LEFT JOIN away from the audit rows it labels.
CREATE TABLE IF NOT EXISTS user_directory (
    user_id      TEXT PRIMARY KEY,
    display_name TEXT,
    email        TEXT,
    email_opt_in BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- REQ-1382: attributes the org records for a member, beside what their identity provider
-- supplies. Bound as session variables (current_setting('provisa.<name>')); where the provider
-- supplies the same name, the org's record wins (provisa.auth.attributes).
CREATE TABLE IF NOT EXISTS user_attributes (
    id         SERIAL PRIMARY KEY,
    user_id    TEXT NOT NULL,
    name       TEXT NOT NULL,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);

-- REQ-1383: the scalar claims each user's identity provider supplied at their last sign-in, so
-- an administrator can see an attribute's source for a user who is not the caller.
CREATE TABLE IF NOT EXISTS user_idp_attributes (
    user_id TEXT NOT NULL,
    name    TEXT NOT NULL,
    value   TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, name)
);

CREATE TABLE IF NOT EXISTS email_send_authority_audit (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email_address   TEXT NOT NULL,
    user_id         TEXT,
    granted_by      TEXT,
    org_id          TEXT,
    action          TEXT NOT NULL CHECK (action IN ('granted', 'revoked', 'verified')),
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_email_audit_email ON email_send_authority_audit(email_address);
CREATE INDEX IF NOT EXISTS idx_email_audit_user ON email_send_authority_audit(user_id);
CREATE INDEX IF NOT EXISTS idx_email_audit_org ON email_send_authority_audit(org_id);

-- tenant_id isolation for _META_TABLES (SaaS multi-tenancy)
DO $$ BEGIN
    ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE domains ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE relationships ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE rls_rules ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE roles ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE tags ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE tag_assignments ADD COLUMN IF NOT EXISTS tenant_id UUID;
    ALTER TABLE tag_param_values ADD COLUMN IF NOT EXISTS tenant_id UUID;  -- REQ-1467
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- REQ-2155: the standard roles (org_admin, analyst, sandbox, platform_admin and the rest) are not
-- seeded here: init_schema writes every one as provisa.core.db.STANDARD_ROLES defines it, on every
-- run (db.assert_standard_roles), once the metadata has reconciled this schema.

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

-- Event substrate (REQ-940/941): the control-plane event table as a transactional outbox.
-- Injectors POST events here in the same transaction as the state change (atomic); table
-- processors CLAIM their work via event_status; repeaters fanout-read events by id cursor.
-- Mirrors provisa.core.schema_org.events.
CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,  -- ordering key AND repeater replay cursor
    source_table TEXT NOT NULL,          -- the data-source table or MV this event is ABOUT
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}',  -- cursor / changed rows / warn|error detail
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_event_type_check
        CHECK (event_type IN ('delta','append','replace','warn','error','quarantine','row_refresh'))
);

-- REQ-1865: row-materialize CDC-triggered background refresh event, added to a table that predates it.
DO $$ BEGIN
    ALTER TABLE events DROP CONSTRAINT IF EXISTS events_event_type_check;
    ALTER TABLE events ADD CONSTRAINT events_event_type_check
        CHECK (event_type IN ('delta','append','replace','warn','error','quarantine','row_refresh'));
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- One row per (event x dependent node): the fanout work item, dispatched from the SQLGlot lineage
-- and claimed exactly once. unclaimed -> claimed (heartbeat-leased) -> completed; a stale heartbeat
-- reverts to unclaimed. Mirrors schema_org.event_status.
CREATE TABLE IF NOT EXISTS event_status (
    event_id        BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    dependent_table TEXT NOT NULL,
    claim_status    TEXT NOT NULL DEFAULT 'unclaimed',
    processor_name  TEXT,                -- lease owner while claimed; the REQ-959 ownership-CAS key
    heartbeat_at    TIMESTAMPTZ,         -- lease; stale -> reaper reclaims
    deadline        TIMESTAMPTZ,         -- REQ-959 fire-by deadline (NULL = heartbeat lapse only)
    completed_at    TIMESTAMPTZ,
    PRIMARY KEY (event_id, dependent_table),
    CONSTRAINT event_status_claim_status_check
        CHECK (claim_status IN ('unclaimed','claimed','completed'))
);

-- Per-node freshness state (REQ-981/982/961): content hash of the last land (output gate),
-- the last probe token (input probe baseline), and the last-refresh timestamp/outcome the
-- periodic freshness contract PULLs (REQ-961). One row per node; upserted on each successful
-- land. Mirrors provisa.core.schema_org.node_freshness_state.
CREATE TABLE IF NOT EXISTS node_freshness_state (
    node            TEXT PRIMARY KEY,    -- the source-table / MV node key
    content_hash    TEXT,                -- REQ-981: hash of the last landed replace-shaped content
    probe_token     TEXT,                -- REQ-982: last probe token (watermark/hash/count baseline)
    last_refresh_at TIMESTAMPTZ,         -- REQ-961: instant of the last completed refresh (NULL = never)
    last_refresh_ok BOOLEAN,             -- REQ-961: outcome of that refresh (freshness-contract input)
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Where the last completed read of a mail source reached (REQ-1923): a token for each mailbox
-- (scope '') or each of its folders (scope = the folder's id); kind 'probe' or 'delta'.
-- Mirrors provisa.core.schema_org.mail_sync_state.
CREATE TABLE IF NOT EXISTS mail_sync_state (
    source_id   TEXT NOT NULL,
    account     TEXT NOT NULL,
    scope       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    token       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_id, account, scope, kind)
);

-- REQ-2054: each trigger's last run (a scheduled one, or one run on a table's change or event).
-- Mirrors provisa.core.schema_org.trigger_runs.
CREATE TABLE IF NOT EXISTS trigger_runs (
    trigger_id TEXT PRIMARY KEY,
    ran_at     TIMESTAMPTZ NOT NULL,
    outcome    TEXT NOT NULL,  -- succeeded | refused | failed
    error      TEXT,
    fired_by   TEXT            -- the table and event that fired it; NULL for a schedule
);

-- REQ-1926: state -- the store schema each session holds its temporary tables in, with the last
-- time its worker said the session is alive (see schema_org.py). Never part of the model.
CREATE TABLE IF NOT EXISTS session_temp_schemas (
    store_schema TEXT NOT NULL PRIMARY KEY,
    session_id   TEXT NOT NULL,
    beat_at      TIMESTAMPTZ NOT NULL
);

-- Replica state (REQ-1912, REQ-826): one row per replica of a source table, shared by every
-- worker and instance. ``promoted`` is the automatic-promotion decision (a Hot-N table past its
-- threshold); the replica build's own state is added to this same row. Keyed by the table's
-- registered identity, source first. Mirrors provisa.core.schema_org.replica_state.
CREATE TABLE IF NOT EXISTS replica_state (
    source_id    TEXT NOT NULL,
    schema_name  TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    promoted     BOOLEAN NOT NULL DEFAULT FALSE,  -- reads go to the replica once it is built
    promoted_at  TIMESTAMPTZ,                     -- when it was last promoted (NULL = never)
    -- REQ-1915: the replica's build. A replica exists when completed_at is set, whatever
    -- build_state says: a refresh that is requested, running or failed leaves the previous
    -- replica readable.
    build_state      TEXT NOT NULL DEFAULT 'idle'
        CONSTRAINT replica_state_build_state_check
        CHECK (build_state IN ('idle','requested','building','failed')),
    requested_at     TIMESTAMPTZ,
    requested_reason TEXT,          -- model | definition | hot | refresh | operator | read
    build_started_at TIMESTAMPTZ,
    build_holder     TEXT,          -- host:pid of the process building it
    build_method     TEXT,          -- engine_statement | stream_batches
    rows_copied      BIGINT,        -- progress of the running build; the total once complete
    completed_rows   BIGINT,        -- rows of the last whole build, kept while the next runs; NULL: none
    completed_at     TIMESTAMPTZ,   -- NULL: there is no replica
    next_refresh_at  TIMESTAMPTZ,   -- NULL: refreshed only on request
    content_hash     TEXT,          -- order-independent hash of the last completed build
    built_store      TEXT,          -- the store the last completed build was written into
    definition_hash  TEXT,          -- what the last completed build was built from
    built_columns    JSONB,         -- the columns the standing replica has
    model_stamp      BIGINT,        -- the model stamp of the process that asked for the build
    load_kind        TEXT,          -- bulk_stream | row_copy, of the running or last build
    -- What a running build is waiting out: {"by", "seconds", "until"} while its source's
    -- service is throttling the read. NULL: it is not waiting.
    build_wait       JSONB,
    -- How far a running build that lands a part at a time has got: {"done", "total", "unit",
    -- "first"} (mailboxes read to their end). NULL: not such a build.
    build_progress   JSONB,
    retired_at       TIMESTAMPTZ,   -- set when the model stops declaring it; dropped after a grace
    last_error       TEXT,
    last_error_code  TEXT,          -- REQ-1350: the failure's code, when its cause is a named one
    last_error_params JSONB,        -- and its params
    build_notes      JSONB,         -- what the last completed build had to say of its copy: [{code, params}]
    verified_at      TIMESTAMPTZ,   -- REQ-855: when its probe last found the source unchanged since the build
    failed_at        TIMESTAMPTZ,
    failed_attempts  INTEGER NOT NULL DEFAULT 0,  -- builds failed in a row since the last completed
    waiting_on       TEXT,          -- why a requested build did not start on the last pass (a code)
    feed_down_since  TIMESTAMPTZ,    -- REQ-1861: since when the change-feed listener is down; NULL = watching
    feed_error       TEXT,           -- REQ-1861: the server's reason while the listener is down
    delta_cursor     JSONB,          -- REQ-874: the stored delta cursor
    delta_skipped    TEXT,           -- REQ-874: why the last build was a whole rebuild, not a delta
    -- REQ-2015: the indexes of the standing replica, built after it lands: {"for": the landing
    -- (completed_at) they belong to, "wanted": [{name, columns, primary}], "built": [name],
    -- "failed": {name: {error, code, columns}}}. NULL: no landing has asked for any.
    index_state      JSONB,
    PRIMARY KEY (source_id, schema_name, table_name)
);

-- The replica builds that ended: one row a build that completed or failed, appended as its
-- record above is written. Bounded per replica: only its last REPLICA_BUILD_LOG_KEEP builds
-- are kept (federation/replica_state.py), pruned as each is written.
CREATE TABLE IF NOT EXISTS replica_build_log (
    id            BIGSERIAL PRIMARY KEY,
    source_id     TEXT NOT NULL,
    schema_name   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    started_at    TIMESTAMPTZ,     -- NULL: the build's start was not recorded
    finished_at   TIMESTAMPTZ NOT NULL,
    outcome       TEXT NOT NULL
        CONSTRAINT replica_build_log_outcome_check CHECK (outcome IN ('completed','failed')),
    method        TEXT,            -- engine_statement | stream_batches | delta
    rows_copied   BIGINT,          -- a completed build's rows
    error_code    TEXT,            -- a failed build's cause, when it is a named one
    error_message TEXT             -- a failed build's error, in English
);
CREATE INDEX IF NOT EXISTS idx_replica_build_log_replica
    ON replica_build_log(source_id, schema_name, table_name, id);

-- Preserved snapshots (REQ-983): a point-in-time dataset MATERIALIZED-AND-SEALED because it is NOT
-- reconstructible from current state + retained event history — the one PIT form that departs from
-- the NRT ideal. DECLARED (never inferred) and MUST be why-tagged. The row is the immutability
-- record — a second seal of the same name fails loud. Mirrors schema_org.preserved_snapshots.
CREATE TABLE IF NOT EXISTS preserved_snapshots (
    name         TEXT PRIMARY KEY,       -- the declared snapshot identity
    reason       TEXT NOT NULL,          -- REQ-983 mandatory why-tag (non-reconstructible reason)
    location     TEXT NOT NULL,          -- the sealed store-table location
    content_hash TEXT NOT NULL,          -- the frozen content digest (immutability proof)
    window_id    TEXT,                   -- optional calendar-addressable period this snapshot froze
    sealed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- REQ-2105: what each code source's calls to Provisa's LLM service used and cost, one row a call;
-- a source's monthly cap is read against the sum of its month's rows. Mirrors
-- schema_org.code_source_llm_usage.
CREATE TABLE IF NOT EXISTS code_source_llm_usage (
    id                 BIGSERIAL PRIMARY KEY,
    source_id          TEXT NOT NULL,
    month              TEXT NOT NULL,               -- YYYY-MM, UTC
    model              TEXT NOT NULL,
    input_tokens       BIGINT NOT NULL DEFAULT 0,
    output_tokens      BIGINT NOT NULL DEFAULT 0,
    cache_read_tokens  BIGINT NOT NULL DEFAULT 0,
    cache_write_tokens BIGINT NOT NULL DEFAULT 0,
    cost_usd           DOUBLE PRECISION,            -- NULL: the model has no polly.model_prices entry
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- REQ-2098: a code source's image builds: each one's state, what it made (a pushed image, or an
-- archive kept on the node that built it), and why it failed. Mirrors schema_org.code_source_builds.
CREATE TABLE IF NOT EXISTS code_source_builds (
    id           TEXT PRIMARY KEY,
    source_id    TEXT NOT NULL,
    code_hash    TEXT NOT NULL,
    arch         TEXT NOT NULL,          -- amd64 | arm64
    registry_id  TEXT,                   -- pushed there; NULL: downloaded
    status       TEXT NOT NULL,          -- building | done | failed
    image_ref    TEXT,
    digest       TEXT,
    archive_path TEXT,
    node         TEXT,                   -- the node that holds the archive
    error        TEXT,
    started_by   TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);


-- REQ-2106: a code source's own cache, shared by every worker of it in every process; a value is
-- JSON, gone at expires_at (NULL: kept until deleted or replaced). Mirrors schema_org.code_source_cache.
CREATE TABLE IF NOT EXISTS code_source_cache (
    source_id  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (source_id, key)
);

-- REQ-2106: a code source's own vector collection, which its code builds and searches; never a
-- governed table's data. vector is the embedding as a JSON array of floats.
-- Mirrors schema_org.code_source_vectors.
CREATE TABLE IF NOT EXISTS code_source_vectors (
    source_id     TEXT NOT NULL,
    item_id       TEXT NOT NULL,
    vector        TEXT NOT NULL,
    text          TEXT,
    item_metadata TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (source_id, item_id)
);


-- REQ-1303/REQ-1308: administrative acts inside THIS org — org_admin granted or revoked, a member
-- offboarded, a member's role changed. Separate from query_audit_log (which records data access):
-- an intervention by a platform_admin must be visible to the org afterward, and the query trail is
-- the wrong shelf for it. Per-org, because the entry belongs to the org it was performed against.
-- Never deleted (REQ-1312) — org deletion drops the schema, account deletion tombstones the actor.
-- Mirrors schema_org.admin_audit_log.
CREATE TABLE IF NOT EXISTS admin_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    action      TEXT NOT NULL,          -- grant_org_admin | revoke_org_admin | remove_member | leave_org | change_role
    actor_id    TEXT NOT NULL,          -- who performed it (tombstoned on account deletion)
    subject_id  TEXT NOT NULL,          -- who it was performed against
    detail      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Provisa config DB schema. V1: no migrations, this file is source of truth.

CREATE TABLE IF NOT EXISTS sources (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    host        TEXT NOT NULL,
    port        INTEGER NOT NULL,
    database    TEXT NOT NULL,
    username    TEXT NOT NULL,
    dialect     TEXT NOT NULL
    -- password never stored; resolved at runtime via secrets provider

);

CREATE TABLE IF NOT EXISTS domains (
    id          TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT ''
);

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
    UNIQUE (source_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS table_columns (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    column_name TEXT NOT NULL,
    visible_to  TEXT[] NOT NULL DEFAULT '{}',
    UNIQUE (table_id, column_name)
);

CREATE TABLE IF NOT EXISTS relationships (
    id              TEXT PRIMARY KEY,
    source_table_id INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    target_table_id INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    source_column   TEXT NOT NULL,
    target_column   TEXT NOT NULL,
    cardinality     TEXT NOT NULL CHECK (cardinality IN ('many-to-one', 'one-to-many'))
);

CREATE TABLE IF NOT EXISTS roles (
    id            TEXT PRIMARY KEY,
    capabilities  TEXT[] NOT NULL DEFAULT '{}',
    domain_access TEXT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS rls_rules (
    id          SERIAL PRIMARY KEY,
    table_id    INTEGER NOT NULL REFERENCES registered_tables(id) ON DELETE CASCADE,
    role_id     TEXT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    filter_expr TEXT NOT NULL,
    UNIQUE (table_id, role_id)
);

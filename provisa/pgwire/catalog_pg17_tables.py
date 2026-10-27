# Copyright (c) 2026 Kenneth Stott
# Canary: a12f2b9d-4237-410f-bc28-1d50ea005ae2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL 17 catalog relations served with PostgreSQL 17's own columns (REQ-2167).

Each relation here is a catalog DB table created from its PG 17 shape (catalog_pg17.RELATIONS),
in PG 17's column order, typed as the rest of the catalog stores those types. It holds what
Provisa has to list in it, derived from the relations the catalog already serves; a relation of
something Provisa has none of (publications, security labels, extended statistics) is empty.
"""

from __future__ import annotations

from provisa.pgwire.catalog_pg17 import RELATIONS, STATIC_ROWS

# A PG 17 column type, as the catalog DB stores it (the types catalog_types then describes it as).
_DUCKDB_TYPES: dict[str, str] = {
    "bigint": "BIGINT",
    "integer": "INTEGER",
    "smallint": "SMALLINT",
    "boolean": "BOOLEAN",
    "real": "FLOAT",
    "double precision": "DOUBLE",
    "numeric": "DECIMAL(38,9)",
    "oid": "INTEGER",
    "regproc": "INTEGER",
    "regtype": "INTEGER",
    "xid": "UINTEGER",
    "int2vector": "SMALLINT[]",
    "oidvector": "INTEGER[]",
    "name": "VARCHAR",
    "text": "VARCHAR",
    '"char"': "VARCHAR",
    "pg_node_tree": "VARCHAR",
    "pg_lsn": "VARCHAR",
    "inet": "VARCHAR",
    "aclitem": "VARCHAR",
    "information_schema.sql_identifier": "VARCHAR",
    "information_schema.character_data": "VARCHAR",
    "information_schema.yes_or_no": "VARCHAR",
    "information_schema.cardinal_number": "INTEGER",
    "information_schema.time_stamp": "TIMESTAMPTZ",
    "timestamp with time zone": "TIMESTAMPTZ",
    "interval": "INTERVAL",
    "bytea": "BLOB",
    # Only the always-empty statistics relations have these (STATISTICS_RELATIONS): no value of
    # one is ever served, so each is held as text (an array of them as a text list).
    "anyarray": "VARCHAR",
    "pg_ndistinct": "VARCHAR",
    "pg_dependencies": "VARCHAR",
    "pg_mcv_list": "VARCHAR",
    "pg_statistic[]": "VARCHAR[]",
}


def duckdb_type(pg_type: str) -> str:
    if pg_type in _DUCKDB_TYPES:
        return _DUCKDB_TYPES[pg_type]
    if pg_type.endswith("[]"):
        return f"{duckdb_type(pg_type[:-2])}[]"
    return _DUCKDB_TYPES[pg_type]


def served_table(schema: str, name: str) -> str:
    """The catalog DB table serving ``schema.name``."""
    return f"_{name}" if schema == "pg_catalog" else f"_is_{name}"


def create_relation(db, schema: str, name: str) -> None:
    """``schema.name``'s table, empty, with PG 17's columns in PG 17's order."""
    columns = ", ".join(f'"{col}" {duckdb_type(t)}' for col, t, _ in RELATIONS[(schema, name)])
    db.execute(f"CREATE TABLE {served_table(schema, name)} ({columns})")


# What pgAdmin, DBeaver and psql read to browse a table, a schema or a role (REQ-2167, tier 1):
# each relation, and the client queries that read it.
CLIENT_RELATIONS: tuple[tuple[str, str], ...] = (
    # psql \d (describe.c "Statistics objects"); pgAdmin statistics nodes.
    ("pg_catalog", "pg_statistic_ext"),
    # psql \d (describe.c "Publications"), \dRp+; pgAdmin publications.
    ("pg_catalog", "pg_publication_rel"),
    ("pg_catalog", "pg_publication_namespace"),
    # pgAdmin publications dependencies.sql.
    ("pg_catalog", "pg_publication_tables"),
    # pgAdmin properties.sql of tables, columns, types, domains, roles, tablespaces.
    ("pg_catalog", "pg_seclabel"),
    ("pg_catalog", "pg_seclabels"),
    ("pg_catalog", "pg_shseclabel"),
    # psql \ddp; pgAdmin schemas defacl.sql / properties.sql; DBeaver.
    ("pg_catalog", "pg_default_acl"),
    # psql \drds; pgAdmin databases get_variables.sql, roles sql.sql; DBeaver.
    ("pg_catalog", "pg_db_role_setting"),
    # pgAdmin row_security_policies properties.sql; DBeaver PostgreTable.
    ("pg_catalog", "pg_policies"),
    # pgAdmin roles sql.sql; DBeaver PostgreConstants.
    ("pg_catalog", "pg_authid"),
    # DBeaver PostgreSequence, PostgreMetaModel.
    ("pg_catalog", "pg_sequences"),
    # DBeaver PostgreRole.
    ("information_schema", "table_privileges"),
    ("information_schema", "routine_privileges"),
    # pgAdmin sqleditor view_base_table.sql.
    ("information_schema", "view_table_usage"),
)


# The statistics relations pgAdmin's dashboard and Statistics tabs and DBeaver read (REQ-2167, tier
# 2). Provisa keeps no statistics, so each is empty: no figure is made up.
STATISTICS_RELATIONS: tuple[tuple[str, str], ...] = tuple(
    ("pg_catalog", name)
    for name in (
        "pg_stat_user_functions",
        "pg_stat_all_tables",
        "pg_stat_all_indexes",
        "pg_statio_all_indexes",
        "pg_stat_replication",
        "pg_stats_ext",
        "pg_statio_all_tables",
        "pg_replication_slots",
        "pg_stat_database",
        "pg_stats",
        "pg_statistic_ext_data",
        "pg_statio_all_sequences",
        "pg_stat_subscription",
        "pg_prepared_xacts",
    )
)


# The relations of particular object types psql, pgAdmin and DBeaver list (REQ-2167, tier 3). Provisa
# runs no text search configuration, holds no large object and grants no parameter privilege, so
# those are empty; the available extensions are the installed ones, Provisa installing no other.
OBJECT_TYPE_RELATIONS: tuple[tuple[str, str], ...] = tuple(
    ("pg_catalog", name)
    for name in (
        # psql \dF, \dFp, \dFd, \dFt (describe.c listTSConfigs, listTSParsers, listTSDictionaries,
        # listTSTemplates); pgAdmin fts_configurations, fts_parsers, fts_dictionaries,
        # fts_templates.
        "pg_ts_config",
        "pg_ts_config_map",
        "pg_ts_parser",
        "pg_ts_dict",
        "pg_ts_template",
        # DBeaver PostgreDatabase.java:1275; pgAdmin extensions/sql/extensions.sql,
        # properties.sql.
        "pg_available_extensions",
        "pg_available_extension_versions",
        # psql \dl (describe.c listLargeObjects).
        "pg_largeobject_metadata",
        # psql \dconfig+ (describe.c describeConfigurationParameters).
        "pg_parameter_acl",
    )
)


def populate_client_relations(db) -> None:  # REQ-2167
    """Create the client, statistics and object type relations; list the installed extensions as
    the available ones, and the roles in pg_authid (never a password, as the
    catalog has none to give) and the sequences in pg_sequences, from pg_roles and pg_sequence."""
    for schema, name in (*CLIENT_RELATIONS, *STATISTICS_RELATIONS, *OBJECT_TYPE_RELATIONS):
        create_relation(db, schema, name)
    db.execute(
        "INSERT INTO _pg_authid SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, "
        "rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, NULL, "
        "rolvaliduntil FROM _pg_roles"
    )
    db.execute(
        "INSERT INTO _pg_sequences SELECT n.nspname, c.relname, r.rolname, s.seqtypid, "
        "s.seqstart, s.seqmin, s.seqmax, s.seqincrement, s.seqcycle, s.seqcache, NULL "
        "FROM _pg_sequence s JOIN _pg_class c ON c.oid = s.seqrelid "
        "JOIN _pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN _pg_roles r ON r.oid = c.relowner"
    )
    db.execute(
        "INSERT INTO _pg_available_extensions "
        "SELECT extname, extversion, extversion, NULL FROM _pg_extension"
    )
    db.execute(
        "INSERT INTO _pg_available_extension_versions SELECT e.extname, e.extversion, TRUE, "
        "FALSE, FALSE, e.extrelocatable, n.nspname, NULL, NULL "
        "FROM _pg_extension e JOIN _pg_namespace n ON n.oid = e.extnamespace"
    )


# What each PostgreSQL 17 relation no client queries holds (REQ-2167, tier 4), where Provisa has
# something to list in it: the rows derived, as PostgreSQL derives them, from relations the catalog
# already serves. Any other is empty.
_DERIVED_ROWS: dict[str, str] = {
    "_pg_shadow": (
        "SELECT rolname, oid, rolcreatedb, rolsuper, rolreplication, rolbypassrls, NULL, "
        "rolvaliduntil, rolconfig FROM _pg_roles WHERE rolcanlogin"
    ),
    "_pg_group": (
        "SELECT r.rolname, r.oid, (SELECT list(m.member) FROM _pg_auth_members m "
        "WHERE m.roleid = r.oid) FROM _pg_roles r WHERE NOT r.rolcanlogin"
    ),
    # A key's index, with the definition PostgreSQL writes for it (pg_get_indexdef).
    "_pg_indexes": (
        "SELECT n.nspname, c.relname, ci.relname, NULL, "
        "'CREATE ' || CASE WHEN i.indisunique THEN 'UNIQUE ' ELSE '' END || 'INDEX ' || "
        "ci.relname || ' ON ' || n.nspname || '.' || c.relname || ' USING btree (' || "
        "(SELECT string_agg(a.attname, ', ' ORDER BY k.ordinal) FROM "
        "(SELECT unnest(i.indkey) AS attnum, generate_subscripts(i.indkey, 1) AS ordinal) k "
        "JOIN _pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) || ')' "
        "FROM _pg_index i JOIN _pg_class c ON c.oid = i.indrelid "
        "JOIN _pg_class ci ON ci.oid = i.indexrelid "
        "JOIN _pg_namespace n ON n.oid = c.relnamespace"
    ),
    # Provisa keeps no view's text (pg_get_viewdef): the definition is NULL.
    "_pg_views": (
        "SELECT n.nspname, c.relname, r.rolname, NULL FROM _pg_class c "
        "JOIN _pg_namespace n ON n.oid = c.relnamespace "
        "LEFT JOIN _pg_roles r ON r.oid = c.relowner WHERE c.relkind = 'v'"
    ),
    # PostgreSQL 17 lists each NOT NULL column of a table as a check constraint, named
    # <namespace oid>_<table oid>_<attnum>_not_null.
    "_is_check_constraints": (
        "SELECT (SELECT datname FROM _pg_database), n.nspname, "
        "n.oid || '_' || c.oid || '_' || a.attnum || '_not_null', "
        "a.attname || ' IS NOT NULL' FROM _pg_attribute a "
        "JOIN _pg_class c ON c.oid = a.attrelid "
        "JOIN _pg_namespace n ON n.oid = c.relnamespace "
        "WHERE a.attnotnull AND a.attnum > 0 AND c.relkind IN ('r', 'p') "
        "AND n.nspname NOT IN ('pg_catalog', 'information_schema')"
    ),
    # The table a key constraint is on, or a foreign key references.
    "_is_constraint_table_usage": (
        "SELECT (SELECT datname FROM _pg_database), nr.nspname, r.relname, "
        "(SELECT datname FROM _pg_database), nc.nspname, k.conname FROM _pg_constraint k "
        "JOIN _pg_namespace nc ON nc.oid = k.connamespace "
        "JOIN _pg_class r ON r.oid = CASE WHEN k.contype = 'f' THEN k.confrelid "
        "ELSE k.conrelid END "
        "JOIN _pg_namespace nr ON nr.oid = r.relnamespace WHERE k.contype IN ('p', 'u', 'f')"
    ),
    # Its columns: a key's own, or the referenced columns of a foreign key.
    "_is_constraint_column_usage": (
        "SELECT (SELECT datname FROM _pg_database), nr.nspname, r.relname, a.attname, "
        "(SELECT datname FROM _pg_database), nc.nspname, k.conname FROM _pg_constraint k "
        "JOIN _pg_namespace nc ON nc.oid = k.connamespace "
        "JOIN _pg_class r ON r.oid = CASE WHEN k.contype = 'f' THEN k.confrelid "
        "ELSE k.conrelid END "
        "JOIN _pg_namespace nr ON nr.oid = r.relnamespace "
        "JOIN _pg_attribute a ON a.attrelid = r.oid AND list_contains("
        "CASE WHEN k.contype = 'f' THEN k.confkey ELSE k.conkey END, a.attnum) "
        "WHERE k.contype IN ('p', 'u', 'f')"
    ),
    # The database's encoding, UTF8; its collation where pg_collation has one matching it.
    "_is_character_sets": (
        "SELECT NULL, NULL, 'UTF8', 'UCS', 'UTF8', "
        "CASE WHEN nc.nspname IS NOT NULL THEN d.datname END, nc.nspname, c.collname "
        "FROM _pg_database d LEFT JOIN (_pg_collation c JOIN _pg_namespace nc "
        "ON c.collnamespace = nc.oid) ON d.datcollate = c.collcollate "
        "AND d.datctype = c.collctype ORDER BY length(c.collname) DESC, c.collname LIMIT 1"
    ),
    "_is_information_schema_catalog_name": "SELECT datname FROM _pg_database",
}


def populate_unqueried_relations(db, relations) -> None:  # REQ-2167
    """Serve every remaining PostgreSQL 17 relation with its columns: the rows derived from the
    catalog where Provisa has something to list (_DERIVED_ROWS), PostgreSQL's own rows for the SQL
    conformance tables, and otherwise none (no statistic, setting or server internal is made
    up)."""
    for schema, name in relations:
        create_relation(db, schema, name)
        table = served_table(schema, name)
        if table in _DERIVED_ROWS:
            db.execute(f"INSERT INTO {table} {_DERIVED_ROWS[table]}")
    for name, rows in STATIC_ROWS.items():
        width = len(RELATIONS[("information_schema", name)])
        db.executemany(f"INSERT INTO _is_{name} VALUES ({', '.join(['?'] * width)})", list(rows))

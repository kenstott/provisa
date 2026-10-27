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

from provisa.pgwire.catalog_pg17 import RELATIONS

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


def populate_client_relations(db) -> None:  # REQ-2167
    """Create the client and statistics relations; list the roles in pg_authid (never a password, as the
    catalog has none to give) and the sequences in pg_sequences, from pg_roles and pg_sequence."""
    for schema, name in (*CLIENT_RELATIONS, *STATISTICS_RELATIONS):
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

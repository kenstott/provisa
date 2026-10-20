# Copyright (c) 2026 Kenneth Stott
# Canary: dd213021-d18a-442f-a08a-277241a2f208
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL catalog of a built host's functions (REQ-2100), as PostgreSQL keeps it.

An engine attaching the host reads its tables from ``pg_catalog`` and ``information_schema``
(a JDBC driver's metadata, DuckDB's postgres scanner, postgres_fdw's IMPORT FOREIGN SCHEMA), and
joins them by PostgreSQL's own object ids. DuckDB's built-in ``pg_catalog`` keeps DuckDB's type ids
instead, so the front end keeps the catalog itself: tables of its in-memory DuckDB under schemas
``pgc`` and ``pgis``, holding each function as a table of schema ``public`` -- its columns the
code's declared names and its ``_nf_`` parameters, typed with PostgreSQL's type ids -- and a
client's catalog statement is rewritten to read them (:func:`rewrite`). ``pg_class.reltuples`` is
each function's row count as last answered, -1 before its first call.
"""

# Requirements: REQ-2100
from __future__ import annotations

from typing import Any

import sqlglot
from sqlglot import exp

PUBLIC = "public"
DATABASE = "code"
OWNER = "provisa"
#: What the server says it is: a PostgreSQL client reads its version from this.
VERSION = "PostgreSQL 16.0 (Provisa code host)"

_SYSTEM_NAMESPACES = {"pg_catalog": 11, "information_schema": 13000}
#: The first oid of the schemas the functions' tables appear in; public's, as PostgreSQL's.
_FIRST_SCHEMA_OID = 2200

#: PostgreSQL's base types: oid, name, length, category, its array's oid, array element oid.
_BASE_TYPES = [
    (16, "bool", 1, "B", 1000, 0),
    (17, "bytea", -1, "U", 1001, 0),
    (18, "char", 1, "Z", 1002, 0),
    (19, "name", 64, "S", 1003, 0),
    (20, "int8", 8, "N", 1016, 0),
    (21, "int2", 2, "N", 1005, 0),
    (23, "int4", 4, "N", 1007, 0),
    (25, "text", -1, "S", 1009, 0),
    (26, "oid", 4, "N", 1028, 0),
    (114, "json", -1, "U", 199, 0),
    (700, "float4", 4, "N", 1021, 0),
    (701, "float8", 8, "N", 1022, 0),
    (1042, "bpchar", -1, "S", 1014, 0),
    (1043, "varchar", -1, "S", 1015, 0),
    (1082, "date", 4, "D", 1182, 0),
    (1083, "time", 8, "D", 1183, 0),
    (1114, "timestamp", 8, "D", 1115, 0),
    (1184, "timestamptz", 8, "D", 1185, 0),
    (1700, "numeric", -1, "N", 1231, 0),
    (2950, "uuid", 16, "U", 2951, 0),
    (3802, "jsonb", -1, "U", 3807, 0),
]
_ARRAYS = [(t[4], "_" + t[1], -1, "A", 0, t[0]) for t in _BASE_TYPES]
#: A function's column type, by IR type: its PostgreSQL type's oid and name.
_IR_TYPES = {
    "text": (25, "text"),
    "double": (701, "float8"),
    "bigint": (20, "int8"),
    "boolean": (16, "bool"),
    "timestamp": (1184, "timestamptz"),
    "date": (1082, "date"),
    "numeric": (1700, "numeric"),
    "json": (114, "json"),
    "list": (1009, "_text"),
}
#: information_schema's name for a type.
_STANDARD_NAMES = {
    "text": "text",
    "float8": "double precision",
    "int8": "bigint",
    "bool": "boolean",
    "timestamptz": "timestamp with time zone",
    "date": "date",
    "numeric": "numeric",
    "json": "json",
    "_text": "ARRAY",
}

_FIRST_TABLE_OID = 16384

_TABLES = {
    "pg_namespace": "oid BIGINT, nspname VARCHAR, nspowner BIGINT, nspacl VARCHAR[]",
    "pg_class": (
        "oid BIGINT, relname VARCHAR, relnamespace BIGINT, reltype BIGINT, reloftype BIGINT, "
        "relowner BIGINT, relam BIGINT, relfilenode BIGINT, reltablespace BIGINT, relpages "
        "INTEGER, reltuples DOUBLE, relallvisible INTEGER, reltoastrelid BIGINT, relhasindex "
        "BOOLEAN, relisshared BOOLEAN, relpersistence VARCHAR, relkind VARCHAR, relnatts SMALLINT, "
        "relchecks SMALLINT, relhasrules BOOLEAN, relhastriggers BOOLEAN, relhassubclass BOOLEAN, "
        "relrowsecurity BOOLEAN, relforcerowsecurity BOOLEAN, relispopulated BOOLEAN, relreplident "
        "VARCHAR, relispartition BOOLEAN, relacl VARCHAR[], reloptions VARCHAR[]"
    ),
    "pg_attribute": (
        "attrelid BIGINT, attname VARCHAR, atttypid BIGINT, attstattarget INTEGER, attlen "
        "SMALLINT, attnum SMALLINT, attndims INTEGER, attcacheoff INTEGER, atttypmod INTEGER, "
        "attbyval BOOLEAN, attstorage VARCHAR, attalign VARCHAR, attnotnull BOOLEAN, atthasdef "
        "BOOLEAN, atthasmissing BOOLEAN, attidentity VARCHAR, attgenerated VARCHAR, attisdropped "
        "BOOLEAN, attislocal BOOLEAN, attinhcount INTEGER, attcollation BIGINT, attacl VARCHAR[], "
        "attoptions VARCHAR[]"
    ),
    "pg_type": (
        "oid BIGINT, typname VARCHAR, typnamespace BIGINT, typowner BIGINT, typlen SMALLINT, "
        "typbyval BOOLEAN, typtype VARCHAR, typcategory VARCHAR, typispreferred BOOLEAN, "
        "typisdefined BOOLEAN, typdelim VARCHAR, typrelid BIGINT, typelem BIGINT, typarray BIGINT, "
        "typinput VARCHAR, typoutput VARCHAR, typreceive VARCHAR, typsend VARCHAR, typnotnull "
        "BOOLEAN, typbasetype BIGINT, typtypmod INTEGER, typndims INTEGER, typcollation BIGINT, "
        "typdefault VARCHAR"
    ),
    "pg_description": "objoid BIGINT, classoid BIGINT, objsubid INTEGER, description VARCHAR",
    "pg_constraint": (
        "oid BIGINT, conname VARCHAR, connamespace BIGINT, contype VARCHAR, condeferrable BOOLEAN, "
        "condeferred BOOLEAN, convalidated BOOLEAN, conrelid BIGINT, contypid BIGINT, conindid "
        "BIGINT, confrelid BIGINT, confupdtype VARCHAR, confdeltype VARCHAR, confmatchtype VARCHAR, "
        "conkey SMALLINT[], confkey SMALLINT[]"
    ),
    "pg_index": (
        "indexrelid BIGINT, indrelid BIGINT, indnatts SMALLINT, indnkeyatts SMALLINT, indisunique "
        "BOOLEAN, indnullsnotdistinct BOOLEAN, indisprimary BOOLEAN, indisexclusion BOOLEAN, "
        "indimmediate BOOLEAN, indisclustered BOOLEAN, indisvalid BOOLEAN, indcheckxmin BOOLEAN, "
        "indisready BOOLEAN, indislive BOOLEAN, indisreplident BOOLEAN, indkey SMALLINT[], "
        "indcollation BIGINT[], indclass BIGINT[], indoption SMALLINT[], indexprs VARCHAR, "
        "indpred VARCHAR"
    ),
    "pg_indexes": "schemaname VARCHAR, tablename VARCHAR, indexname VARCHAR, indexdef VARCHAR",
    "pg_attrdef": "oid BIGINT, adrelid BIGINT, adnum SMALLINT, adbin VARCHAR",
    "pg_inherits": "inhrelid BIGINT, inhparent BIGINT, inhseqno INTEGER",
    "pg_enum": "oid BIGINT, enumtypid BIGINT, enumsortorder DOUBLE, enumlabel VARCHAR",
    "pg_settings": "name VARCHAR, setting VARCHAR, unit VARCHAR, category VARCHAR",
    "pg_database": (
        "oid BIGINT, datname VARCHAR, datdba BIGINT, encoding INTEGER, datcollate VARCHAR, "
        "datctype VARCHAR, datistemplate BOOLEAN, datallowconn BOOLEAN"
    ),
    "pg_roles": "oid BIGINT, rolname VARCHAR, rolsuper BOOLEAN",
    "pg_user": "usesysid BIGINT, usename VARCHAR, usesuper BOOLEAN",
    "pg_tables": (
        "schemaname VARCHAR, tablename VARCHAR, tableowner VARCHAR, tablespace VARCHAR, hasindexes "
        "BOOLEAN, hasrules BOOLEAN, hastriggers BOOLEAN, rowsecurity BOOLEAN"
    ),
    "pg_views": "schemaname VARCHAR, viewname VARCHAR, viewowner VARCHAR, definition VARCHAR",
    "pg_matviews": "schemaname VARCHAR, matviewname VARCHAR, definition VARCHAR",
    "pg_am": "oid BIGINT, amname VARCHAR, amhandler VARCHAR, amtype VARCHAR",
    "pg_proc": "oid BIGINT, proname VARCHAR, pronamespace BIGINT, prokind VARCHAR",
    "pg_collation": "oid BIGINT, collname VARCHAR, collnamespace BIGINT",
    "pg_tablespace": "oid BIGINT, spcname VARCHAR",
    "pg_extension": "oid BIGINT, extname VARCHAR, extversion VARCHAR",
    "pg_foreign_table": "ftrelid BIGINT, ftserver BIGINT",
    "pg_partitioned_table": "partrelid BIGINT",
    # A function's rows have no column statistics: its table's row count is pg_class.reltuples.
    "pg_stats": (
        "schemaname VARCHAR, tablename VARCHAR, attname VARCHAR, inherited BOOLEAN, null_frac "
        "DOUBLE, avg_width INTEGER, n_distinct DOUBLE, most_common_vals VARCHAR[], "
        "most_common_freqs DOUBLE[], histogram_bounds VARCHAR[], correlation DOUBLE"
    ),
}
_STANDARD = {
    "tables": (
        "table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, table_type VARCHAR"
    ),
    "columns": (
        "table_catalog VARCHAR, table_schema VARCHAR, table_name VARCHAR, column_name VARCHAR, "
        "ordinal_position INTEGER, column_default VARCHAR, is_nullable VARCHAR, data_type VARCHAR, "
        "character_maximum_length INTEGER, numeric_precision INTEGER, numeric_scale INTEGER, "
        "datetime_precision INTEGER, udt_schema VARCHAR, udt_name VARCHAR"
    ),
    "schemata": "catalog_name VARCHAR, schema_name VARCHAR, schema_owner VARCHAR",
}

_SETTINGS = [
    ("server_version", "16.0"),
    ("server_version_num", "160000"),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("standard_conforming_strings", "on"),
    ("TimeZone", "UTC"),
    ("DateStyle", "ISO, MDY"),
    ("max_identifier_length", "63"),
]


def _insert(db: Any, table: str, rows: list[tuple]) -> None:
    if rows:
        marks = ", ".join("?" for _ in rows[0])
        db.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)


def build(db: Any, names: dict, schemas: tuple[str, ...]) -> dict[str, list[int]]:
    """The catalog of ``names``' functions in ``db``, each a table of every one of ``schemas``
    (the schemas the attaching Provisa registered the source's tables under); each function's
    tables' oids."""
    namespaces = {
        **_SYSTEM_NAMESPACES,
        **{schema: _FIRST_SCHEMA_OID + i for i, schema in enumerate(schemas)},
    }
    db.execute("CREATE SCHEMA IF NOT EXISTS pgc")
    db.execute("CREATE SCHEMA IF NOT EXISTS pgis")
    for table, columns in _TABLES.items():
        db.execute(f"CREATE TABLE pgc.{table} ({columns})")
    for table, columns in _STANDARD.items():
        db.execute(f"CREATE TABLE pgis.{table} ({columns})")
    _insert(db, "pgc.pg_namespace", [(oid, n, 10, None) for n, oid in namespaces.items()])
    types = []
    for oid, name, length, category, array, element in [*_BASE_TYPES, *_ARRAYS]:
        types.append(
            (oid, name, 11, 10, length, length in (1, 2, 4, 8), "b", category, False, True, ",",
             0, element, array, f"{name}in", f"{name}out", f"{name}recv", f"{name}send", False,
             0, -1, 0, 0, None)
        )  # fmt: skip
    oids: dict[str, list[int]] = {}
    classes, attributes, standard_tables, standard_columns, tables_view = [], [], [], [], []
    placed = [(s, f, t) for s in schemas for f, t in sorted(names.items())]
    for index, (schema, function, table) in enumerate(placed):
        oid = _FIRST_TABLE_OID + 2 * index
        rowtype = oid + 1
        oids.setdefault(function, []).append(oid)
        columns = [*table["columns"], *table["parameters"]]
        classes.append(
            (oid, function, namespaces[schema], rowtype, 0, 10, 2, oid, 0, 0, -1.0, 0, 0, False,
             False, "p", "r", len(columns), 0, False, False, False, False, False, True, "d", False,
             None, None)
        )  # fmt: skip
        types.append(
            (rowtype, function, namespaces[schema], 10, -1, False, "c", "C", False, True, ",",
             oid, 0, 0, "record_in", "record_out", "record_recv", "record_send", False, 0, -1, 0,
             0, None)
        )  # fmt: skip
        standard_tables.append((DATABASE, schema, function, "BASE TABLE"))
        tables_view.append((schema, function, OWNER, None, False, False, False, False))
        for number, column in enumerate(columns, start=1):
            type_oid, type_name = _IR_TYPES[column["type"]]
            length = next(t[2] for t in [*_BASE_TYPES, *_ARRAYS] if t[0] == type_oid)
            attributes.append(
                (oid, column["name"], type_oid, -1, length, number, 1 if column["type"] == "list"
                 else 0, -1, -1, length in (1, 2, 4, 8), "p", "d", False, False, False, "", "",
                 False, True, 0, 0, None, None)
            )  # fmt: skip
            standard_columns.append(
                (DATABASE, schema, function, column["name"], number, None, "YES",
                 _STANDARD_NAMES[type_name], None, None, None, None, "pg_catalog", type_name)
            )  # fmt: skip
    _insert(db, "pgc.pg_type", types)
    _insert(db, "pgc.pg_class", classes)
    _insert(db, "pgc.pg_attribute", attributes)
    _insert(db, "pgc.pg_tables", tables_view)
    _insert(db, "pgc.pg_settings", [(n, v, None, "") for n, v in _SETTINGS])
    _insert(db, "pgc.pg_database", [(1, DATABASE, 10, 6, "C", "C", False, True)])
    _insert(db, "pgc.pg_roles", [(10, OWNER, False)])
    _insert(db, "pgc.pg_user", [(10, OWNER, False)])
    _insert(db, "pgis.tables", standard_tables)
    _insert(db, "pgis.columns", standard_columns)
    _insert(db, "pgis.schemata", [(DATABASE, n, OWNER) for n in namespaces])
    _macros(db)
    return oids


def _macros(db: Any) -> None:
    """The PostgreSQL functions catalog statements call, over the catalog's own tables."""
    for definition in (
        "pgc_format_type(t, m) AS (SELECT CASE WHEN typcategory = 'A' THEN "
        "substr(typname, 2) || '[]' ELSE typname END FROM pgc.pg_type WHERE oid = t)",
        "pgc_regclass(name) AS (SELECT oid FROM pgc.pg_class WHERE relname = "
        "split_part(replace(name, '\"', ''), '.', -1))",
        "pgc_relname(o) AS (SELECT relname FROM pgc.pg_class WHERE oid = o)",
        "pgc_typname(o) AS (SELECT typname FROM pgc.pg_type WHERE oid = o)",
        # A function's table has no key or index: an array of key columns is only ever bound.
        "pgc_expandarray(a) AS {'x': a[1], 'n': 1}",
    ):
        db.execute(f"CREATE MACRO {definition}")


#: A PostgreSQL function, by name, as the catalog answers it: a macro, a constant, or None for
#: DuckDB's own.
_FUNCTIONS: dict[str, Any] = {
    "format_type": "pgc_format_type",
    "to_regclass": "pgc_regclass",
    "_pg_expandarray": "pgc_expandarray",
}
#: Functions whose answer is the same whatever they are given: no comments, no definitions (a
#: function's table has no index, constraint or default), every object visible and permitted.
_ANSWERED = {
    **dict.fromkeys(
        ("pg_get_expr", "col_description", "obj_description", "shobj_description",
         "pg_get_indexdef", "pg_get_constraintdef", "pg_get_viewdef"),
        exp.Null(),
    ),
    **dict.fromkeys(
        ("pg_table_is_visible", "pg_type_is_visible", "has_table_privilege",
         "has_schema_privilege", "has_database_privilege", "has_column_privilege",
         "has_any_column_privilege"),
        exp.true(),
    ),
    "pg_get_userbyid": exp.Literal.string(OWNER),
    "pg_encoding_to_char": exp.Literal.string("UTF8"),
}  # fmt: skip
_CONSTANTS = {
    "version": exp.Literal.string(VERSION),
    "current_database": exp.Literal.string(DATABASE),
    "current_catalog": exp.Literal.string(DATABASE),
    "current_user": exp.Literal.string(OWNER),
    "session_user": exp.Literal.string(OWNER),
    "user": exp.Literal.string(OWNER),
    "pg_backend_pid": exp.Literal.number(1),
}


def _arguments(node: exp.Func) -> list[exp.Expr]:
    """A function call's arguments, in order, whatever node sqlglot made of it."""
    out: list[exp.Expr] = []
    for key in node.arg_types:
        value = node.args.get(key)
        if isinstance(value, list):
            out.extend(value)
        elif isinstance(value, exp.Expr):
            out.append(value)
    return out


def _function_name(node: exp.Func) -> str:
    return (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()


def rewrite(tree: exp.Expr, schemas: tuple[str, ...]) -> exp.Expr:
    """A client's catalog statement, reading the catalog's own tables and functions;
    ``current_schema()`` the first of the session's ``schemas``."""

    def _catalog(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Dot) and isinstance(node.this, (exp.Column, exp.Identifier)):
            # pg_catalog.format_type(...): the function itself.
            schema = node.this.name.lower()
            if schema in ("pg_catalog", "information_schema") and isinstance(
                node.expression, exp.Func
            ):
                return _catalog(node.expression)
        if isinstance(node, exp.Table):
            schema, name = node.db.lower(), node.name.lower()
            if name in _TABLES and schema in ("", "pg_catalog"):
                node.set("db", exp.to_identifier("pgc"))
                node.set("this", exp.to_identifier(name))
                if not node.alias:
                    node.set("alias", exp.TableAlias(this=exp.to_identifier(name)))
            elif schema == "information_schema" and name in _STANDARD:
                node.set("db", exp.to_identifier("pgis"))
                if not node.alias:
                    node.set("alias", exp.TableAlias(this=exp.to_identifier(name)))
            return node
        if isinstance(node, exp.Column) and node.table.lower() == "pg_catalog":
            node.set("table", None)
            return node
        if isinstance(node, exp.Cast):
            target = node.to.sql(dialect="postgres").lower()
            if target in ("regclass", "pg_catalog.regclass"):
                inner = node.this
                if isinstance(inner, exp.Literal) and inner.is_string:
                    return exp.Anonymous(this="pgc_regclass", expressions=[inner])
                return exp.Anonymous(this="pgc_relname", expressions=[inner])
            if target in ("regtype", "pg_catalog.regtype"):
                return exp.Anonymous(this="pgc_typname", expressions=[node.this])
            if target in ("oid", "regproc", "regnamespace", "xid", "tid"):
                return exp.cast(node.this, "BIGINT")
            if target in ("name", "bpchar", "pg_catalog.name"):
                return exp.cast(node.this, "VARCHAR")
            return node
        if isinstance(node, exp.Func):
            name = _function_name(node)
            if name == "current_schema":
                return exp.Literal.string(schemas[0])
            if name in _CONSTANTS:
                return _CONSTANTS[name].copy()
            if name in _ANSWERED:
                return _ANSWERED[name].copy()
            if name in _FUNCTIONS:
                return exp.Anonymous(this=_FUNCTIONS[name], expressions=_arguments(node))
        return node

    return tree.transform(_catalog)


def parse(sql: str) -> exp.Expr:
    return sqlglot.parse_one(sql, read="postgres")

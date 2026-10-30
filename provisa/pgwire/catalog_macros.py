# Copyright (c) 2026 Kenneth Stott
# Canary: e35899de-80d7-421f-b244-7b32b6337b08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The catalog functions psql, pgAdmin and DBeaver call that the catalog DB answers as macros
over its own relations (REQ-2167, PGWIRE'S CATALOG FUNCTIONS): each as PostgreSQL answers it for
what the catalog holds. A set-returning function is a table macro, and where a client calls it in
a select list too, a scalar macro of the same name unnests its rows."""

# Requirements: REQ-2167

from __future__ import annotations

from provisa.federation.duckdb_pg_functions import register as register_pg_functions
from provisa.pgwire.catalog_pg17_overloads import PG_KEYWORDS, TS_TOKEN_TYPES
from provisa.pgwire.catalog_rewrite import _PG_TYPE_DISPLAY_NAMES, next_txid
from provisa.pgwire.catalog_server_answers import PG_ONLY_TYPES, macros as server_macros

# The schemas on the search path (catalog_data's search_path, and pg_catalog): an object in one is
# named unqualified, as PostgreSQL names a visible object.
_VISIBLE = "('public', 'pg_catalog')"

# A macro's argument is written into its body as the caller wrote it (c.oid), so every relation
# the body reads has an alias no caller writes (_c, _n, ...), and the argument is taken in a
# FROM-less select (_a) before the body reads a relation with it.


def quoted_identifier(name: str) -> str:
    """A name as quote_ident writes it: bare when it is a lowercase identifier."""
    return (
        f"CASE WHEN regexp_full_match({name}, '[a-z_][a-z0-9_$]*') THEN {name} "
        f"ELSE '\"' || replace({name}, '\"', '\"\"') || '\"' END"
    )


def _type_name(oid: str, qualified: bool = False) -> str:
    """The name of the type ``oid`` (an expression over the body's own columns) as format_type
    writes it; ``qualified``, as format_type_be_qualified does (a type with an SQL name of its own
    stays unqualified)."""
    cases = " ".join(
        f"WHEN '{internal}' THEN '{display}'"
        for internal, display in _PG_TYPE_DISPLAY_NAMES.items()
    )
    other = (
        f"{quoted_identifier('_tn.nspname')} || '.' || {quoted_identifier('_ty.typname')}"
        if qualified
        else "_ty.typname"
    )
    return (
        f"(SELECT CASE _ty.typname {cases} ELSE {other} END FROM _pg_type _ty "
        f"JOIN _pg_namespace _tn ON _tn.oid = _ty.typnamespace WHERE _ty.oid = {oid})"
    )


def type_display_name(oid: str) -> str:
    """The name of the type ``oid`` (an SQL expression) as format_type writes it."""
    return _type_name(oid)


def _relation_kind(relkind: str) -> str:
    return (
        f"CASE {relkind} WHEN 'r' THEN 'table' WHEN 'p' THEN 'table' WHEN 'v' THEN 'view' "
        "WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'I' THEN 'index' "
        "WHEN 'S' THEN 'sequence' WHEN 'f' THEN 'foreign table' WHEN 'c' THEN 'composite type' "
        "WHEN 't' THEN 'TOAST table' END"
    )


def _type_names(vector: str, separator: str, qualified: bool) -> str:
    """The names of a vector of type oids, in order, joined by ``separator``."""
    return (
        f"(SELECT coalesce(string_agg({_type_name('_v._k', qualified)}, '{separator}' "
        f"ORDER BY _v._i), '') FROM (SELECT unnest({vector}) AS _k, "
        f"generate_subscripts({vector}, 1) AS _i) _v)"
    )


def _partition_relation(columns: str) -> str:
    """The relation ``rel`` names when it can be a partition (a table or an index): no relation
    is partitioned, so it is its own partition tree, a leaf at level 0, and its own only
    ancestor."""
    return (
        f"SELECT {columns} FROM (SELECT rel AS _k) _a JOIN _pg_class _c ON _c.oid = _a._k "
        "WHERE _c.relkind IN ('r', 'p', 'i', 'I')"
    )


def _set_returning(name: str, params: str, select: str, columns: str) -> tuple[str, str]:
    """A set-returning function's two macros (its FROM form and its select-list form, unnesting
    its rows), over ``select``, whose columns are ``columns``."""
    fields = ", ".join(f"{c} := _r.{c}" for c in columns.split(", "))
    return (
        f"CREATE MACRO {name}({params}) AS TABLE {select}",
        f"CREATE MACRO {name}({params}) AS unnest((SELECT list(struct_pack({fields})) "
        f"FROM ({select}) _r))",
    )


def _literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value).lower()


def _values(rows: tuple[tuple, ...]) -> str:
    return ", ".join("(" + ", ".join(_literal(v) for v in row) + ")" for row in rows)


_KEYWORDS_SELECT = (
    f"SELECT * FROM (VALUES {_values(PG_KEYWORDS)}) _k(word, catcode, barelabel, catdesc, baredesc)"
)
# The default parser, the one parser PostgreSQL ships, by its oid or its name.
_TOKENS_SELECT = (
    f"SELECT * FROM (VALUES {_values(TS_TOKEN_TYPES)}) _t(tokid, alias, description) "
    "WHERE CAST(parser AS VARCHAR) IN ('3722', 'default', 'pg_catalog.default')"
)
_SETTINGS_SELECT = "SELECT * FROM _pg_settings"

_MACROS = (
    # The database's encoding, and an encoding's number: UTF8 is 6, a name naming none -1.
    "CREATE MACRO getdatabaseencoding() AS 'UTF8'",
    "CREATE MACRO pg_char_to_encoding(name) AS CASE "
    "WHEN lower(regexp_replace(name, '[^A-Za-z0-9]', '', 'g')) IN ('utf8', 'unicode') THEN 6 "
    "WHEN name IS NOT NULL THEN -1 END",
    "CREATE MACRO pg_statistics_obj_is_visible(oid) AS true",
    # Provisa keeps no ACLs, no sequences and no cursors.
    "CREATE MACRO acldefault(objtype, owner) AS CAST(NULL AS VARCHAR[])",
    "CREATE MACRO pg_sequence_last_value(seq) AS CAST(NULL AS BIGINT)",
    "CREATE MACRO pg_cursor() AS TABLE SELECT CAST(NULL AS VARCHAR) AS name, "
    "CAST(NULL AS VARCHAR) AS statement, CAST(NULL AS BOOLEAN) AS is_holdable, "
    "CAST(NULL AS BOOLEAN) AS is_binary, CAST(NULL AS BOOLEAN) AS is_scrollable, "
    "CAST(NULL AS TIMESTAMPTZ) AS creation_time WHERE false",
    # An ACL and an options list are always NULL here: neither has an item.
    "CREATE MACRO aclexplode(acl) AS TABLE SELECT CAST(NULL AS INTEGER) AS grantor, "
    "CAST(NULL AS INTEGER) AS grantee, CAST(NULL AS VARCHAR) AS privilege_type, "
    "CAST(NULL AS BOOLEAN) AS is_grantable WHERE false",
    "CREATE MACRO aclexplode(acl) AS unnest(CAST([] AS STRUCT(grantor INTEGER, grantee INTEGER, "
    "privilege_type VARCHAR, is_grantable BOOLEAN)[]))",
    *_set_returning(
        "pg_options_to_table",
        "options",
        "SELECT CAST(NULL AS VARCHAR) AS option_name, CAST(NULL AS VARCHAR) AS option_value "
        "WHERE false",
        "option_name, option_value",
    ),
    *_set_returning(
        "pg_partition_tree",
        "rel",
        _partition_relation(
            "_c.oid AS relid, CAST(NULL AS INTEGER) AS parentrelid, "
            "_c.relkind NOT IN ('p', 'I') AS isleaf, 0 AS level"
        ),
        "relid, parentrelid, isleaf, level",
    ),
    "CREATE MACRO pg_partition_ancestors(rel) AS TABLE " + _partition_relation("_c.oid AS relid"),
    "CREATE MACRO pg_partition_ancestors(rel) AS unnest(("
    + _partition_relation("list(_c.oid)")
    + "))",
    # pgAdmin's extension list; its FROM form is DuckDB's own (catalog_rewrite).
    "CREATE MACRO pg_available_extensions() AS unnest((SELECT list(struct_pack(name := _e.name, "
    "default_version := _e.default_version, comment := _e.comment)) "
    "FROM _pg_available_extensions _e))",
    "CREATE MACRO pg_partition_root(rel) AS ("
    + _partition_relation("struct_pack(oid := _c.oid, name := _c.relname)")
    + ")",
    "CREATE MACRO pg_partition_ancestors_named(rel) AS unnest(("
    + _partition_relation("list(struct_pack(oid := _c.oid, name := _c.relname))")
    + "))",
    # No table has a replica identity index, no statistics object an expression.
    "CREATE MACRO pg_get_replica_identity_index(rel) AS "
    "CAST(NULL AS STRUCT(oid INTEGER, name VARCHAR))",
    "CREATE MACRO pg_get_statisticsobjdef_expressions(stat) AS CAST(NULL AS VARCHAR[])",
    *_set_returning(
        "pg_show_all_settings",
        "",
        _SETTINGS_SELECT,
        "name, setting, unit, category, short_desc, extra_desc, context, vartype, source, "
        "min_val, max_val, enumvals, boot_val, reset_val, sourcefile, sourceline, pending_restart",
    ),
    *_set_returning(
        "pg_get_keywords",
        "",
        _KEYWORDS_SELECT,
        "word, catcode, barelabel, catdesc, baredesc",
    ),
    *_set_returning("ts_token_type", "parser", _TOKENS_SELECT, "tokid, alias, description"),
    # A character value of a "char".
    "CREATE MACRO bpchar(value) AS CAST(value AS VARCHAR)",
    # Every type is usable by every role: PostgreSQL grants USAGE on a type to PUBLIC, and
    # Provisa keeps no ACLs. The function is strict.
    "CREATE MACRO has_type_privilege(type, priv) AS "
    "CASE WHEN type IS NOT NULL AND priv IS NOT NULL THEN true END, "
    "(role, type, priv) AS "
    "CASE WHEN role IS NOT NULL AND type IS NOT NULL AND priv IS NOT NULL THEN true END",
    # Advisory locks are the only locks a connection takes here, and pg_locks does not list them.
    "CREATE MACRO pg_blocking_pids(pid) AS "
    "CASE WHEN pid IS NOT NULL THEN CAST([] AS INTEGER[]) END",
    # No log file: as PostgreSQL answers with the logging collector off.
    "CREATE MACRO pg_current_logfile() AS CAST(NULL AS VARCHAR), (format) AS CAST(NULL AS VARCHAR)",
    # Provisa keeps no statistics: there is nothing to reset (void, its empty text).
    "CREATE MACRO pg_stat_reset_single_table_counters(tbl) AS "
    "CASE WHEN tbl IS NOT NULL THEN '' END",
    # Strict; on a value PostgreSQL refuses this form: a record needs a column definition list.
    "CREATE MACRO json_to_record(value) AS CASE WHEN value IS NOT NULL THEN "
    "error('function returning record called in context that cannot accept type record') "
    "ELSE CAST(NULL AS VARCHAR) END",
    # A vector of type oids, as its types' names.
    "CREATE MACRO oidvectortypes(types) AS CASE WHEN types IS NOT NULL THEN "
    + _type_names("types", ", ", False)
    + " END",
)


def _type_spellings() -> str:
    """A type's internal name from the name format_type writes, for to_regtype."""
    spelled = " ".join(
        f"WHEN '{display}' THEN '{internal}'"
        for internal, display in _PG_TYPE_DISPLAY_NAMES.items()
    )
    return f"CREATE MACRO provisa_type_name(n) AS CASE n {spelled} ELSE n END"


def _named_in_path(schema: str, name: str) -> str:
    return (
        f"CASE WHEN {schema} IN {_VISIBLE} THEN '' ELSE {quoted_identifier(schema)} || '.' END "
        f"|| {quoted_identifier(name)}"
    )


# The catalog a class id names: its oid, or -- a 'pg_class'::regclass literal, as the rewrite
# writes one (catalog_rewrite._rewrite_pg_cast) -- its name.
_CLASS_ID = (
    "CASE CAST(classid AS VARCHAR) WHEN 'pg_class' THEN '1259' WHEN 'pg_proc' THEN '1255' "
    "WHEN 'pg_type' THEN '1247' WHEN 'pg_namespace' THEN '2615' "
    "ELSE CAST(classid AS VARCHAR) END"
)

_CLASS = (
    "FROM (SELECT objid AS _k, objsubid AS _s) _a JOIN _pg_class _c ON _c.oid = _a._k "
    "JOIN _pg_namespace _n ON _n.oid = _c.relnamespace"
)
_PROC = (
    "FROM (SELECT objid AS _k) _a JOIN _pg_proc _p ON _p.oid = _a._k "
    "JOIN _pg_namespace _n ON _n.oid = _p.pronamespace"
)
_COLUMN = (
    f"(SELECT {quoted_identifier('_att.attname')} FROM _pg_attribute _att "
    "WHERE _att.attrelid = _c.oid AND _att.attnum = _a._s)"
)


def _describe_object() -> str:
    """pg_describe_object for the objects the catalog holds -- relations and their columns,
    functions, types and schemas -- named as PostgreSQL names them (qualified when not on the
    search path). Any other object is none the catalog holds, which PostgreSQL answers NULL."""
    relation = (
        f"{_relation_kind('_c.relkind')} || ' ' || {_named_in_path('_n.nspname', '_c.relname')}"
    )
    return (
        f"CREATE MACRO pg_describe_object(classid, objid, objsubid) AS CASE {_CLASS_ID} "
        f"WHEN '1259' THEN (SELECT CASE WHEN _a._s = 0 THEN {relation} "
        f"ELSE 'column ' || {_COLUMN} || ' of ' || {relation} END {_CLASS}) "
        "WHEN '1255' THEN (SELECT CASE _p.prokind WHEN 'p' THEN 'procedure ' ELSE 'function ' END "
        f"|| {_named_in_path('_n.nspname', '_p.proname')} || '(' "
        f"|| {_type_names('_p.proargtypes', ',', False)} || ')' {_PROC}) "
        f"WHEN '1247' THEN (SELECT 'type ' || {_type_name('_a._k')} FROM (SELECT objid AS _k) _a) "
        "WHEN '2615' THEN (SELECT 'schema ' || _n.nspname FROM (SELECT objid AS _k) _a "
        "JOIN _pg_namespace _n ON _n.oid = _a._k) END"
    )


def _identify_object() -> str:
    """pg_identify_object for the same objects: (type, schema, name, identity), the identity
    schema-qualified; the name only where it alone names the object (not a function's, nor a
    column's)."""

    def row(kind: str, schema: str, name: str, identity: str) -> str:
        return (
            f"struct_pack(type := {kind}, schema := {schema}, name := {name}, "
            f"identity := {identity})"
        )

    relation = f"{quoted_identifier('_n.nspname')} || '.' || {quoted_identifier('_c.relname')}"
    no_name = "CAST(NULL AS VARCHAR)"
    return (
        f"CREATE MACRO pg_identify_object(classid, objid, objsubid) AS CASE {_CLASS_ID} "
        "WHEN '1259' THEN (SELECT CASE WHEN _a._s = 0 THEN "
        + row(_relation_kind("_c.relkind"), "_n.nspname", quoted_identifier("_c.relname"), relation)
        + " ELSE "
        + row(
            _relation_kind("_c.relkind") + " || ' column'",
            "_n.nspname",
            no_name,
            f"{relation} || '.' || {_COLUMN}",
        )
        + f" END {_CLASS}) WHEN '1255' THEN (SELECT "
        + row(
            "CASE _p.prokind WHEN 'p' THEN 'procedure' ELSE 'function' END",
            "_n.nspname",
            no_name,
            f"{quoted_identifier('_n.nspname')} || '.' || {quoted_identifier('_p.proname')} || '(' || "
            f"{_type_names('_p.proargtypes', ',', True)} || ')'",
        )
        + f" {_PROC}) WHEN '1247' THEN (SELECT "
        + row("'type'", "_n.nspname", quoted_identifier("_t.typname"), _type_name("_t.oid", True))
        + " FROM (SELECT objid AS _k) _a JOIN _pg_type _t ON _t.oid = _a._k "
        "JOIN _pg_namespace _n ON _n.oid = _t.typnamespace) WHEN '2615' THEN (SELECT "
        + row("'schema'", no_name, quoted_identifier("_n.nspname"), quoted_identifier("_n.nspname"))
        + " FROM (SELECT objid AS _k) _a JOIN _pg_namespace _n ON _n.oid = _a._k) END"
    )


def populate_catalog_macros(db) -> None:  # REQ-2167
    """Create the macros, over the catalog relations they read (created before)."""
    for alias in PG_ONLY_TYPES:
        db.execute(alias)
    register_pg_functions(db)
    db.create_function("provisa_next_xid", next_txid, [], "BIGINT", side_effects=True)
    for macro in (
        *_MACROS,
        _describe_object(),
        _identify_object(),
        _type_spellings(),
        *server_macros(),
    ):
        db.execute(macro)

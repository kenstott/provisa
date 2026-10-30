# Copyright (c) 2026 Kenneth Stott
# Canary: c5d81e27-6f40-4a19-9b3e-27a4f06c8d15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's metadata functions, in the compiler (REQ-2190, with REQ-2167's rule): a function on
a dialect is its native name, else existing functions, else inline operators, else refused by name.

``VALUE_FORMS`` are functions of their arguments alone: rows of the macro table
(provisa.transpiler.function_macros), native on PostgreSQL, a form everywhere else.

``FORMS`` are functions on what a catalog holds. None of them is native anywhere: a statement about
the model runs on the control plane, whose own pg_get_indexdef reads the control plane's catalog, a
different one with oids of its own. A form is SQL in PostgreSQL's dialect over the meta domain and
the statement's facts (provisa.transpiler.metadata_facts), and :func:`expand` writes it into a
statement BEFORE governance, so the meta tables it reads are governed as any other read: a role
gets what it may list, and a key's definition is NULL for one it may not. Each reads one registered
table, so no form relates two of them (REQ-603). What the dialect then runs is the ordinary
transpile of the statement.

:data:`READS_THE_SERVER` are the functions on a server's catalog with no form yet: refused by name,
never sent to the control plane to answer about itself."""

# Requirements: REQ-2190, REQ-2167

from __future__ import annotations

import functools
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core.ir_types import SUBTYPES
from provisa.pgwire.catalog_pg17 import TYPES
from provisa.transpiler.metadata_facts import COLUMNS, PRIVILEGES, TABLES, CatalogFacts, relation
from provisa.transpiler.metadata_oids import FIRST_USER_OID, key_id_sql, table_id_sql

#: The search path a definition names a table against (catalog_data's search_path).
_ON_THE_PATH = "('public', 'pg_catalog')"

# --- format_type --------------------------------------------------------------------------------

_ARRAY_OF = {entry[10]: oid for oid, entry in TYPES.items() if entry[4] == "A" and entry[3] == "b"}

#: The header a varlena type's typmod counts (VARHDRSZ).
_HEADER = 4
#: A numeric typmod's scale: eleven bits, signed since PostgreSQL 15.
_NUMERIC_SCALE_MASK = 2047
_NUMERIC_SCALE_SIGN = 1024
_INTERVAL_FULL_PRECISION = 65535
#: INTERVAL_RANGE_MASK's bits of an interval typmod, as PostgreSQL's interval_typmodout writes them.
_INTERVAL_RANGES = (
    (4, "year"),
    (2, "month"),
    (8, "day"),
    (1024, "hour"),
    (2048, "minute"),
    (4096, "second"),
    (6, "year to month"),
    (1032, "day to hour"),
    (3080, "day to minute"),
    (7176, "day to second"),
    (3072, "hour to minute"),
    (7168, "hour to second"),
    (6144, "minute to second"),
)


def _number(sql: str) -> str:
    return f"CAST({sql} AS TEXT)"


def _sized(name: str) -> str:
    return f"'{name}' || CASE WHEN typmod > {_HEADER} THEN '(' || {_number(f'typmod - {_HEADER}')} || ')' ELSE '' END"


def _numeric() -> str:
    packed = f"(typmod - {_HEADER})"
    low = f"({packed} & {_NUMERIC_SCALE_MASK})"
    scale = (
        f"CASE WHEN {low} >= {_NUMERIC_SCALE_SIGN} THEN {low} - {_NUMERIC_SCALE_MASK + 1} "
        f"ELSE {low} END"
    )
    return (
        f"'numeric' || CASE WHEN typmod >= {_HEADER} THEN '(' "
        f"|| {_number(f'(({packed} >> 16) & 65535)')} || ',' || {_number(scale)} || ')' "
        "ELSE '' END"
    )


def _bits(name: str) -> str:
    return f"'{name}(' || {_number('typmod')} || ')'"


def _timed(name: str, zone: str) -> str:
    return f"'{name}(' || {_number('typmod')} || ') {zone}'"


def _interval() -> str:
    ranges = " ".join(f"WHEN {bit} THEN ' {text}'" for bit, text in _INTERVAL_RANGES)
    precision = "(typmod & 65535)"
    return (
        f"'interval' || COALESCE(CASE ((typmod >> 16) & 32767) {ranges} END, '') || "
        f"CASE WHEN {precision} = {_INTERVAL_FULL_PRECISION} THEN '' "
        f"ELSE '(' || {_number(precision)} || ')' END"
    )


#: Each type a typmod names more of: the oid of its base type, and its name with the typmod.
_MODIFIED: dict[int, str] = {
    1042: _sized("character"),
    1043: _sized("character varying"),
    1700: _numeric(),
    1560: _bits("bit"),
    1562: _bits("bit varying"),
    1083: _timed("time", "without time zone"),
    1266: _timed("time", "with time zone"),
    1114: _timed("timestamp", "without time zone"),
    1184: _timed("timestamp", "with time zone"),
    1186: _interval(),
}
#: Named otherwise with a typmod given and negative, as format_type names one (bit is quoted: bare,
#: it would mean bit(1)).
_NEGATIVE_TYPMOD = {1042: "bpchar", 1560: '"bit"'}


def _format_type() -> str:
    names = " ".join(
        f"WHEN {oid} THEN '{entry[-1].replace(chr(39), chr(39) * 2)}'"
        for oid, entry in sorted(TYPES.items())
    )
    modified = " ".join(
        f"WHEN {oid} THEN {form}{suffix}"
        for base, form in _MODIFIED.items()
        for oid, suffix in ((base, ""), (_ARRAY_OF[base], " || '[]'"))
    )
    negative = " ".join(
        f"WHEN {oid} THEN '{name}{suffix}'"
        for base, name in _NEGATIVE_TYPMOD.items()
        for oid, suffix in ((base, ""), (_ARRAY_OF[base], "[]"))
    )
    return (
        "CASE WHEN type_oid IS NULL THEN CAST(NULL AS TEXT) "
        f"WHEN typmod >= 0 THEN COALESCE(CASE type_oid {modified} END, "
        f"CASE type_oid {names} ELSE '???' END) "
        f"WHEN typmod < 0 THEN COALESCE(CASE type_oid {negative} END, "
        f"CASE type_oid {names} ELSE '???' END) "
        f"ELSE CASE type_oid {names} ELSE '???' END END"
    )


#: pg_encoding_to_char's names, by encoding number (common/encnames.c's pg_enc2name_tbl).
_ENCODINGS = (
    "SQL_ASCII", "EUC_JP", "EUC_CN", "EUC_KR", "EUC_TW", "EUC_JIS_2004", "UTF8", "MULE_INTERNAL",
    "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5", "LATIN6", "LATIN7", "LATIN8", "LATIN9",
    "LATIN10", "WIN1256", "WIN1258", "WIN866", "WIN874", "KOI8R", "WIN1251", "WIN1252",
    "ISO_8859_5", "ISO_8859_6", "ISO_8859_7", "ISO_8859_8", "WIN1250", "WIN1253", "WIN1254",
    "WIN1255", "WIN1257", "KOI8U", "SJIS", "BIG5", "GBK", "UHC", "GB18030", "JOHAB",
    "SHIFT_JIS_2004",
)  # fmt: skip


def _encoding_to_char() -> str:
    names = " ".join(f"WHEN {n} THEN '{name}'" for n, name in enumerate(_ENCODINGS))
    return f"CASE WHEN encoding IS NULL THEN CAST(NULL AS TEXT) ELSE CASE encoding {names} ELSE '' END END"


#: (name, parameters, the form in PostgreSQL's dialect): rows of function_macros' table.
VALUE_FORMS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("format_type", ("type_oid", "typmod"), _format_type()),
    ("pg_encoding_to_char", ("encoding",), _encoding_to_char()),
)

# --- the type a column is listed as -------------------------------------------------------------

#: A registered type's PostgreSQL name and oid (the catalog lists every other as text).
_LISTED_TYPES: dict[str, tuple[str, int]] = {
    "varchar": ("character varying", 1043),
    "char": ("character", 18),
    "character varying": ("character varying", 1043),  # REQ-1961
    "character": ("character", 1042),
    "bpchar": ("character", 1042),
    "numeric": ("numeric", 1700),
    "integer": ("integer", 23),
    "int": ("integer", 23),
    "bigint": ("bigint", 20),
    "smallint": ("smallint", 21),
    "boolean": ("boolean", 16),
    "double": ("double precision", 701),
    "real": ("real", 700),
    "date": ("date", 1082),
    "time": ("time without time zone", 1083),
    "timestamp": ("timestamp without time zone", 1114),
    "timestamp with time zone": ("timestamp with time zone", 1184),
    "timestamptz": ("timestamp with time zone", 1184),  # REQ-2050
    "decimal": ("numeric", 1700),
    "json": ("jsonb", 3802),
    "row": ("jsonb", 3802),
    "array": ("ARRAY", 2277),
    "varbinary": ("bytea", 17),
    "uuid": ("uuid", 2950),
    **{name: (name, subtype.pg_oid) for name, subtype in SUBTYPES.items()},  # REQ-2171
}
#: Oids the name above does not decide: jsonb's name for a jsonb spelling.
_LISTED_OID_ONLY = {"jsonb": 3802}
_TEXT_TYPE = ("text", 25)
#: The character spellings a text column's native type is listed as (cast_types' character kinds).
_CHARACTER_TYPES = ("varchar", "char", "character", "character varying", "bpchar")


def _bare_type(spelling: str) -> str:
    """A spelling without its modifiers, in lower case."""
    return f"TRIM(LOWER(SPLIT_PART({spelling}, '(', 1)))"


def listed_type_sql(data_type: str, native_type: str, subtype: str) -> str:
    """The spelling a column is listed by, of its columns in meta (REQ-1961, REQ-2171): its
    subtype; a text column's native type, where that is a character type; its registered type."""
    characters = ", ".join(f"'{name}'" for name in _CHARACTER_TYPES)
    return (
        f"COALESCE({subtype}, CASE WHEN LOWER({data_type}) = 'text' "
        f"AND {_bare_type(native_type)} IN ({characters}) THEN {native_type} ELSE {data_type} END)"
    )


def pg_type_name_sql(spelling: str) -> str:
    """The PostgreSQL type name a listed ``spelling`` (SQL) has, as information_schema writes it."""
    cases = " ".join(f"WHEN '{key}' THEN '{name}'" for key, (name, _) in _LISTED_TYPES.items())
    return f"CASE {_bare_type(spelling)} {cases} ELSE '{_TEXT_TYPE[0]}' END"


def pg_type_oid_sql(spelling: str) -> str:
    """The oid of the PostgreSQL type a listed ``spelling`` (SQL) has, as pg_attribute holds it."""
    oids = {**{key: oid for key, (_, oid) in _LISTED_TYPES.items()}, **_LISTED_OID_ONLY}
    cases = " ".join(f"WHEN '{key}' THEN {oid}" for key, oid in oids.items())
    return f"CASE {_bare_type(spelling)} {cases} ELSE {_TEXT_TYPE[1]} END"


def pg_typmod_sql(spelling: str) -> str:
    """The typmod of a listed ``spelling`` (SQL): a sized numeric's, a character type's length; -1
    for any other, as pgwire describes it (cast_types.described_type)."""
    inside = f"SPLIT_PART(SPLIT_PART({spelling}, '(', 2), ')', 1)"
    first = f"CAST(TRIM(SPLIT_PART({inside}, ',', 1)) AS INTEGER)"
    scale = f"COALESCE(CAST(NULLIF(TRIM(SPLIT_PART({inside}, ',', 2)), '') AS INTEGER), 0)"
    characters = ", ".join(f"'{name}'" for name in _CHARACTER_TYPES)
    return (
        f"CASE WHEN {inside} = '' THEN -1 "
        f"WHEN {_bare_type(spelling)} IN ('numeric', 'decimal') "
        f"THEN {first} * 65536 + {scale} + {_HEADER} "
        f"WHEN {_bare_type(spelling)} IN ({characters}) THEN {first} + {_HEADER} ELSE -1 END"
    )


# --- what a catalog holds -----------------------------------------------------------------------


@dataclass(frozen=True)
class Form:
    """One overload of a function on a catalog: its parameters, its form, the facts it reads."""

    name: str
    params: tuple[str, ...]
    sql: str
    facts: frozenset[str] = frozenset()


#: A table's name, as a definition names it: with its schema unless that is on the search path.
def _named(alias: str) -> str:
    bare = f"QUOTE_IDENT({alias}.table_name)"
    schema = f"QUOTE_IDENT({alias}.schema_name)"
    return f"CASE WHEN {alias}.schema_name IN {_ON_THE_PATH} THEN {bare} ELSE {schema} || '.' || {bare} END"


def _key_columns(column: str, alias: str, fact: str, table: str) -> str:
    """A key's columns, each as a statement names it."""
    return (
        f"STRING_AGG(QUOTE_IDENT(COALESCE((SELECT {fact}.sql_name FROM {COLUMNS} {fact} "
        f"WHERE {fact}.table_id = {table} AND {fact}.column_name = {column}), {alias}, {column})), "
        "', ' ORDER BY k.position)"
    )


_KEY_COLUMNS = _key_columns("k.column_name", "k.column_alias", "_cf", "k.table_id")
_REFERENCED_COLUMNS = _key_columns(
    "k.referenced_column", "k.referenced_column_alias", "_rf", "k.referenced_table_id"
)


def _constraint_definition(key: str) -> str:
    target = (
        f"(SELECT {_named('_t')} FROM {TABLES} _t WHERE _t.table_id = MIN(k.referenced_table_id))"
    )
    return (
        f"(SELECT CASE MIN(k.kind) WHEN 'p' THEN 'PRIMARY KEY (' || {_KEY_COLUMNS} || ')' "
        f"WHEN 'u' THEN 'UNIQUE (' || {_KEY_COLUMNS} || ')' "
        f"ELSE 'FOREIGN KEY (' || {_KEY_COLUMNS} || ') REFERENCES ' || {target} "
        f"|| '(' || {_REFERENCED_COLUMNS} || ')' END "
        f"FROM meta.table_keys k WHERE k.key_id = {key_id_sql(key)} GROUP BY k.key_id)"
    )


def _index_definition(key: str) -> str:
    """As pg_get_indexdef writes a key's index: PostgreSQL always names the table's schema."""
    table = "(SELECT QUOTE_IDENT(_t.schema_name) || '.' || QUOTE_IDENT(_t.table_name) FROM "
    table += f"{TABLES} _t WHERE _t.table_id = MIN(k.table_id))"
    name = f"(SELECT _t.table_name FROM {TABLES} _t WHERE _t.table_id = MIN(k.table_id))"
    return (
        "(SELECT 'CREATE UNIQUE INDEX ' || QUOTE_IDENT(COALESCE(MIN(k.key_name), 'pk_' || "
        f"{name})) || ' ON ' || {table} || ' USING btree (' || {_KEY_COLUMNS} || ')' "
        f"FROM meta.table_keys k WHERE k.key_id = {key_id_sql(key)} AND k.kind IN ('p', 'u') "
        "GROUP BY k.key_id)"
    )


def _index_column(key: str, position: str) -> str:
    """pg_get_indexdef(index, column, pretty) of a key column: that column's name."""
    return (
        f"(SELECT QUOTE_IDENT(COALESCE((SELECT _cf.sql_name FROM {COLUMNS} _cf "
        "WHERE _cf.table_id = k.table_id AND _cf.column_name = k.column_name), k.column_alias, "
        f"k.column_name)) FROM meta.table_keys k WHERE k.key_id = {key_id_sql(key)} "
        f"AND k.kind IN ('p', 'u') AND k.position = {position})"
    )


_RELATION_COMMENT = (
    f"(SELECT _d.comment FROM meta.table_details _d WHERE _d.id = {table_id_sql('object_oid')})"
)
_SCHEMA_COMMENT = (
    "(SELECT NULLIF(MIN(_d.description), '') FROM meta.domains _d WHERE _d.id IN "
    f"(SELECT _t.domain_id FROM {TABLES} _t WHERE _t.schema_oid = object_oid))"
)

#: PostgreSQL's oid of the catalogs obj_description is asked about, by the name it is asked with.
_COMMENTS_BY_CATALOG = f"""
    CASE catalog WHEN 'pg_class' THEN {_RELATION_COMMENT}
                 WHEN 'pg_namespace' THEN {_SCHEMA_COMMENT} END
"""

_LISTED = f"(SELECT TRUE FROM {TABLES} _t WHERE _t.table_oid = relation_oid)"

_NAMED_TABLE = (
    f"FROM {TABLES} _t WHERE REPLACE(CAST(tbl AS TEXT), '\"', '') IN "
    "(CAST(_t.table_oid AS TEXT), _t.schema_name || '.' || _t.table_name, "
    "CASE WHEN _t.schema_name = 'public' THEN _t.table_name END)"
)


def _held(privileges: str) -> str:
    """Whether the role holds any privilege of the list ``privileges``, on the table of ``_t``."""
    listed = f"',' || REPLACE(UPPER({privileges}), ' ', '') || ','"
    return (
        f"EXISTS (SELECT 1 FROM {PRIVILEGES} _p WHERE _p.table_id = _t.table_id "
        f"AND POSITION(',' || _p.privilege || ',' IN {listed}) > 0)"
    )


_TABLE_PRIVILEGE = f"(SELECT {_held('priv')} {_NAMED_TABLE})"
_OWN_TYPE_NAMES = " ".join(
    f"WHEN {subtype.pg_oid} THEN '{name}'"
    for name, subtype in sorted(SUBTYPES.items())
    if subtype.pg_oid not in TYPES
)


def _form(name: str, params: str, sql: str, *facts: str) -> Form:
    return Form(name, tuple(p for p in params.split(", ") if p), sql, frozenset(facts))


#: Functions on an object kind Provisa keeps nothing of, or does not list: NULL of the type
#: PostgreSQL gives, each with the argument counts PostgreSQL 17 has.
_NOTHING_KEPT: tuple[tuple[str, tuple[int, ...], str], ...] = (
    ("pg_get_function_arg_default", (2,), "TEXT"),
    ("pg_get_function_arguments", (1,), "TEXT"),
    ("pg_get_function_identity_arguments", (1,), "TEXT"),
    ("pg_get_function_result", (1,), "TEXT"),
    ("pg_get_function_sqlbody", (1,), "TEXT"),
    ("pg_get_functiondef", (1,), "TEXT"),
    ("pg_get_partition_constraintdef", (1,), "TEXT"),
    ("pg_get_partkeydef", (1,), "TEXT"),
    ("pg_get_ruledef", (1, 2), "TEXT"),
    ("pg_get_serial_sequence", (2,), "TEXT"),
    ("pg_get_statisticsobjdef", (1,), "TEXT"),
    ("pg_get_statisticsobjdef_columns", (1,), "TEXT"),
    ("pg_get_triggerdef", (1, 2), "TEXT"),
    ("pg_get_viewdef", (1, 2), "TEXT"),
    ("pg_tablespace_location", (1,), "TEXT"),
    ("shobj_description", (2,), "TEXT"),
    # Provisa reports no storage: a size is unknown, never a made-up 0.
    ("pg_relation_size", (1, 2), "BIGINT"),
    ("pg_total_relation_size", (1,), "BIGINT"),
    ("pg_indexes_size", (1,), "BIGINT"),
    ("pg_table_size", (1,), "BIGINT"),
    ("pg_database_size", (1,), "BIGINT"),
    ("pg_tablespace_size", (1,), "BIGINT"),
)
#: Every object a catalog holds is on the search path, and every role holds every role.
_ALWAYS = (
    ("pg_function_is_visible", 1),
    ("pg_opclass_is_visible", 1),
    ("pg_operator_is_visible", 1),
    ("pg_ts_config_is_visible", 1),
    ("pg_ts_dict_is_visible", 1),
    ("pg_ts_parser_is_visible", 1),
    ("pg_ts_template_is_visible", 1),
    ("pg_type_is_visible", 1),
    ("pg_has_role", 2),
    ("pg_has_role", 3),
)


def _unused_params(arity: int) -> str:
    return ", ".join(f"arg_{n}" for n in range(arity))


_FORMS: tuple[Form, ...] = (
    _form(
        "format_type",
        "type_oid, typmod",
        f"CASE type_oid {_OWN_TYPE_NAMES} ELSE format_type(type_oid, typmod) END",
    ),
    # A catalog keeps a default's expression as its text.
    _form("pg_get_expr", "expression, relation", "CAST(expression AS TEXT)"),
    _form("pg_get_expr", "expression, relation, pretty", "CAST(expression AS TEXT)"),
    _form("pg_get_constraintdef", "conoid", _constraint_definition("conoid"), TABLES, COLUMNS),
    _form(
        "pg_get_constraintdef",
        "conoid, pretty",
        _constraint_definition("conoid"),
        TABLES,
        COLUMNS,
    ),
    _form("pg_get_indexdef", "indexoid", _index_definition("indexoid"), TABLES, COLUMNS),
    _form(
        "pg_get_indexdef",
        "indexoid, position, pretty",
        f"CASE WHEN position = 0 THEN {_index_definition('indexoid')} "
        f"ELSE {_index_column('indexoid', 'position')} END",
        TABLES,
        COLUMNS,
    ),
    _form(
        "obj_description", "object_oid", f"COALESCE({_RELATION_COMMENT}, {_SCHEMA_COMMENT})", TABLES
    ),
    _form("obj_description", "object_oid, catalog", _COMMENTS_BY_CATALOG, TABLES),
    _form(
        "col_description",
        "relation_oid, attnum",
        "(SELECT _c.comment FROM meta.column_details _c "
        f"WHERE _c.table_id = {table_id_sql('relation_oid')} AND _c.ordinal_position = attnum)",
    ),
    # Every table has one owner, the role a client is shown as owning it (pg_class.relowner).
    _form("pg_get_userbyid", "role_oid", "CASE WHEN role_oid IS NOT NULL THEN 'provisa' END"),
    _form(
        "pg_table_is_visible",
        "relation_oid",
        f"CASE WHEN relation_oid < {FIRST_USER_OID} THEN TRUE ELSE {_LISTED} END",
        TABLES,
    ),
    _form("pg_relation_is_publishable", "relation_oid", f"COALESCE({_LISTED}, FALSE)", TABLES),
    _form("pg_is_other_temp_schema", "schema_oid", "FALSE"),
    _form("has_table_privilege", "tbl, priv", _TABLE_PRIVILEGE, TABLES, PRIVILEGES),
    *(
        _form(name, _unused_params(arity), f"CAST(NULL AS {sql_type})")
        for name, arities, sql_type in _NOTHING_KEPT
        for arity in arities
    ),
    *(_form(name, _unused_params(arity), "TRUE") for name, arity in _ALWAYS),
)

FORMS: dict[tuple[str, int], Form] = {(f.name, len(f.params)): f for f in _FORMS}

#: A function that reads a server's catalog with no form yet: refused, never run on the control
#: plane (it would answer about the control plane's own objects).
READS_THE_SERVER = frozenset(
    {
        "has_any_column_privilege",
        "has_column_privilege",
        "has_database_privilege",
        "has_foreign_data_wrapper_privilege",
        "has_function_privilege",
        "has_language_privilege",
        "has_schema_privilege",
        "has_sequence_privilege",
        "has_server_privilege",
        "has_tablespace_privilege",
        "has_type_privilege",
        "oidvectortypes",
        "pg_describe_object",
        "pg_get_object_address",
        "pg_identify_object",
        "pg_identify_object_as_address",
        "pg_partition_ancestors",
        "pg_partition_root",
        "pg_partition_tree",
        "pg_relation_filenode",
        "pg_relation_filepath",
        "pg_sequence_last_value",
        "pg_sequence_parameters",
        "to_regclass",
        "to_regcollation",
        "to_regnamespace",
        "to_regoper",
        "to_regoperator",
        "to_regproc",
        "to_regprocedure",
        "to_regrole",
        "to_regtype",
        "to_regtypemod",
    }
)


class MetadataFunctionRefused(ValueError):
    """A function on a catalog that has no form: refused by name (query.function_unsupported)."""

    code = "query.function_unsupported"

    def __init__(self, function: str) -> None:
        self.params = {"function": function, "dialect": "meta"}
        super().__init__(f"{function}() has no form over the meta domain")


@functools.lru_cache(maxsize=None)
def _parsed(name: str, arity: int) -> exp.Expr:
    return sqlglot.parse_one(FORMS[(name, arity)].sql, read="postgres")


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


def _argument(value: exp.Expr) -> exp.Expr:
    if isinstance(value, exp.Cast) and value.to.sql(dialect="postgres").lower() == "pg_node_tree":
        value = value.this  # the cast names pg_get_expr's argument type; a catalog keeps text
    return value if isinstance(value, _ATOMIC) else exp.Paren(this=value)


def _written(form: Form, args: list[exp.Expr]) -> exp.Expr:
    """The form with each parameter its argument, which is evaluated wherever the form names it."""
    values = dict(zip(form.params, args))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name]).copy()
        return node

    return _parsed(form.name, len(args)).copy().transform(bound)


def _call(node: exp.Expr) -> exp.Anonymous | None:
    """The call ``node`` is, its ``pg_catalog.`` qualifier aside."""
    if (
        isinstance(node, exp.Dot)
        and isinstance(node.this, exp.Identifier)
        and node.this.name.lower() == "pg_catalog"
    ):
        node = node.expression
    return node if isinstance(node, exp.Anonymous) else None


def expand(tree: exp.Expr, facts: CatalogFacts) -> exp.Expr:
    """``tree`` (a statement in PostgreSQL's dialect, before governance) with each call of a
    function on a catalog written in its form, and the facts its forms read as CTEs. Raises
    :class:`MetadataFunctionRefused` for one that has no form. ``has_table_privilege`` with a user
    is the session role's where the user is, and NULL for any other: a catalog holds one role's
    grants."""
    needed: set[str] = set()

    def rewrite(node: exp.Expr) -> exp.Expr:
        call = _call(node)
        if call is None:
            return node
        name, args = call.name.lower(), list(call.expressions)
        if name == "has_table_privilege" and len(args) == 3:
            user, *args = args
            written = _privilege_of_user(user, args, facts, rewrite, needed)
        else:
            form = FORMS.get((name, len(args)))
            if form is None:
                if name in READS_THE_SERVER:
                    raise MetadataFunctionRefused(name)
                return node
            needed.update(form.facts)
            # transform does not revisit what a replaced node holds: a call in an argument.
            written = _written(form, [a.transform(rewrite) for a in args])
        if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
            return exp.alias_(written, name)  # the column is named for the function
        return written

    out = tree.transform(rewrite)
    if not isinstance(out, exp.Query):
        return out
    for name in sorted(needed):
        out = out.with_(name, as_=relation(name, facts.rows(name)), append=True, copy=False)
    return out


def _privilege_of_user(
    user: exp.Expr, args: list[exp.Expr], facts: CatalogFacts, rewrite, needed: set[str]
) -> exp.Expr:
    form = FORMS[("has_table_privilege", 2)]
    needed.update(form.facts)
    role = exp.Literal.string(facts.role_id)
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(
                    this=exp.cast(_argument(user.transform(rewrite)), "TEXT"), expression=role
                ),
                true=_written(form, [a.transform(rewrite) for a in args]),
            )
        ],
    )

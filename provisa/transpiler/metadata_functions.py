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
different one with oids of its own. A form is SQL in PostgreSQL's dialect over the meta domain's
views, and :func:`expand` writes it into a statement BEFORE governance, so the views it reads are
governed as any other read: a role gets what it may list, and a key's definition is NULL for one it
may not. The privilege functions also read the statement's facts
(provisa.transpiler.metadata_facts). What the dialect then runs is the ordinary transpile of the
statement.

:data:`READS_THE_SERVER` are the functions on a server's catalog with no form yet: refused by name,
never sent to the control plane to answer about itself."""

# Requirements: REQ-2190, REQ-2167

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

import sqlglot
import sqlglot.expressions as exp

from provisa.core.ir_types import SUBTYPES
from provisa.pgwire.catalog_pg17 import FUNCTION_RETURNS, TYPES
from provisa.transpiler.metadata_facts import PRIVILEGES, UNSERVED, CatalogFacts, relation
from provisa.transpiler.metadata_oids import FIRST_METRIC_OID, KEY_OID_BASE, key_id_sql
from provisa.transpiler.metadata_server_forms import SCALAR_FORMS, TABLE_FORMS

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
    """One overload of a function on a catalog: its parameters, its form, the facts it reads, and
    the type each argument is read as where it is not written in as it is."""

    name: str
    params: tuple[str, ...]
    sql: str
    facts: frozenset[str] = frozenset()
    reads_as: tuple[str, ...] = ()


def _named(schema: str, table: str) -> str:
    """A table's name as a definition names it: with its schema unless that is on the path."""
    bare = f"QUOTE_IDENT({table})"
    return f"CASE WHEN {schema} IN {_ON_THE_PATH} THEN {bare} ELSE QUOTE_IDENT({schema}) || '.' || {bare} END"


_KEY_COLUMNS = "STRING_AGG(QUOTE_IDENT(k.column_sql_name), ', ' ORDER BY k.position)"
_REFERENCED_COLUMNS = (
    "STRING_AGG(QUOTE_IDENT(k.referenced_column_sql_name), ', ' ORDER BY k.position)"
)
_REFERENCED_TABLE = _named("MIN(k.referenced_schema_name)", "MIN(k.referenced_table_name)")


def _entries(entry: str, value: str, source: str) -> str:
    """The one row a statement reads a form's answers from, the answers by their key in a jsonb
    object: a client asks of a catalog once for each row, and a lookup there costs no view."""
    return (
        "SELECT JSONB_OBJECT_AGG(_e.entry, _e.answer) AS entries "
        f"FROM (SELECT CAST({entry} AS TEXT) AS entry, {value} AS answer {source}) _e"
    )


def _answer(relation: str, entry: str) -> str:
    return f"(SELECT _m.entries ->> CAST({entry} AS TEXT) FROM {relation} _m)"


#: A key's definition, as pg_get_constraintdef writes it.
KEY_DEFINITIONS = "_provisa_key_definitions"
#: A unique key's index, as pg_get_indexdef writes it: PostgreSQL always names the table's schema.
INDEX_DEFINITIONS = "_provisa_index_definitions"
#: The columns of a unique key's index, by the key and the column's place.
INDEX_COLUMNS = "_provisa_index_columns"
#: A relation's comment, and a comment by what it is on.
OBJECT_COMMENTS = "_provisa_object_comments"
COMMENTS = "_provisa_comments"
#: The relations a role lists, read once by a statement however many rows ask: a client calls
#: pg_table_is_visible for each row of pg_class. Its indexes apart: only a call about one reads
#: them, which is the keys.
LISTED_RELATIONS = "_provisa_listed_relations"
LISTED_INDEXES = "_provisa_listed_indexes"
PUBLISHABLE = "_provisa_publishable_relations"

#: What a form reads once, by the relation a statement holds it in. A statement reads only those
#: its forms name, each as a role reads the view: what it does not list has no answer here.
_READ_ONCE: dict[str, str] = {
    KEY_DEFINITIONS: _entries(
        "k.key_id",
        f"CASE MIN(k.kind) WHEN 'p' THEN 'PRIMARY KEY (' || {_KEY_COLUMNS} || ')' "
        f"WHEN 'u' THEN 'UNIQUE (' || {_KEY_COLUMNS} || ')' "
        f"ELSE 'FOREIGN KEY (' || {_KEY_COLUMNS} || ') REFERENCES ' || {_REFERENCED_TABLE} "
        f"|| '(' || {_REFERENCED_COLUMNS} || ')' END",
        "FROM meta.table_keys k GROUP BY k.key_id",
    ),
    INDEX_DEFINITIONS: _entries(
        "k.key_id",
        "'CREATE UNIQUE INDEX ' || QUOTE_IDENT(COALESCE(MIN(k.key_name), 'pk_' || "
        "MIN(k.table_name))) || ' ON ' || QUOTE_IDENT(MIN(k.schema_name)) || '.' || "
        f"QUOTE_IDENT(MIN(k.table_name)) || ' USING btree (' || {_KEY_COLUMNS} || ')'",
        "FROM meta.table_keys k WHERE k.kind IN ('p', 'u') GROUP BY k.key_id",
    ),
    INDEX_COLUMNS: _entries(
        "CAST(k.key_id AS TEXT) || ':' || CAST(k.position AS TEXT)",
        "QUOTE_IDENT(k.column_sql_name)",
        "FROM meta.table_keys k WHERE k.kind IN ('p', 'u')",
    ),
    OBJECT_COMMENTS: _entries(
        "_d.objoid",
        "MIN(_d.description)",
        "FROM meta.catalog_pg_description _d WHERE _d.objsubid = 0 GROUP BY _d.objoid",
    ),
    COMMENTS: _entries(
        "CAST(_d.objoid AS TEXT) || ':' || CAST(_d.classoid AS TEXT) || ':' "
        "|| CAST(_d.objsubid AS TEXT)",
        "_d.description",
        "FROM meta.catalog_pg_description _d",
    ),
    LISTED_RELATIONS: "SELECT _l.oid FROM meta.catalog_pg_class _l WHERE _l.relkind <> 'i'",
    LISTED_INDEXES: "SELECT _l.oid FROM meta.catalog_pg_class _l WHERE _l.relkind = 'i'",
    PUBLISHABLE: (
        "SELECT _c.oid FROM meta.catalog_pg_class _c WHERE _c.relkind IN ('r', 'p') "
        "AND _c.relnamespace NOT IN (11, 12)"
    ),
}


def _index_column(key: str, position: str) -> str:
    """pg_get_indexdef(index, column, pretty) of a key column: that column's name."""
    entry = f"CAST({key_id_sql(key)} AS TEXT) || ':' || CAST({position} AS TEXT)"
    return _answer(INDEX_COLUMNS, entry)


_OBJECT_COMMENT = _answer(OBJECT_COMMENTS, "object_oid")
#: PostgreSQL's oid of the catalog obj_description is asked about, by the name it is asked with.
_COMMENT_IN_CATALOG = _answer(
    COMMENTS,
    "CAST(object_oid AS TEXT) || ':' || CAST(CASE catalog WHEN 'pg_class' THEN 1259 "
    "WHEN 'pg_namespace' THEN 2615 END AS TEXT) || ':0'",
)
_COLUMN_COMMENT = _answer(
    COMMENTS, "CAST(relation_oid AS TEXT) || ':1259:' || CAST(attnum AS TEXT)"
)

_KEY_DEFINITION = _answer(KEY_DEFINITIONS, key_id_sql("conoid"))
_INDEX_DEFINITION = _answer(INDEX_DEFINITIONS, key_id_sql("indexoid"))
_LISTED = (
    f"CASE WHEN CAST(relation_oid AS BIGINT) >= {KEY_OID_BASE} "
    f"AND CAST(relation_oid AS BIGINT) < {FIRST_METRIC_OID} "
    f"THEN CASE WHEN relation_oid IN (SELECT _c.oid FROM {LISTED_INDEXES} _c) THEN TRUE END "
    f"WHEN relation_oid IN (SELECT _c.oid FROM {LISTED_RELATIONS} _c) THEN TRUE END"
)
_PUBLISHABLE = f"COALESCE(relation_oid IN (SELECT _c.oid FROM {PUBLISHABLE} _c), FALSE)"

#: A relation's name as written: a quoted one as it is, an unquoted one folded to lower case.
_WRITTEN_NAME = (
    "CASE WHEN POSITION('\"' IN CAST(tbl AS TEXT)) > 0 "
    "THEN REPLACE(CAST(tbl AS TEXT), '\"', '') ELSE LOWER(CAST(tbl AS TEXT)) END"
)
#: The relation ``tbl`` names, an oid or a name as regclass reads one, on the search path or by
#: its schema; the path's first where both hold the name.
_NAMED_RELATION = (
    "FROM meta.catalog_pg_class _c JOIN meta.catalog_pg_namespace _n ON _n.oid = _c.relnamespace "
    f"WHERE {_WRITTEN_NAME} IN (CAST(_c.oid AS TEXT), "
    "_n.nspname || '.' || _c.relname, "
    f"CASE WHEN _n.nspname IN {_ON_THE_PATH} THEN _c.relname END)"
)
_PATH_FIRST = " ORDER BY _n.nspname = 'pg_catalog' DESC LIMIT 1"


def _held(privileges: str) -> str:
    """Whether the role holds any privilege of the list ``privileges`` on the relation of
    ``_c``: on a system relation, SELECT, which PostgreSQL grants every role."""
    listed = f"',' || REPLACE(UPPER({privileges}), ' ', '') || ','"
    return (
        f"CASE WHEN _c.relnamespace IN (11, 12) THEN POSITION(',SELECT,' IN {listed}) > 0 "
        f"ELSE EXISTS (SELECT 1 FROM {PRIVILEGES} _p WHERE _p.relation_oid = _c.oid "
        f"AND POSITION(',' || _p.privilege || ',' IN {listed}) > 0) END"
    )


_TABLE_PRIVILEGE = f"(SELECT {_held('priv')} {_NAMED_RELATION}{_PATH_FIRST})"
#: A listed column the role is not served is false: its table is read, and it is not.
_COLUMN_PRIVILEGE = (
    f"(SELECT NOT EXISTS (SELECT 1 FROM {UNSERVED} _u WHERE _u.relation_oid = _c.oid "
    f"AND _u.column_name = _a.attname) AND {_held('priv')} "
    "FROM meta.catalog_pg_class _c JOIN meta.catalog_pg_namespace _n ON _n.oid = _c.relnamespace "
    "JOIN meta.catalog_pg_attribute _a ON _a.attrelid = _c.oid "
    "AND CAST(col AS TEXT) IN (_a.attname, CAST(_a.attnum AS TEXT)) "
    f"WHERE {_WRITTEN_NAME} IN (CAST(_c.oid AS TEXT), "
    "_n.nspname || '.' || _c.relname, "
    f"CASE WHEN _n.nspname IN {_ON_THE_PATH} THEN _c.relname END){_PATH_FIRST})"
)
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
    ("pg_column_size", (1,), "INTEGER"),
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
    _form("pg_get_constraintdef", "conoid", _KEY_DEFINITION, KEY_DEFINITIONS),
    _form("pg_get_constraintdef", "conoid, pretty", _KEY_DEFINITION, KEY_DEFINITIONS),
    _form("pg_get_indexdef", "indexoid", _INDEX_DEFINITION, INDEX_DEFINITIONS),
    _form(
        "pg_get_indexdef",
        "indexoid, position, pretty",
        f"CASE WHEN position = 0 THEN {_INDEX_DEFINITION} "
        f"ELSE {_index_column('indexoid', 'position')} END",
        INDEX_DEFINITIONS,
        INDEX_COLUMNS,
    ),
    _form("obj_description", "object_oid", _OBJECT_COMMENT, OBJECT_COMMENTS),
    _form("obj_description", "object_oid, catalog", _COMMENT_IN_CATALOG, COMMENTS),
    _form("col_description", "relation_oid, attnum", _COLUMN_COMMENT, COMMENTS),
    # Every table has one owner, the role a client is shown as owning it (pg_class.relowner).
    _form(
        "pg_get_userbyid",
        "role_oid",
        "CAST(CASE WHEN role_oid IS NOT NULL THEN 'provisa' END AS NAME)",
    ),
    _form("pg_table_is_visible", "relation_oid", _LISTED, LISTED_RELATIONS, LISTED_INDEXES),
    _form("pg_relation_is_publishable", "relation_oid", _PUBLISHABLE, PUBLISHABLE),
    _form("pg_is_other_temp_schema", "schema_oid", "FALSE"),
    _form("has_table_privilege", "tbl, priv", _TABLE_PRIVILEGE, PRIVILEGES),
    _form("has_any_column_privilege", "tbl, priv", _TABLE_PRIVILEGE, PRIVILEGES),
    _form("has_column_privilege", "tbl, col, priv", _COLUMN_PRIVILEGE, PRIVILEGES, UNSERVED),
    *(
        _form(name, _unused_params(arity), f"CAST(NULL AS {sql_type})")
        for name, arities, sql_type in _NOTHING_KEPT
        for arity in arities
    ),
    *(_form(name, _unused_params(arity), "TRUE") for name, arity in _ALWAYS),
)

FORMS: dict[tuple[str, int], Form] = {
    **{
        (name, arity): Form(
            name, tuple(p for p, _ in params), sql, reads_as=tuple(t for _, t in params)
        )
        for (name, arity), (params, sql) in SCALAR_FORMS.items()
    },
    **{(f.name, len(f.params)): f for f in _FORMS},
}
#: The forms of a function called in FROM, where it has columns of its own.
RELATION_FORMS: dict[tuple[str, int], Form] = {
    (name, arity): Form(
        name, tuple(p for p, _ in params), sql, reads_as=tuple(t for _, t in params)
    )
    for (name, arity), (params, sql) in TABLE_FORMS.items()
}

#: A function that would read the server, with no form: refused by name, never run on the control
#: plane. These run SQL they are given, or read what a server keeps, on the connection that calls
#: them: the role a client's SQL runs as (metadata_settings.CLIENT_ROLE) would otherwise take a
#: statement past governance.
READS_THE_SERVER = frozenset(
    {
        "cursor_to_xml",
        "cursor_to_xmlschema",
        "database_to_xml",
        "database_to_xml_and_xmlschema",
        "database_to_xmlschema",
        "dblink",
        "pg_cancel_backend",
        "pg_notify",
        "pg_terminate_backend",
        "query_to_xml",
        "query_to_xml_and_xmlschema",
        "query_to_xmlschema",
        "schema_to_xml",
        "schema_to_xml_and_xmlschema",
        "schema_to_xmlschema",
        "table_to_xml",
        "table_to_xml_and_xmlschema",
        "table_to_xmlschema",
        "ts_rewrite",
        "ts_stat",
    }
)
#: The statistics of a server: a count is 0 and anything else NULL, as where nothing has happened.
#: Answered by prefix, which covers each function PostgreSQL adds under it.
STATISTICS_PREFIXES = ("pg_stat_get_", "pg_statio_")


#: The overloads naming a user first, each answered for the session role alone.
_OF_A_USER = frozenset(
    {
        ("has_table_privilege", 3),
        ("has_any_column_privilege", 3),
        ("has_column_privilege", 4),
    }
)


class MetadataFunctionRefused(ValueError):
    """A function on a catalog that has no form: refused by name (query.function_unsupported)."""

    code = "query.function_unsupported"

    def __init__(self, function: str) -> None:
        self.params = {"function": function, "dialect": "meta"}
        super().__init__(f"{function}() has no form over the meta domain")


#: An argument that needs no parentheses wherever a form writes it.
_ATOMIC = (exp.Column, exp.Literal, exp.Func, exp.Cast, exp.Paren, exp.Null, exp.Boolean)


def _argument(value: exp.Expr, reads_as: str = "") -> exp.Expr:
    if reads_as == "text":
        return exp.cast(value, "TEXT")
    if reads_as:
        # An argument of any type, as the form reads it: a value of a type no form operates on
        # (xid8, pg_lsn) still has its text.
        return exp.Paren(
            this=exp.Cast(
                this=exp.cast(value, "TEXT"),
                to=exp.DataType.build(reads_as, dialect="postgres", udt=True),
            )
        )
    if isinstance(value, exp.Cast) and value.to.sql(dialect="postgres").lower() == "pg_node_tree":
        value = value.this  # the cast names pg_get_expr's argument type; a catalog keeps text
    return value if isinstance(value, _ATOMIC) else exp.Paren(this=value)


def _written(form: Form, args: list[exp.Expr]) -> exp.Expr:
    """The form with each parameter its argument, which is evaluated wherever the form names it."""
    values = dict(zip(form.params, args))
    reads_as = dict(zip(form.params, form.reads_as))

    def bound(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and not node.table and node.name in values:
            return _argument(values[node.name], reads_as.get(node.name, "")).copy()
        return node

    return sqlglot.parse_one(form.sql, read="postgres").transform(bound)


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
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Anonymous):
            return _relation_form(node, rewrite)
        field_of = _field_of_call(node, rewrite)
        if field_of is not None:
            return field_of
        call = _call(node)
        if call is None:
            return node
        name, args = call.name.lower(), list(call.expressions)
        if (name, len(args)) in _OF_A_USER:
            user, *args = args
            written = _of_session_role(
                user, _expanded_form(name, args, rewrite, needed), facts, rewrite
            )
        else:
            if (name, len(args)) not in FORMS:
                if name in READS_THE_SERVER:
                    raise MetadataFunctionRefused(name)
                if name.startswith(STATISTICS_PREFIXES):
                    return (
                        exp.alias_(_statistic(name), name) if _is_column(node) else _statistic(name)
                    )
                return node
            written = _expanded_form(name, args, rewrite, needed)
        if _is_column(node):
            return exp.alias_(written, name)  # the column is named for the function
        return written

    out = tree.transform(rewrite)
    if not isinstance(out, exp.Query):
        return out
    for name in sorted(needed):
        if name in _READ_ONCE:
            out = out.with_(name, as_=_READ_ONCE[name], append=True, copy=False, materialized=True)
        else:
            out = out.with_(name, as_=relation(name, facts.rows(name)), append=True, copy=False)
    if needed:
        # Before a client's own: a form in one of those reads these.
        written = out.args["with_"]
        written.set("expressions", sorted(written.expressions, key=lambda c: c.alias not in needed))
    return out


def _is_column(node: exp.Expr) -> bool:
    return isinstance(node.parent, exp.Select) and node.arg_key == "expressions"


#: The types a statistic is 0 of; of any other it is NULL.
_COUNTS = frozenset({"bigint", "integer", "smallint", "oid", "double precision", "real"})


def _statistic(name: str) -> exp.Expr:
    returns = FUNCTION_RETURNS.get(name, "text")
    if returns == "record":
        return exp.cast(exp.null(), "TEXT")
    return exp.Cast(
        this=exp.Literal.number(0) if returns in _COUNTS else exp.null(),
        to=exp.DataType.build(returns, dialect="postgres", udt=True),
    )


def _field_of_call(node: exp.Expr, rewrite) -> exp.Expr | None:
    """``(f(...)).field``: a field of a function a form answers as a text record is read from its
    form in FROM, which has columns."""
    if not (isinstance(node, exp.Dot) and isinstance(node.this, exp.Paren)):
        return None
    call = _call(node.this.this)
    if call is None:
        return None
    form = RELATION_FORMS.get((call.name.lower(), len(call.expressions)))
    if form is None:
        return None
    select = _written(form, [a.transform(rewrite) for a in call.expressions])
    return exp.Subquery(
        this=exp.select(exp.column(node.expression.name, table="_f")).from_(
            exp.Subquery(this=select, alias=exp.TableAlias(this=exp.to_identifier("_f")))
        )
    )


def _relation_form(table: exp.Table, rewrite) -> exp.Expr:
    """A function in FROM: its rows, under the name the statement gives them, a form of one column
    taking that name for its column as PostgreSQL's does."""
    call = table.this
    name, args = call.name.lower(), list(call.expressions)
    form = RELATION_FORMS.get((name, len(args)))
    alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
    if form is None:
        if name in READS_THE_SERVER:
            raise MetadataFunctionRefused(name)
        value = FORMS.get((name, len(args)))
        if value is None:
            return table
        # A function of a value, as FROM reads one: a row of it.
        select = exp.select(exp.alias_(_written(value, [a.transform(rewrite) for a in args]), name))
    else:
        select = cast(exp.Select, _written(form, [a.transform(rewrite) for a in args]))
    if not alias.columns and len(select.selects) == 1 and not select.is_star:
        alias = exp.TableAlias(this=alias.this.copy(), columns=[exp.to_identifier(alias.name)])
    return exp.Subquery(this=select, alias=alias)


def _expanded_form(name, args, rewrite, needed: set[str]) -> exp.Expr:
    form = FORMS[(name, len(args))]
    needed.update(form.facts)
    # transform does not revisit what a replaced node holds: a call in an argument.
    return _written(form, [a.transform(rewrite) for a in args])


def _of_session_role(user: exp.Expr, answer: exp.Expr, facts: CatalogFacts, rewrite) -> exp.Expr:
    """``answer`` where ``user`` is the session role; NULL for another, whose grants a statement
    does not hold."""
    named = exp.cast(_argument(user.transform(rewrite)), "TEXT")
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.EQ(this=named, expression=exp.Literal.string(facts.role_id)), true=answer
            )
        ]
    )

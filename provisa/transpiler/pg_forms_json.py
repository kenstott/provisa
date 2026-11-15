# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0c7a12-3b94-4d6f-a1c8-2f7d9e64b301
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's json and jsonb functions as a table of forms (REQ-2167, REQ-2171), replayed against
postgres:17 on DuckDB, Trino and ClickHouse (tests/unit/pgwire/pg17_json_matrix.py holds what each
serves, refuses and answers differently). A dialect a row does not list is refused by name.

A jsonb is its text on the engine. ``jsonb_out`` writes a column the statement says is jsonb at the
wire; nothing here runs per row on the engine for a statement that only reads, and ``->`` and ``->>``
stay the engine's own. On DuckDB, what its SQL does not carry is a UDF of
provisa.federation.duckdb_pg_json (``_DUCKDB``), which is DuckDB's alone until it is written as
SQL."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import functools
import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import raising

_D, _T, _C = "duckdb", "trino", "clickhouse"
_ENGINES = (_D, _T, _C)
_OF = {_D: frozenset({_D}), _T: frozenset({_T, "athena", "presto"}), _C: frozenset({_C})}
Forms = dict[frozenset[str], str]
Row = tuple[str, tuple[str, ...], Forms]
_NAME = """replace(k, '"', '\\"')"""
#: A place in a value: a key, and an index ``{i}``.
_KEY = {_D: f"""('$."' || {_NAME} || '"')""", _T: f"""('$["' || {_NAME} || '"]')""", _C: "k"}
_AT = {
    _D: "{i}",
    # an array's alone, as $[n] reads an object's key "n" there; a negative counts from the end
    _T: "('$[' || CAST(CASE WHEN json_array_length(v) IS NULL THEN NULL WHEN {i} >= 0 THEN {i} "
    "WHEN json_array_length(v) + {i} >= 0 THEN json_array_length(v) + {i} END AS VARCHAR) || ']')",
    _C: "if(startsWith(trimLeft(v), '['), if({i} < 0, {i}, {i} + 1), 0)",  # a key's place otherwise
}
_INDEX = {_D: "CAST(i AS BIGINT)", _T: "i", _C: "i"}
_KEY_INDEX = {_D: "TRY_CAST(k AS BIGINT)", _T: "TRY_CAST(k AS BIGINT)", _C: "toInt64OrNull(k)"}
_READ = {_D: "json_extract(v, @)", _T: "json_extract(v, @)", _C: "nullif(JSONExtractRaw(v, @), '')"}
_READ_TEXT = {
    _D: "json_extract_string(v, @)",
    _T: "coalesce(json_extract_scalar(v, @), nullif(json_format(json_extract(v, @)), 'null'))",
    _C: "if(JSONType(v, @) = 'String', JSONExtractString(v, @), "
    "nullif(nullif(JSONExtractRaw(v, @), ''), 'null'))",
}
_LENGTH = {_D: "json_array_length(v)", _T: "json_array_length(v)", _C: "JSONLength(v)"}


def _kind(d: str) -> str:
    text = {_D: "CAST(v AS VARCHAR)", _T: "json_format(v)", _C: "v"}[d]
    return (
        f"CASE substr(ltrim({text}), 1, 1) WHEN '{{' THEN 'object' WHEN '[' THEN 'array' "
        "WHEN '\"' THEN 'string' WHEN 't' THEN 'boolean' WHEN 'f' THEN 'boolean' "
        "WHEN 'n' THEN 'null' ELSE 'number' END"
    )


def _guard(d: str, answer: str, wanted: str, called: str) -> str:
    """``answer`` for a value of kind ``wanted``; refused with PostgreSQL's SQLSTATE for another."""
    says = f"cannot call {called} on this kind of value"
    fail = {_D: raising("22023", f"'{says}'"), _T: f"fail('{says}')", _C: f"throwIf(1, '{says}')"}
    return f"CASE WHEN v IS NULL THEN NULL WHEN {_kind(d)} = '{wanted}' THEN {answer} ELSE {fail[d]} END"


def _read(reader: dict[str, str], place: dict[str, str], d: str, index: str = "") -> str:
    return reader[d].replace("@", place[d].replace("{i}", index or _INDEX[d]))


def _step(reader: dict[str, str], d: str) -> str:
    """A path's step of digits: an object's key, else an array's index."""
    return f"coalesce({_read(reader, _KEY, d)}, {_read(reader, _AT, d, _KEY_INDEX[d])})"


def _rows(name: str, params: str, form: Callable[[str, str], str]) -> list[Row]:
    """A row each of the json and the jsonb function."""
    return [
        (n, tuple(params.split(", ")), {_OF[d]: form(d, n) for d in _ENGINES})
        for n in (f"json_{name}", f"jsonb_{name}")
    ]


_EACH = (
    f"unnest(list_transform(json_keys(CAST({_guard(_D, 'v', 'object', 'json_each')} AS JSON)), "
    """_k -> struct_pack(key := _k, value := json_extract(v, '$."' || """
    """replace(_k, '"', '\\"') || '"'))))"""
)
#: A row: name, parameters, each dialect's form.
ROWS: tuple[Row, ...] = (
    *_rows("object_field", "v, k", lambda d, _: _read(_READ, _KEY, d)),
    *_rows("object_field_text", "v, k", lambda d, _: _read(_READ_TEXT, _KEY, d)),
    *_rows("array_element", "v, i", lambda d, _: _read(_READ, _AT, d)),
    *_rows("array_element_text", "v, i", lambda d, _: _read(_READ_TEXT, _AT, d)),
    *_rows("path_step", "v, k", lambda d, _: _step(_READ, d)),
    *_rows("path_step_text", "v, k", lambda d, _: _step(_READ_TEXT, d)),
    *_rows("typeof", "v", lambda d, _: f"CASE WHEN v IS NOT NULL THEN {_kind(d)} END"),
    *_rows("array_length", "v", lambda d, name: _guard(d, _LENGTH[d], "array", name)),
    ("to_jsonb", ("v",), {_OF[_D]: "to_json(v)", _OF[_T]: "CAST(v AS JSON)"}),
    ("array_to_json", ("v",), {_OF[_D]: "to_json(v)"}),
    ("row_to_json", ("v",), {_OF[_D]: "to_json(v)"}),
    # In a select list a record each key, on DuckDB alone; in a FROM, _table's rows.
    ("json_each", ("v",), {_OF[_D]: _EACH}),
)
_ON_JSON = frozenset(row[0] for row in ROWS) - {"to_jsonb", "array_to_json", "row_to_json"}
#: What no engine has a form of.
REFUSED = frozenset(
    """
    jsonb_pretty jsonb_hash jsonb_hash_extended jsonb_cmp
    jsonb_eq jsonb_ne jsonb_lt jsonb_le jsonb_gt jsonb_ge jsonb_concat json_strip_nulls
    jsonb_strip_nulls jsonb_set jsonb_set_lax jsonb_insert jsonb_delete jsonb_delete_path
    jsonb_object json_to_record jsonb_to_record json_to_recordset jsonb_to_recordset
    json_populate_record jsonb_populate_record jsonb_populate_record_valid
    json_populate_recordset jsonb_populate_recordset json_agg_strict jsonb_agg_strict
    jsonb_contains jsonb_contained jsonb_exists jsonb_exists_any jsonb_exists_all
    json_object_agg_strict jsonb_object_agg_strict json_object_agg_unique
    jsonb_object_agg_unique json_object_agg_unique_strict jsonb_object_agg_unique_strict
    jsonb_path_exists jsonb_path_match jsonb_path_query jsonb_path_query_array
    jsonb_path_query_first jsonb_path_exists_tz jsonb_path_match_tz jsonb_path_query_tz
    jsonb_path_query_array_tz jsonb_path_query_first_tz jsonb_path_exists_opr
    jsonb_path_match_opr json_build_array
    """.split()
)
_ONLY_ON_DUCKDB = frozenset({"jsonb_agg", "json_object_agg", "jsonb_object_agg"})
_NOT_ON_CLICKHOUSE = frozenset({"json_build_object", "jsonb_build_object", "to_json"})
_AGGREGATES = {exp.JSONObjectAgg: "json_object_agg", exp.JSONBObjectAgg: "jsonb_object_agg"}
#: Each engine's FROM of an array's elements ``e``, and of an object's members ``k`` and ``e``.
_ELEMENTS = {
    _D: "(SELECT unnest(json_extract(v, '$[*]')) AS e FROM (SELECT CAST(@ AS JSON) AS v))",
    _T: "(SELECT @ AS v), UNNEST(CAST(v AS ARRAY(JSON))) AS _u(e)",
}
_MEMBERS = {
    _D: "(SELECT unnest(json_keys(v)) AS k, v FROM (SELECT CAST(@ AS JSON) AS v)), "
    f"""LATERAL (SELECT json_extract(v, '$."' || {_NAME} || '"') AS e)""",
    _T: "(SELECT @ AS v), UNNEST(CAST(v AS MAP(VARCHAR, JSON))) AS _u(k, e)",
}
_E_TEXT = {
    _D: "json_extract_string(e, '$')",
    _T: "coalesce(json_extract_scalar(e, '$'), nullif(json_format(e), 'null'))",
}
#: A set-returning function -> the columns of its rows, the kind it reads.
_TABLES = {
    f"{p}_{name}": (columns, source, wanted)
    for p in ("json", "jsonb")
    for name, columns, source, wanted in (
        ("array_elements", "{e} AS value", _ELEMENTS, "array"),
        ("each", "k AS key, {e} AS value", _MEMBERS, "object"),
        ("object_keys", "k AS {name}", _MEMBERS, "object"),
    )
}
TABLES = frozenset({*_TABLES, *(f"{n}_text" for n in _TABLES if "keys" not in n)})
#: An operator -> the name it is refused by: of any operand, and of an operand said to be jsonb.
_ON_KEYS = {
    exp.JSONBContainsTopKey: "jsonb_exists",
    exp.JSONBContainsAnyTopKeys: "jsonb_exists_any",
    exp.JSONBContainsAllTopKeys: "jsonb_exists_all",
    exp.JSONBDeleteAtPath: "jsonb_delete_path",
    exp.JSONBPathExists: "jsonb_path_exists_opr",
}
_COMPARED = frozenset({"jsonb_lt", "jsonb_le", "jsonb_gt", "jsonb_ge", "jsonb_eq", "jsonb_ne"})
_ON_JSONB = {
    exp.ArrayContainsAll: "jsonb_contains",
    exp.ArrayContainedBy: "jsonb_contained",
    exp.DPipe: "jsonb_concat",
    exp.Sub: "jsonb_delete",
    exp.LT: "jsonb_lt",
    exp.LTE: "jsonb_le",
    exp.GT: "jsonb_gt",
    exp.GTE: "jsonb_ge",
}


def stored_keys(members: dict[str, Any]) -> list[str]:
    """An object's keys in jsonb's order: shortest first, then by their bytes."""
    return sorted(members, key=lambda k: (len(k.encode()), k.encode()))


def jsonb_written(value: Any) -> str:
    """A parsed value as jsonb_out writes it."""
    if isinstance(value, dict):
        members = (
            f"{json.dumps(k, ensure_ascii=False)}: {jsonb_written(value[k])}"
            for k in stored_keys(value)
        )
        return "{" + ", ".join(members) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(jsonb_written(v) for v in value) + "]"
    if isinstance(value, Decimal):
        return "0" if value.is_zero() and "." not in str(value) else f"{value:f}"
    return json.dumps(value, ensure_ascii=False)


def jsonb_text(text: str) -> str:
    """``text`` as jsonb_out writes it: spaced, its keys in order, a duplicate's last, a number's
    own digits."""
    return jsonb_written(json.loads(text, parse_float=Decimal, parse_int=Decimal))


def jsonb_out(value: Any) -> Any:
    """A cell of a column the statement says is jsonb, as a client reads it."""
    return jsonb_text(value) if isinstance(value, str) and value else value


class NoForm(ValueError):
    """A json operator or use no form writes: ``called``, the name it is refused by."""

    def __init__(self, called: str) -> None:
        self.called = called
        super().__init__(called)


#: What may be json in a statement, for function_macros.mentioned. ``->`` of a literal key or index
#: is not: a statement that holds only it costs nothing here. ``->`` of a step the rows hold is.
OPERATORS = r"#>|#-|@>|<@|@\?|\?\||\?&|\s\?\s|\bjsonb?\b|\bjsonb?_|\bto_json|->>?\s*[^\s'\d>-]"


def _said_jsonb(node: exp.Expr) -> bool:
    node = node.unnest()
    if isinstance(node, exp.Cast):
        return node.is_type("jsonb")
    if isinstance(node, exp.JSONExtract):
        return _said_jsonb(node.this)
    name = node.name.lower() if isinstance(node, exp.Anonymous) else ""
    computed = name.startswith(("jsonb_", "to_jsonb"))
    return isinstance(node, exp.JSONBObjectAgg) or (
        computed and not name.endswith(("_text", "_typeof", "_length", "_keys"))
    )


def _literal_jsonb(node: exp.Expr) -> bool:
    return isinstance(node.unnest(), exp.Cast) and isinstance(node.unnest().this, exp.Literal)


def _path(value: exp.Expr, steps: list[str], scalar: bool) -> exp.Expr:
    """``value`` read along ``steps``: a step of digits is a key or an index."""
    for position, step in enumerate(steps):
        row = "json_path_step" if step.lstrip("-").isdigit() else "json_object_field"
        row += "_text" if scalar and position == len(steps) - 1 else ""
        value = exp.Anonymous(this=row, expressions=[value, exp.Literal.string(step)])
    return value


_INDEX_TYPES = (exp.DataType.Type.SMALLINT, exp.DataType.Type.INT, exp.DataType.Type.BIGINT)
_KEY_TYPES = (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR)


def _stepped(
    node: exp.JSONExtract | exp.JSONExtractScalar,
    dialect: str,
    inner: Callable[[exp.Expr], exp.Expr],
) -> exp.Expr | None:
    """``->`` and ``->>`` of a step the rows hold, as the row of the type the statement states
    for it: an integer an array's index, a text an object's key whatever it spells. The engine's
    own operator reads a text as a path. A step of no stated type stays DuckDB's own, which takes
    an integer column as an index; None there, refused by name elsewhere."""
    scalar = isinstance(node, exp.JSONExtractScalar)
    step = node.expression.unnest()
    negative = isinstance(step, exp.Neg) and step.this.is_int  # -1: the last element
    if negative or (isinstance(step, exp.Cast) and step.to.this in _INDEX_TYPES):
        row = "json_array_element"
    elif isinstance(step, exp.Cast) and step.to.this in _KEY_TYPES:
        row = "json_object_field"
    elif dialect in _OF[_D]:
        return None
    else:
        raise NoForm("->>" if scalar else "->")
    called = row + ("_text" if scalar else "")
    return exp.Anonymous(
        this=called, expressions=[node.this.transform(inner), node.expression.transform(inner)]
    )


def _steps(called: str, path: exp.Expr) -> list[str]:
    """A text[] path literal's elements."""
    inner = path.this.strip()[1:-1] if isinstance(path, exp.Literal) else ""
    if not inner:
        raise NoForm(called)
    return [step.strip().strip('"') for step in inner.split(",")]


def _extracted(node: exp.Expr, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """``#>`` and ``#>>`` on DuckDB, of a path no literal elements say: the empty path reads the
    value itself."""
    scalar = isinstance(node, exp.JSONBExtractScalar)
    path = node.expression
    if isinstance(path, exp.Literal) and not path.this.strip()[1:-1]:
        value = node.this.transform(inner)
        read = "json_extract_string(_v, '$')" if scalar else "_v"
        return _duckdb(read, _v=value)
    kind = "jsonb" if _said_jsonb(node.this) else "json"
    call = exp.Anonymous(
        this="provisa_json_extract_path",
        expressions=[
            _text(node.this, inner),
            _text_array(path, inner),
            exp.Boolean(this=scalar),
            exp.Literal.string(f"{kind}_extract_path"),
        ],
    )
    return call if scalar else exp.cast(call, "JSON")


def _parsed(value: exp.Expr, dialect: str) -> exp.Expr:
    """An argument of a json parameter: Trino's json is no text."""
    is_text = isinstance(value, exp.Literal) and value.is_string
    return exp.ParseJSON(this=value) if is_text and dialect in _OF[_T] else value


def _table(table: exp.Table, dialect: str) -> exp.Expr:
    """A set-returning call in a FROM as a subquery of its rows."""
    name = table.this.name.lower()
    d = next(engine for engine in _ENGINES if dialect in _OF[engine])
    columns, source, wanted = _TABLES[name.removesuffix("_text")]
    text = columns.format(e=_E_TEXT[d] if name.endswith("_text") else "e", name=name)
    guard = _guard(d, "v", wanted, name)
    query = sqlglot.parse_one(f"SELECT {text} FROM {source[d].replace('@', guard)}", read=dialect)
    value = _parsed(table.this.expressions[0], dialect)

    def bound(node: exp.Expr) -> exp.Expr:
        reads = isinstance(node, exp.Column) and node.name == "v"
        return value.copy() if reads and node.find_ancestor(exp.Case) else node

    alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
    if dialect in _OF[_T] and isinstance(table.parent, exp.Join):  # a statement's earlier FROM
        return exp.Lateral(this=exp.Subquery(this=query.transform(bound)), alias=alias.copy())
    return exp.Subquery(this=query.transform(bound), alias=alias.copy())


#: The key of a node's meta that holds the json type a statement said of a value ClickHouse holds
#: as its text: "json" or "jsonb" (provisa.transpiler.polyfill.trino.said reads it).
SAID = "provisa_said_json_type"


def _cast(node: exp.Cast, dialect: str) -> exp.Expr:
    """A cast to a json type as the engine reads one: Trino's cast of text is a json string, and
    ClickHouse holds json as its text."""
    literal = isinstance(node.this, exp.Literal)
    if dialect in _OF[_T]:
        return exp.ParseJSON(this=node.this) if literal else exp.cast(node.this, "JSON")
    text = exp.DataType.build("Nullable(String)", dialect=_C)
    held = node.this if literal else exp.cast(node.this, text)
    # Its text, still the json value the statement says: a polyfill grid row reads the type of
    # what it is handed (bool and numeric of a jsonb value refuse it by name).
    held.meta[SAID] = node.to.this.value.lower()
    return held


#: DuckDB's UDF of a function: its name, each parameter's kind (j a json, t a text, p a jsonpath,
#: a a text[], b a boolean, i a bigint), the defaults of its trailing parameters, whether it answers
#: a json.
_Udf = tuple[str, str, tuple[str, ...], bool]
_PATH_DEFAULTS = ("'{}'", "FALSE")
_OPERATOR_DEFAULTS = ("'{}'", "TRUE")
_DUCKDB: dict[str, _Udf] = {
    "jsonb_contains": ("provisa_jsonb_contains", "jj", (), False),
    "jsonb_contained": ("provisa_jsonb_contained", "jj", (), False),
    "jsonb_exists": ("provisa_jsonb_exists", "jt", (), False),
    "jsonb_exists_any": ("provisa_jsonb_exists_any", "ja", (), False),
    "jsonb_exists_all": ("provisa_jsonb_exists_all", "ja", (), False),
    "jsonb_concat": ("provisa_jsonb_concat", "jj", (), True),
    "jsonb_delete_path": ("provisa_jsonb_delete_path", "ja", (), True),
    "jsonb_set": ("provisa_jsonb_set", "jajb", ("TRUE",), True),
    "jsonb_set_lax": ("provisa_jsonb_set_lax", "jajbt", ("TRUE", "'use_json_null'"), True),
    "jsonb_insert": ("provisa_jsonb_insert", "jajb", ("FALSE",), True),
    "jsonb_pretty": ("provisa_jsonb_pretty", "j", (), False),
    "jsonb_cmp": ("provisa_jsonb_cmp", "jj", (), False),
    "jsonb_hash": ("provisa_jsonb_hash", "j", (), False),
    "jsonb_hash_extended": ("provisa_jsonb_hash_extended", "ji", (), False),
    "jsonb_object": ("provisa_json_object", "aa", ("CAST(NULL AS VARCHAR[])",), True),
    "pg_json_object": ("provisa_json_object", "aa", ("CAST(NULL AS VARCHAR[])",), True),
    "jsonb_path_exists_opr": ("provisa_jsonb_path_exists", "jpjb", _OPERATOR_DEFAULTS, False),
    "jsonb_path_match_opr": ("provisa_jsonb_path_match", "jpjb", _OPERATOR_DEFAULTS, False),
    **{
        f"jsonb_path_{name}{suffix}": (f"provisa_jsonb_path_{name}", "jpjb", _PATH_DEFAULTS, json)
        for name, json in (
            ("exists", False),
            ("match", False),
            ("query_array", True),
            ("query_first", True),
        )
        for suffix in ("", "_tz")
    },
}
_ROWS_OF_PATH = ("provisa_jsonb_path_query", "jpjb", _PATH_DEFAULTS, False)
_ORDER = {
    "jsonb_eq": exp.EQ,
    "jsonb_ne": exp.NEQ,
    "jsonb_lt": exp.LT,
    "jsonb_le": exp.LTE,
    "jsonb_gt": exp.GT,
    "jsonb_ge": exp.GTE,
}
_EXTRACT_PATH = frozenset(
    f"{kind}_extract_path{suffix}" for kind in ("json", "jsonb") for suffix in ("", "_text")
)
_ELEMENTS_CALLED = frozenset(
    f"{kind}_array_elements{suffix}" for kind in ("json", "jsonb") for suffix in ("", "_text")
)
_EACH_CALLED = frozenset({"json_each_text", "jsonb_each", "jsonb_each_text"})
_KEYS_CALLED = frozenset({"json_object_keys", "jsonb_object_keys"})
_RECORDS = frozenset(
    f"{kind}_to_{rows}" for kind in ("json", "jsonb") for rows in ("record", "recordset")
)
_POPULATE = frozenset(
    f"{kind}_populate_{rows}" for kind in ("json", "jsonb") for rows in ("record", "recordset")
)
_AGG_STRICT = frozenset({"json_agg_strict", "jsonb_agg_strict"})
#: An object aggregate -> whether it drops a null value, whether a repeated key is an error.
_OBJECT_AGGS = {
    f"{kind}_object_agg{suffix}": (strict, unique)
    for kind in ("json", "jsonb")
    for suffix, strict, unique in (
        ("_strict", True, False),
        ("_unique", False, True),
        ("_unique_strict", True, True),
    )
}
#: A set-returning function DuckDB has as rows of a list, in a FROM.
_LIST_TABLES = {
    **dict.fromkeys(("jsonb_path_query", "jsonb_path_query_tz"), _ROWS_OF_PATH),
}


def _duckdb(sql: str, **nodes: exp.Expr) -> exp.Expr:
    """``sql`` read as DuckDB's, each column named in ``nodes`` that node."""

    def filled(node: exp.Expr) -> exp.Expr:
        return (
            nodes[node.name].copy() if isinstance(node, exp.Column) and node.name in nodes else node
        )

    return _form(sql).transform(filled)


@functools.lru_cache(maxsize=256)
def _form(sql: str) -> exp.Expr:
    """``sql`` as DuckDB reads it, once: a form is one of this module's own, and a statement
    writes one for each member of each JSON it builds. ``transform`` copies it before filling."""
    return sqlglot.parse_one(sql, read=_D)


#: DuckDB writes a timestamp as its text; PostgreSQL's json has the T, and an offset's minutes.
_TO_JSON = (
    "CASE WHEN typeof(_v) IN ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP_S', "
    "'TIMESTAMP_MS', 'TIMESTAMP_NS') THEN to_json(regexp_replace("
    "replace(CAST(_v AS VARCHAR), ' ', 'T'), '([+-][0-9][0-9])$', '\\1:00')) "
    "ELSE to_json(_v) END"
)
#: What already is JSON, or holds a value that was written on its way in.
_JSON_NODES = (exp.JSONObject, exp.JSONArray, exp.JSONArrayAgg)
#: What _TO_JSON's typeof test can never find to be a timestamp.
_NEVER_AN_INSTANT = (exp.Map, exp.Array)
_JSON_CALLS = frozenset(
    {
        "json_group_array",
        "json_group_object",
        "json_arrayagg",
        "json_agg",
        "jsonb_agg",
        "json_object",
        "json_build_object",
        "jsonb_build_object",
        "to_json",
    }
)


def _is_json_call(node: exp.Expr) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.lower() in _JSON_CALLS


def json_encoded(value: exp.Expr) -> exp.Expr:
    """``value`` as its JSON value on DuckDB, whatever its type: one form for every JSON a
    statement builds, so a timestamp reads alike at any depth. A map or a list is never an
    instant, so it takes the form's ELSE alone: the form names its value three times, and around
    each level of a nested statement that multiplied the text."""
    if isinstance(value.unnest(), _NEVER_AN_INSTANT):
        return exp.Anonymous(this="to_json", expressions=[value])
    return _duckdb(_TO_JSON, _v=value)


def json_member(value: exp.Expr) -> exp.Expr:
    """A value going into a JSON object or array on DuckDB. A literal has no timestamp to write,
    and one holding JSON has had its own members written: wrapped again, a statement's text would
    multiply at each level of nesting."""
    if isinstance(value, (exp.Literal, exp.Null, exp.Boolean)):
        return value
    if value.find(*_JSON_NODES) or any(_is_json_call(n) for n in value.find_all(exp.Anonymous)):
        return value
    return json_encoded(value)


def _text(value: exp.Expr, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    return exp.cast(value.transform(inner), "VARCHAR")


def _jsonpath(value: exp.Expr, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A jsonpath argument: its text, a cast to the type DuckDB has not removed."""
    node = value.unnest()
    said = isinstance(node, exp.Cast) and node.to.sql(dialect="postgres").lower() == "jsonpath"
    return _text(node.this if said else value, inner)


def _array_items(text: str) -> list[str | None]:
    """A text[] literal's elements, a sub-array's in place: a NULL unquoted is None."""
    found: list[str | None] = []
    position, end = 0, len(text)
    while position < end:
        char = text[position]
        if char in "{},\t\n ":
            position += 1
        elif char == '"':
            position += 1
            word: list[str] = []
            while text[position] != '"':
                position += text[position] == "\\"
                word.append(text[position])
                position += 1
            position += 1
            found.append("".join(word))
        else:
            start = position
            while position < end and text[position] not in "{},":
                position += 1
            bare = text[start:position].strip()
            found.append(None if bare.upper() == "NULL" else bare)
    return found


def _text_array(value: exp.Expr, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    node = value.unnest()
    if isinstance(node, exp.Literal) and node.is_string:
        cells = [
            exp.Null() if item is None else exp.Literal.string(item)
            for item in _array_items(node.this)
        ]
        return exp.cast(exp.Array(expressions=cells), "VARCHAR[]", dialect=_D)
    return exp.cast(value.transform(inner), "VARCHAR[]", dialect=_D)


_READ_ARGUMENT: dict[str, Callable[[exp.Expr, Callable[[exp.Expr], exp.Expr]], exp.Expr]] = {
    "j": _text,
    "t": _text,
    "p": _jsonpath,
    "a": _text_array,
    "b": lambda value, inner: exp.cast(value.transform(inner), "BOOLEAN"),
    "i": lambda value, inner: exp.cast(value.transform(inner), "BIGINT"),
}


def _udf(
    udf: _Udf, args: list[exp.Expr], called: str, inner: Callable[[exp.Expr], exp.Expr]
) -> exp.Expr:
    name, kinds, defaults, is_json = udf
    if not len(kinds) - len(defaults) <= len(args) <= len(kinds):
        raise NoForm(called)
    written = [_READ_ARGUMENT[kind](arg, inner) for kind, arg in zip(kinds, args)]
    skipped = len(kinds) - len(args)
    written += [sqlglot.parse_one(sql, read=_D) for sql in defaults[len(defaults) - skipped :]]
    call = exp.Anonymous(this=name, expressions=written)
    return exp.cast(call, "JSON") if is_json else call


def _arguments(node: exp.Func) -> list[exp.Expr]:
    """A call's arguments, in order, whatever sqlglot parsed the call into."""
    if isinstance(node, exp.Anonymous):
        return list(node.expressions)
    found: list[exp.Expr] = []
    for key in node.arg_types:
        held = node.args.get(key)
        found += held if isinstance(held, list) else [] if held is None else [held]
    return [a for a in found if isinstance(a, exp.Expr)]


def _key_of(path: exp.Expr) -> exp.Expr:
    """jsonb_exists's text argument, which sqlglot reads as a path of one key."""
    if isinstance(path, exp.JSONPath):
        return exp.Literal.string(path.expressions[-1].this)
    return path


def _spread(args: list[exp.Expr]) -> tuple[list[exp.Expr], bool]:
    """A call's arguments, a VARIADIC array's elements in place of it; True for a VARIADIC NULL."""
    variadic = args[-1] if args else None
    if not isinstance(variadic, exp.Variadic):
        return args, False
    array = variadic.this.unnest()
    if isinstance(array, exp.Cast) and isinstance(array.this, exp.Null):
        return args[:-1], True
    if isinstance(array, exp.Cast) and isinstance(array.this, exp.Array):
        array = array.this
    return (
        ([*args[:-1], *array.expressions], False) if isinstance(array, exp.Array) else (args, False)
    )


def _key_text(key: exp.Expr) -> exp.Expr:
    """An object key as PostgreSQL writes its text: a number's digits, a boolean's name."""
    if isinstance(key, exp.Boolean):
        return exp.Literal.string("true" if key.this else "false")
    return exp.Literal.string(key.this) if isinstance(key, exp.Literal) else key


def _built(node: exp.Func, args: list[exp.Expr], name: str, inner: Callable) -> exp.Expr | None:
    """json_build_array and json_build_object."""
    is_array = name in ("json_build_array", "jsonb_build_array")
    if not is_array and name not in ("json_build_object", "jsonb_build_object"):
        return None  # before any transform: every call of a statement passes here
    written = [a.transform(inner) for a in args]
    if is_array:
        if len(written) == 1 and isinstance(written[0], exp.Variadic):
            return exp.Anonymous(this="to_json", expressions=[written[0].this])
        return exp.Anonymous(this="json_array", expressions=written)
    if any(isinstance(a, exp.Variadic) for a in written):
        raise NoForm(name)
    if len(written) % 2:
        odd = exp.Literal.string("argument list must have even number of elements")
        return _duckdb(raising("22023", "_says"), _says=odd)
    pairs = [_key_text(a) if i % 2 == 0 else a for i, a in enumerate(written)]
    return exp.Anonymous(this=node.name, expressions=pairs)


def _row_of(call: exp.Expr, called: str) -> exp.Expr:
    """A list's elements as the rows of a call in a select list."""
    rows = exp.Anonymous(this="unnest", expressions=[call])
    return rows if called.endswith(("_text", "_keys", "_each")) else exp.cast(rows, "JSON")


def _set_returning(name: str, args: list[exp.Expr], inner: Callable) -> exp.Expr | None:
    """A set-returning function called in a select list."""
    called = exp.Literal.string(name)
    if name in _ELEMENTS_CALLED:
        udf = "provisa_json_elements"
    elif name in _EACH_CALLED:
        udf = "provisa_json_each"
    elif name in _KEYS_CALLED:
        udf = "provisa_json_object_keys"
    elif name in _LIST_TABLES:
        return _row_of(_udf(_LIST_TABLES[name], args, name, inner), name)
    else:
        return None
    return _row_of(exp.Anonymous(this=udf, expressions=[_text(args[0], inner), called]), name)


def _under_filter(node: exp.Func, aggregate: exp.Expr, inner: Callable) -> exp.Expr:
    """``aggregate`` with the FILTER of the call it answers: DuckDB filters an aggregate, and a
    call's form here is a scalar over one."""
    over = node.parent
    if not isinstance(over, exp.Filter):
        return aggregate
    return exp.Filter(this=aggregate, expression=over.expression.transform(inner))


def _aggregate(node: exp.Func, name: str, args: list[exp.Expr], inner: Callable) -> exp.Expr | None:
    """An aggregate DuckDB's json_group_array and json_group_object macros do not take."""
    filtered = isinstance(node.parent, exp.Filter)

    def rows(member: exp.Expr) -> exp.Expr:
        listed = exp.Anonymous(this="list", expressions=[member])
        return _under_filter(node, listed, inner)

    def member(value: exp.Expr) -> exp.Expr:
        written = value.transform(inner)
        if isinstance(written, exp.Order):
            written.set("this", json_member(written.this))
            return written
        if isinstance(written, exp.Distinct):
            written.set("expressions", [json_member(e) for e in written.expressions])
            return written
        return json_member(written)

    if name in _AGG_STRICT:
        return _duckdb(
            "to_json(list_filter(_rows, _v -> _v IS NOT NULL))", _rows=rows(member(args[0]))
        )
    # An ordered one too: DuckDB's json_group_object is a macro, which takes no ORDER BY.
    ordered_value = len(args) > 1 and isinstance(args[1], exp.Order)
    plain = name in ("json_object_agg", "jsonb_object_agg")
    if name in _OBJECT_AGGS or ((filtered or ordered_value) and plain):
        strict, unique = _OBJECT_AGGS.get(name, (False, False))
        # value ORDER BY ...: the order is the pairs' list's, around the pair.
        value, order = args[1], None
        if isinstance(value, exp.Order):
            value, order = value.this, value
        pairs = _duckdb(
            "struct_pack(k := CAST(_k AS VARCHAR), v := CAST(_x AS VARCHAR))",
            _k=args[0].transform(inner),
            _x=json_encoded(value.transform(inner)),
        )
        if order is not None:
            pairs = exp.Order(
                this=pairs, expressions=[o.transform(inner) for o in order.expressions]
            )
        flags = [exp.Boolean(this=strict), exp.Boolean(this=unique), exp.Literal.string(name)]
        call = exp.Anonymous(this="provisa_json_object_agg", expressions=[rows(pairs), *flags])
        return exp.cast(call, "JSON")
    ordered = isinstance(args[0], (exp.Order, exp.Distinct)) if args else False
    array_agg = isinstance(node, exp.JSONArrayAgg) or name in ("json_agg", "jsonb_agg")
    if array_agg and (ordered or filtered):
        return _duckdb("to_json(_rows)", _rows=rows(member(args[0])))
    return None


def _filtered(node: exp.Filter, inner: Callable) -> exp.Expr | None:
    """A filtered aggregate whose form holds the FILTER itself; None for one that takes it as it
    stands."""
    call = node.this
    if not isinstance(call, exp.Func):
        return None
    return _aggregate(call, _called(call), _arguments(call), inner)


def _is_null(value: exp.Expr) -> bool:
    node = value.unnest()
    return isinstance(node.this if isinstance(node, exp.Cast) else node, exp.Null)


def _populated(name: str, args: list[exp.Expr]) -> exp.Expr:
    """A populate function in a select list, its base a row type the engine has no columns of: the
    NULL of a NULL base from a NULL value, and refused by name for any other value."""
    if len(args) < 2 or not (_is_null(args[0]) and _is_null(args[1])):
        raise NoForm(name)
    if name.endswith("_valid"):
        return exp.cast(exp.Null(), "BOOLEAN")  # PostgreSQL's answer of a NULL value
    return _duckdb(
        "unnest(CAST([] AS VARCHAR[]))" if name.endswith("set") else "CAST(NULL AS VARCHAR)"
    )


def _not_a_row_type(name: str, value: exp.Expr, inner: Callable) -> exp.Expr:
    """A record function in a select list: PostgreSQL has no row type for a value, and NULL for
    none."""
    says = exp.Literal.string(f"could not determine row type for result of {name}")
    fail = _duckdb(raising("0A000", "_says"), _says=says)
    return _duckdb(
        "CASE WHEN _v IS NULL THEN NULL ELSE _fail END", _v=_text(value, inner), _fail=fail
    )


def _named_fields(value: exp.Expr) -> None:
    """An anonymous row (``ROW(...)``, or one cast to record) with its fields named as PostgreSQL
    names them, f1, f2, ..., and so each row within it: DuckDB's row has no names, and its JSON
    keys are empty."""
    node = value.unnest()
    cast = isinstance(node, exp.Cast) and node.to.sql(dialect="postgres").lower() == "record"
    row = node.this if cast else node
    if not (isinstance(row, exp.Anonymous) and row.name.lower() == "row" and row.expressions):
        return
    for field in row.expressions:
        _named_fields(field)  # a row within names its fields too
    named = exp.Anonymous(
        this="struct_pack",
        expressions=[
            exp.PropertyEQ(this=exp.to_identifier(f"f{i}"), expression=field.copy())
            for i, field in enumerate(row.expressions, 1)
        ],
    )
    node.replace(named)


def _on_duckdb(node: exp.Func, name: str, inner: Callable) -> exp.Expr | None:
    """``node`` as DuckDB's SQL over its UDFs; None for a call of what the rows answer."""
    if name in ("row_to_json", "to_json", "to_jsonb") and node.expressions:
        _named_fields(node.expressions[0])
    args, of_null = _spread([_key_of(a) for a in _arguments(node)])
    if isinstance(node, exp.JSONExtract):
        name = "json_extract_path"
    elif isinstance(node, exp.JSONExtractScalar):
        name = "json_extract_path_text"
    if of_null and (name in _EXTRACT_PATH or "build" in name or name == "jsonb_delete"):
        return exp.cast(exp.Null(), "JSON")
    if name in _DUCKDB:
        return _udf(_DUCKDB[name], args, name, inner)
    if name in _ORDER:
        return _ORDER[name](
            this=_udf(_DUCKDB["jsonb_cmp"], args, name, inner), expression=exp.Literal.number(0)
        )
    if name in ("json_strip_nulls", "jsonb_strip_nulls"):
        return exp.cast(
            exp.Anonymous(
                this="provisa_json_strip_nulls",
                expressions=[_text(args[0], inner), exp.Literal.string(name)],
            ),
            "JSON",
        )
    if name == "jsonb_delete":
        keys = args[1] if len(args) == 2 and not isinstance(args[1], exp.Variadic) else None
        key = (
            keys
            if keys is not None
            else exp.Array(
                expressions=[a.this if isinstance(a, exp.Variadic) else a for a in args[1:]]
            )
        )
        wrapped = exp.Anonymous(this="to_json", expressions=[key.transform(inner)])
        udf = exp.Anonymous(
            this="provisa_jsonb_delete", expressions=[_text(args[0], inner), wrapped]
        )
        return exp.cast(udf, "JSON")
    if name in _EXTRACT_PATH and (
        node.find(exp.Variadic) or not isinstance(node, exp.JSONExtract | exp.JSONExtractScalar)
    ):
        steps = args[1].this if len(args) == 2 and isinstance(args[1], exp.Variadic) else None
        path = (
            _text_array(steps, inner)
            if steps is not None
            else exp.cast(
                exp.Array(expressions=[a.transform(inner) for a in args[1:]]),
                "VARCHAR[]",
                dialect=_D,
            )
        )
        call = exp.Anonymous(
            this="provisa_json_extract_path",
            expressions=[
                _text(args[0], inner),
                path,
                exp.Boolean(this=name.endswith("_text")),
                exp.Literal.string(name),
            ],
        )
        return call if name.endswith("_text") else exp.cast(call, "JSON")
    if name in _RECORDS:
        return _not_a_row_type(name, args[0], inner)
    if name in _POPULATE or name == "jsonb_populate_record_valid":
        return _populated(name, args)
    if name in ("to_json", "to_jsonb") and len(args) == 1:
        written_value = args[0].transform(inner)
        if _is_json_call(
            args[0].unnest()
        ):  # already JSON, so never an instant: json_encoded's ELSE
            return exp.Anonymous(this="to_json", expressions=[written_value])
        return json_encoded(written_value)
    if name in ("row_to_json", "array_to_json") and len(args) == 2:
        if _is_null(args[0]):
            return exp.cast(exp.Null(), "JSON")
        answer = exp.Anonymous(this="to_json", expressions=[args[0].transform(inner)])
        if isinstance(args[1], exp.Boolean):  # pretty: JSON's spacing is no difference
            return answer
        return _duckdb(
            "CASE WHEN _p IS NOT NULL THEN _a END", _p=args[1].transform(inner), _a=answer
        )
    return (
        _built(node, args, name, inner)
        or _set_returning(name, args, inner)
        or _aggregate(node, name, args, inner)
    )


def _records_table(table: exp.Table, name: str, value: exp.Expr, inner: Callable) -> exp.Expr:
    """json_to_record(set) in a FROM: its rows, each column cast to the type the statement says."""
    alias = table.args.get("alias")
    columns = [c for c in alias.columns if isinstance(c, exp.ColumnDef)] if alias else []
    if alias is None or not columns:
        raise NoForm(name)
    kinds = [c.args["kind"] for c in columns]
    names = exp.Array(expressions=[exp.Literal.string(c.name) for c in columns])
    spoken = [k.sql(dialect="postgres").lower() for k in kinds]
    rows = exp.Anonymous(
        this="provisa_json_records",
        expressions=[
            _text(value, inner),
            names,
            exp.Array(expressions=[exp.Literal.string(k) for k in spoken]),
            exp.Literal.string(name),
        ],
    )
    cells = [
        exp.alias_(exp.cast(sqlglot.parse_one(f"_r[{i + 1}]", read=_D), kinds[i].copy()), c.name)
        for i, c in enumerate(columns)
    ]
    source = exp.Subquery(this=_duckdb("SELECT unnest(_rows) AS _r", _rows=rows))
    query = exp.select(*cells).from_(source)
    return exp.Subquery(this=query, alias=exp.TableAlias(this=alias.this.copy()))


def _is_null_record(value: exp.Expr) -> bool:
    node = value.unnest()
    said = node.to.sql(dialect="postgres").lower() if isinstance(node, exp.Cast) else ""
    return said == "record" and isinstance(node.this, exp.Null)


def _duckdb_table(table: exp.Table, name: str, inner: Callable) -> exp.Expr | None:
    """A set-returning call in a FROM that a UDF answers; None for one _table has."""
    args = _arguments(table.this)
    if name in _RECORDS:
        return _records_table(table, name, args[0], inner)
    if name in _POPULATE:
        if len(args) < 2 or not _is_null_record(args[0]):
            raise NoForm(name)  # a row type's columns are not known to the engine
        return _records_table(table, name, args[1], inner)
    if name in _LIST_TABLES:
        rows = exp.alias_(_row_of(_udf(_LIST_TABLES[name], args, name, inner), name), name)
        alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(name))
        return exp.Subquery(this=exp.select(rows), alias=alias.copy())
    return None


def _called(node: exp.Func) -> str:
    """The function a call names, whatever sqlglot parsed the call into."""
    name = node.name.lower() if isinstance(node, exp.Anonymous) else node.sql_name().lower()
    return _AGGREGATES.get(type(node), name)


def _call(node: exp.Func, dialect: str, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A call the table's rows do not take as they stand."""
    name = _called(node)
    if dialect in _OF[_D]:
        answer = _on_duckdb(node, name, inner)
        if answer is not None:
            if isinstance(node.parent, exp.Select) and node.arg_key == "expressions":
                return exp.alias_(answer, name.removeprefix("pg_"))  # named for the function
            return answer
    lacking = name in REFUSED or (dialect in _OF[_C] and name in _NOT_ON_CLICKHOUSE)
    lacking = lacking or (dialect not in _OF[_D] and name in _ONLY_ON_DUCKDB)
    second = name in ("row_to_json", "array_to_json") and len(node.expressions) > 1
    # A Table's call never reaches here: _table has it.
    listed = name in TABLES and not (name == "json_each" and dialect in _OF[_D])
    if lacking or second or listed:
        raise NoForm(name)
    if node.find(exp.Variadic) and name.startswith("json"):
        raise NoForm(name)
    if name == "jsonb_build_array":
        if dialect not in _OF[_D]:
            raise NoForm(name)
        return exp.Anonymous(
            this="json_array", expressions=[a.transform(inner) for a in node.expressions]
        )
    if name in ("jsonb_extract_path", "jsonb_extract_path_text"):
        if not all(isinstance(a, exp.Literal) for a in node.expressions[1:]):
            raise NoForm(name)
        steps = [a.this for a in node.expressions[1:]]
        return _path(node.expressions[0].transform(inner), steps, name.endswith("_text"))
    if name in _ON_JSON and node.expressions:
        node.expressions[0].replace(_parsed(node.expressions[0].copy(), dialect))
    return node


def lowered(tree: exp.Expr, dialect: str) -> exp.Expr:
    """``tree``'s json operators, set-returning calls and casts as the table writes them. Raises
    :class:`NoForm` for what it does not."""
    json_is_text = dialect in _OF[_T] | _OF[_C]
    on_duckdb = dialect in _OF[_D]

    def operated(name: str, node: exp.Expr, kind: type) -> exp.Expr:
        """An operator as the function it is, on DuckDB; refused by that name elsewhere."""
        if not on_duckdb:
            raise NoForm(name)
        args = [node.this, node.expression]
        if name in _COMPARED:
            call = _udf(_DUCKDB["jsonb_cmp"], args, name, rewrite)
            return kind(this=call, expression=exp.Literal.number(0))
        if name == "jsonb_delete":
            key = exp.Anonymous(this="to_json", expressions=[node.expression.transform(rewrite)])
            udf = exp.Anonymous(
                this="provisa_jsonb_delete", expressions=[_text(node.this, rewrite), key]
            )
            return exp.cast(udf, "JSON")
        return _udf(_DUCKDB[name], args, name, rewrite)

    def rewrite(node: exp.Expr) -> exp.Expr:
        kind = type(node)
        if kind in (exp.JSONBExtract, exp.JSONBExtractScalar):
            scalar = kind is exp.JSONBExtractScalar
            path = node.expression
            literal = isinstance(path, exp.Literal) and bool(path.this.strip()[1:-1])
            if on_duckdb and not literal:
                return _extracted(node, rewrite)
            steps = _steps("#>>" if scalar else "#>", path)
            return _path(node.this.transform(rewrite), steps, scalar)
        if isinstance(node, exp.JSONExtract | exp.JSONExtractScalar) and not isinstance(
            node.expression, exp.JSONPath
        ):
            stepped = _stepped(node, dialect, rewrite)
            if stepped is not None:
                return stepped
        if kind in _ON_KEYS:
            return operated(_ON_KEYS[kind], node, kind)
        if kind in _ON_JSONB and (_said_jsonb(node.this) or _said_jsonb(node.expression)):
            return operated(_ON_JSONB[kind], node, kind)
        if kind is exp.ArrayContainsAll and isinstance(node.expression, exp.Literal):
            if ":" in node.expression.this:  # a text[] literal has none outside its quotes
                return operated(_ON_JSONB[kind], node, kind)
        if kind in (exp.EQ, exp.NEQ) and _said_jsonb(node.this) and _said_jsonb(node.expression):
            if on_duckdb or (_literal_jsonb(node.this) and _literal_jsonb(node.expression)):
                return operated("jsonb_eq" if kind is exp.EQ else "jsonb_ne", node, kind)
        if isinstance(node, exp.Cast):
            if node.to.this in (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR):
                if _said_jsonb(node.this):
                    raise NoForm("jsonb_out")
            elif json_is_text and node.is_type("json", "jsonb"):
                return _cast(node, dialect)
        if on_duckdb and isinstance(node, exp.Filter):
            answered = _filtered(node, rewrite)
            if answered is not None:
                return answered
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Anonymous):
            name = node.this.name.lower()
            answered = _duckdb_table(node, name, rewrite) if on_duckdb else None
            if answered is not None:
                return answered
            if name in TABLES:
                if dialect in _OF[_C]:
                    raise NoForm(name)
                return _table(node, dialect)
        return _call(node, dialect, rewrite) if isinstance(node, exp.Func) else node

    return tree.transform(rewrite, copy=False)

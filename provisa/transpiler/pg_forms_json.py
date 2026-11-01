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
wire; nothing here runs per row on the engine, and ``->`` and ``->>`` stay the engine's own."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

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
    _T: "('$[' || CAST({i} AS VARCHAR) || ']')",
    _C: "if(startsWith(trimLeft(v), '['), if({i} < 0, {i}, {i} + 1), 0)",  # a key's place otherwise
}
_INDEX = {_D: "CAST(i AS BIGINT)", _T: "i", _C: "i"}
_KEY_INDEX = {_D: "TRY_CAST(k AS BIGINT)", _T: "k", _C: "toInt64OrNull(k)"}
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


def jsonb_text(text: str) -> str:
    """``text`` as jsonb_out writes it: spaced, its keys in order, a duplicate's last, a number's
    own digits."""

    def written(value: Any) -> str:
        if isinstance(value, dict):
            keys = sorted(value, key=lambda k: (len(k.encode()), k.encode()))
            members = (f"{json.dumps(k, ensure_ascii=False)}: {written(value[k])}" for k in keys)
            return "{" + ", ".join(members) + "}"
        if isinstance(value, list):
            return "[" + ", ".join(written(v) for v in value) + "]"
        if isinstance(value, Decimal):
            return "0" if value.is_zero() and "." not in str(value) else f"{value:f}"
        return json.dumps(value, ensure_ascii=False)

    return written(json.loads(text, parse_float=Decimal, parse_int=Decimal))


def jsonb_out(value: Any) -> Any:
    """A cell of a column the statement says is jsonb, as a client reads it."""
    return jsonb_text(value) if isinstance(value, str) and value else value


class NoForm(ValueError):
    """A json operator or use no form writes: ``called``, the name it is refused by."""

    def __init__(self, called: str) -> None:
        self.called = called
        super().__init__(called)


#: What may be json in a statement, for function_macros.mentioned. ``->`` is not: a statement that
#: holds only it costs nothing here.
OPERATORS = r"#>|#-|@>|<@|@\?|\?\||\?&|\s\?\s|\bjsonb?\b|\bjsonb?_|\bto_json"


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


def _steps(called: str, path: exp.Expr) -> list[str]:
    """A text[] path literal's elements."""
    inner = path.this.strip()[1:-1] if isinstance(path, exp.Literal) else ""
    if not inner:
        raise NoForm(called)
    return [step.strip().strip('"') for step in inner.split(",")]


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


def _cast(node: exp.Cast, dialect: str) -> exp.Expr:
    """A cast to a json type as the engine reads one: Trino's cast of text is a json string, and
    ClickHouse holds json as its text."""
    literal = isinstance(node.this, exp.Literal)
    if dialect in _OF[_T]:
        return exp.ParseJSON(this=node.this) if literal else exp.cast(node.this, "JSON")
    text = exp.DataType.build("Nullable(String)", dialect=_C)
    return node.this if literal else exp.cast(node.this, text)


def _call(node: exp.Func, dialect: str, inner: Callable[[exp.Expr], exp.Expr]) -> exp.Expr:
    """A call the table's rows do not take as they stand."""
    name = node.name.lower() if isinstance(node, exp.Anonymous) else node.sql_name().lower()
    name = _AGGREGATES.get(type(node), name)
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

    def rewrite(node: exp.Expr) -> exp.Expr:
        kind = type(node)
        if kind in (exp.JSONBExtract, exp.JSONBExtractScalar):
            scalar = kind is exp.JSONBExtractScalar
            steps = _steps("#>>" if scalar else "#>", node.expression)
            return _path(node.this.transform(rewrite), steps, scalar)
        if kind in _ON_KEYS:
            raise NoForm(_ON_KEYS[kind])
        if kind in _ON_JSONB and (_said_jsonb(node.this) or _said_jsonb(node.expression)):
            raise NoForm(_ON_JSONB[kind])
        if kind is exp.ArrayContainsAll and isinstance(node.expression, exp.Literal):
            if ":" in node.expression.this:  # a text[] literal has none outside its quotes
                raise NoForm(_ON_JSONB[kind])
        if kind in (exp.EQ, exp.NEQ) and _said_jsonb(node.this) and _said_jsonb(node.expression):
            if _literal_jsonb(node.this) and _literal_jsonb(node.expression):
                raise NoForm("jsonb_eq" if kind is exp.EQ else "jsonb_ne")
        if isinstance(node, exp.Cast):
            if node.to.this in (exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR):
                if _said_jsonb(node.this):
                    raise NoForm("jsonb_out")
            elif json_is_text and node.is_type("json", "jsonb"):
                return _cast(node, dialect)
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Anonymous):
            if node.this.name.lower() in TABLES:
                if dialect in _OF[_C]:
                    raise NoForm(node.this.name.lower())
                return _table(node, dialect)
        return _call(node, dialect, rewrite) if isinstance(node, exp.Func) else node

    return tree.transform(rewrite)

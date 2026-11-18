# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e2b7a-91d4-4f3e-b8a6-5d1f7e3c9a24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON built of SQL values on ClickHouse (REQ-2236; REQ-2171), where a json or jsonb value is its
text: to_json, to_jsonb, json_build_array, jsonb_build_array, array_to_json, and the aggregates
json_agg, jsonb_agg, json_agg_strict and jsonb_agg_strict.

A value goes into JSON by the type the statement says of it: an integer or a numeric as its
digits, a boolean as true or false, a text as a JSON string, a json or jsonb value as its own text;
NULL as null inside a container. A value of any other type, or of no said type, is refused by name:
ClickHouse's text of it (a float's, a date's) is not PostgreSQL's. An array, for array_to_json, is
an array of one of those types, written by ClickHouse's toJSONString, at any depth.

A jsonb's text is written again at the wire (pg_forms_json.jsonb_out), so its spacing and key order
are jsonb_out's whatever ClickHouse wrote; json's spacing is ClickHouse's, the same JSON.

An aggregate keeps a NULL value as null (the _strict ones drop it), answers NULL of no rows (or of
no value the _strict ones keep), and holds an ORDER BY of one key, either way and its NULLs where
the statement puts them. A FILTER, a window, DISTINCT or a second key is refused by name. What a
rule writes is not walked again, so each value is a literal, a column or a cast of one."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from collections.abc import Callable

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import clickhouse, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refused, Rewrite, replacement
from provisa.transpiler.polyfill.numbers import ch_only
from provisa.transpiler.polyfill.server import plain

FAMILY = "JSON"

_INTEGERS = frozenset({"smallint", "integer", "bigint"})
_TEXTS = frozenset({"text", "character varying", "name"})
_JSONS = frozenset({"json", "jsonb"})
#: A said type -> ClickHouse's JSON text of a value ``{v}`` of it, not NULL.
_SCALAR = {
    **dict.fromkeys(_INTEGERS, "toString({v})"),
    "numeric": "toString({v})",
    "boolean": "if({v}, 'true', 'false')",
    **dict.fromkeys(_TEXTS, "toJSONString(assumeNotNull({v}))"),
    **dict.fromkeys(_JSONS, "assumeNotNull({v})"),
}
#: The element types of an array toJSONString writes as array_to_json does.
_ARRAYS = frozenset(f"{t}[]" for t in (*_INTEGERS, "boolean", *_TEXTS))


def said_of(node: exp.Expr) -> str | None:
    """The type the statement says of ``node``; a string literal is text, as PostgreSQL reads an
    unknown literal here (and as a call by pg_catalog leaves a text literal, its cast removed)."""
    said = trino.said(node)
    if said is None and isinstance(node, exp.Literal) and node.is_string:
        return "text"
    return said


def value_of(node: exp.Expr, function: str, dialect: str) -> tuple[str, exp.Expr]:
    """The said type of a value going into JSON and the value as the form holds it, refused by
    name where the type is none of ``_SCALAR``'s or the value is no literal, column or cast of one.
    A literal integer is read as PostgreSQL reads it (22003 past its type): what a rule writes is
    not walked again."""
    said = said_of(node)
    if said not in _SCALAR or not plain(node):
        raise Refused(trino.no_form(function, dialect))
    return said, integer_literal(node, dialect)


def integer_literal(node: exp.Expr, dialect: str) -> exp.Expr:
    """``node``, a literal integer read as PostgreSQL reads it: its 22003 or 22P02 is the call's
    answer, before any refusal of the call by its name."""
    try:
        return clickhouse.integer_read(node, dialect) or node
    except Refused as refused:
        raise Refused(refused.refusal, ruling=True) from refused


def element(said: str, hole: str) -> str:
    """A value's JSON text inside a container: null of NULL."""
    return f"if(isNull({hole}), 'null', {_SCALAR[said].format(v=hole)})"


def written(form: str, holes: dict[str, exp.Expr], dialect: str) -> exp.Expr:
    pattern = f"f({', '.join(f'${h}' for h in holes)})" if holes else "f()"
    return graft(replacement(form, dialect, pattern), holes)


def _named(node: exp.Expr, names: frozenset[str]) -> tuple[str, list[exp.Expr]] | None:
    call = trino.called(node)
    if call is None or not (found := call[0] & names):
        return None
    return min(found), call[1]


_SCALARS = frozenset({"to_json", "to_jsonb"})


def scalar_json(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """to_json and to_jsonb of a value the statement says the type of: its JSON text, NULL of
    NULL."""
    named = _named(node, _SCALARS)
    if named is None:
        return None
    function, args = named
    if len(args) != 1:
        return None
    said, value = value_of(args[0], function, dialect)
    form = f"CAST(if(isNull($v), NULL, {_SCALAR[said].format(v='$v')}) AS Nullable(String))"
    return written(form, {"v": value}, dialect)


_BUILT = frozenset({"json_build_array", "jsonb_build_array"})


def built_array(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_build_array and jsonb_build_array of values the statement says the types of: a JSON
    array of each, a NULL as null; of none, the empty array."""
    named = _named(node, _BUILT)
    if named is None:
        return None
    function, args = named
    if any(isinstance(a, exp.Variadic) for a in args):
        raise Refused(trino.no_form(function, dialect))
    values = [value_of(a, function, dialect) for a in args]
    holes = {f"a{i}": value for i, (_, value) in enumerate(values)}
    elements = [element(said, f"$a{i}") for i, (said, _) in enumerate(values)]
    form = f"concat('[', arrayStringConcat([{', '.join(elements)}], ', '), ']')" if args else "'[]'"
    return written(form, holes, dialect)


#: What an array literal is once read (transpile writes '{1,2}'::integer[] as ClickHouse's array).
_ARRAY_LITERAL = (exp.Cast, exp.Array, exp.Literal, exp.Null, exp.DataType, exp.Neg, exp.Paren)


def literal_array(node: exp.Expr) -> bool:
    return all(isinstance(n, _ARRAY_LITERAL) for n in node.walk())


_T = exp.DataType.Type
#: The element types of an array read as ClickHouse's whose JSON toJSONString writes as
#: array_to_json does.
_HELD_ELEMENTS = frozenset(
    {_T.SMALLINT, _T.INT, _T.BIGINT, _T.BOOLEAN, _T.TEXT, _T.VARCHAR, _T.NAME}
)


def _array_of_held(node: exp.Expr) -> bool:
    """Whether ``node`` is an array of ``_ARRAYS``' element types: by the type the statement says,
    or, for an array of arrays the statement's literal was read as, by the element type its
    ClickHouse cast names."""
    if trino.said(node) in _ARRAYS:
        return True
    if not isinstance(node, exp.Cast) or not literal_array(node):
        return False
    element = node.to
    while element.this == _T.ARRAY and element.expressions:
        element = element.expressions[0]
    return element.this in _HELD_ELEMENTS


def array_json(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """array_to_json of an array of integers, truth values or texts the statement says: its JSON
    array at any depth, a NULL element null; NULL of NULL. Its pretty flag adds only whitespace,
    which is no difference in JSON; a NULL flag answers NULL."""
    named = _named(node, frozenset({"array_to_json"}))
    if named is None:
        return None
    function, args = named
    if not 1 <= len(args) <= 2:
        return None
    if not _array_of_held(args[0]) or not all(plain(a) or literal_array(a) for a in args):
        raise Refused(trino.no_form(function, dialect))
    if len(args) == 2 and trino.said(args[1]) != "boolean":
        raise Refused(trino.no_form(function, dialect))
    holes = {f"a{i}": a for i, a in enumerate(args)}
    nulls = " OR ".join(f"isNull($a{i})" for i in range(len(args)))
    form = f"CAST(if({nulls}, NULL, toJSONString(assumeNotNull($a0))) AS Nullable(String))"
    return written(form, holes, dialect)


#: An aggregate -> whether it drops a NULL value.
_AGGREGATES = {
    "json_agg": False,
    "jsonb_agg": False,
    "json_agg_strict": True,
    "jsonb_agg_strict": True,
}


def _aggregated(node: exp.Expr) -> tuple[str, exp.Expr] | None:
    """The aggregate ``node`` calls and its one argument, or None."""
    if isinstance(node, exp.JSONArrayAgg):
        return "json_agg", node.this
    if isinstance(node, exp.Anonymous) and node.name.lower() in _AGGREGATES:
        if len(node.expressions) != 1:
            return None
        return node.name.lower(), node.expressions[0]
    named = _named(node, frozenset(_AGGREGATES))
    if named is None or len(named[1]) != 1:
        return None
    return named[0], named[1][0]


def sorted_by(ordered: exp.Ordered, null_at: int = 2, value_at: int = 3) -> tuple[str, str]:
    """ClickHouse's sort of rows ``_t`` by a key held as ``_t.<null_at>`` (whether NULL) and
    ``_t.<value_at>`` (its value): the sort function and the key, the NULLs where the statement
    puts them (PostgreSQL's default: last ascending, first descending)."""
    desc = bool(ordered.args.get("desc"))
    nulls_first = ordered.args.get("nulls_first")
    first = desc if nulls_first is None else bool(nulls_first)
    # arraySort puts the smaller first, arrayReverseSort the larger: the NULL flag decides first.
    null_rank = f"_t.{null_at}" if first == desc else f"NOT _t.{null_at}"
    return ("arrayReverseSort" if desc else "arraySort"), f"({null_rank}, _t.{value_at})"


def aggregate_json(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_agg, jsonb_agg and their _strict forms of a value the statement says the type of: a
    JSON array of its values in the ORDER BY's order, NULL of no rows."""
    found = _aggregated(node)
    if found is None:
        return None
    function, value = found
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if isinstance(outer, (exp.Filter, exp.Window)) or isinstance(value, exp.Distinct):
        raise Refused(trino.no_form(function, dialect))
    holes: dict[str, exp.Expr] = {}
    sort = None
    if isinstance(value, exp.Order):
        keys = value.expressions
        if len(keys) != 1 or not plain(keys[0].this):
            raise Refused(trino.no_form(function, dialect))
        holes["k"] = keys[0].this
        sort = sorted_by(keys[0])
        value = value.this
    said, holes["v"] = value_of(value, function, dialect)
    key = "isNull($k), assumeNotNull($k)" if sort else "0, 0"
    rows = f"groupArray(tuple({element(said, '$v')}, {key}, isNull($v)))"
    if _AGGREGATES[function]:
        rows = f"arrayFilter(_t -> NOT _t.4, {rows})"
    if sort:
        rows = f"{sort[0]}(_t -> {sort[1]}, {rows})"
    # NULL of no rows; a _strict one's rows all NULL are the empty array.
    form = (
        "if(count() = 0, NULL, "
        f"concat('[', arrayStringConcat(arrayMap(_t -> _t.1, {rows}), ', '), ']'))"
    )
    return written(form, holes, dialect)


TYPES = Limitation(
    "Of a value the statement says is an integer, a numeric, a boolean, a text, json or jsonb, "
    "as a literal, a column or a cast of one: any other value is refused by name, as ClickHouse's "
    "text of it differs."
)


def called_names(node: exp.Expr) -> frozenset[str]:
    """The names of the function ``node`` calls, as PostgreSQL names it: sqlglot reads json_agg,
    json_object_agg and jsonb_object_agg as nodes of their own, and json_extract_path(v, VARIADIC
    path) as -> of the path."""
    if isinstance(node, exp.JSONArrayAgg):
        return frozenset({"json_agg"})
    if isinstance(node, exp.JSONObjectAgg):
        return frozenset({"json_object_agg"})
    if isinstance(node, exp.JSONBObjectAgg):
        return frozenset({"jsonb_object_agg"})
    if isinstance(node, exp.JSONExtract | exp.JSONExtractScalar) and isinstance(
        node.expression, exp.Variadic
    ):
        text = "_text" if isinstance(node, exp.JSONExtractScalar) else ""
        return frozenset({f"json_extract_path{text}"})
    call = trino.called(node)
    return frozenset() if call is None else call[0]


def for_name(name: str, rewrite: Callable[[exp.Expr, str], exp.Expr | None]) -> Rewrite:
    """``rewrite`` of a call of ``name`` alone: each row answers its own function's calls."""

    def only(node: exp.Expr, dialect: str) -> exp.Expr | None:
        return rewrite(node, dialect) if name in called_names(node) else None

    only.__doc__ = rewrite.__doc__
    only.__qualname__ = only.__name__ = f"{rewrite.__name__}_{name}"
    return Rewrite(only)


def _feature(name: str, summary: str, rewrite, *extra: Limitation) -> Feature:
    return Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, ch_only(for_name(name, rewrite))),),
            limitations={"clickhouse": (TYPES, *extra)},
        ),
    )


AGGREGATE_LIMIT = Limitation(
    "An ORDER BY of one key; a FILTER, a window, DISTINCT or a second key is refused by name."
)

FEATURES: tuple[Feature, ...] = (
    *(_feature(name, "A value as its JSON.", scalar_json) for name in sorted(_SCALARS)),
    *(_feature(name, "A JSON array of the values given.", built_array) for name in sorted(_BUILT)),
    _feature(
        "array_to_json",
        "An array as a JSON array.",
        array_json,
        Limitation("Of an array of integers, booleans or texts only."),
    ),
    *(
        _feature(name, "A JSON array of an aggregate's values.", aggregate_json, AGGREGATE_LIMIT)
        for name in ("jsonb_agg", "json_agg_strict", "jsonb_agg_strict")
    ),
)

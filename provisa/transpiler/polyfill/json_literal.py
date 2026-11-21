# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2c8e71-4a9b-4f36-b1e7-8c3a6f2d9e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""jsonb's comparisons, hashes, containment, changes at a path, jsonpath and the other json and
jsonb functions whose answer needs jsonb's ordering or its canonical text, on ClickHouse
(REQ-2236; REQ-2171), of literal arguments: computed as the statement is read, by the functions
DuckDB's UDFs run (provisa.federation.duckdb_pg_json, each following PostgreSQL's jsonfuncs.c,
jsonb_op.c, jsonb_util.c and jsonpath_exec.c), and written as a literal of the answer.

PostgreSQL folds an immutable function of constants as it plans the statement, so its error is the
statement's whatever rows it reads; so it is here. A call with an argument that is no literal (a
column, a call) is refused by name: ClickHouse's JSON functions write no jsonb text and hold no
jsonb order."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.core.pg_error import END, MARK
from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.json_built import called_names, for_name
from provisa.transpiler.polyfill.notation import Refusal, Refused
from provisa.transpiler.polyfill.numbers import ch_only

FAMILY = "JSON"

#: A constant argument no literal gives.
_NOT_LITERAL = object()
#: A text[] literal of more than one dimension.
_NESTED = object()


def _peeled(node: exp.Expr) -> exp.Expr:
    node = node.unnest()
    while isinstance(node, exp.Cast):
        node = node.this.unnest()
    return node


def _text(node: exp.Expr) -> Any:
    inner = _peeled(node)
    if isinstance(inner, exp.Null):
        return None
    if isinstance(inner, exp.Literal) and inner.is_string:
        return inner.this
    return _NOT_LITERAL


def _boolean(node: exp.Expr) -> Any:
    inner = _peeled(node)
    if isinstance(inner, exp.Null):
        return None
    if isinstance(inner, exp.Boolean):
        return bool(inner.this)
    return _NOT_LITERAL


def _integer(node: exp.Expr) -> Any:
    inner = node.unnest()
    while isinstance(inner, exp.Cast):
        inner = inner.this.unnest()
    negative = False
    while isinstance(inner, (exp.Neg, exp.Paren)):
        negative ^= isinstance(inner, exp.Neg)
        inner = inner.this
    if isinstance(inner, exp.Null):
        return None
    if isinstance(inner, exp.Literal) and not inner.is_string and re.fullmatch(r"\d+", inner.this):
        return -int(inner.this) if negative else int(inner.this)
    if (
        isinstance(inner, exp.Literal)
        and inner.is_string
        and re.fullmatch(r"\s*-?\d+\s*", inner.this)
    ):
        return int(inner.this)
    return _NOT_LITERAL


class _Nested(list):
    """A text[] literal of two dimensions: its rows, each a list of its elements."""


def _flat(items: list[exp.Expr]) -> list[str | None] | object:
    out: list[str | None] = []
    for item in items:
        item = _peeled(item)
        if isinstance(item, exp.Null):
            out.append(None)
        elif isinstance(item, exp.Literal):
            out.append(item.this)
        else:
            return _NESTED if isinstance(item, exp.Array) else _NOT_LITERAL
    return out


def _texts(node: exp.Expr) -> Any:
    """A text[] literal's elements (a NULL element None), as transpile reads one (ClickHouse's
    array) or as written ('{a,b}'); a ``_Nested`` of its rows of two dimensions, ``_NESTED`` of
    more."""
    inner = _peeled(node)
    if isinstance(inner, exp.Null):
        return None
    if isinstance(inner, exp.Array):
        items = [_peeled(item) for item in inner.expressions]
        if items and all(isinstance(item, exp.Array) for item in items):
            rows = [_flat(item.expressions) for item in items]
            if any(not isinstance(row, list) for row in rows):
                return _NESTED if _NESTED in rows else _NOT_LITERAL
            return _Nested(rows)
        return _flat(items)
    if isinstance(inner, exp.Literal) and inner.is_string:
        written = inner.this.strip()
        if not written.startswith("{") or "{" in written[1:]:
            return _NOT_LITERAL
        from provisa.transpiler.pg_forms_json import _array_items

        return _array_items(written)
    return _NOT_LITERAL


_READERS: dict[str, Callable[[exp.Expr], Any]] = {
    "j": _text,
    "t": _text,
    "p": _text,
    "b": _boolean,
    "i": _integer,
    "a": _texts,
}


@dataclass(frozen=True)
class _Call:
    """A function answered of literals: what computes it (by the name of the function of
    provisa.federation.duckdb_pg_json it calls, and how), its parameters' kinds (j a json, t a
    text, p a jsonpath, b a boolean, i an integer, a a text[]), the defaults of its trailing
    parameters, what it answers, and whether it answers NULL of a NULL argument."""

    compute: Callable[..., Any]
    kinds: str
    defaults: tuple[Any, ...]
    answers: str  # bool, int, bigint, text, rows
    strict: bool = True


def _udf(name: str) -> Callable[..., Any]:
    def call(*args: Any) -> Any:
        from provisa.federation import duckdb_pg_json

        return getattr(duckdb_pg_json, name)(*args)

    return call


def _ordered(test: Callable[[int], bool]) -> Callable[..., Any]:
    cmp = _udf("cmp")
    return lambda left, right: test(cmp(left, right))


def _jsonb_object(keys: list[str | None], values: list[str | None] | None = None) -> str:
    from provisa.federation.duckdb_pg_scalars import json_object
    from provisa.transpiler.pg_forms_json import jsonb_text

    return jsonb_text(json_object(keys, values))


def _json_object(keys: list[str | None], values: list[str | None] | None = None) -> str:
    from provisa.federation.duckdb_pg_scalars import json_object

    return json_object(keys, values)


def _named(name: str) -> Callable[..., Any]:
    udf = _udf("strip_nulls")
    return lambda text: udf(text, name)


_PATH = ("{}", False)
_OPERATOR = ("{}", True)
CALLS: dict[str, _Call] = {
    "jsonb_contains": _Call(_udf("contains"), "jj", (), "bool"),
    "jsonb_contained": _Call(_udf("contained"), "jj", (), "bool"),
    "jsonb_cmp": _Call(_udf("cmp"), "jj", (), "int"),
    "jsonb_eq": _Call(_ordered(lambda c: c == 0), "jj", (), "bool"),
    "jsonb_ne": _Call(_ordered(lambda c: c != 0), "jj", (), "bool"),
    "jsonb_lt": _Call(_ordered(lambda c: c < 0), "jj", (), "bool"),
    "jsonb_le": _Call(_ordered(lambda c: c <= 0), "jj", (), "bool"),
    "jsonb_gt": _Call(_ordered(lambda c: c > 0), "jj", (), "bool"),
    "jsonb_ge": _Call(_ordered(lambda c: c >= 0), "jj", (), "bool"),
    "jsonb_hash": _Call(_udf("jsonb_hash"), "j", (), "int"),
    "jsonb_hash_extended": _Call(_udf("jsonb_hash_extended"), "ji", (), "bigint"),
    "jsonb_pretty": _Call(_udf("pretty"), "j", (), "text"),
    "jsonb_set": _Call(_udf("set_path"), "jajb", (True,), "text"),
    "jsonb_set_lax": _Call(_udf("set_lax"), "jajbt", (True, "use_json_null"), "text", False),
    "jsonb_insert": _Call(_udf("insert"), "jajb", (False,), "text"),
    "jsonb_delete_path": _Call(_udf("delete_path"), "ja", (), "text"),
    "json_strip_nulls": _Call(_named("json_strip_nulls"), "j", (), "text"),
    "jsonb_strip_nulls": _Call(_named("jsonb_strip_nulls"), "j", (), "text"),
    "json_object": _Call(_json_object, "aa", ((),), "text"),
    "jsonb_object": _Call(_jsonb_object, "aa", ((),), "text"),
    **{
        f"jsonb_path_{name}{suffix}": _Call(_udf(f"path_{name}"), "jpjb", _PATH, answers)
        for name, answers in (
            ("exists", "bool"),
            ("match", "bool"),
            ("query", "rows"),
            ("query_array", "text"),
            ("query_first", "text"),
        )
        for suffix in ("", "_tz")
    },
    "jsonb_path_exists_opr": _Call(_udf("path_exists"), "jpjb", _OPERATOR, "bool"),
    "jsonb_path_match_opr": _Call(_udf("path_match"), "jpjb", _OPERATOR, "bool"),
}
#: A function whose second argument, NULL, leaves the call to its one-argument form: json_object
#: of one array.
_OPTIONAL_SECOND = frozenset({"json_object", "jsonb_object"})

#: The functions that change a jsonb at a path: setPath's 22004 of a NULL step it reaches.
_AT_A_PATH = frozenset({"jsonb_set", "jsonb_set_lax", "jsonb_insert", "jsonb_delete_path"})


def _null_step(text: str, path: list[str | None], name: str) -> None:
    """PostgreSQL's 22004 of a NULL path element its descent reaches (setPath checks the step of
    each level it visits); the shared functions read a NULL step as no step."""
    from provisa.federation import duckdb_pg_json

    try:
        value = duckdb_pg_json._jsonb(text)
    except ValueError as error:
        raise _raised(error) from error
    if not path or not isinstance(value, (dict, list)):
        return  # a scalar raises its own error, no path changes nothing
    if not value and name == "jsonb_delete_path":
        return  # an empty container is answered before its path is read
    level, current = 0, value
    while True:
        if path[level] is None:
            raise Refused(
                Refusal("22004", f"path element at position {level + 1} is null"), ruling=True
            )
        if not isinstance(current, (dict, list)) or level == len(path) - 1:
            return
        step = path[level]
        assert step is not None  # a NULL step raised above
        if isinstance(current, dict):
            if step not in current:
                return
            current = current[step]
        else:
            if not re.fullmatch(r"\s*[-+]?\d+\s*", step):
                return  # setPathArray's own 22P02
            index = int(step)
            index = len(current) + index if index < 0 else index
            if not 0 <= index < len(current):
                return
            current = current[index]
        level += 1


_ERROR = re.compile(re.escape(MARK) + r" (\w{5}): (.*?)" + re.escape(END), re.DOTALL)


def _raised(error: Exception) -> Refused:
    """A function's PostgreSQL error, as the call's refusal."""
    found = _ERROR.search(str(error))
    if found is None:
        raise error
    return Refused(Refusal(found.group(1), found.group(2)), ruling=True)


_TYPES = {
    "bool": "Nullable(Bool)",
    "int": "Nullable(Int32)",
    "bigint": "Nullable(Int64)",
    "text": "Nullable(String)",
}


def _literal(answer: Any, answers: str) -> exp.Expr:
    if answers == "rows":
        texts = [exp.Literal.string(t) for t in answer or []]
        array = exp.cast(exp.Array(expressions=texts), "Array(String)", dialect="clickhouse")
        return exp.Anonymous(this="arrayJoin", expressions=[array])
    if answer is None:
        value: exp.Expr = exp.Null()
    elif answers == "bool":
        value = exp.Boolean(this=bool(answer))
    elif answers in ("int", "bigint"):
        value = exp.Literal.number(int(answer))
    else:
        value = exp.Literal.string(str(answer))
    return exp.cast(value, _TYPES[answers], dialect="clickhouse")


def literal_call(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """A call of one of ``CALLS`` whose arguments are literals: its answer, computed as the
    statement is read, as a literal; PostgreSQL's error the call's. Refused by name where an
    argument is no literal."""
    found = called_names(node) & set(CALLS)
    if not found:
        return None
    name = min(found)
    call = trino.called(node)
    assert call is not None
    args = list(call[1])
    spec = CALLS[name]
    if not len(spec.kinds) - len(spec.defaults) <= len(args) <= len(spec.kinds):
        return None
    values: list[Any] = []
    for kind, arg in zip(spec.kinds, args, strict=False):
        value = _READERS[kind](arg)
        if value is _NOT_LITERAL:
            raise Refused(trino.no_form(name, dialect))
        values.append(value)
    if name not in _OPTIONAL_SECOND:  # json_object of one array reads it as pairs
        values.extend(spec.defaults[len(args) - (len(spec.kinds) - len(spec.defaults)) :])
    if spec.strict and any(v is None for v in values):
        return _literal(None, spec.answers)
    if name in _OPTIONAL_SECOND and len(values) == 1 and isinstance(values[0], _Nested):
        if any(len(row) != 2 for row in values[0]):
            raise Refused(Refusal("2202E", "array must have two columns"), ruling=True)
        values = [[text for row in values[0] for text in row]]  # json_object of pairs
    if any(v is _NESTED or isinstance(v, _Nested) for v in values):
        raise Refused(Refusal("2202E", "wrong number of array subscripts"), ruling=True)
    if name in _AT_A_PATH and values[0] is not None and values[1] is not None:
        _null_step(values[0], values[1], name)
    try:
        answer = spec.compute(*values)
    except ValueError as error:
        raise _raised(error) from error
    return _literal(answer, spec.answers)


LITERALS = Limitation(
    "Of literal arguments only, computed as the statement is read (PostgreSQL folds an immutable "
    "function of constants as it plans): an argument that is a column or a call is refused by "
    "name, as ClickHouse's JSON functions write no jsonb text and hold no jsonb order."
)

#: The family's place in the grid (polyfill.GRID), which tries the rules of one function in
#: the order of their families.
ORDER = 240
FEATURES: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        "A json or jsonb function computed of literal arguments.",
        read=Side(
            rules=(Rule(None, ch_only(for_name(name, literal_call))),),
            limitations={"clickhouse": (LITERALS,)},
        ),
    )
    for name in sorted(CALLS)
    if name != "json_object"  # calls.py's row, beside Trino's refusal
)

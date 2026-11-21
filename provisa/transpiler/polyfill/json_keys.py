# Copyright (c) 2026 Kenneth Stott
# Canary: a23a7252-356f-4377-acd0-482840b1e8fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A json or jsonb value's keys on ClickHouse (REQ-2236; REQ-2171), where a jsonb is its text and
ClickHouse's JSON functions read it.

jsonb_exists, jsonb_exists_any and jsonb_exists_all (jsonb_ops' ?, ?| and ?|&): whether a text is
a key of an object at its top level, a string element of an array, or the string a scalar is
(jsonb's raw scalar is an array of one). The NULL elements of a text[] are skipped, and its
dimensions read element by element: no text exists in nothing, and every text of none does.

jsonb_object_keys: an object's distinct keys in jsonb's order, the shorter first and then by bytes
(jsonb's key order); json_object_keys: an object's keys as written, a repeated key each time. Of a
scalar or an array each raises PostgreSQL's 22023, and of NULL answers no rows."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Rule, Side
from provisa.transpiler.polyfill.json_built import for_name, written
from provisa.transpiler.polyfill.notation import Refused
from provisa.transpiler.polyfill.numbers import ch_fail, ch_once, ch_only

FAMILY = "JSON"

#: A value's kind by its first character, as jsonb_typeof names it.
_FIRST = "substring(trimLeft({v}), 1, 1)"


def _strings(v: str) -> str:
    """The texts a value holds for ?: an object's keys, an array's string elements, a scalar
    string."""
    first = _FIRST.format(v=v)
    elements = (
        f"arrayFilter(_e -> _e IS NOT NULL, arrayMap(_i -> if(JSONType({v}, _i) = 'String', "
        f"JSONExtractString({v}, _i), NULL), range(1, JSONLength({v}) + 1)))"
    )
    return (
        f"multiIf({first} = '{{', JSONExtractKeys({v}), {first} = '[', "
        f"CAST({elements} AS Array(String)), {first} = '\"', [JSONExtractString({v})], "
        "CAST([] AS Array(String)))"
    )


def _texts(a: str) -> str:
    """A text[]'s elements that are not NULL, of every dimension."""
    return f"arrayFilter(_t -> _t IS NOT NULL, arrayFlatten({a}))"


def _strict(holes: str, answer: str) -> str:
    nulls = " OR ".join(f"${h} IS NULL" for h in holes)
    return ch_once(f"CAST(if({nulls}, NULL, {answer}) AS Nullable(Bool))")


def _feature(name: str, pattern: str, summary: str, form: str) -> Feature:
    return Feature(name, FAMILY, summary, read=Side(rules=(Rule(pattern, ch_only(form)),)))


def _text_array(node: exp.Expr, name: str, dialect: str) -> exp.Expr:
    """A text[] argument as ClickHouse holds one: a literal of unknown type written as an array
    ('{a,b}', which PostgreSQL reads as the parameter's text[]) as an array of its elements, a
    sub-array's in place (the form reads every dimension); any other argument as it stands."""
    inner = node.unnest()
    if not (isinstance(inner, exp.Literal) and inner.is_string):
        return node
    if not inner.this.lstrip().startswith("{"):
        raise Refused(trino.no_form(name, dialect))
    from provisa.transpiler.pg_forms_json import _array_items

    cells = [
        exp.Null() if item is None else exp.Literal.string(item)
        for item in _array_items(inner.this)
    ]
    return exp.cast(exp.Array(expressions=cells), "Array(Nullable(String))", dialect="clickhouse")


def _exists(name: str, form: str):
    """``form`` of a call of ``name`` (a jsonb and a text[]), its text[] read by _text_array."""

    def exists(node: exp.Expr, dialect: str) -> exp.Expr | None:
        called = trino.called(node)
        if called is None or len(called[1]) != 2:
            return None
        value, texts = called[1]
        return written(form, {"v": value, "a": _text_array(texts, name, dialect)}, dialect)

    exists.__doc__ = (
        f"{form}, $a a text[] argument, or a literal of unknown type written as an array ('{{a,b}}') "
        "as an Array(Nullable(String)) of its elements; another literal is refused by name."
    )
    return for_name(name, exists)


def _keys(name: str, answer: str) -> str:
    """The rows of ``answer`` (an array of $v's keys); 22023 of a scalar or an array."""
    first = _FIRST.format(v="$v")
    raises = [
        ch_fail("22023", f"cannot call {name} on a scalar").format(
            condition=f"$v IS NOT NULL AND {first} NOT IN ('{{', '[')"
        ),
        ch_fail("22023", f"cannot call {name} on an array").format(
            condition=f"$v IS NOT NULL AND {first} = '['"
        ),
    ]
    # ClickHouse holds no Nullable(Array): each raise stands beside the answer, 0 where none does.
    none = "CAST([] AS Array(String))"
    return f"arrayJoin(if({' + '.join(raises)} = 0 AND $v IS NOT NULL, {answer}, {none}))"


_OBJECT = "JSONExtractKeys(ifNull($v, '{}'))"
#: The value, never NULL where it is read (the call is NULL of a NULL first): ClickHouse holds no
#: Nullable(Array) of its keys.
_HELD = "ifNull(_v, 'null')"

FEATURES: tuple[Feature, ...] = (
    _feature(
        "jsonb_exists",
        "jsonb_exists($v, $k)",
        "Whether a text is a top-level key, a string element, or the scalar string of a jsonb.",
        _strict("vk", f"has({_strings(_HELD)}, _k)"),
    ),
    Feature(
        "jsonb_exists_any",
        FAMILY,
        "Whether any text of an array is a top-level key or string element of a jsonb.",
        read=Side(
            rules=(
                Rule(
                    None,
                    ch_only(
                        _exists(
                            "jsonb_exists_any",
                            _strict("va", f"hasAny({_strings(_HELD)}, {_texts('_a')})"),
                        )
                    ),
                ),
            )
        ),
    ),
    Feature(
        "jsonb_exists_all",
        FAMILY,
        "Whether every text of an array is a top-level key or string element of a jsonb.",
        read=Side(
            rules=(
                Rule(
                    None,
                    ch_only(
                        _exists(
                            "jsonb_exists_all",
                            _strict("va", f"hasAll({_strings(_HELD)}, {_texts('_a')})"),
                        )
                    ),
                ),
            )
        ),
    ),
    _feature(
        "jsonb_object_keys",
        "jsonb_object_keys($v)",
        "An object's distinct keys, in jsonb's order: the shorter first, then by bytes.",
        _keys(
            "jsonb_object_keys",
            f"arraySort(_k -> (length(_k), _k), arrayDistinct({_OBJECT}))",
        ),
    ),
    _feature(
        "json_object_keys",
        "json_object_keys($v)",
        "An object's keys as written, a repeated key each time.",
        _keys("json_object_keys", _OBJECT),
    ),
)

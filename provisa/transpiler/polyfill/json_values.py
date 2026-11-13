# Copyright (c) 2026 Kenneth Stott
# Canary: 6721a7ce-fc34-4a23-a2a6-778f2931c303
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""JSON built of values that are no JSON text, on Trino (REQ-2236; the function support measure,
run 43337203459, REQ-2171).

json_agg: Trino's form of it (transpile._rewrite_json_arrayagg_for_trino, after the grid) parses
each value as JSON text, which an integer is not. Of integers the statement says, it is Trino's
array of them cast to JSON: each integer's digits, NULL as null, as to_json writes them. Of a
text the statement says, refused by name: that form would read '1' as the number 1, where
PostgreSQL's answer is the string "1".

json_build_object and jsonb_build_object: PostgreSQL casts each key to its text; Trino's
JSON_OBJECT takes a text key only. A key of an integer or a boolean is cast to text, whose digits
and true/false are PostgreSQL's. A key of any other type the statement says (a number, a date) is
refused by name, as Trino's text of it may differ; a key of no said type is the engine's, and
an odd count of arguments is refused as PostgreSQL refuses it.

What a rule writes is not walked again (function_macros), so a call it would hold is refused by
name: each value is a literal, a column or a cast of one.

On ClickHouse (run 43410717058, which sent json_agg as written), json_agg of a number, a text or a
truth value the statement says is toJSONString of its values, each in a tuple so a NULL is kept,
and NULL of no rows; of any other value it is refused by name."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refusal, Refused, Rewrite, replacement
from provisa.transpiler.polyfill.server import plain

FAMILY = "JSON of values on Trino"

_INTEGERS = frozenset({"smallint", "integer", "bigint"})
#: Key types whose text Trino's cast to varchar writes as PostgreSQL's output function does.
_KEYS_CAST = _INTEGERS | {"boolean"}
_TEXTS = frozenset({"text", "character varying", "character", "name", "bpchar", "unknown"})
_OBJECTS = ("json_build_object", "jsonb_build_object")
#: json_build_object's own refusal of an odd count (json.c).
ODD = Refusal("22023", "argument list must have even number of elements")


def json_agg_of_integers(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_agg of integers the statement says: Trino's array of them cast to JSON."""
    if isinstance(node, exp.JSONArrayAgg):
        value = node.this
    elif isinstance(node, exp.Anonymous) and node.name.lower() == "json_agg":
        if len(node.expressions) != 1:
            return None
        value = node.expressions[0]  # called by pg_catalog, quoted
    else:
        return None
    said = trino.said(value)
    if said in _TEXTS - {"unknown"}:
        # Trino's form parses a text as JSON: '1' would be the number 1, not the string "1".
        raise Refused(trino.no_form("json_agg", dialect))
    if said not in _INTEGERS:
        return None
    if isinstance(node.parent, exp.Filter) or not plain(value):
        raise Refused(trino.no_form("json_agg", dialect))
    return trino.written("json_format(CAST(array_agg($v) AS JSON))", "json_agg($v)", {"v": value})


def _key(key: exp.Expr, function: str, dialect: str) -> exp.Expr | None:
    """``key`` as its text, or None where it is already text or of no said type."""
    said = trino.said(key)
    if said in _KEYS_CAST:
        return exp.Cast(this=key.copy(), to=exp.DataType.build("TEXT"))
    if said is None and isinstance(key.unnest(), exp.Null):
        raise Refused(trino.no_form(function, dialect))
    if said is None or said in _TEXTS:
        return None
    raise Refused(trino.no_form(function, dialect))


def object_keys_as_text(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_build_object's keys as PostgreSQL's text of them, where one is no text."""
    call = trino.called(node)
    if call is None or not (names := call[0] & set(_OBJECTS)):
        return None
    function, args = min(names), call[1]
    if any(isinstance(a, exp.Variadic) for a in args):
        return None  # pg_forms_json refuses it by name
    if len(args) % 2:
        raise Refused(ODD)
    keys = [_key(k, function, dialect) for k in args[0::2]]
    if all(k is None for k in keys):
        return None
    if not all(plain(a) for a in args):
        raise Refused(trino.no_form(function, dialect))
    return exp.JSONObject(
        expressions=[
            exp.JSONKeyValue(this=key or args[2 * i].copy(), expression=args[2 * i + 1].copy())
            for i, key in enumerate(keys)
        ]
    )


#: The types whose values ClickHouse's toJSONString writes as json_agg does: a number, a text,
#: a truth value.
_JSON_SCALARS = frozenset({"smallint", "integer", "bigint", "text", "character varying", "boolean"})
_JSON_AGG = "json_agg($a)"
_JSON_ARRAY = "if(count() = 0, NULL, toJSONString(arrayMap(_t -> _t.1, groupArray(tuple($a)))))"


def json_array(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """json_agg of a number, a text or a truth value the statement says: its values as a JSON
    array, a NULL kept, NULL of no rows; of any other value, or of an order, DISTINCT, a FILTER
    or a window, refused by name."""
    if isinstance(node, exp.JSONArrayAgg):
        value = node.this
    elif isinstance(node, exp.Anonymous) and node.name.lower() == "json_agg":
        if len(node.expressions) != 1:
            return None
        value = node.expressions[0]  # called by pg_catalog, quoted
    else:
        return None
    outer = node.parent.parent if isinstance(node.parent, exp.Dot) else node.parent
    if (
        isinstance(value, (exp.Order, exp.Distinct))
        or isinstance(outer, (exp.Filter, exp.Window))
        or trino.said(value) not in _JSON_SCALARS
    ):
        raise Refused(trino.no_form("json_agg", dialect))
    return graft(replacement(_JSON_ARRAY, dialect, _JSON_AGG), {"a": value})


_PLAIN_ONLY = Limitation(
    "Where a rule writes the call, of a literal, a column or a cast of one only: any other value "
    "is refused by name."
)

JSON_AGG = Feature(
    "json_agg",
    FAMILY,
    "A JSON array of an aggregate's values.",
    read=Side(
        rules=(
            Rule(
                None,
                {
                    **trino.on_trino(Rewrite(json_agg_of_integers)),
                    "clickhouse": Rewrite(json_array),
                },
            ),
        ),
        limitations={
            **dict.fromkeys(
                trino.TRINO,
                (
                    _PLAIN_ONLY,
                    Limitation("Of integers under a FILTER: refused by name."),
                    Limitation(
                        "Of a value the statement says is text: refused by name, as Trino's form "
                        "parses each value as JSON text."
                    ),
                ),
            ),
            "clickhouse": (
                Limitation(
                    "Of a number, a text or a truth value the statement says the type of: any "
                    "other value, and an order, DISTINCT, a FILTER or a window, refused by name."
                ),
            ),
        },
    ),
)

FEATURES: tuple[Feature, ...] = (
    JSON_AGG,
    *(
        Feature(
            name,
            FAMILY,
            "A JSON object of keys and values; each key is cast to its text.",
            read=Side(
                rules=(Rule(None, trino.on_trino(Rewrite(object_keys_as_text))),),
                limitations=dict.fromkeys(
                    trino.TRINO,
                    (
                        _PLAIN_ONLY,
                        Limitation(
                            "A key of a type the statement says other than text, an integer or a "
                            "boolean, or a NULL key: refused by name, as Trino's text of it may "
                            "differ."
                        ),
                        Limitation(
                            "An odd count of arguments is refused as the statement is read, not "
                            "where it runs.",
                            ODD,
                        ),
                    ),
                ),
            ),
        )
        for name in _OBJECTS
    ),
)

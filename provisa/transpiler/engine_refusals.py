# Copyright (c) 2026 Kenneth Stott
# Canary: 9b1d4e07-6c2a-4f58-8a3e-71d0c5e2f6b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a replay on a real engine refused (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED): a function
or an overload of it that no form serves there, refused by name rather than sent to fail or to
answer differently. tests/integration/test_forms_on_engines.py replays these tables against
postgres:17's answers on Trino and ClickHouse.

The overloads are by the type a statement says of any argument; a column of no said type is the
engine's to read."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections.abc import Callable

import sqlglot.expressions as exp

_T, _C = "trino", "clickhouse"
_ENGINES = {"trino": _T, "presto": _T, "athena": _T, "clickhouse": _C}
_ANY = None
_NUMBERS = frozenset({"decimal", "double precision", "real", "float", "bigint", "int", "smallint"})
_NUMERIC = frozenset({"decimal"})

#: No value of these types reaches a call: the engine has no type to hold it, and sqlglot's nearest
#: (BOOLEAN of a bit, for one) answers something else.
_NOT_HELD: dict[str, frozenset[str]] = {
    _T: frozenset({"interval", "bit", "bit varying", "varbit", "oid", "tid", "aclitem", "xml"}),
    _C: frozenset(
        {"interval", "bit", "bit varying", "varbit", "oid", "tid", "aclitem", "xml", "timetz"}
    ),
}

#: What both engines refuse: its name, and the said types of the overloads refused (None for all).
_ON_BOTH: dict[str, frozenset[str] | None] = {
    # numeric's exact digits, which an engine's double or 38-digit decimal does not reach.
    "avg": _NUMERIC,
    "exp": _NUMERIC,
    "ln": _NUMERIC,
    "log": _NUMERIC,
    "log10": _NUMERIC,
    "pow": _NUMERIC,
    "power": _NUMERIC,
    "sqrt": _NUMERIC,
    "stddev": _NUMERIC,
    "stddev_pop": _NUMERIC,
    "stddev_samp": _NUMERIC,
    "variance": _NUMERIC,
    "variance_pop": _NUMERIC,
    "numeric": frozenset({"decimal", "double precision", "real", "float"}),
    "to_char": _NUMBERS,
    "time_to_str": _NUMBERS,
    "exploding_generate_series": _NUMERIC,
    "generate_series": _NUMERIC,
    "rand": _NUMERIC,
    # No form, and nothing the engine has answers as PostgreSQL does.
    "percentile_cont": _ANY,
    "percentile_disc": _ANY,
    "pg_size_pretty": _ANY,
    "pg_sleep": _ANY,
    "setseed": _ANY,
}
_ON: dict[str, dict[str, frozenset[str] | None]] = {
    _T: {"gcd": _ANY, "lcm": _ANY},
    _C: {"gcd": _NUMERIC, "lcm": _NUMERIC, "to_timestamp": _NUMBERS},
}

NAMES = frozenset({*_ON_BOTH, *_ON[_T], *_ON[_C]})


def _called(node: exp.Expr) -> tuple[str, list[exp.Expr]] | None:
    """A call's name, as PostgreSQL's or sqlglot's, and its arguments."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower(), list(node.expressions)
    if not isinstance(node, exp.Func):
        return None
    args = [value for value in node.args.values() if isinstance(value, exp.Expr)]
    args.extend(item for value in node.args.values() if isinstance(value, list) for item in value)
    return node.sql_name().lower(), args


def refused_call(
    node: exp.Expr, dialect: str, said_type: Callable[[exp.Expr], str | None]
) -> str | None:
    """The name of the function ``node`` calls, where a replay refused that call on ``dialect``."""
    engine = _ENGINES.get(dialect)
    if engine is None:
        return None
    call = _called(node)
    if call is None:
        return None
    name, args = call
    said = {type_name.split("(")[0] for type_name in map(said_type, args) if type_name}
    if said & _NOT_HELD[engine]:
        return name
    table = _ON[engine]
    types = table[name] if name in table else _ON_BOTH.get(name, frozenset())
    return name if types is None or said & types else None

# Copyright (c) 2026 Kenneth Stott
# Canary: e85b1c37-6f2a-4d09-93c4-2a7e0d5b1f86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Calls Trino takes in no form PostgreSQL's statement gives them (REQ-2236; the function support
measure, run 43307392116, REQ-2171), each refused by name there.

A set-returning call in a select list: sqlglot writes generate_series as UNNEST, which Trino reads
only in a FROM clause. lag and lead of a window of no order: Trino requires one, and PostgreSQL's
answer is of the rows' order as read, which no form names. string_agg of a delimiter that is no
constant text, or of bytea: Trino's listagg takes a constant separator, and no bytes. json_object
of arrays of keys and values: sqlglot reads it as key-value pairs, which Trino takes of a text key
only. And the functions of the server Trino has no form of, sent as written (current_schemas,
refused on ClickHouse too, is its family's: provisa.transpiler.polyfill.clickhouse).

ClickHouse (run 43410717058): its groupConcat takes a constant separator, and its json_object no
arrays, so string_agg and json_object are refused there as on Trino; pg_conf_load_time,
pg_my_temp_schema and pg_postmaster_start_time have no form there either. generate_series in a select
list is ClickHouse's arrayJoin of its range (:func:`series`)."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import clickhouse, trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Refused, Rewrite, replacement

FAMILY = "calls of no Trino form"

#: What a select list's call passes through: anything else holds it in another clause.
_SELECTED_THROUGH = (exp.Alias, exp.Cast, exp.Paren)


def _in_a_select_list(node: exp.Expr, _args: list[exp.Expr]) -> bool:
    """Whether ``node`` is a select list's value, or of one through an alias or a cast."""
    child = node
    while isinstance(child.parent, _SELECTED_THROUGH):
        child = child.parent
    return isinstance(child.parent, exp.Select) and child.arg_key == "expressions"


#: What a select list's call passes through on ClickHouse: by pg_catalog too.
_CH_SELECTED_THROUGH = (*_SELECTED_THROUGH, exp.Dot)
_INTEGERS = frozenset({"smallint", "integer", "bigint"})
_SERIES = "generate_series($a, $b, $s)"
#: The series of integers from $a to $b by $s, both ends included, as ClickHouse's range (whose end
#: is not): no rows of a NULL bound or step, and PostgreSQL's error of a step of 0.
_CH_SERIES = (
    "arrayJoin(if(isNull($a) OR isNull($b) OR isNull($s), [], "
    "range(ifNull($a, 0), ifNull($b, 0) + if(ifNull($s, 1) > 0, 1, -1), "
    "ifNull($s, 1) + throwIf(ifNull($s = 0, 0), "
    "'postgresql-error 22023: step size cannot equal zero<end of postgresql-error>'))))"
)


def series(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """generate_series of integers in a select list, as ClickHouse's arrayJoin of its range; of
    any other type there, refused by name; anywhere else, kept."""
    call = trino.called(node)
    if call is None or "generate_series" not in call[0] or not 2 <= len(call[1]) <= 3:  # noqa: PLR2004
        return None
    child = node
    while isinstance(child.parent, _CH_SELECTED_THROUGH):
        child = child.parent
    if not (isinstance(child.parent, exp.Select) and child.arg_key == "expressions"):
        return None
    args = call[1]
    if not all(trino.said(arg) in _INTEGERS for arg in args):
        raise Refused(trino.no_form("generate_series", dialect))
    step = args[2] if len(args) == 3 else exp.Literal.number(1)  # noqa: PLR2004
    bindings = {"a": args[0], "b": args[1], "s": step}
    return graft(replacement(_CH_SERIES, dialect, _SERIES), bindings)


def _of_no_order(node: exp.Expr, _args: list[exp.Expr]) -> bool:
    """Whether ``node`` is a window function's call over a window of no order."""
    window = node.parent
    while isinstance(window, exp.IgnoreNulls | exp.RespectNulls):
        window = window.parent
    return isinstance(window, exp.Window) and window.args.get("order") is None


def _a_constant(value: exp.Expr) -> bool:
    node = value.unnest()
    node = node.this if isinstance(node, exp.Cast) else node
    return isinstance(node, exp.Literal) and node.is_string


def _of_no_listagg(node: exp.Expr, args: list[exp.Expr]) -> bool:
    """Whether string_agg's value is bytes, or its delimiter is no constant text."""
    if not args:
        return False
    if trino.said(args[0]) == "bytea":
        return True
    separator = node.args.get("separator") if isinstance(node, exp.GroupConcat) else None
    separator = separator if separator is not None else (args[1] if len(args) > 1 else None)
    return separator is not None and not _a_constant(separator)


def _of_arrays(_node: exp.Expr, args: list[exp.Expr]) -> bool:
    return any((trino.said(arg) or "").endswith("[]") for arg in args)


_SERVER = {
    "pg_conf_load_time": "The time the server's configuration was last read.",
    "pg_my_temp_schema": "The oid of the session's temporary schema.",
    "pg_postmaster_start_time": "The time the server started.",
    "pg_isolation_test_session_is_blocked": "Whether a session waits on another's lock.",
    "postgresql_fdw_validator": "Whether the options of a foreign-data wrapper are valid.",
}

#: The server functions ClickHouse has no form of either (run 43410717058 sent each as written).
_NO_CLICKHOUSE_FORM = frozenset(
    {"pg_conf_load_time", "pg_my_temp_schema", "pg_postmaster_start_time"}
)

FEATURES: tuple[Feature, ...] = (
    Feature(
        "generate_series",
        FAMILY,
        "A series of integers, as rows.",
        read=Side(
            rules=(
                Rule(
                    None,
                    {
                        **trino.on_trino(
                            trino.refusing(
                                "generate_series",
                                "generate_series in a select list: refused by name, as Trino "
                                "unnests only in FROM.",
                                _in_a_select_list,
                            )
                        ),
                        "clickhouse": Rewrite(series),
                    },
                ),
            ),
            limitations={
                **dict.fromkeys(
                    trino.TRINO,
                    (
                        Limitation(
                            "In a select list, refused by name: Trino reads a set of rows only in "
                            "a FROM clause."
                        ),
                    ),
                ),
                "clickhouse": (
                    Limitation(
                        "In a select list, of integers only: of any other type, refused by name."
                    ),
                ),
            },
        ),
    ),
    *(
        Feature(
            name,
            FAMILY,
            f"The value of the {'previous' if name == 'lag' else 'next'} row of a window.",
            read=Side(
                rules=(
                    Rule(
                        None,
                        trino.on_trino(
                            trino.refusing(
                                name,
                                f"{name} over a window of no order: refused by name, as Trino "
                                "requires one.",
                                _of_no_order,
                            )
                        ),
                    ),
                    # ClickHouse's answers its type's default past the partition (its own row).
                    Rule(None, {"clickhouse": clickhouse.framed(name)}),
                ),
                limitations={
                    **dict.fromkeys(
                        trino.TRINO,
                        (
                            Limitation(
                                "Over a window of no order, refused by name: Trino requires one."
                            ),
                        ),
                    ),
                    "clickhouse": (clickhouse.HOLDS_A_CALL,),
                },
            ),
        )
        for name in ("lag", "lead")
    ),
    trino.refused_feature(
        "string_agg",
        FAMILY,
        "The values of an aggregate joined by a delimiter.",
        "Of bytea, or of a delimiter that is no constant text, refused by name: Trino's listagg "
        "takes a constant separator and no bytes.",
        "string_agg of bytea or of a delimiter that is no constant: refused by name.",
        _of_no_listagg,
        (*trino.TRINO, "clickhouse"),
    ),
    trino.refused_feature(
        "json_object",
        FAMILY,
        "A JSON object of arrays of keys and values.",
        "Of arrays, refused by name: Trino builds an object of text keys only.",
        "json_object of arrays: refused by name.",
        _of_arrays,
        (*trino.TRINO, "clickhouse"),
    ),
    *(
        trino.by_name(
            name,
            FAMILY,
            summary,
            "Refused by name: the dialect has no form of it.",
            (*trino.TRINO, "clickhouse") if name in _NO_CLICKHOUSE_FORM else trino.TRINO,
        )
        for name, summary in _SERVER.items()
    ),
)

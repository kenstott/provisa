# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c9a27-5b1e-4d84-a6e2-c47d18f0b953
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the rows of Trino's engine errors share (REQ-2236; the function support measure, run
43307392116, REQ-2171): the dialects whose forms are Trino's SQL, a refusal by name, a row that
writes Trino's form only, and a refusal of a call by the type a statement says of an argument.

A row whose function another dialect still takes from the old tables (provisa.transpiler
.function_macros) writes :func:`kept` there: a rewrite that answers nothing, so the call goes on to
that table as before. A row that refuses a call by its argument's said type is a rewrite, since a
typed hole of a pattern also matches an argument whose type no statement says."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

from collections.abc import Callable, Iterable

import sqlglot.expressions as exp

from provisa.pgwire import function_types
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Form, Refusal, Refused, Rewrite, replacement

#: The dialects whose forms are Trino's SQL.
TRINO = ("trino", "presto", "athena")
#: Every dialect but PostgreSQL's and Trino's.
OTHERS = tuple(d for d in DIALECTS if d not in {*NATIVE, *TRINO})


def no_form(function: str, dialect: str) -> Refusal:
    """``function`` refused by its name on ``dialect``."""
    return Refusal("0A000", f"{function}() has no form on {dialect}")


def refused_on_trino(function: str) -> dict[str, Refusal]:
    """``function`` refused by its name on each dialect whose forms are Trino's SQL."""
    return {dialect: no_form(function, dialect) for dialect in TRINO}


def on_trino(form: Form) -> dict[str, Form]:
    """``form`` on each dialect whose forms are Trino's SQL."""
    return dict.fromkeys(TRINO, form)


def kept(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """The old table's form (provisa.transpiler.function_macros): this row writes Trino's only."""
    return None


#: The form of a dialect that keeps the old table's.
KEPT = Rewrite(kept)


def trino_only(trino: Form) -> dict[str, Form]:
    """Trino's ``trino``, and every other dialect the old table's form."""
    return {**dict.fromkeys(OTHERS, KEPT), **on_trino(trino)}


def said(node: exp.Expr) -> str | None:
    """PostgreSQL's name of the type a statement says of ``node``, or None. A jsonb literal reaches
    the grid as JSON_PARSE of its text (transpile.postgres_json_of_text): still a jsonb value."""
    if isinstance(node, exp.ParseJSON):
        return "jsonb"
    return function_types.argument_type(node)


#: PostgreSQL's name of a call sqlglot reads as a node of another name: unnest in a select list
#: is sqlglot's Explode.
_PG_NAMES = {"exploding_generate_series": "generate_series", "explode": "unnest"}


def called(node: exp.Expr) -> tuple[frozenset[str], list[exp.Expr]] | None:
    """The names ``node`` is a call of (sqlglot's typed node answers to each of its names) and its
    arguments in order; None where it is no call."""
    if isinstance(node, exp.Anonymous):
        return frozenset({node.name.lower()}), list(node.expressions)
    if not isinstance(node, exp.Func):
        return None
    args: list[exp.Expr] = []
    for value in node.args.values():
        if isinstance(value, exp.Expr):
            args.append(value)
        elif isinstance(value, list):
            args.extend(item for item in value if isinstance(item, exp.Expr))
    names = {name.lower() for name in type(node).sql_names()}
    return frozenset(names | {_PG_NAMES[n] for n in names if n in _PG_NAMES}), args


def refusing(
    function: str,
    purpose: str,
    applies: Callable[[exp.Expr, list[exp.Expr]], bool],
) -> Rewrite:
    """A rewrite that refuses a call of ``function`` by its name where ``applies`` holds of the
    node and its arguments, and leaves any other node. ``purpose`` is what the docs print."""

    def refuse(node: exp.Expr, dialect: str) -> exp.Expr | None:
        call = called(node)
        if call is None or function not in call[0] or not applies(node, call[1]):
            return None
        raise Refused(no_form(function, dialect))

    refuse.__doc__ = purpose
    refuse.__qualname__ = f"refused_{function}"
    refuse.__name__ = refuse.__qualname__
    return Rewrite(refuse)


def of_said(types: Iterable[str]) -> Callable[[exp.Expr, list[exp.Expr]], bool]:
    """Whether any argument of a call is said to be of one of ``types``."""
    wanted = frozenset(types)
    return lambda _node, args: any(said(arg) in wanted for arg in args)


def refused_feature(
    function: str,
    family: str,
    summary: str,
    limitation: str,
    purpose: str,
    applies,
    dialects: tuple[str, ...] = TRINO,
) -> Feature:
    """A row that refuses ``function`` on ``dialects`` (Trino's) where ``applies`` holds, every
    other call kept."""
    return Feature(
        function,
        family,
        summary,
        read=Side(
            rules=(Rule(None, dict.fromkeys(dialects, refusing(function, purpose, applies))),),
            limitations=dict.fromkeys(dialects, (Limitation(limitation),)),
        ),
    )


def by_name(
    function: str, family: str, summary: str, limitation: str, dialects: tuple[str, ...] = TRINO
) -> Feature:
    """A row that refuses every call of ``function`` on ``dialects`` (Trino's), by its name,
    whatever its arguments, every other dialect keeping what it had."""
    refused = refusing(function, f"{function}: {limitation}", lambda *_: True)
    return Feature(
        function,
        family,
        summary,
        read=Side(
            rules=(Rule(None, dict.fromkeys(dialects, refused)),),
            limitations=dict.fromkeys(dialects, (Limitation(limitation),)),
        ),
    )


def written(form: str, pattern: str, bindings: dict[str, exp.Expr]) -> exp.Expr:
    """``form``, Trino's SQL with ``pattern``'s holes, each hole the subtree bound to it."""
    return graft(replacement(form, "trino", pattern), bindings)

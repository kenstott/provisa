# Copyright (c) 2026 Kenneth Stott
# Canary: 6c3e8a52-1f94-4d7b-a2c0-b58e9d14f7a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Text functions Trino answered otherwise at an edge (REQ-2236; the function support measure,
run 44670380355, REQ-2171).

position(text, text) called by its name is textpos(string, substring): position(B IN A) is
PostgreSQL's grammar for position(A, B). The call by name reached Trino as its POSITION(a, b),
which reads the first as the substring (position('', 'abc') answered 1, PostgreSQL 0).

replace(string, '', to): PostgreSQL leaves the string as it is where the text to replace is empty;
Trino's replace puts the replacement between every character."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Rewrite

FAMILY = "texts on Trino"
#: The types of position's other overloads (byteapos, bitposition), which this row does not read.
_NOT_TEXT = frozenset({"bytea", "bit", "bit varying"})


def position_called(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """position(string, substring) called by its name: Trino's strpos of the two in that order."""
    if not isinstance(node, exp.Anonymous) or node.name.lower() != "position":
        return None
    if len(node.expressions) != 2:  # noqa: PLR2004 -- textpos takes two
        return None
    string, substring = node.expressions
    if any(trino.said(arg) in _NOT_TEXT for arg in node.expressions):
        return None  # bytea's and bit's position: strpos takes neither
    return trino.written("strpos($a, $b)", "position($a, $b)", {"a": string, "b": substring})


def replaced(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """replace(string, from, to): the string as it is where ``from`` is empty; strict."""
    call = trino.called(node)
    if call is None or "replace" not in call[0] or len(call[1]) != 3:  # noqa: PLR2004
        return None
    string, old, new = call[1]
    form = "CASE WHEN $c IS NULL THEN NULL WHEN $b = '' THEN $a ELSE replace($a, $b, $c) END"
    return trino.written(form, "replace($a, $b, $c)", {"a": string, "b": old, "c": new})


FEATURES: tuple[Feature, ...] = tuple(
    Feature(
        name,
        FAMILY,
        summary,
        read=Side(
            rules=(Rule(None, trino.on_trino(Rewrite(form, mentions=mentions, first=True))),),
            limitations=dict.fromkeys(trino.TRINO, (Limitation(stated),)),
        ),
    )
    for name, summary, form, mentions, stated in (
        (
            "position(text, text) on trino",
            "The place of a substring in a string, called by its name.",
            position_called,
            r"\bposition\b",
            "Called by its name, position(string, substring) is Trino's strpos of the two in "
            "that order, as PostgreSQL's textpos reads them.",
        ),
        (
            "replace(text, text, text) on trino",
            "A string with each occurrence of a text replaced.",
            replaced,
            r"\breplace\b",
            "An empty text to replace leaves the string as it is, as PostgreSQL's replace does.",
        ),
    )
)

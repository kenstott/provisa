# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8e1f63-c2d5-4b7e-9f01-6d3b2a9c8e57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Casts Trino has no form of (REQ-2236; the function support measure, run 43324155455,
REQ-2171).

Trino casts no array to text. pgwire's form of a server function (provisa.transpiler
.metadata_functions) reads each argument as its text, so pg_isolation_test_session_is_blocked's
integer[] and postgresql_fdw_validator's text[] reached Trino as such a cast, and failed. An array's
text is array_out's: its elements in braces, a NULL as NULL, a text quoted where array_out quotes
it. Trino's form writes that for an array said to be of integers or of text; any other is refused
by name. The row names no function: it is read where a statement says an array type."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import Feature, Limitation, Rule, Side
from provisa.transpiler.polyfill.notation import Refused, Rewrite

FAMILY = "casts"

#: array_out's text of each element, as Trino writes it: an integer's digits; a text quoted where
#: it is empty, says NULL, or holds a brace, a comma, a quote, a backslash or white space (as
#: array_isspace reads it, Java's \\s), its quotes and backslashes escaped. NULL is NULL.
_ELEMENT_TEXT = {
    "integer": "coalesce(CAST(_x AS VARCHAR), 'NULL')",
    "text": "CASE WHEN _x IS NULL THEN 'NULL' "
    "WHEN _x = '' OR lower(_x) = 'null' OR regexp_like(_x, '[{},\"\\\\\\s]') "
    "THEN '\"' || replace(replace(_x, '\\', '\\\\'), '\"', '\\\"') || '\"' ELSE _x END",
}
_TEXT_OF = {
    "smallint[]": "integer",
    "integer[]": "integer",
    "bigint[]": "integer",
    "text[]": "text",
    "character varying[]": "text",
}
_ARRAY_OUT = "array_out($a)"
#: What a cast's operand may hold: a value the walk has nothing in to write in a form.
_PLAIN = (exp.Cast, exp.Array, exp.Literal, exp.Null, exp.Neg, exp.Paren, exp.DataType)


def _array_text(element: str) -> str:
    return (
        "CASE WHEN strpos(typeof($a), 'array(array(') = 1 THEN "
        "fail('postgresql-error 0A000: ' || 'array_out does not read this value' "
        "|| ' <end of postgresql-error>') "
        f"ELSE '{{' || array_join(transform($a, _x -> {_ELEMENT_TEXT[element]}), ',') || '}}' END"
    )


def array_text(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """An array cast to text, as array_out writes it: an array of integers or of text said so of a
    value; of any other element, or of an operand holding a call, refused by name."""
    if not isinstance(node, exp.Cast) or not node.to.is_type(*exp.DataType.TEXT_TYPES):
        return None
    operand = node.this
    type_said = trino.said(operand) or ""
    if not type_said.endswith("[]"):
        return None
    plain = all(isinstance(n, _PLAIN) or n.parent is None for n in operand.walk())
    element = _TEXT_OF.get(type_said)
    if element is None or not plain:
        raise Refused(trino.no_form("array_out", dialect))
    return trino.written(_array_text(element), _ARRAY_OUT, {"a": operand})


ARRAY_OUT = Feature(
    "array_out",
    FAMILY,
    "An array's text, as a cast of it to text writes it: {1,NULL,3}.",
    read=Side(
        rules=(Rule(None, trino.on_trino(Rewrite(array_text, mentions=r"\[\]"))),),
        limitations=dict.fromkeys(
            trino.TRINO,
            (
                Limitation(
                    "Only an array said to be of smallint, integer, bigint, text or character "
                    "varying, of one dimension and holding no call: any other is refused by name, "
                    "as Trino casts no array to text."
                ),
            ),
        ),
    ),
)


FEATURES: tuple[Feature, ...] = (ARRAY_OUT,)

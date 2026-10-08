# Copyright (c) 2026 Kenneth Stott
# Canary: 0cd74d74-0617-40b1-bd74-154807cfaad2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read that a row limit cut short says so (REQ-1949).

Governance bounds a read at the role's row limit, or at a table's where that is the smaller
(``stage2``). When that limit -- not the statement's own LIMIT -- is what bounds the read, the
governed statement carries it (:class:`RowLimit`); an answer that fills it exactly is asked once
more for the one row after it (:func:`next_row_sql`), and when that row exists the answer carries
:func:`cut_warning`. The statement itself is never changed: no terminal can return more than the
limit, and an answer within it is read once, as before.
"""

# Requirements: REQ-1949

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

from provisa.core.statement_warnings import ServerWarning

ROLE = "role"
TABLE = "table"


@dataclass(frozen=True)
class RowLimit:
    """The limit that bounds a governed read: its value, and whose it is (``role`` | ``table``)."""

    limit: int
    kind: str


def binds(sql: str, limit: int) -> bool:
    """Whether a ceiling of ``limit`` is what bounds ``sql``: the statement gives no LIMIT of its
    own, one above the ceiling, or one whose value is not written in it."""
    tree = sqlglot.parse_one(sql, read="postgres")
    node = tree if isinstance(tree, (exp.Select, exp.SetOperation)) else tree.find(exp.Select)
    if node is None:
        return False
    own = node.args.get("limit")
    if own is None:
        return True
    value = own.expression
    if isinstance(value, exp.Literal) and not value.is_string:
        return int(value.name) > limit
    return True


def next_row_sql(sql: str, limit: int, dialect: str) -> str | None:
    """``sql`` -- a statement bounded at ``limit`` rows, in ``dialect`` -- asking instead for the
    one row after those. None when its outermost LIMIT is not that bound: the statement was not
    left as governance bounded it, and nothing is known of the rows after."""
    tree = sqlglot.parse_one(sql, read=dialect)
    if not isinstance(tree, (exp.Select, exp.SetOperation)):
        return None
    own = tree.args.get("limit")
    value = own.expression if own is not None else None
    if not (isinstance(value, exp.Literal) and not value.is_string and int(value.name) == limit):
        return None
    offset = tree.args.get("offset")
    skipped: exp.Expr = exp.Literal.number(limit)
    if offset is not None:
        skipped = exp.Add(this=exp.paren(offset.expression.copy()), expression=skipped)
    tree.set("offset", exp.Offset(expression=skipped))
    tree.set("limit", exp.Limit(expression=exp.Literal.number(1)))
    return tree.sql(dialect=dialect)


def cut_warning(row_limit: RowLimit) -> ServerWarning:
    """What an answer cut at ``row_limit`` says about itself, on every surface (REQ-1350)."""
    whose = "the role" if row_limit.kind == ROLE else "a table it reads"
    return ServerWarning(
        code="statement.rows_cut",
        params={"limit": row_limit.limit, "kind": row_limit.kind},
        message=(
            f"The answer was cut at {row_limit.limit} rows, the row limit of {whose}: "
            f"more rows match than were returned."
        ),
    )


def unchecked_warning(row_limit: RowLimit, reason: str) -> ServerWarning:
    """What an answer that fills ``row_limit`` says when the row after it could not be asked
    for: the answer stands, and whether it was cut is not known -- said, with why, never passed
    over (REQ-1949: the warning is never an error, and its failure is not silence)."""
    whose = "the role" if row_limit.kind == ROLE else "a table it reads"
    return ServerWarning(
        code="statement.rows_cut_unchecked",
        params={"limit": row_limit.limit, "kind": row_limit.kind, "reason": reason},
        message=(
            f"The answer has {row_limit.limit} rows, the row limit of {whose}. Whether more "
            f"rows match could not be checked: {reason}"
        ),
    )

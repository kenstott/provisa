# Copyright (c) 2026 Kenneth Stott
# Canary: 744db784-0c81-4624-ad29-10f2fde12fff
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's clauses in the order PostgreSQL takes them (REQ-264).

sqlglot's parser reads a query's modifiers (WHERE, GROUP BY, HAVING, WINDOW, ORDER BY, LIMIT,
OFFSET, FETCH) in any order and writes them back in the right one, so a statement PostgreSQL
refuses -- ... LIMIT 1 ORDER BY n -- was answered as another statement, the one with its
clauses in order. A clause written after one it precedes is refused as PostgreSQL refuses it."""

# Requirements: REQ-264

from __future__ import annotations

from sqlglot.dialects.postgres import Postgres
from sqlglot.errors import ParseError
from sqlglot.tokens import TokenType

#: Each clause's place in a query; LIMIT and OFFSET share one, PostgreSQL taking them either way.
_PLACE = {
    TokenType.WHERE: 1,
    TokenType.GROUP_BY: 2,
    TokenType.HAVING: 3,
    TokenType.WINDOW: 4,
    TokenType.ORDER_BY: 5,
    TokenType.LIMIT: 6,
    TokenType.OFFSET: 6,
    TokenType.FETCH: 6,
}

#: Where a query's clauses start over at one level of parentheses: another query of a set
#: operation, and a write's own clauses (ON CONFLICT ... DO UPDATE SET ... WHERE, RETURNING).
_START = {
    TokenType.SELECT,
    TokenType.UNION,
    TokenType.EXCEPT,
    TokenType.INTERSECT,
    TokenType.SET,
    TokenType.RETURNING,
}


def refuse_out_of_order(sql: str) -> None:
    """Raise ParseError naming the first clause of sql written after one it precedes."""
    last: list[tuple[int, str]] = [(0, "")]  # per level of parentheses: the latest clause's place
    for token in Postgres().tokenize(sql):
        kind = token.token_type
        if kind == TokenType.L_PAREN:
            last.append((0, ""))
        elif kind == TokenType.R_PAREN:
            if len(last) > 1:
                last.pop()
        elif kind in _START:
            last[-1] = (0, "")
        elif kind in _PLACE:
            place, text = _PLACE[kind], token.text.upper()
            if place < last[-1][0]:
                raise ParseError(
                    f'syntax error at or near "{text}": {text} cannot follow {last[-1][1]}'
                )
            last[-1] = (place, text)

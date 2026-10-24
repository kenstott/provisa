# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement as DuckDB runs it to answer as PostgreSQL does (REQ-066, REQ-2100): the one form
for every DuckDB that answers one -- the DuckDB engine (transpiler.transpile_to_duckdb), and a code
source's DuckDB over its functions' rows, read DIRECT or through the pgwire front end an engine
attaches (codehost.residual).

- LIKE and ILIKE with no ESCAPE clause are given PostgreSQL's, a backslash: DuckDB's LIKE has no
  escape character unless one is written, so ``'a%b' LIKE 'a\\%b'`` was false there.
- An ORDER BY key with its nulls first is written as a null-test key of its own (#294):
  ``x DESC NULLS FIRST`` as ``(x IS NULL) DESC, x DESC``. DuckDB's postgres scanner answers an
  attached table's ORDER BY ... NULLS FIRST LIMIT n with rows missing (1.5.5); the orderings are
  the same. Nulls last is DuckDB's own order in either direction, and needs no key. Only a
  SELECT's own ORDER BY: a window's or an aggregate's is no scan's, and a UNION's orders the
  union's rows, which no one scan answers (DuckDB takes no expression key there).

Each is idempotent. This module is carried, as it is, by every built image's front end
(provisa/codesource/image_build.py): it imports nothing but sqlglot.
"""

# Requirements: REQ-066, REQ-2100
from __future__ import annotations

from sqlglot import exp


def escape_likes(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each LIKE and ILIKE that has no ESCAPE clause given PostgreSQL's."""

    def _escaped(node: exp.Expr) -> exp.Expr:
        if isinstance(node, (exp.Like, exp.ILike)) and not isinstance(node.parent, exp.Escape):
            return exp.Escape(this=node.copy(), expression=exp.Literal.string("\\"))
        return node

    return tree.transform(_escaped)


def null_order_as_key(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each ORDER BY key of a SELECT whose nulls come first written as a null-test
    key and the key itself, each in DuckDB's own null order."""
    for order in tree.find_all(exp.Order):
        select = order.parent
        if not isinstance(select, exp.Select):
            continue
        keys: list[exp.Expr] = []
        for ordered in order.expressions:
            if not isinstance(ordered, exp.Ordered) or not ordered.args.get("nulls_first"):
                keys.append(ordered)
                continue
            desc = bool(ordered.args.get("desc"))
            key = ordered.this
            if isinstance(key, exp.Literal) and key.is_int:
                # ORDER BY 2: its null test is of the second item the SELECT returns.
                item = select.expressions[int(key.name) - 1]
                key = exp.column(item.alias, quoted=True) if item.alias else item.unalias()
            is_null = exp.Paren(this=exp.Is(this=key.copy(), expression=exp.Null()))
            # A boolean key is never null: true (a null) first is its descending order.
            keys.append(exp.Ordered(this=is_null, desc=True, nulls_first=False))
            keys.append(exp.Ordered(this=ordered.this.copy(), desc=desc, nulls_first=False))
        order.set("expressions", keys)
    return tree


def as_duckdb(tree: exp.Expr) -> str:
    """``tree`` (parsed in any dialect) as DuckDB text that answers as PostgreSQL does."""
    return null_order_as_key(escape_likes(tree)).sql(dialect="duckdb")

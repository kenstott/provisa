# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement is parsed once per request (REQ-913).

One request's statement passes through validation, governance, routing, its engine form and its
transpile, and each stage parsed the text it was handed: a cold ``MATCH (n)`` over a large model
parsed one ~425 KB statement sixteen times, seven distinct texts. Within a request's
:func:`parse_scope`, :func:`parse_once` keeps the trees of the texts it parsed last and answers a
copy of the kept tree for a text it already holds, so a stage that changes what it was handed
changes its own copy. Outside a scope it parses, as ``sqlglot.parse_one`` does.

A tree of a statement that large holds ~33 MB, so a scope keeps the last ``KEPT`` texts only:
the stages that reparse one text run one after another."""

from __future__ import annotations

import contextlib
from collections import OrderedDict
from collections.abc import Generator
from contextvars import ContextVar

import sqlglot
import sqlglot.expressions as exp

# The trees one request keeps, most recent last; bounded by the memory a large statement's tree
# holds (module docstring).
KEPT = 2

_scope: ContextVar[OrderedDict[tuple[str, str], exp.Expr] | None] = ContextVar(
    "provisa_parse_scope", default=None
)


@contextlib.contextmanager
def parse_scope() -> Generator[None]:
    """The span of one request: its parses are kept for it. A scope opened inside another is the
    outer one's."""
    if _scope.get() is not None:
        yield
        return
    token = _scope.set(OrderedDict())
    try:
        yield
    finally:
        _scope.reset(token)


def parse_once(sql: str, dialect: str) -> exp.Expr:
    """``sql`` parsed in ``dialect``: a copy of the tree this request's scope keeps for it."""
    kept = _scope.get()
    if kept is None:
        return sqlglot.parse_one(sql, read=dialect)
    key = (sql, dialect)
    tree = kept.get(key)
    if tree is None:
        tree = sqlglot.parse_one(sql, read=dialect)
        kept[key] = tree
        while len(kept) > KEPT:
            kept.popitem(last=False)
    else:
        kept.move_to_end(key)
    return tree.copy()

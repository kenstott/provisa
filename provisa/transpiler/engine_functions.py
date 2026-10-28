# Copyright (c) 2026 Kenneth Stott
# Canary: e3ec1c4a-8515-4fb1-b866-57295b6c52a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The functions a Cypher read is lowered to as the federation engine names them
(gql/expr_visitor): a map literal is MAP(keys, values), a list element element_at, a list
comprehension filter / transform, a quantifier any_match / all_match / none_match, reduce and a
slice. A source's own SQL computes them only where its dialect is the engine's, so a statement a
source would otherwise answer directly is answered by the engine -- as a similarity search is
(REQ-423) -- naming the function and the source (REQ-345)."""

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp

#: The source dialects whose own SQL computes every one of them: the engines' own.
COMPUTED_BY = frozenset({"trino", "presto", "athena"})

_TYPED: dict[type, str] = {
    exp.Map: "MAP",
    exp.ArrayFilter: "filter",
    exp.Transform: "transform",
    exp.Reduce: "reduce",
}
_NAMED = frozenset(
    {
        "element_at",
        "any_match",
        "all_match",
        "none_match",
        "slice",
        "filter",
        "transform",
        "reduce",
        "sequence",  # Cypher's range(start, end[, step])
        "json_size",  # size() of an item of a list of several types (expr_visitor)
        "json_order_key",  # its place in openCypher's order (expr_visitor)
        "is_nan",  # a float comparison's NaN guard (expr_visitor)
        "range_step",  # a range's computed step, refused at 0 (expr_visitor)
        "json_keys",  # keys() of a map the client gives (expr_visitor)
    }
)
#: A statement naming none of them is not parsed: the check is on every direct route.
_MENTIONED = re.compile(
    r"\b(map|element_at|filter|transform|any_match|all_match|none_match|reduce|slice|sequence"
    r"|json_size|json_order_key|is_nan|range_step|json_keys)\s*\(",
    re.IGNORECASE,
)


def uncomputable(sql: str, dialect: str) -> str | None:
    """The first engine function ``sql`` (PostgreSQL-dialect text) calls that a source of
    ``dialect`` does not compute, or None. A lambda, which only those functions take, counts."""
    if dialect in COMPUTED_BY or not _MENTIONED.search(sql):
        return None
    for node in sqlglot.parse_one(sql, read="postgres").walk():
        typed = _TYPED.get(type(node))
        if typed is not None:
            return typed
        if isinstance(node, exp.Anonymous) and node.name.lower() in _NAMED:
            return node.name.lower()
        if isinstance(node, exp.Lambda):
            return "a lambda"
    return None

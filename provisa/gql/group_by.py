# Copyright (c) 2026 Kenneth Stott
# Canary: 8f4b2e1a-5c3d-4a9b-7e6f-2d1c8b5a3e7f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GroupByMixin — implicit GROUP BY for aggregating RETURN/WITH clauses.

Mixed into _Translator; relies on _parse_expr and _has_aggregate.
"""

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.gql.ir import ReturnClause, ReturnItem
from provisa.gql.expr_ast import CypherExpr, FunctionCall, MapLiteral, Variable, walk

# Requirements: REQ-347


AGGREGATES = frozenset(
    {"count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp", "percentilecont",
     "percentiledisc", "group_concat"}
)  # fmt: skip


def _has_aggregate(expr: CypherExpr) -> bool:
    """Whether ``expr`` calls an aggregate anywhere in it."""
    return any(
        isinstance(node, FunctionCall) and node.name.lower() in AGGREGATES for node in walk(expr)
    )


def _group_keys(expr: CypherExpr) -> list[CypherExpr]:
    """The keys an item that aggregates nothing groups by: a map literal groups by its values
    (the same groups as by the map, and each value is then a key an ORDER BY can read)."""
    if isinstance(expr, MapLiteral):
        return [key for _name, value in expr.entries for key in _group_keys(value)]
    return [expr]


class GroupByMixin:  # REQ-653
    """Mixin for _Translator: builds implicit GROUP BY for aggregating queries."""

    def _parse_expr(self, expr: CypherExpr) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        raise NotImplementedError

    def _group_by_expr_for_item(self, expr: CypherExpr) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Match `_build_with_select_items`'s shape for a bare node/rel variable: the SELECT list
        expands it to `alias.*`, so GROUP BY must group by the same `alias.*`, not a bare column
        named after the Cypher variable — else the two column sets disagree at the engine."""
        var_table = getattr(self, "_var_table", {})
        unwound = getattr(self, "_unwind_aliases", set())
        if (
            isinstance(expr, Variable)
            and expr.name in var_table
            and not (var_table[expr.name][1] is None and var_table[expr.name][0] in unwound)
        ):
            return exp.Column(
                this=exp.Star(),
                table=exp.Identifier(this=expr.name),
            )
        return self._parse_expr(expr)

    def _build_group_by(self, return_clause: ReturnClause) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__  # REQ-347
        if return_clause.group_by is not None:
            # REQ-2135: the keys the statement names.
            return [self._parse_expr(key) for key in return_clause.group_by]
        items = return_clause.items
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._group_by_expr_for_item(key)
            for item in items
            if not _has_aggregate(item.expression)
            for key in _group_keys(item.expression)
        ]

    def _build_group_by_for_with(self, items: list[ReturnItem]) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__  # REQ-347
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._group_by_expr_for_item(key)
            for item in items
            if not _has_aggregate(item.expression)
            for key in _group_keys(item.expression)
        ]

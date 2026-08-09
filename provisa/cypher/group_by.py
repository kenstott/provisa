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

from provisa.cypher.parser import ReturnClause, ReturnItem
from provisa.cypher.translator_helpers import _is_bare_variable

# Requirements: REQ-347


def _has_aggregate(text: str) -> bool:
    import re

    return bool(
        re.search(
            r"\b(count|sum|avg|min|max|collect|stDev|stDevP|percentileCont|percentileDisc)\s*\(",
            text,
            re.IGNORECASE,
        )
    )


class GroupByMixin:  # REQ-653
    """Mixin for _Translator: builds implicit GROUP BY for aggregating queries."""

    def _parse_expr(self, text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        raise NotImplementedError

    def _group_by_expr_for_item(self, expr_text: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__
        """Match `_build_with_select_items`'s shape for a bare node/rel variable: the SELECT list
        expands it to `alias.*`, so GROUP BY must group by the same `alias.*`, not a bare column
        named after the Cypher variable — else the two column sets disagree at the engine."""
        var_table = getattr(self, "_var_table", {})
        if _is_bare_variable(expr_text) and expr_text in var_table:
            return exp.Column(
                this=exp.Star(),
                table=exp.Identifier(this=expr_text),
            )
        return self._parse_expr(expr_text)

    def _build_group_by(self, return_clause: ReturnClause) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__  # REQ-347
        items = return_clause.items
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._group_by_expr_for_item(item.expression.strip())
            for item in items
            if not _has_aggregate(item.expression)
        ]

    def _build_group_by_for_with(self, items: list[ReturnItem]) -> list[exp.Expression]:  # pyright: ignore[reportPrivateImportUsage]  # lib omits __all__  # REQ-347
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._group_by_expr_for_item(item.expression.strip())
            for item in items
            if not _has_aggregate(item.expression)
        ]

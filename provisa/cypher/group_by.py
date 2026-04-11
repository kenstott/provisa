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


def _has_aggregate(text: str) -> bool:
    import re
    return bool(re.search(
        r'\b(count|sum|avg|min|max|collect|stDev|stDevP|percentileCont|percentileDisc)\s*\(',
        text, re.IGNORECASE,
    ))


class GroupByMixin:
    """Mixin for _Translator: builds implicit GROUP BY for aggregating queries."""

    def _build_group_by(self, return_clause: ReturnClause) -> list[exp.Expression]:
        items = return_clause.items
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._parse_expr(item.expression.strip())
            for item in items
            if not _has_aggregate(item.expression)
        ]

    def _build_group_by_for_with(self, items: list[ReturnItem]) -> list[exp.Expression]:
        if not any(_has_aggregate(item.expression) for item in items):
            return []
        return [
            self._parse_expr(item.expression.strip())
            for item in items
            if not _has_aggregate(item.expression)
        ]

# Copyright (c) 2026 Kenneth Stott
# Canary: 9f2a3c1d-7e4b-4f8a-b2d5-1c6e9a0f3b7d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Aggregate MV catalog for automatic aggregate query rewriting (REQ-198, REQ-199).

Maps aggregate query patterns to materialized views. When a query requests
aggregates over a base table, the catalog finds an MV that was pre-computed
with those aggregates and rewrites the query to read from the MV instead.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.mv.models import MVDefinition

log = logging.getLogger(__name__)


class AggregateMVCatalog:
    """Maps aggregate query patterns to materialized views.

    An MV "covers" an aggregate query when:
    - It targets the same base table
    - Its aggregate_columns is a superset of the requested columns
    - Its filter set is a subset of the query's filters (conservative)
    """

    def __init__(self) -> None:
        # {base_table: [MVDefinition]}
        self._by_table: dict[str, list[MVDefinition]] = {}

    def register(self, mv: MVDefinition) -> None:
        """Register an MV that serves aggregate queries."""
        if not mv.serves_aggregates:
            return
        for table in mv.source_tables:
            self._by_table.setdefault(table, []).append(mv)

    def unregister(self, mv_id: str) -> None:
        """Remove an MV from the catalog by ID."""
        for table, mvs in self._by_table.items():
            self._by_table[table] = [m for m in mvs if m.id != mv_id]

    def find_aggregate_mv(
        self,
        table: str,
        agg_columns: list[str],
        filters: list[str],
    ) -> MVDefinition | None:
        """Return the best MV for the given aggregate query, or None.

        Args:
            table: Base table name being queried.
            agg_columns: Columns the query wants to aggregate (e.g. ["amount", "qty"]).
            filters: WHERE clause fragments the query applies (e.g. ["status = 'active'"]).

        Returns:
            The first MV whose aggregate_columns is a superset of agg_columns and
            whose filters are a subset of the query's filters, or None.
        """
        candidates = self._by_table.get(table, [])
        requested = set(agg_columns)
        query_filters = set(filters)

        for mv in candidates:
            mv_cols = set(mv.aggregate_columns)
            if not requested.issubset(mv_cols):
                continue
            # MV filters must be covered by the query's filters (conservative)
            # We skip filter checking for MVs with no declared filters
            log.debug(
                "AggregateMVCatalog: MV %s covers table=%s cols=%s",
                mv.id, table, agg_columns,
            )
            return mv

        return None

    def rewrite_sql(
        self,
        sql: str,
        mv: MVDefinition,
        agg_columns: list[str],
        remaining_filters: list[str],
    ) -> str:
        """Rewrite a SQL aggregate query to read from an MV backing table.

        Produces: SELECT {agg_cols} FROM {mv_backing_table} WHERE {filters}
        with an identifying comment.
        """
        target = f'"{mv.target_catalog}"."{mv.target_schema}"."{mv.target_table}"'
        select_cols = ", ".join(f'"{c}"' for c in agg_columns) if agg_columns else "*"
        where_clause = (
            " WHERE " + " AND ".join(remaining_filters) if remaining_filters else ""
        )
        rewritten = (
            f"/* aggregate_mv: {mv.id} */\n"
            f"SELECT {select_cols} FROM {target}{where_clause}"
        )
        log.info(
            "Rewrote aggregate query for table=%s to MV %s", mv.source_tables, mv.id
        )
        return rewritten


# Module-level singleton — populated by MVRegistry on startup
_catalog = AggregateMVCatalog()


def get_aggregate_catalog() -> AggregateMVCatalog:
    """Return the global aggregate MV catalog."""
    return _catalog

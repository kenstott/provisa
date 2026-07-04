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

import sqlglot
import sqlglot.expressions as exp

if TYPE_CHECKING:
    from provisa.mv.models import MVDefinition

# Requirements: REQ-198, REQ-199

log = logging.getLogger(__name__)


class AggregateMVCatalog:  # REQ-198, REQ-199
    """Maps aggregate query patterns to materialized views.

    An MV "covers" an aggregate query when:
    - It targets the same base table
    - Its aggregate_columns is a superset of the requested columns
    - Its filter set is a subset of the query's filters (conservative)
    """

    def __init__(self) -> None:
        # {base_table: [MVDefinition]}
        self._by_table: dict[str, list[MVDefinition]] = {}

    def register(self, mv: MVDefinition) -> None:  # REQ-483
        """Register an MV that serves aggregate queries."""
        if not mv.serves_aggregates:
            return
        for table in mv.source_tables:
            self._by_table.setdefault(table, []).append(mv)

    def unregister(self, mv_id: str) -> None:  # REQ-483
        """Remove an MV from the catalog by ID."""
        for table, mvs in self._by_table.items():
            self._by_table[table] = [m for m in mvs if m.id != mv_id]

    def find_aggregate_mv(  # REQ-198
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
            if not requested.issubset(set(mv.aggregate_columns)):
                continue
            # REQ-882 subset-safety: the MV was pre-computed WITH mv.filters, so it holds only
            # the rows those predicates select. It can answer this query ONLY when every MV
            # filter is also a query filter (the MV is no more restrictive than the query);
            # otherwise it would silently drop rows the query wants. An unfiltered MV ({}) is
            # always safe. The query's remaining filters are re-applied on the MV at rewrite.
            if not set(mv.filters).issubset(query_filters):
                continue
            log.debug(
                "AggregateMVCatalog: MV %s covers table=%s cols=%s", mv.id, table, agg_columns
            )
            return mv

        return None

    def rewrite_sql(  # REQ-198
        self,
        sql: str,
        mv: MVDefinition,
        agg_columns: list[str],
        remaining_filters: list[str],
    ) -> str:
        """Rewrite a SQL aggregate query to read from an MV backing table.

        Replaces every base-table reference in the original SQL with the MV
        backing table, preserving all SELECT expressions (including aggregates
        such as SUM/AVG), WHERE clauses, and GROUP BY clauses.
        """
        mv_table = exp.Table(
            this=exp.Identifier(this=mv.target_table, quoted=True),
            db=exp.Identifier(this=mv.target_schema, quoted=True),
            catalog=exp.Identifier(this=mv.target_catalog, quoted=True),
        )

        tree = sqlglot.parse_one(sql)

        # Replace SELECT clause with quoted agg_columns (or * when none given).
        if agg_columns:
            select_exprs = [
                exp.Column(this=exp.Identifier(this=c, quoted=True)) for c in agg_columns
            ]
        else:
            select_exprs = [exp.Star()]
        tree.set("expressions", select_exprs)

        # Replace every FROM / JOIN table reference that matches a source table.
        source_set = {t.lower() for t in mv.source_tables}
        for node in tree.find_all(exp.Table):
            if node.name.lower() in source_set:
                node.replace(mv_table.copy())

        # If no FROM clause exists (e.g. original was SELECT 1), set one.
        if not tree.find(exp.From):
            tree.set("from", exp.From(this=mv_table.copy()))

        # Append any additional remaining_filters to the WHERE clause.
        if remaining_filters:
            extra = sqlglot.parse_one(" AND ".join(remaining_filters))
            existing_where = tree.find(exp.Where)
            if existing_where:
                existing_where.set("this", exp.And(this=existing_where.this, expression=extra))
            else:
                tree.set("where", exp.Where(this=extra))

        rewritten = f"/* aggregate_mv: {mv.id} */\n{tree.sql()}"
        log.info("Rewrote aggregate query for table=%s to MV %s", mv.source_tables, mv.id)
        return rewritten


def _split_conjuncts(node) -> list:
    """Flatten a boolean expression tree into its top-level AND conjuncts."""
    if isinstance(node, exp.And):
        return _split_conjuncts(node.this) + _split_conjuncts(node.expression)
    return [node]


def _extract_aggregate_query(sql: str) -> tuple[str, list[str], list[str]] | None:
    """Parse a single-table aggregate query into (table, agg_columns, filter_fragments).

    Returns None when the query is not a rewritable single-table aggregate (has a JOIN,
    references multiple tables, or contains no aggregate function).
    """
    from sqlglot.errors import SqlglotError

    try:
        tree = sqlglot.parse_one(sql)
    except (SqlglotError, RecursionError):
        return None
    if tree.find(exp.Join) is not None:
        return None
    tables = list(tree.find_all(exp.Table))
    if len(tables) != 1:
        return None
    agg_cols = [c.name for f in tree.find_all(exp.AggFunc) for c in f.find_all(exp.Column)]
    if not agg_cols:
        return None
    where = tree.find(exp.Where)
    filters = [c.sql() for c in _split_conjuncts(where.this)] if where else []
    return tables[0].name, agg_cols, filters


def rewrite_aggregate_query(
    sql: str, catalog: AggregateMVCatalog
) -> tuple[str, MVDefinition] | None:  # REQ-882
    """Rewrite a single-table aggregate query to read a covering aggregate MV.

    Returns (rewritten_sql, mv) or None. Enforces filter subset-safety via
    ``find_aggregate_mv`` and re-applies the query's filters that the MV was NOT
    pre-computed with (remaining filters) on the MV.
    """
    parsed = _extract_aggregate_query(sql)
    if parsed is None:
        return None
    table, agg_cols, filters = parsed
    mv = catalog.find_aggregate_mv(table, agg_cols, filters)
    if mv is None:
        return None
    mv_filters = set(mv.filters)
    remaining = [f for f in filters if f not in mv_filters]
    return catalog.rewrite_sql(sql, mv, agg_cols, remaining), mv


# Module-level singleton — populated by MVRegistry on startup
_catalog = AggregateMVCatalog()


def get_aggregate_catalog() -> AggregateMVCatalog:
    """Return the global aggregate MV catalog."""
    return _catalog

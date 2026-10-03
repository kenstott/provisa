# Copyright (c) 2026 Kenneth Stott
# Canary: 6d3f0a85-2c71-4e9b-8f14-a7b5c0e9d263
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The query complexity guard (REQ-1174).

A request-rate limit caps how many statements a role sends; this caps what one statement may
ask for. It is measured on the semantic statement -- the form every surface's request takes
once it is parsed and before it is governed -- so a GraphQL query, a SQL statement, a Cypher
query and whatever else lowers to that form are priced the same way, by one function.

A statement's complexity is one score, the sum of:

- the relations it reads, a relation fetched from a remote API on each read counting for more
  (it spends a remote budget every reader of that source shares);
- its joins;
- the columns it projects, a ``*`` counting as the columns it stands for;
- the query blocks nested inside it (subqueries, common table expressions, set branches).

The limit is the role's ``max_query_complexity``, under an org-wide default
(``limits.max_query_complexity``) that a role may tighten and not loosen: the smaller of the
two that are set applies, and a statement over it is refused before it is governed or run.
"""

# Requirements: REQ-1174
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import sqlglot.expressions as exp

from provisa.compiler.stage2 import GovernanceContext, _get_tables_from_select, _table_id_for_node

# Source types whose tables are fetched from a remote API when read, against a rate budget
# the remote sets and every reader of the source shares.
REMOTE_API_SOURCE_TYPES = frozenset({"openapi", "graphql_remote", "grpc_remote"})
# What reading one relation of such a source counts for; any other relation counts for one.
REMOTE_RELATION_POINTS = 10


@dataclass(frozen=True)
class Complexity:
    """What a statement asks for, and the score those add up to."""

    relations: int
    remote_relations: int  # how many of ``relations`` are fetched from a remote API
    joins: int
    columns: int
    blocks: int  # query blocks nested inside the outermost one

    @property
    def score(self) -> int:
        local = self.relations - self.remote_relations
        remote = self.remote_relations * REMOTE_RELATION_POINTS
        return local + remote + self.joins + self.columns + self.blocks

    def describe(self) -> str:
        return (
            f"{self.relations} relations ({self.remote_relations} remote), {self.joins} joins, "
            f"{self.columns} columns, {self.blocks} nested queries"
        )


class ComplexityLimitExceeded(PermissionError):
    """A statement's complexity is over the limit that applies to the role."""

    def __init__(self, complexity: Complexity, limit: int, limit_of: str) -> None:
        self.complexity = complexity
        self.limit = limit
        self.limit_of = limit_of
        super().__init__(
            f"query complexity {complexity.score} exceeds the {limit_of} limit of {limit}: "
            f"{complexity.describe()}"
        )


def _star_width(select: exp.Select, star: exp.Expression, gov_ctx: GovernanceContext) -> int:
    """The columns a ``*`` (or ``t.*``) in ``select`` stands for: the columns the role may see
    of each registered table it expands over. A relation that is not a registered table (a
    subquery, a common table expression) counts for one."""
    qualifier = star.table if isinstance(star, exp.Column) else ""
    width = 0
    for table, table_id in _get_tables_from_select(select, gov_ctx):
        if qualifier and qualifier not in (table.alias, table.name):
            continue
        if table_id is None:
            width += 1
            continue
        visible = gov_ctx.visible_columns.get(table_id)
        width += len(visible) if visible is not None else len(gov_ctx.all_columns[table_id])
    return max(width, 1)


def _is_star(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.Star) or (
        isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star)
    )


def measure(tree: Any, gov_ctx: GovernanceContext, remote_table_ids: frozenset[int]) -> Complexity:
    """The complexity of a parsed semantic statement."""
    cte_names = {cte.alias for cte in tree.find_all(exp.CTE)}
    relations = remote = 0
    for table in tree.find_all(exp.Table):
        if not table.db and table.name in cte_names:
            continue  # a reference to a block already counted, not a relation read
        relations += 1
        if _table_id_for_node(table, gov_ctx) in remote_table_ids:
            remote += 1
    selects = list(tree.find_all(exp.Select))
    columns = 0
    for select in selects:
        for expression in select.expressions:
            columns += _star_width(select, expression, gov_ctx) if _is_star(expression) else 1
    return Complexity(
        relations=relations,
        remote_relations=remote,
        joins=sum(1 for _ in tree.find_all(exp.Join)),
        columns=columns,
        blocks=max(len(selects) - 1, 0),
    )


def role_max_query_complexity(role: Any) -> int | None:
    """A role's own ``max_query_complexity``, from the dict a role loads as or the model."""
    rate_limit = (
        role.get("rate_limit") if isinstance(role, dict) else getattr(role, "rate_limit", None)
    )
    if rate_limit is None:
        return None
    if isinstance(rate_limit, dict):
        return rate_limit.get("max_query_complexity")
    return rate_limit.max_query_complexity


def limit_for(role: Any) -> tuple[int, str] | None:
    """The complexity limit that applies to ``role`` and whose it is -- the smaller of the role's
    own and the org default, of those that are set. None when neither is."""
    from provisa.core.limits import max_query_complexity

    limits = [
        (limit, whose)
        for limit, whose in (
            (role_max_query_complexity(role), "role"),
            (max_query_complexity(), "org"),
        )
        if limit is not None
    ]
    return min(limits) if limits else None


def guard_complexity(tree: Any, gov_ctx: GovernanceContext, ctx: Any, role: Any) -> None:
    """Raise :class:`ComplexityLimitExceeded` when the statement asks for more than the role may.

    ``ctx`` is the role's compilation context, which says which source each table is from. A
    role under no limit is not measured.
    """
    limit = limit_for(role)
    if limit is None:
        return
    remote = frozenset(
        meta.table_id for meta in ctx.tables.values() if meta.source_type in REMOTE_API_SOURCE_TYPES
    )
    complexity = measure(tree, gov_ctx, remote)
    if complexity.score > limit[0]:
        raise ComplexityLimitExceeded(complexity, limit[0], limit[1])

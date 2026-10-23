# Copyright (c) 2026 Kenneth Stott
# Canary: 24dfe355-fd25-4c8e-a67f-300efdd54f30
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a connector may answer without landing (REQ-863's trusted_to_answer gate, the source
contract). Every connector declares ``answers_directly`` and plans a statement over its table as
``(pushed, unsatisfied)``: what it takes into its own call and what it could not satisfy exactly.
A statement is answered by its source only when the connector answers directly and nothing is left
unsatisfied; otherwise its rows are landed and the engine runs it, the route's reason naming each
unsatisfied part. ``pushed`` is a pre-filter for a landed read: the engine applies the whole
statement again.

A relational connector runs the SQL itself (the dialect capability list judges its constructs);
a Qdrant collection answers its similarity top-k search; an API, a remote, a SPARQL endpoint or a
code connector does not answer directly and lands."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PushdownPlan:
    """A connector's plan of one statement: what it pushes into its call, what it cannot satisfy."""

    pushed: tuple[str, ...] = ()
    unsatisfied: tuple[str, ...] = ()


#: A planner: (state, source id, governed statement, its compilation context, table ids) -> its plan.
Planner = Callable[[Any, str, str, Any, tuple[int, ...]], Awaitable[PushdownPlan]]


@dataclass(frozen=True)
class Connector:
    answers_directly: bool
    plan: Planner | None = None  # None: it runs the statement whole (a relational connector)


def _tables(state: Any, table_ids: tuple[int, ...]) -> list[dict]:
    wanted = set(table_ids)
    return [t for t in getattr(state, "tables", None) or [] if t.get("id") in wanted]


async def _qdrant_plan(
    state: Any, sid: str, statement: str, ctx: Any, table_ids: tuple[int, ...]
) -> PushdownPlan:
    """REQ-2000: a collection's similarity top-k search -- ORDER BY cosine_similarity(<vector>, q)
    DESC LIMIT k over its payload filters -- is Qdrant's query_points, satisfied whole; a retrieve,
    a scroll, a search table or anything Qdrant does not state exactly is not."""
    from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

    tables = _tables(state, table_ids)
    if len(tables) != 1:
        return PushdownPlan(unsatisfied=("a read of more than one table",))
    driver = state.source_pools.get(sid)
    kind, why = await driver.plan_kind(rewrite_semantic_to_physical(statement, ctx), tables[0])
    if why is not None:
        return PushdownPlan(unsatisfied=(why,))
    if kind != "query":
        return PushdownPlan(
            pushed=(kind,),
            unsatisfied=("a read of the collection that is no similarity top-k search",),
        )
    return PushdownPlan(pushed=("query_points",))


async def _code_plan(
    state: Any, sid: str, statement: str, ctx: Any, table_ids: tuple[int, ...]
) -> PushdownPlan:
    """REQ-2100: a code table's wrapper pushes the predicates its function declares parameters
    for (codesource.pushdown, codehost.residual.pushed); everything else -- the rest of the
    conditions, orderings, limits, projections, joins, aggregates -- is the engine's."""
    from provisa.codesource.table import pushed_arguments

    tables = _tables(state, table_ids)
    pushed: list[str] = []
    for table in tables:
        given = await pushed_arguments(state, table, table["table_name"], statement)
        pushed.extend(f"{table['table_name']}.{name}" for name in sorted(given))
    return PushdownPlan(pushed=tuple(pushed), unsatisfied=("the statement beyond its arguments",))


_LANDS = Connector(answers_directly=False)

#: Each connector's contract, by source type. A type not named runs the statement whole.
CONNECTORS: dict[str, Connector] = {
    "qdrant": Connector(answers_directly=True, plan=_qdrant_plan),
    "node": Connector(answers_directly=False, plan=_code_plan),
    "python": Connector(answers_directly=False, plan=_code_plan),
    "openapi": _LANDS,
    "graphql_api": _LANDS,
    "grpc_api": _LANDS,
    "grpc_remote": _LANDS,
    "sparql": _LANDS,
}

_RELATIONAL = Connector(answers_directly=True)


def connector_for(source_type: str) -> Connector:
    """``source_type``'s connector contract."""
    return CONNECTORS.get(source_type, _RELATIONAL)

# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3a9e14-5c2b-4a68-b1f7-0e4c8d6a2b59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed plan of a GraphQL query, kept so a repeated request does not rebuild it (REQ-1877).

Parsing, validating, compiling and governing one GraphQL query costs several milliseconds of pure
Python and yields the same governed SQL every time its inputs are the same. A plan is the result of
those stages — the request's directives and its governed ``CompiledQuery`` list — kept through
``provisa.pgwire.governed_plan``, the one mechanism every surface keeps a governed plan with
(same store, same key discipline, same invalidation). A request whose plan is cached goes straight
to execution, where the response cache is consulted.

Beyond what every stage's key carries (generation, role, person), this stage's inputs are the
query text, its variables, the as-of time and the session variables RLS predicates resolve
against; and beyond the role's governance objects, a plan is anchored to the GraphQL schema object
and the fresh-MV set it was compiled against.

What is never kept here: a deployment with an approval hook (the hook decides per request), a
query over a Kafka-windowed table (its filter is chosen per call), and anything that is not a
plain query — mutations, subscriptions, introspection, action fields and normalized reads take the
ordinary path and are not offered here."""

# Requirements: REQ-1877

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from provisa.compiler.directives import QueryDirectives
from provisa.pgwire.governed_plan import PlanSlot
from provisa.compiler.sql_types import CompiledQuery
from provisa.core.request_context import session_vars_for


@dataclass(frozen=True)
class GraphQLPlan:
    """One query's directives and governed compiled fields."""

    directives: QueryDirectives
    prepared: tuple[CompiledQuery, ...]

    def prepared_copies(self) -> list[CompiledQuery]:
        """Fresh copies for one request: execution rewrites a compiled field in place."""
        return copy.deepcopy(list(self.prepared))


class PlanRequest:
    """One request's view of the plan store: look the plan up, or record the one it builds."""

    def __init__(
        self,
        state: Any,
        *,
        role_id: str,
        role: dict | None,
        schema: Any,
        query: str,
        variables: dict | None,
        as_of: str | None,
        fresh_mvs: list,
        eligible: bool,
    ) -> None:
        self._state = state
        self.fresh_mvs = fresh_mvs
        # The approval hook (REQ-203) may deny or add a filter on any request: it has to run.
        self._slot: PlanSlot | None = None
        if eligible and state.approval_hook is None:
            self._slot = PlanSlot(
                state,
                "graphql",
                role_id,
                query,
                variables or {},
                as_of,
                sorted(session_vars_for(role).items()),
                extra_anchors=(schema, *fresh_mvs),
            )

    def cached(self) -> GraphQLPlan | None:
        return self._slot.cached() if self._slot is not None else None

    def _windowed_kafka(self, prepared: list[CompiledQuery]) -> bool:
        if not self._state.kafka_table_configs:
            return False
        source_types = self._state.source_types
        return any(source_types.get(s) == "kafka" for cq in prepared for s in cq.sources)

    def record(self, directives: QueryDirectives, prepared: list[CompiledQuery]) -> None:
        """Keep the plan this request just built."""
        if self._slot is None or self._windowed_kafka(prepared):
            return
        self._slot.keep(GraphQLPlan(directives=directives, prepared=tuple(copy.deepcopy(prepared))))

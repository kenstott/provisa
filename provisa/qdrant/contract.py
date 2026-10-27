# Copyright (c) 2026 Kenneth Stott
# Canary: 1b4138b2-1bbc-42c3-a2fe-798f5e87152a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Qdrant's side of the source contract (REQ-2000, REQ-1756). A statement Qdrant's API states
exactly -- a retrieve by id, a scroll, a similarity top-k (query_points, the filter applied before
the top k) -- is a complete dataset, answered by Qdrant; anything else, a search table among it
(read under its arguments, REQ-1983), lands and the engine runs the statement, the collection's
part read live for it."""

from __future__ import annotations

from typing import Any

from provisa.federation.connector_contract import Plan, Read, SourceContract


class QdrantContract(SourceContract):
    async def plan(self, state: Any, read: Read, ctx: Any = None) -> Plan:
        from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

        driver = state.source_pools.get(read.source_id)
        why = await driver.reason(rewrite_semantic_to_physical(read.statement, ctx), read.table)
        if why is not None:
            return Plan(read=read, arguments=dict(read.arguments or {}), incomplete_because=why)
        return Plan(read=read, arguments=dict(read.arguments or {}), complete_dataset=True)

    async def call(self, state: Any, plan: Plan) -> Any:
        """A search table's rows by its arguments, for landing; a complete dataset is read by the
        Qdrant driver the route sends the statement to."""
        from provisa.qdrant.table import read

        return await read(state, plan.read.table, plan.arguments)

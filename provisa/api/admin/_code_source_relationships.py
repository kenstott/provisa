# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e1d63-92a7-4f0c-b5e8-7d3a6c1f9e24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The relationships a code source's ``@discover`` declares, written to the semantic layer as its
tables are registered (REQ-2267).

A declared relationship joins two of the code's tables. It is written once both are registered:
each registration of one of the source's tables writes the declared relationships it completes,
so a table registered before its partner has its relationship written when the partner is. A
relationship to a table never registered is never written. Each is written through the upsert
every other source's relationships take (``_upsert_relationships_to_semantic_layer``), with its
cardinality and join columns as declared.
"""

# Requirements: REQ-2267
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import select

from provisa.codesource.manifest import Relationship
from provisa.core.schema_org import registered_tables


def relationship_id(source_id: str, name: str) -> str:
    """The id a declared relationship is kept under: its source's id and the name it declares."""
    return f"{source_id}__{name}"  # naming-ok: a stored relationship id, as gql_remote__ ids are


def completed_relationships(
    source_id: str,
    declared: Iterable[Relationship],
    registered: dict[str, str],
    table_name: str,
) -> list[dict]:
    """The declared relationships the registration of ``table_name`` completes: those with it at
    one end and a registered table at the other, as the upsert takes them. ``registered`` maps
    each of the source's registered tables (the function it is named for) to the name the model
    knows it by (its alias, else its name)."""
    return [
        {
            "id": relationship_id(source_id, r.name),
            "source_table_id": registered[r.source_table],
            "target_table_id": registered[r.target_table],
            "source_column": r.source_column,
            "target_column": r.target_column,
            "cardinality": r.cardinality,
        }
        for r in declared
        if table_name in (r.source_table, r.target_table)
        and r.source_table in registered
        and r.target_table in registered
    ]


async def store_declared_relationships(
    pool: Any, state: Any, source_id: str, table_name: str
) -> int:
    """Write the declared relationships the registration of code source ``source_id``'s
    ``table_name`` completes, and return how many."""
    from provisa.api.admin.graphql_remote_router import _upsert_relationships_to_semantic_layer
    from provisa.codesource import discovery

    async with pool.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(registered_tables.c.table_name, registered_tables.c.alias).where(
                    registered_tables.c.source_id == source_id
                )
            )
        ).fetchall()
    registered = {row.table_name: row.alias or row.table_name for row in rows}
    declared = await discovery.relationships(state, source_id)
    # The source's row was read by this registration; the source is there.
    assert declared is not None
    completed = completed_relationships(source_id, declared, registered, table_name)
    if completed:
        await _upsert_relationships_to_semantic_layer(completed, pool, state)
    return len(completed)

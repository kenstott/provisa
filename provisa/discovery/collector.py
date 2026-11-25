# Copyright (c) 2026 Kenneth Stott
# Canary: 0f166822-96d8-4ead-871b-a59f24fc0552
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The foreign keys a discovery run's tables declare, as relationship candidates (REQ-413)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from provisa.codesource.languages import CODE_SOURCE_TYPES

if TYPE_CHECKING:
    from provisa.codesource.manifest import Relationship
    from provisa.core.database import Connection

#: REQ-2267: a code source's declared relationships, by its id (codesource.discovery.relationships).
CodeRelationships = Callable[[str], Awaitable["tuple[Relationship, ...] | None"]]


# Requirements: REQ-018, REQ-413, REQ-2267


async def collect_fk_for_tables(  # REQ-413
    source_pools: Any,
    source_types: dict[str, str],
    pg_conn: "Connection",
    table_ids: set[int],
    *,
    code_relationships: CodeRelationships,
) -> list:
    """The relationship candidates of the foreign keys the tables of a run's source set declare,
    each read straight from the table's own source connection (never the federation engine's
    attached view, which is a plain SELECT and carries no constraint metadata), its referenced
    table found among the registered tables. A code source's tables declare theirs in its
    ``@discover`` (REQ-2267), read by ``code_relationships``."""
    all_tables = [
        dict(r)
        for r in await pg_conn.fetch(
            "SELECT id, source_id, domain_id, schema_name, table_name FROM registered_tables "
            "ORDER BY id"
        )
    ]
    table_by_name: dict[str, dict] = {t["table_name"]: t for t in all_tables}
    tables = [t for t in all_tables if t["id"] in table_ids]
    code = [t for t in tables if source_types.get(t["source_id"], "") in CODE_SOURCE_TYPES]
    declaring = [t for t in tables if t not in code]
    existing = await _existing(pg_conn)
    return await _fk_candidates(
        source_pools, source_types, declaring, table_by_name, existing
    ) + await _code_candidates(code, all_tables, existing, code_relationships)


async def _existing(pg_conn: "Connection") -> set[tuple]:
    """Every stored relationship's (source table, source column, target table, target column)."""
    return {
        (r["source_table_id"], r["source_column"], r["target_table_id"], r["target_column"])
        for r in await pg_conn.fetch(
            "SELECT source_table_id, source_column, target_table_id, target_column FROM relationships"
        )
    }


async def _code_candidates(
    tables: list[dict],
    all_tables: list[dict],
    existing: set[tuple],
    code_relationships: CodeRelationships,
) -> list:
    """REQ-2267: the relationships a code source's ``@discover`` declares from ``tables``, each to
    a registered table of the same source and not stored, as candidates a declared foreign key
    is: its many side first (a one-to-many is offered from its target), named as declared."""
    from provisa.discovery.analyzer import RelationshipCandidate

    candidates: list[RelationshipCandidate] = []
    by_source: dict[str, list[dict]] = {}
    for t in tables:
        by_source.setdefault(t["source_id"], []).append(t)
    for source_id, own in by_source.items():
        registered = {t["table_name"]: t for t in all_tables if t["source_id"] == source_id}
        declared = await code_relationships(source_id)
        # The source's tables are registered, so the source is there to be read.
        assert declared is not None
        asked = {t["table_name"] for t in own}
        for r in declared:
            if r.source_table not in asked or r.target_table not in registered:
                continue
            many = (r.source_table, r.source_column)
            one = (r.target_table, r.target_column)
            if r.cardinality == "one-to-many":
                many, one = one, many
            key = (registered[many[0]]["id"], many[1], registered[one[0]]["id"], one[1])
            if key in existing:
                continue
            candidates.append(
                RelationshipCandidate(
                    source_table_id=key[0],
                    source_column=key[1],
                    target_table_id=key[2],
                    target_column=key[3],
                    cardinality="many-to-one",  # a Cardinality value (provisa.core.models)
                    confidence=1.0,
                    reasoning="Declared by the source's code",
                    suggested_name=r.name,
                )
            )
    return candidates


async def same_source_ids(  # REQ-2262
    pg_conn: "Connection", table_ids: set[int], among: set[int]
) -> set[int]:
    """Those of ``among`` registered on a source one of ``table_ids`` is registered on."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables as rt

    if not table_ids or not among:
        return set()
    their_sources = select(rt.c.source_id).where(rt.c.id.in_(sorted(table_ids)))
    rows = (
        await pg_conn.execute_core(
            select(rt.c.id).where(rt.c.id.in_(sorted(among)), rt.c.source_id.in_(their_sources))
        )
    ).fetchall()
    return {int(r[0]) for r in rows}


async def _fk_candidates(
    source_pools: Any,
    source_types: dict[str, str],
    tables: list[dict],
    table_by_name: dict[str, dict],
    existing: set[tuple],
) -> list:
    from provisa.discovery.analyzer import RelationshipCandidate
    from provisa.discovery.fk_introspect import introspect_fk_constraints

    from provisa.discovery.declared_keys import declared_keys

    candidates: list[RelationshipCandidate] = []
    for t in tables:
        source_type = source_types.get(t["source_id"], "")
        # REQ-2170: the foreign keys the source declares, from the reader that records its keys;
        # a source read through its bundled pgwire server has its own reader.
        declared = await declared_keys(
            source_pools, source_type, t["source_id"], t["schema_name"], t["table_name"]
        )
        fks = (
            [
                {
                    "column_name": fk.column,
                    "referenced_table": fk.referenced_table,
                    "referenced_column": fk.referenced_column,
                }
                for fk in declared.foreign
            ]
            if declared is not None
            else await introspect_fk_constraints(
                source_pools, source_type, t["source_id"], t["schema_name"], t["table_name"]
            )
        )
        for fk in fks:
            target = table_by_name.get(fk["referenced_table"])
            if target is None:
                continue
            key = (t["id"], fk["column_name"], target["id"], fk["referenced_column"])
            if key in existing:
                continue
            candidates.append(
                RelationshipCandidate(
                    source_table_id=t["id"],
                    source_column=fk["column_name"],
                    target_table_id=target["id"],
                    target_column=fk["referenced_column"],
                    cardinality="many-to-one",  # a Cardinality value (provisa.core.models)
                    confidence=1.0,
                    reasoning="Foreign key constraint",
                    suggested_name=f"{t['table_name']}-{fk['column_name']}-to-{fk['referenced_table']}",
                )
            )
    return candidates

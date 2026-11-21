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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from provisa.core.database import Connection


# Requirements: REQ-018, REQ-413


async def collect_fk_for_tables(  # REQ-413
    source_pools: Any,
    source_types: dict[str, str],
    pg_conn: "Connection",
    table_ids: set[int],
) -> list:
    """The relationship candidates of the foreign keys the tables of a run's source set declare,
    each read straight from the table's own source connection (never the federation engine's
    attached view, which is a plain SELECT and carries no constraint metadata), its referenced
    table found among the registered tables."""
    all_tables = [
        dict(r)
        for r in await pg_conn.fetch(
            "SELECT id, source_id, domain_id, schema_name, table_name FROM registered_tables "
            "ORDER BY id"
        )
    ]
    table_by_name: dict[str, dict] = {t["table_name"]: t for t in all_tables}
    tables = [t for t in all_tables if t["id"] in table_ids]
    return await _fk_candidates(source_pools, source_types, pg_conn, tables, table_by_name)


async def _fk_candidates(
    source_pools: Any,
    source_types: dict[str, str],
    pg_conn: "Connection",
    tables: list[dict],
    table_by_name: dict[str, dict],
) -> list:
    from provisa.discovery.analyzer import RelationshipCandidate
    from provisa.discovery.fk_introspect import introspect_fk_constraints

    existing = {
        (r["source_table_id"], r["source_column"], r["target_table_id"], r["target_column"])
        for r in await pg_conn.fetch(
            "SELECT source_table_id, source_column, target_table_id, target_column FROM relationships"
        )
    }

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

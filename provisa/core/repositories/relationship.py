# Copyright (c) 2026 Kenneth Stott
# Canary: b8353796-185d-43e1-810b-0febb812669d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Relationship repository — CRUD for relationships in PG config DB."""

# Requirements: REQ-018, REQ-019, REQ-020, REQ-399, REQ-400

import asyncpg

from provisa.core.models import Relationship
from provisa.core.repositories import table as table_repo


async def upsert(
    conn: asyncpg.Connection, rel: Relationship
) -> None:  # REQ-019, REQ-020, REQ-399, REQ-400
    """Upsert a relationship. Resolves table names to registered_tables IDs."""
    source_tbl = await table_repo.find_by_table_name(conn, rel.source_table_id)
    if source_tbl is None:
        raise ValueError(f"Source table not registered: {rel.source_table_id}")

    target_tbl_id = None
    if rel.target_function_name:
        # Computed relationship — no target table
        target_tbl_id = None
    else:
        target_tbl = await table_repo.find_by_table_name(conn, rel.target_table_id)
        if target_tbl is None:
            raise ValueError(f"Target table not registered: {rel.target_table_id}")
        target_tbl_id = target_tbl["id"]

    try:
        await conn.execute(
            """
            INSERT INTO relationships (id, source_table_id, target_table_id,
                                       source_column, target_column, cardinality,
                                       materialize, refresh_interval,
                                       target_function_name, function_arg, alias, graphql_alias,
                                       disable_cypher, source_json_key,
                                       owner, version, needs_review)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            ON CONFLICT (id) DO UPDATE SET
                source_table_id = EXCLUDED.source_table_id,
                target_table_id = EXCLUDED.target_table_id,
                source_column = EXCLUDED.source_column,
                target_column = EXCLUDED.target_column,
                cardinality = EXCLUDED.cardinality,
                materialize = EXCLUDED.materialize,
                refresh_interval = EXCLUDED.refresh_interval,
                target_function_name = EXCLUDED.target_function_name,
                function_arg = EXCLUDED.function_arg,
                alias = EXCLUDED.alias,
                graphql_alias = EXCLUDED.graphql_alias,
                disable_cypher = EXCLUDED.disable_cypher,
                source_json_key = EXCLUDED.source_json_key,
                -- REQ-020: bump version on every save, clear the re-review flag (a
                -- save is an explicit re-review), preserve the original owner.
                version = relationships.version + 1,
                needs_review = FALSE
            """,
            rel.id,
            source_tbl["id"],
            target_tbl_id,
            rel.source_column,
            rel.target_column or None,
            rel.cardinality.value,
            rel.materialize,
            rel.refresh_interval,
            rel.target_function_name,
            rel.function_arg,
            rel.alias or None,
            rel.graphql_alias or None,
            rel.disable_cypher,
            rel.source_json_key or None,
            rel.owner or None,
            rel.version,
            rel.needs_review,
        )
    except Exception as e:
        if "relationships_source_alias_unique" in str(e):
            raise ValueError(
                f"Alias {rel.alias!r} already exists for source table {rel.source_table_id!r}"
            ) from e
        raise

    # Mark source_column as FK on source table
    if rel.source_column:
        await conn.execute(
            "UPDATE table_columns SET is_foreign_key = TRUE WHERE table_id = $1 AND column_name = $2",
            source_tbl["id"],
            rel.source_column,
        )

    # Mark target_column as PK (or AK if another PK already exists) on target table.
    # Only applies for many-to-one: target_column is the PK of the target table.
    # For one-to-many, target_column is a FK in the target — do not mark as PK.
    from provisa.core.models import Cardinality

    if target_tbl_id and rel.target_column and rel.cardinality == Cardinality.many_to_one:
        conflicting_pk = await conn.fetchval(
            "SELECT COUNT(*) FROM table_columns WHERE table_id = $1 AND is_primary_key = TRUE AND column_name != $2",
            target_tbl_id,
            rel.target_column,
        )
        if conflicting_pk:
            await conn.execute(
                "UPDATE table_columns SET is_alternate_key = TRUE WHERE table_id = $1 AND column_name = $2",
                target_tbl_id,
                rel.target_column,
            )
        else:
            await conn.execute(
                "UPDATE table_columns SET is_primary_key = TRUE WHERE table_id = $1 AND column_name = $2",
                target_tbl_id,
                rel.target_column,
            )


async def get(conn: asyncpg.Connection, rel_id: str) -> dict | None:  # REQ-018, REQ-019
    row = await conn.fetchrow("SELECT * FROM relationships WHERE id = $1", rel_id)
    return dict(row) if row else None


async def list_all(conn: asyncpg.Connection) -> list[dict]:  # REQ-018, REQ-019
    rows = await conn.fetch("SELECT * FROM relationships ORDER BY id")
    return [dict(r) for r in rows]


async def delete(conn: asyncpg.Connection, rel_id: str) -> bool:  # REQ-019
    result = await conn.execute("DELETE FROM relationships WHERE id = $1", rel_id)
    return result == "DELETE 1"


async def mark_relationships_for_review(  # REQ-020
    conn: asyncpg.Connection, table_id: int, valid_columns: list[str]
) -> list[str]:
    """Flag relationships whose join column on ``table_id`` is no longer present (REQ-020).

    Called after a table's columns change; any relationship whose source/target join
    column on this table is absent from ``valid_columns`` is flagged ``needs_review``
    (and its version bumped). Returns the flagged relationship ids.
    """
    rows = await conn.fetch(
        "SELECT id, source_table_id, target_table_id, source_column, target_column "
        "FROM relationships WHERE source_table_id = $1 OR target_table_id = $1",
        table_id,
    )
    valid = set(valid_columns)
    flagged: list[str] = []
    for r in rows:
        stale = (
            r["source_table_id"] == table_id
            and r["source_column"]
            and r["source_column"] not in valid
        ) or (
            r["target_table_id"] == table_id
            and r["target_column"]
            and r["target_column"] not in valid
        )
        if stale:
            flagged.append(r["id"])
    if flagged:
        await conn.execute(
            "UPDATE relationships SET needs_review = TRUE, version = version + 1 "
            "WHERE id = ANY($1)",
            flagged,
        )
    return flagged

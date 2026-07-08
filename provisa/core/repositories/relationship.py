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

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, func, or_, select, update

from provisa.core.models import Relationship
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import relationships, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def upsert(
    conn: "Connection", rel: Relationship
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

    vals = {
        "id": rel.id,
        "source_table_id": source_tbl["id"],
        "target_table_id": target_tbl_id,
        "source_column": rel.source_column,
        "target_column": rel.target_column or None,
        "cardinality": rel.cardinality.value,
        "materialize": rel.materialize,
        "refresh_interval": rel.refresh_interval,
        "target_function_name": rel.target_function_name,
        "function_arg": rel.function_arg,
        "alias": rel.alias or None,
        "graphql_alias": rel.graphql_alias or None,
        "disable_cypher": rel.disable_cypher,
        "source_json_key": rel.source_json_key or None,
        "owner": rel.owner or None,
        "version": rel.version,
        "needs_review": rel.needs_review,
    }
    try:
        # REQ-020: on conflict bump version, clear the re-review flag (a save is an explicit
        # re-review), and preserve the original owner (owner is not in the update set).
        await conn.upsert(
            relationships,
            vals,
            index_elements=["id"],
            update_columns=[
                "source_table_id",
                "target_table_id",
                "source_column",
                "target_column",
                "cardinality",
                "materialize",
                "refresh_interval",
                "target_function_name",
                "function_arg",
                "alias",
                "graphql_alias",
                "disable_cypher",
                "source_json_key",
            ],
            set_extra={"version": relationships.c.version + 1, "needs_review": False},
        )
    except Exception as e:
        if "relationships_source_alias_unique" in str(e):
            raise ValueError(
                f"Alias {rel.alias!r} already exists for source table {rel.source_table_id!r}"
            ) from e
        raise

    # Mark source_column as FK on source table
    if rel.source_column:
        await conn.execute_core(
            update(table_columns)
            .where(
                table_columns.c.table_id == source_tbl["id"],
                table_columns.c.column_name == rel.source_column,
            )
            .values(is_foreign_key=True)
        )

    # Mark target_column as PK (or AK if another PK already exists) on target table.
    # Only applies for many-to-one: target_column is the PK of the target table.
    # For one-to-many, target_column is a FK in the target — do not mark as PK.
    from provisa.core.models import Cardinality

    if target_tbl_id and rel.target_column and rel.cardinality == Cardinality.many_to_one:
        result = await conn.execute_core(
            select(func.count())
            .select_from(table_columns)
            .where(
                table_columns.c.table_id == target_tbl_id,
                table_columns.c.is_primary_key == True,  # noqa: E712
                table_columns.c.column_name != rel.target_column,
            )
        )
        conflicting_pk = result.scalar()
        if conflicting_pk:
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == target_tbl_id,
                    table_columns.c.column_name == rel.target_column,
                )
                .values(is_alternate_key=True)
            )
        else:
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id == target_tbl_id,
                    table_columns.c.column_name == rel.target_column,
                )
                .values(is_primary_key=True)
            )


async def get(conn: "Connection", rel_id: str) -> dict | None:  # REQ-018, REQ-019
    result = await conn.execute_core(select(relationships).where(relationships.c.id == rel_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-018, REQ-019
    result = await conn.execute_core(select(relationships).order_by(relationships.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", rel_id: str) -> bool:  # REQ-019
    result = await conn.execute_core(_delete(relationships).where(relationships.c.id == rel_id))
    return (result.rowcount or 0) > 0


async def mark_relationships_for_review(  # REQ-020
    conn: "Connection", table_id: int, valid_columns: list[str]
) -> list[str]:
    """Flag relationships whose join column on ``table_id`` is no longer present (REQ-020).

    Called after a table's columns change; any relationship whose source/target join
    column on this table is absent from ``valid_columns`` is flagged ``needs_review``
    (and its version bumped). Returns the flagged relationship ids.
    """
    result = await conn.execute_core(
        select(
            relationships.c.id,
            relationships.c.source_table_id,
            relationships.c.target_table_id,
            relationships.c.source_column,
            relationships.c.target_column,
        ).where(
            or_(
                relationships.c.source_table_id == table_id,
                relationships.c.target_table_id == table_id,
            )
        )
    )
    rows = result.fetchall()
    valid = set(valid_columns)
    flagged: list[str] = []
    for r in rows:
        stale = (
            r._mapping["source_table_id"] == table_id
            and r._mapping["source_column"]
            and r._mapping["source_column"] not in valid
        ) or (
            r._mapping["target_table_id"] == table_id
            and r._mapping["target_column"]
            and r._mapping["target_column"] not in valid
        )
        if stale:
            flagged.append(r._mapping["id"])
    if flagged:
        await conn.execute_core(
            update(relationships)
            .where(relationships.c.id.in_(flagged))
            .values(needs_review=True, version=relationships.c.version + 1)
        )
    return flagged

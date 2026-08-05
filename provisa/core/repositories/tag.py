# Copyright (c) 2026 Kenneth Stott
# Canary: 21fe6831-1a92-4290-a21a-ec4ddd5d5d5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tag registry + assignment repository, via SQLAlchemy Core (dialect-portable).

One org-level registry (REQ-1373); assignments attach tags to sources, tables,
columns, and relationships (REQ-1377). The three system tags are code-defined
intrinsics (models.SYSTEM_TAGS) — present in every install, never stored — so the
tags table holds user tags only, and reads synthesize the system tags in front of
it. System-tag immutability is enforced at the mutation layer.
"""

# Requirements: REQ-1373, REQ-1375, REQ-1377

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select

from provisa.core.models import SYSTEM_TAG_IDS, SYSTEM_TAGS, Tag, TagAssignment
from provisa.core.schema_org import registered_tables, tag_assignments, tags

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _system_tag_row(tag: Tag) -> dict:
    return {
        "id": tag.id,
        "description": tag.description,
        "applies_to": list(tag.applies_to),
        "is_system": True,
        "reason_policy": tag.reason_policy,
        "expires_policy": tag.expires_policy,
    }


async def upsert(conn: "Connection", tag: Tag) -> None:
    await conn.upsert(
        tags,
        {
            "id": tag.id,
            "description": tag.description,
            "applies_to": tag.applies_to,
            "is_system": tag.is_system,
            "reason_policy": tag.reason_policy,
            "expires_policy": tag.expires_policy,
        },
        index_elements=["id"],
        update_columns=["description", "applies_to", "reason_policy", "expires_policy"],
    )


async def get(conn: "Connection", tag_id: str) -> dict | None:
    for system in SYSTEM_TAGS:
        if system.id == tag_id:
            return _system_tag_row(system)
    result = await conn.execute_core(select(tags).where(tags.c.id == tag_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(select(tags).order_by(tags.c.id))
    user_rows = [
        dict(r._mapping) for r in result.fetchall() if r._mapping["id"] not in SYSTEM_TAG_IDS
    ]
    return [_system_tag_row(t) for t in SYSTEM_TAGS] + user_rows


async def delete(conn: "Connection", tag_id: str) -> bool:
    """Delete a user tag and its assignments (no FK carries this: system tags have no row)."""
    result = await conn.execute_core(_delete(tags).where(tags.c.id == tag_id))
    if (result.rowcount or 0) == 0:
        return False
    await conn.execute_core(_delete(tag_assignments).where(tag_assignments.c.tag_id == tag_id))
    return True


async def assignment_count(conn: "Connection", tag_id: str) -> int:
    result = await conn.execute_core(
        select(tag_assignments.c.id).where(tag_assignments.c.tag_id == tag_id)
    )
    return len(result.fetchall())


async def assign(conn: "Connection", assignment: TagAssignment) -> None:
    await conn.upsert(
        tag_assignments,
        {
            "tag_id": assignment.tag_id,
            "object_type": assignment.object_type,
            "source_id": assignment.source_id,
            "table_id": assignment.table_id,
            "column_name": assignment.column_name,
            "relationship_id": assignment.relationship_id,
            "object_key": assignment.object_key(),
            "reason": assignment.reason,
            "expires_on": assignment.expires_on,
        },
        index_elements=["tag_id", "object_key"],
        update_columns=["object_type", "reason", "expires_on"],
    )


async def unassign(conn: "Connection", tag_id: str, object_key: str) -> bool:
    result = await conn.execute_core(
        _delete(tag_assignments).where(
            (tag_assignments.c.tag_id == tag_id) & (tag_assignments.c.object_key == object_key)
        )
    )
    return (result.rowcount or 0) > 0


async def list_assignments(conn: "Connection") -> list[dict]:
    """All assignments, with the qualified table address joined in for table/column targets.

    ``table_ref`` ("source.schema.table") is the config-vocabulary identity the export
    builder resolves through TableIndex; DB serials never leave the repository layer.
    """
    result = await conn.execute_core(
        select(
            tag_assignments,
            registered_tables.c.source_id.label("ref_source_id"),
            registered_tables.c.schema_name.label("ref_schema_name"),
            registered_tables.c.table_name.label("ref_table_name"),
        )
        .join(
            registered_tables,
            tag_assignments.c.table_id == registered_tables.c.id,
            isouter=True,
        )
        .order_by(tag_assignments.c.tag_id, tag_assignments.c.object_key)
    )
    rows = []
    for r in result.fetchall():
        row = dict(r._mapping)
        src = row.pop("ref_source_id", None)
        schema = row.pop("ref_schema_name", None)
        table = row.pop("ref_table_name", None)
        row["table_ref"] = f"{src}.{schema}.{table}" if src is not None else None
        rows.append(row)
    return rows


async def resolve_table_id(
    conn: "Connection", source_id: str, schema_name: str, table_name: str
) -> int | None:
    result = await conn.execute_core(
        select(registered_tables.c.id).where(
            (registered_tables.c.source_id == source_id)
            & (registered_tables.c.schema_name == schema_name)
            & (registered_tables.c.table_name == table_name)
        )
    )
    row = result.fetchone()
    return row.id if row is not None else None

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

from provisa.core.models import (
    DERIVED_TAG_IDS,
    DERIVED_TAGS,
    SYSTEM_TAG_IDS,
    SYSTEM_TAGS,
    Tag,
    TagAssignment,
    TagParamValue,
    base_tag_id,
)
from provisa.core.schema_org import (
    registered_tables,
    tag_assignments,
    tag_param_values,
    tags,
)

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _code_tag_row(tag: Tag) -> dict:
    return {
        "id": tag.id,
        "description": tag.description,
        "applies_to": list(tag.applies_to),
        "is_system": True,
        "derived": tag.derived,
        "reason_policy": tag.reason_policy,
        "expires_policy": tag.expires_policy,
        "param_policy": tag.param_policy,
    }


def _user_tag_row(row) -> dict:
    # The tags table holds user tags only, so nothing in it is ever derived (REQ-1443) — the
    # column does not exist and the flag is supplied here rather than defaulted at every reader.
    return {**dict(row._mapping), "derived": False}


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
            "param_policy": tag.param_policy,
        },
        index_elements=["id"],
        update_columns=[
            "description",
            "applies_to",
            "reason_policy",
            "expires_policy",
            "param_policy",
        ],
    )


async def get(conn: "Connection", tag_id: str) -> dict | None:
    """Look up the registry tag *tag_id* names, accepting an assigned parameterized id.

    REQ-1467: callers hold ids in assigned form ("entity:customer"), and the registry is
    keyed by the base id — resolving here keeps every caller from having to split first.
    """
    base = base_tag_id(tag_id)
    for code_tag in SYSTEM_TAGS + DERIVED_TAGS:
        if code_tag.id == base:
            return _code_tag_row(code_tag)
    result = await conn.execute_core(select(tags).where(tags.c.id == base))
    row = result.fetchone()
    return _user_tag_row(row) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(select(tags).order_by(tags.c.id))
    reserved = SYSTEM_TAG_IDS + DERIVED_TAG_IDS
    user_rows = [_user_tag_row(r) for r in result.fetchall() if r._mapping["id"] not in reserved]
    return [_code_tag_row(t) for t in SYSTEM_TAGS + DERIVED_TAGS] + user_rows


async def delete(conn: "Connection", tag_id: str) -> bool:
    """Delete a user tag, its assignments, and its parameter values.

    No FK carries any of this: system tags are code-defined with no row to reference.
    """
    base = base_tag_id(tag_id)
    result = await conn.execute_core(_delete(tags).where(tags.c.id == base))
    if (result.rowcount or 0) == 0:
        return False
    # base_tag_id, not tag_id: a parameterized tag's assignments are stored in "{tag}:{value}"
    # form, so matching on tag_id would leave every one of them orphaned (REQ-1467).
    await conn.execute_core(_delete(tag_assignments).where(tag_assignments.c.base_tag_id == base))
    await conn.execute_core(_delete(tag_param_values).where(tag_param_values.c.tag_id == base))
    return True


async def assignment_count(conn: "Connection", tag_id: str) -> int:
    """How many objects carry this tag, counting every parameter value (REQ-1467)."""
    result = await conn.execute_core(
        select(tag_assignments.c.id).where(tag_assignments.c.base_tag_id == base_tag_id(tag_id))
    )
    return len(result.fetchall())


async def assign(conn: "Connection", assignment: TagAssignment) -> None:
    await conn.upsert(
        tag_assignments,
        {
            "tag_id": assignment.tag_id,
            "base_tag_id": assignment.base_tag_id(),
            "object_type": assignment.object_type,
            "source_id": assignment.source_id,
            "table_id": assignment.table_id,
            "column_name": assignment.column_name,
            "relationship_id": assignment.relationship_id,
            "command_name": assignment.command_name,
            "object_key": assignment.object_key(),
            "reason": assignment.reason,
            "expires_on": assignment.expires_on,
        },
        index_elements=["base_tag_id", "object_key"],
        # tag_id updates: re-assigning entity:employee where entity:customer sat is a
        # correction of the parameter, which is the only way to change one (REQ-1467).
        update_columns=["tag_id", "object_type", "reason", "expires_on"],
    )


async def unassign(conn: "Connection", tag_id: str, object_key: str) -> bool:
    # Matched on the base id so removing "the entity tag" from a column succeeds whether the
    # caller names the parameter or not; (base_tag_id, object_key) is unique, so it is exact.
    result = await conn.execute_core(
        _delete(tag_assignments).where(
            (tag_assignments.c.base_tag_id == base_tag_id(tag_id))
            & (tag_assignments.c.object_key == object_key)
        )
    )
    return (result.rowcount or 0) > 0


# ---------------------------------------------------------------------------
# Parameter values (REQ-1467)
# ---------------------------------------------------------------------------


async def list_param_values(conn: "Connection", tag_id: str) -> list[dict]:
    result = await conn.execute_core(
        select(tag_param_values)
        .where(tag_param_values.c.tag_id == base_tag_id(tag_id))
        .order_by(tag_param_values.c.value)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def list_all_param_values(conn: "Connection") -> list[dict]:
    result = await conn.execute_core(
        select(tag_param_values).order_by(tag_param_values.c.tag_id, tag_param_values.c.value)
    )
    return [dict(r._mapping) for r in result.fetchall()]


async def upsert_param_value(conn: "Connection", param: TagParamValue) -> None:
    await conn.upsert(
        tag_param_values,
        {
            "tag_id": base_tag_id(param.tag_id),
            "value": param.value,
            "description": param.description,
        },
        index_elements=["tag_id", "value"],
        update_columns=["description"],
    )


async def param_value_assignment_count(conn: "Connection", tag_id: str, value: str) -> int:
    """Assignments carrying exactly this parameter value — the delete guard (REQ-1467)."""
    base = base_tag_id(tag_id)
    result = await conn.execute_core(
        select(tag_assignments.c.id).where(tag_assignments.c.tag_id == f"{base}:{value}")
    )
    return len(result.fetchall())


async def delete_param_value(conn: "Connection", tag_id: str, value: str) -> bool:
    result = await conn.execute_core(
        _delete(tag_param_values).where(
            (tag_param_values.c.tag_id == base_tag_id(tag_id))
            & (tag_param_values.c.value == value)
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

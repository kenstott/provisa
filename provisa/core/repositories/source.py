# Copyright (c) 2026 Kenneth Stott
# Canary: 4b6b9c56-68fd-47f8-be86-c55348492b7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source repository — CRUD for data sources, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-012, REQ-013, REQ-014, REQ-250

from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.models import Source
from provisa.core.schema_org import registered_tables, sources

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _source_values(source: Source) -> dict:
    return {
        "id": source.id,
        "type": source.type.value,
        "host": source.host,
        "port": source.port,
        "database": source.database,
        "username": source.username,
        "dialect": source.dialect or "",
        "path": source.path,
        "description": source.description,
        # JSON columns take Python objects directly — SQLAlchemy serializes per dialect.
        "mapping": source.mapping or {},
        "cdc": source.cdc.model_dump() if source.cdc else None,  # REQ-824
        "change_signal": getattr(source, "change_signal", "ttl"),  # REQ-929
        "load_protected": getattr(source, "load_protected", False),  # REQ-1141
        "off_peak_window": getattr(source, "off_peak_window", None),  # REQ-1141
        "off_peak_tz": getattr(source, "off_peak_tz", "UTC"),  # REQ-1141
    }


async def upsert(conn: "Connection", source: Source) -> None:  # REQ-012, REQ-250
    await conn.upsert(sources, _source_values(source), index_elements=["id"])


async def get(conn: "Connection", source_id: str) -> dict | None:  # REQ-012
    result = await conn.execute_core(select(sources).where(sources.c.id == source_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-012
    result = await conn.execute_core(select(sources).order_by(sources.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", source_id: str) -> bool:  # REQ-014
    result = await conn.execute_core(_delete(sources).where(sources.c.id == source_id))
    return (result.rowcount or 0) > 0


async def rename(conn: "Connection", old_id: str, new_id: str) -> bool:  # REQ-012
    """Rename a source: copy to new_id, retarget registered_tables, delete old_id."""
    async with conn.transaction():
        result = await conn.execute_core(select(sources).where(sources.c.id == old_id))
        row = result.fetchone()
        if row is None:
            return False
        vals = dict(row._mapping)
        vals["id"] = new_id
        # Insert the copy; leave an existing new_id untouched (DO NOTHING semantics).
        await conn.upsert(sources, vals, index_elements=["id"], update_columns=[])
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.source_id == old_id)
            .values(source_id=new_id)
        )
        await conn.execute_core(_delete(sources).where(sources.c.id == old_id))
    return True

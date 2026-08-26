# Copyright (c) 2026 Kenneth Stott
# Canary: e810f1db-4864-4c73-9f15-6e457a526e56
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
        "federation_hints": source.federation_hints or {},
        "cdc": source.cdc.model_dump() if source.cdc else None,  # REQ-824
        "change_signal": getattr(source, "change_signal", "ttl"),  # REQ-929
        "load_protected": getattr(source, "load_protected", False),  # REQ-1141
        "off_peak_window": getattr(source, "off_peak_window", None),  # REQ-1141
        "off_peak_tz": getattr(source, "off_peak_tz", "UTC"),  # REQ-1141
    }


async def upsert(conn: "Connection", source: Source) -> None:  # REQ-012, REQ-250
    await conn.upsert(sources, _source_values(source), index_elements=["id"])


async def count_billable(conn: "Connection") -> int:  # REQ-1513
    """How many sources the org has registered — the number a plan's source ceiling is read against.

    The built-in rows are excluded: they are seeded by Provisa into every org, so counting them
    would spend part of the allowance the customer bought before the customer registered anything.
    """
    from sqlalchemy import func

    from provisa.core.models import BUILT_IN_SOURCE_IDS

    result = await conn.execute_core(
        select(func.count())
        .select_from(sources)
        .where(sources.c.id.notin_(sorted(BUILT_IN_SOURCE_IDS)))
    )
    row = result.fetchone()
    assert row is not None  # COUNT over an empty table is a row holding 0, never no row
    return int(row[0])


async def get(conn: "Connection", source_id: str) -> dict | None:  # REQ-012
    result = await conn.execute_core(select(sources).where(sources.c.id == source_id))
    row = result.fetchone()
    return dict(row._mapping) if row is not None else None


async def list_all(conn: "Connection") -> list[dict]:  # REQ-012
    result = await conn.execute_core(select(sources).order_by(sources.c.id))
    return [dict(r._mapping) for r in result.fetchall()]


async def delete(conn: "Connection", source_id: str) -> bool:  # REQ-014
    """Delete a source and, in the same transaction, every table registered against it.

    A registered_tables row whose source_id names no source is a referential inconsistency:
    _refresh_summary._load_source raises on it (REQ-1143), and because refreshPolicySummary is
    resolved per row inside the `tables` query, one orphan turns that whole query into a partial
    error — Apollo's default errorPolicy discards the data with it, so every admin view relying
    on the table list renders empty. `rename` already retargets these rows for the same reason;
    delete is its missing counterpart.
    """
    from provisa.core.repositories import glossary as glossary_repo

    async with conn.transaction():
        # REQ-1591: the domains a departing term belongs to are derived from the very tables about
        # to be deleted, so the snapshot is taken first and handed to the sweep.
        domains_before = await glossary_repo.term_domains(conn)
        await conn.execute_core(
            _delete(registered_tables).where(registered_tables.c.source_id == source_id)
        )
        result = await conn.execute_core(_delete(sources).where(sources.c.id == source_id))
        # REQ-1387: the table deletes cascaded the glossary refs; settle newly refless terms.
        await glossary_repo.sweep_refless_terms(conn, domains_before=domains_before)
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

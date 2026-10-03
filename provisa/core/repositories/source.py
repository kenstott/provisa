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

# Requirements: REQ-012, REQ-013, REQ-014, REQ-250, REQ-1695

from provisa.core import model_change
from typing import TYPE_CHECKING

from sqlalchemy import delete as _delete, select, update

from provisa.core.models import BUILT_IN_SOURCE_IDS, Source
from provisa.core.repositories.integrity import Dependent, ObjectRef, guard, remove_parts
from provisa.core.repositories.origin import require as require_origin
from provisa.core.repositories.origin import take_over
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
        "cache_enabled": source.cache_enabled,
        "cache_ttl": source.cache_ttl,
        "replicate": source.replicate,  # REQ-826
        "max_live_concurrency": source.max_live_concurrency,  # REQ-1909
        "sentinel_path": source.sentinel_path,  # REQ-1148
        "freshness_gate": source.freshness_gate,  # REQ-860
        # REQ-1695: the REFERENCE, never the credential. ``Source.password`` is documented as a
        # secret reference (provisa/core/models.py) and the mutation layer has already put any
        # literal a person typed into the org vault, so what arrives here is ``${provider:name}``
        # or the empty string.
        "password_ref": source.password,
    }


def source_from_row(row: dict) -> Source:  # REQ-1695
    """A Source from a control-plane row, with ``password_ref`` read back as ``Source.password``.

    The one mapper every reader of a ``sources`` row goes through, so the column and the model
    field cannot drift apart. The names differ deliberately: the column says what it holds (a
    reference) and the model field says what it is used as (the password, once resolved).
    """
    fields = {k: v for k, v in row.items() if k in Source.model_fields and v is not None}
    fields["password"] = row["password_ref"]
    return Source.model_validate(fields)


async def upsert(  # REQ-012, REQ-250, REQ-1919
    conn: "Connection", source: Source, *, origin: str
) -> None:
    """Create the source, or replace its definition. ``origin`` says where it comes from
    (``repositories.origin``): written when the source is CREATED and left alone after, except
    that a config load takes over a source made through the admin."""
    model_change.name("upsert", "source", source.id)  # REQ-1524
    require_origin(origin)
    values = _source_values(source)
    await conn.upsert(
        sources,
        {**values, "origin": origin},
        index_elements=["id"],
        update_columns=[c for c in values if c != "id"],
    )
    await take_over(
        conn, sources, (sources.c.id == source.id,), kind="source", ident=source.id, origin=origin
    )


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


class SourceDeleteRefused(Exception):
    """A source that may not be deleted, and why. ``reason`` is ``"system"`` (a source the
    deployment keeps) or ``"dependents"`` (objects refer to it; ``dependents`` lists them)."""

    def __init__(
        self, source_id: str, reason: str, dependents: "list[Dependent] | None" = None
    ) -> None:
        self.source_id = source_id
        self.reason = reason
        self.dependents = dependents or []
        if reason == "dependents":
            named = ", ".join(f"{d.ref.kind} {d.ref.id}" for d in self.dependents)
            message = f"Source {source_id!r} is still referred to by: {named}"
        else:
            message = f"Source {source_id!r} is a system source and cannot be deleted"
        super().__init__(message)


async def delete(conn: "Connection", source_id: str) -> bool:  # REQ-014, REQ-1918
    """Delete one source: THE delete, for every surface. False when there is no such source.

    Refused (:class:`SourceDeleteRefused`), naming each dependent, while a table is registered
    against it or a command is bound to it: the operator removes those first, each through its
    own action. (It used to delete the source's registered tables with it.) A source the
    deployment keeps is refused. Its parts go with it: its tag assignments and its remote
    registration rows — an API or Kafka registration with its endpoints or topics. One
    transaction; no database cascade is relied on.
    """
    model_change.name("delete", "source", source_id)  # REQ-1524
    ref = ObjectRef("source", source_id)
    async with conn.transaction():
        if await get(conn, source_id) is None:
            return False
        if source_id in BUILT_IN_SOURCE_IDS:
            raise SourceDeleteRefused(source_id, "system")
        blocking = await guard(conn, ref)
        if blocking:
            raise SourceDeleteRefused(source_id, "dependents", blocking)
        await discard(conn, source_id)
    return True


async def discard(conn: "Connection", source_id: str) -> None:
    """Remove a source's parts and its row WITHOUT asking the guard: for a caller that has
    already established it may go — :func:`delete`, and the config loader once its own check of
    everything the file dropped has passed."""
    await remove_parts(conn, ObjectRef("source", source_id))
    await conn.execute_core(_delete(sources).where(sources.c.id == source_id))


async def remove_where(conn: "Connection", *where) -> None:
    """Remove every source matching ``where`` (clauses on ``sources``) — for the code that
    declares sources as a set: the config loader's full replace, and the seed retiring a row it
    wrote. It is not a deletion of one object and does not ask the dependency guard."""
    await conn.execute_core(_delete(sources).where(*where))


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

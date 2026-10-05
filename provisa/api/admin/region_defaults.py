# Copyright (c) 2026 Kenneth Stott
# Canary: 5aa51dbf-6d71-4186-8c38-d74e172ddf4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The region an object made through the admin starts in (REQ-1921, "a table carries its own
region; a source's region is its default").

The region on a TABLE is the only one that decides where its copies live, and it is stored on the
table. Made through the admin, a new table starts in its source's region, else in the region the
operator is connected to; a new view starts in the region the operator is connected to. The
operator may change either, and a table has no region only when the operator removes it. With no
platform regions nothing has a region.
"""

# Requirements: REQ-1921

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.core.database import Connection


def connected_region() -> str | None:
    """The region the operator is connected to — the one this node serves; None with no
    platform regions."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION

    region = process_region.region()
    return None if region == DEFAULT_REGION else region


async def new_table_region(conn: "Connection", source_id: str, *, is_view: bool) -> str | None:
    """The region a table (or, ``is_view``, a view) registered through the admin starts in."""
    connected = connected_region()
    if connected is None or is_view:
        return connected
    from sqlalchemy import select

    from provisa.core.schema_org import sources

    row = (
        await conn.execute_core(select(sources.c.region).where(sources.c.id == source_id))
    ).fetchone()
    if row is None:  # a table's source is registered before it
        raise LookupError(f"source {source_id!r} is not registered")
    return row.region if row.region is not None else connected


async def registration_region(conn: "Connection", inp, *, is_view: bool) -> str | None:
    """The region a table registered through the admin is saved with: the one the operator sent
    (null = no region), else where a new table or view starts (:func:`new_table_region`)."""
    import strawberry

    if inp.region is not strawberry.UNSET:
        return inp.region
    return await new_table_region(conn, inp.source_id, is_view=is_view)


async def admin_registration_draft(
    conn: "Connection", source_id: str, schema_name: str, table_name: str
) -> bool:
    """Whether a table a bulk admin registration writes is draft (REQ-1921): one it creates
    starts so, like every table registered through the admin; one already registered keeps what
    it has (a re-sync is not a registration)."""
    from provisa.core.repositories import table as table_repo

    held = await table_repo.get_by_name(conn, source_id, schema_name, table_name)
    return True if held is None else bool(held["draft"])


async def kept_placement(conn: "Connection", model) -> tuple[str | None, bool]:
    """The region and draft an edit of a registered table keeps: the stored ones. A table's
    region changes only through ``setTableRegion`` and its draft only through ``setTableDraft``;
    an edit that rebuilt the table from the form wrote over them — moving its data (its copies
    retired wherever it had been kept), or putting it in service unreleased."""
    from sqlalchemy import select

    from provisa.core.schema_org import registered_tables as t

    row = (
        await conn.execute_core(
            select(t.c.region, t.c.draft).where(
                t.c.source_id == model.source_id,
                t.c.schema_name == model.schema_name,
                t.c.table_name == model.table_name,
            )
        )
    ).fetchone()
    if row is None:
        raise LookupError(
            f"table {model.source_id}/{model.schema_name}.{model.table_name} is not registered"
        )
    return row.region, row.draft

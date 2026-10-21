# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9c6a13-8e47-4d50-b2a9-71c3e5d08f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's links to catalog assets, what was imported through them, and its lock (REQ-2122).

A link is the table's catalog binding, the same row an export records (``catalog_binding``): one
link per table per catalog, so a steward's pick and an export share it. A picked link is marked,
and the export then updates the picked asset in place.

``catalog_imports`` records, per table and catalog, when its descriptive fields were last imported
and the value each had as imported, and whether the table is locked to that catalog.
"""

# Requirements: REQ-2122

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update

from provisa.core.schema_org import catalog_bindings, catalog_imports

if TYPE_CHECKING:
    from provisa.core.database import Connection


@dataclass(frozen=True)
class Link:
    """The asset a table is linked to in one catalog."""

    catalog: str
    vendor_ref: str
    name: str
    picked: bool


async def pick(conn: "Connection", catalog: str, semantic_uri: str, ref: str, name: str) -> None:
    """Link the table at ``semantic_uri`` to the asset ``ref`` the steward picked in ``catalog``,
    in place of any link it had there."""
    await conn.upsert(
        catalog_bindings,
        {
            "target": catalog,
            "semantic_uri": semantic_uri,
            "vendor_ref": ref,
            "physical_key": name,
            "picked": True,
        },
        index_elements=["target", "semantic_uri"],
        update_columns=["vendor_ref", "physical_key", "picked"],
        set_extra={"updated_at": func.now()},
    )


async def links(conn: "Connection", semantic_uri: str) -> dict[str, Link]:
    """The table's link in each catalog it is linked to, by catalog."""
    rows = (
        await conn.execute_core(
            select(catalog_bindings).where(catalog_bindings.c.semantic_uri == semantic_uri)
        )
    ).fetchall()
    return {r.target: Link(r.target, r.vendor_ref, r.physical_key, bool(r.picked)) for r in rows}


async def linked_in(conn: "Connection", catalog: str) -> dict[str, Link]:
    """Every link in ``catalog``, by the linked table's semantic URI."""
    rows = (
        await conn.execute_core(
            select(catalog_bindings).where(catalog_bindings.c.target == catalog)
        )
    ).fetchall()
    return {
        r.semantic_uri: Link(r.target, r.vendor_ref, r.physical_key, bool(r.picked)) for r in rows
    }


async def picked_in(conn: "Connection", catalog: str) -> dict[str, tuple[str, str]]:
    """The picked links in ``catalog``: ``{semantic_uri: (vendor_ref, name)}``."""
    rows = (
        await conn.execute_core(
            select(catalog_bindings).where(
                catalog_bindings.c.target == catalog, catalog_bindings.c.picked.is_(True)
            )
        )
    ).fetchall()
    return {r.semantic_uri: (r.vendor_ref, r.physical_key) for r in rows}


@dataclass(frozen=True)
class Imported:
    """What was last imported into a table from one catalog, and whether it is locked to it."""

    catalog: str
    imported_at: datetime | None
    values: dict[str, Any] | None
    locked: bool
    #: What a scheduled import found changed and held for review, and when.
    pending: list[dict[str, Any]] | None = None
    pending_at: datetime | None = None


async def record_import(
    conn: "Connection", table_id: int, catalog: str, values: dict[str, Any]
) -> None:
    """Record that ``values`` were imported into the table from ``catalog`` now. What a scheduled
    import held for review is settled by it."""
    await conn.upsert(
        catalog_imports,
        {"table_id": table_id, "catalog": catalog, "imported": values, "pending": None},
        index_elements=["table_id", "catalog"],
        update_columns=["imported", "pending"],
        set_extra={"imported_at": func.now()},
    )


async def hold_for_review(
    conn: "Connection", table_id: int, catalog: str, pending: list[dict[str, Any]] | None
) -> None:
    """Hold what a scheduled import found changed for the table in ``catalog`` for a steward's
    review (None: nothing changed)."""
    await conn.upsert(
        catalog_imports,
        {"table_id": table_id, "catalog": catalog, "pending": pending},
        index_elements=["table_id", "catalog"],
        update_columns=["pending"],
        set_extra={"pending_at": func.now()},
    )


async def imports(conn: "Connection", table_id: int) -> dict[str, Imported]:
    """What was imported into the table from each catalog, by catalog."""
    rows = (
        await conn.execute_core(
            select(catalog_imports).where(catalog_imports.c.table_id == table_id)
        )
    ).fetchall()
    return {r.catalog: Imported(r.catalog, r.imported_at, r.imported, bool(r.locked)) for r in rows}


async def lock(conn: "Connection", table_id: int, catalog: str | None) -> None:
    """Lock the table to ``catalog``, releasing any other lock it held; None releases it."""
    await conn.execute_core(
        update(catalog_imports).where(catalog_imports.c.table_id == table_id).values(locked=False)
    )
    if catalog is None:
        return
    await conn.upsert(
        catalog_imports,
        {"table_id": table_id, "catalog": catalog, "locked": True},
        index_elements=["table_id", "catalog"],
        update_columns=["locked"],
    )


async def locked_to(conn: "Connection", catalog: str) -> set[int]:
    """The tables locked to ``catalog``."""
    rows = (
        await conn.execute_core(
            select(catalog_imports.c.table_id).where(
                catalog_imports.c.catalog == catalog, catalog_imports.c.locked.is_(True)
            )
        )
    ).fetchall()
    return {r.table_id for r in rows}

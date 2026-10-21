# Copyright (c) 2026 Kenneth Stott
# Canary: 4c7e2b95-1a3d-4f68-90b2-d85e6a1c3f07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's catalogs, as its editor uses them (REQ-2122).

The table editor's From catalog opens the pick list of each catalog of the table's domain
(``candidates``), reads the picked asset beside the table's own fields (``offer``) and writes what
the steward accepted (``apply``). Refresh from catalog is the same offer, read from the asset the
table is linked to. The editor also shows when its descriptions were last imported from which
catalog, and lets a steward lock the table to one catalog (``lock``).

Each is a table edit, so each is gated on ``table_registration``.
"""

# Requirements: REQ-2122

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.catalogs_router import catalogs_config, stored_settings
from provisa.api.errors import ApiError
from provisa.api.metadata_export import linking
from provisa.api.metadata_export.reader import CatalogReaderNotAvailable, catalog_reader
from provisa.core.repositories import catalog_link

router = APIRouter(prefix="/admin/tables/{table_id}/catalog", tags=["admin"])


def _refusal(refused: linking.LinkRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


def _db():
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "catalog_link.database_not_connected", "Database not connected")
    return state.model_db


def _uri_org_id(request: Request) -> str | None:
    """The org a table's semantic URI names: the org under multitenancy, none otherwise (as the
    export names it, so a pick and an export share one link)."""
    from provisa.api.admin._guards import require_active_org_id
    from provisa.api.app import state

    return require_active_org_id(request) if state.multitenancy else None


async def _context(conn: Any, request: Request, table_id: int) -> linking.TableContext:
    try:
        return await linking.table_context(conn, table_id, _uri_org_id(request))
    except linking.LinkRefused as refused:
        raise _refusal(refused) from None


@router.get("")
async def table_catalogs(request: Request, table_id: int) -> dict:  # REQ-2122
    """The catalogs of the table's domain, the table's link in each, and what was imported from
    each, when, and whether the table is locked to it."""
    require_capability_request(request, "table_registration")
    catalogs = catalogs_config(await stored_settings())
    async with _db().acquire() as conn:
        ctx = await _context(conn, request, table_id)
        links = await catalog_link.links(conn, ctx.semantic_uri)
        imports = await catalog_link.imports(conn, ctx.id)

    def _imported_at(catalog_id: str) -> str | None:
        held = imports.get(catalog_id)
        return held.imported_at.isoformat() if held and held.imported_at else None

    return {
        "catalogs": [
            {
                "id": c.id,
                "provider": c.provider,
                "link": (
                    {
                        "ref": links[c.id].vendor_ref,
                        "name": links[c.id].name,
                        "url": catalog_reader(c).asset_url(
                            links[c.id].vendor_ref, links[c.id].name
                        ),
                    }
                    if c.id in links
                    else None
                ),
                "imported_at": _imported_at(c.id),
                "locked": c.id in imports and imports[c.id].locked,
            }
            for c in linking.domain_catalogs(catalogs, ctx.domain_id)
        ]
    }


@router.get("/candidates")
async def table_candidates(
    request: Request, table_id: int, search: str | None = None
) -> dict:  # REQ-2122
    """Each catalog's pick list for the table, the linked or same-physical asset preselected."""
    require_capability_request(request, "table_registration")
    catalogs = catalogs_config(await stored_settings())
    async with _db().acquire() as conn:
        ctx = await _context(conn, request, table_id)
        return {"catalogs": await linking.candidates(conn, catalogs, ctx, search=search)}


@router.get("/{catalog_id}/offer")
async def table_offer(
    request: Request, table_id: int, catalog_id: str, ref: str | None = None
) -> dict:  # REQ-2122
    """The asset's descriptive metadata beside the table's own. Without ``ref``, the asset the
    table is linked to: Refresh from catalog."""
    require_capability_request(request, "table_registration")
    catalogs = catalogs_config(await stored_settings())
    async with _db().acquire() as conn:
        ctx = await _context(conn, request, table_id)
        try:
            catalog = linking.catalog_of(catalogs, ctx, catalog_id)
        except linking.LinkRefused as refused:
            raise _refusal(refused) from None
        if ref is None:
            link = (await catalog_link.links(conn, ctx.semantic_uri)).get(catalog_id)
            if link is None:
                raise ApiError(
                    400,
                    "catalog_link.not_linked",
                    f"The table is not linked to an asset of {catalog_id!r}",
                    catalog=catalog_id,
                )
            ref = link.vendor_ref
        return await linking.offer(conn, catalog, ctx, ref)


class TagBody(BaseModel):
    column: str | None = None
    tag: str


class ApplyBody(BaseModel):
    ref: str
    description: bool = False
    #: Each Provisa column that takes a catalog column's description, by the Provisa column.
    columns: dict[str, str] = {}
    tags: list[TagBody] = []
    #: Each Provisa column and the live glossary term to link it to.
    terms: dict[str, str] = {}


@router.post("/{catalog_id}/apply")
async def table_apply(
    request: Request, table_id: int, catalog_id: str, body: ApplyBody
) -> dict:  # REQ-2122
    """Write what the steward accepted of the asset ``body.ref``, link the table to it and record
    the import. The governed model is rebuilt so the new descriptions serve at once."""
    require_capability_request(request, "table_registration")
    from provisa.api.admin.schema_helpers import _rebuild_schemas

    catalogs = catalogs_config(await stored_settings())
    async with _db().acquire() as conn:
        ctx = await _context(conn, request, table_id)
        try:
            catalog = linking.catalog_of(catalogs, ctx, catalog_id)
            meta = await catalog_reader(catalog).read(body.ref)
            async with conn.transaction():
                imported = await linking.apply(
                    conn,
                    catalog,
                    ctx,
                    meta,
                    linking.Accepted(
                        description=body.description,
                        columns=body.columns,
                        tags=tuple((t.column, t.tag) for t in body.tags),
                        terms=body.terms,
                    ),
                )
        except linking.LinkRefused as refused:
            raise _refusal(refused) from None
        except CatalogReaderNotAvailable as exc:
            raise ApiError(400, "catalog_link.unreadable", str(exc), catalog=catalog_id) from exc
    await _rebuild_schemas()
    return {"success": True, "imported": imported}


class LockBody(BaseModel):
    catalog: str | None


@router.put("/lock")
async def table_lock(request: Request, table_id: int, body: LockBody) -> dict:  # REQ-2122
    """Lock the table to one of its domain's catalogs, the one it is linked to there, or release
    the lock (``catalog`` null)."""
    require_capability_request(request, "table_registration")
    catalogs = catalogs_config(await stored_settings())
    async with _db().acquire() as conn:
        ctx = await _context(conn, request, table_id)
        if body.catalog is not None:
            try:
                linking.catalog_of(catalogs, ctx, body.catalog)
            except linking.LinkRefused as refused:
                raise _refusal(refused) from None
            if body.catalog not in await catalog_link.links(conn, ctx.semantic_uri):
                raise ApiError(
                    400,
                    "catalog_link.not_linked",
                    f"The table is not linked to an asset of {body.catalog!r}",
                    catalog=body.catalog,
                )
        await catalog_link.lock(conn, ctx.id, body.catalog)
    return {"success": True, "locked": body.catalog}

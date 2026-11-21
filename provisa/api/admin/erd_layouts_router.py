# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8f6d03-71ce-4e95-a0b4-9c5e1f7a3d62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes of the saved ERD layouts (REQ-2189).

* ``GET    /admin/erd-layouts`` -- every layout's name, and who saved it last and when.
* ``GET    /admin/erd-layouts/{name}`` -- one layout, with the places of what the caller reads.
* ``PUT    /admin/erd-layouts/{name}`` -- save it: create it, or replace the places of it the
  caller reads.
* ``DELETE /admin/erd-layouts/{name}`` -- delete it.

A layout is shared in the environment, and read rights apply to it as to the diagram itself: the
tables are those the caller is answered (``catalog_scope``), and the domains those it is listed.
"""

# Requirements: REQ-2189

from __future__ import annotations

import types
from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin", tags=["admin", "erd"])


class LayoutOptions(BaseModel):
    """Every option in the ERD's bar (useErdOptions.ts)."""

    columnDetail: Literal["all", "key", "none"]
    edgeRouting: Literal["bezier", "taxi"]
    gridSnap: int = Field(ge=0)
    showOrphans: bool


class DomainPlace(BaseModel):
    domainId: str
    selected: bool
    collapsed: bool
    x: float | None
    y: float | None
    w: float | None
    h: float | None


class TablePlace(BaseModel):
    tableId: int
    selected: bool
    x: float | None
    y: float | None


class LayoutBody(BaseModel):
    options: LayoutOptions
    domains: list[DomainPlace]
    tables: list[TablePlace]


def _db() -> Any:
    from provisa.api.app import state

    if state.model_db is None:
        raise ApiError(503, "erd.database_unavailable", "Database unavailable")
    return state.model_db


def _require_write(request: Request) -> None:
    """A layout is shared in the environment; arranging the model's diagram is modelling."""
    require_capability_request(request, "table_registration")


def _caller(request: Request) -> str | None:
    identity = getattr(request.state, "identity", None)
    return None if identity is None else getattr(identity, "user_id", None)


async def _readable(conn: Any, request: Request) -> Any:
    """What of the model the caller reads, as the ERD's tables and domains are listed to it."""
    from provisa.api.admin.catalog_scope import catalog_scope
    from provisa.core.repositories.erd_layout import Readable
    from provisa.core.schema_org import registered_tables

    scope = catalog_scope(types.SimpleNamespace(context={"request": request}))
    if scope.whole:
        return Readable(table=lambda table_id: True, domain=lambda domain_id: True)
    res = await conn.execute_core(select(registered_tables.c.id, registered_tables.c.domain_id))
    holding = {r.domain_id for r in res.fetchall() if scope.lists_table(r.id)}
    return Readable(
        table=scope.lists_table,
        domain=lambda domain_id: scope.lists_domain(domain_id, domain_id in holding),
    )


def _not_found(name: str) -> ApiError:
    return ApiError(404, "erd.layout_not_found", f"No ERD layout named {name!r}", name=name)


def _answer(layout: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": layout["name"],
        "options": layout["options"],
        "updatedBy": layout["updated_by"],
        "updatedAt": layout["updated_at"].isoformat(),
        "domains": [
            {
                "domainId": d["domain_id"],
                "selected": d["selected"],
                "collapsed": d["collapsed"],
                "x": d["x"],
                "y": d["y"],
                "w": d["w"],
                "h": d["h"],
            }
            for d in layout["domains"]
        ],
        "tables": [
            {"tableId": t["table_id"], "selected": t["selected"], "x": t["x"], "y": t["y"]}
            for t in layout["tables"]
        ],
    }


@router.get("/erd-layouts")
async def list_layouts(request: Request) -> dict:
    from provisa.core.repositories import erd_layout

    async with _db().acquire() as conn:
        saved = await erd_layout.names(conn)
    return {
        "layouts": [
            {
                "name": r["name"],
                "updatedBy": r["updated_by"],
                "updatedAt": r["updated_at"].isoformat(),
            }
            for r in saved
        ]
    }


@router.get("/erd-layouts/{name}")
async def get_layout(request: Request, name: str) -> dict:
    from provisa.core.repositories import erd_layout

    async with _db().acquire() as conn:
        layout = await erd_layout.read(conn, name, await _readable(conn, request))
    if layout is None:
        raise _not_found(name)
    return _answer(layout)


@router.put("/erd-layouts/{name}")
async def save_layout(request: Request, name: str, body: LayoutBody) -> dict:
    from provisa.core.repositories import erd_layout

    _require_write(request)
    if name.strip() == "":
        raise ApiError(422, "erd.layout_name_required", "A layout needs a name")
    async with _db().acquire() as conn:
        readable = await _readable(conn, request)
        try:
            async with conn.transaction():
                await erd_layout.save(
                    conn,
                    name,
                    body.options.model_dump(),
                    [
                        {
                            "domain_id": d.domainId,
                            "selected": d.selected,
                            "collapsed": d.collapsed,
                            "x": d.x,
                            "y": d.y,
                            "w": d.w,
                            "h": d.h,
                        }
                        for d in body.domains
                    ],
                    [
                        {"table_id": t.tableId, "selected": t.selected, "x": t.x, "y": t.y}
                        for t in body.tables
                    ],
                    readable,
                    _caller(request),
                )
        except erd_layout.UnreadablePlace as exc:
            raise ApiError(
                403, "erd.layout_place_unreadable", str(exc), kind=exc.kind, id=str(exc.ident)
            ) from exc
        layout = await erd_layout.read(conn, name, readable)
    if layout is None:
        raise _not_found(name)
    return _answer(layout)


@router.delete("/erd-layouts/{name}")
async def delete_layout(request: Request, name: str) -> dict:
    from provisa.core.repositories import erd_layout

    _require_write(request)

    async with _db().acquire() as conn:
        async with conn.transaction():
            removed = await erd_layout.remove(conn, name)
    if not removed:
        raise _not_found(name)
    return {"deleted": name}

# Copyright (c) 2026 Kenneth Stott
# Canary: 1a152f45-62b2-46e6-95f9-740fca277227
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Opening a demo sandbox from the live demos page (REQ-2006).

``POST /public/demos/{name}`` needs no sign-in: it opens a visitor's sandbox of the published demo
``name`` and answers the sandbox's org and the visitor's token, which the page keeps and the
sandbox's own address is then used with. ``GET /public/demos/{name}/status`` says whether the
demo can be opened now and how many places are free, and opens nothing. Served by the sandbox tier only (the UI server routes the
path there, ui_server.DEMO_OPEN_PREFIXES); every other process refuses it by name."""

# Requirements: REQ-2006, REQ-1035

from __future__ import annotations

from fastapi import APIRouter, Request

from provisa.api.errors import ApiError

router = APIRouter(prefix="/public/demos", tags=["demo sandboxes"])
#: A named ("Try it Out") sandbox, opened by a signed-in person (REQ-2006).
named_router = APIRouter(prefix="/sandboxes", tags=["demo sandboxes"])


def _require_tier() -> None:
    from provisa.api.sandbox_tier import serves_sandboxes

    if not serves_sandboxes():
        raise ApiError(
            421,
            "sandbox.wrong_tier",
            "Demo sandboxes are opened by the sandbox tier only",
        )


@router.post("/{name}")
async def open_demo(name: str, request: Request) -> dict:
    from provisa.api.demo_sandbox import VISITOR, open_sandbox
    from provisa.api.demo_warmup import warm_tour

    _require_tier()
    opened = await open_sandbox(name, kind=VISITOR)
    assert opened.token is not None  # a visitor's sandbox is always opened with its token
    warm_tour(request.app, opened.org_id, opened.demo, {"Authorization": f"Bearer {opened.token}"})
    return {"org_id": opened.org_id, "demo": opened.demo, "token": opened.token}


@router.get("/{name}/status")
async def demo_status(name: str) -> dict:
    """Whether the published demo ``name`` can be opened now, and how many places are free: a
    probe for the live demos page and for monitoring, which opens nothing (REQ-2006)."""
    from provisa.api.demo_sandbox import demo_status as _status

    _require_tier()
    return await _status(name)


@named_router.post("/closing", status_code=204)
async def closing(request: Request) -> None:
    """The visitor's tab of this sandbox is closing (REQ-2065), said by the tab as it unloads. A
    visitor's sandbox is deleted unless a request reaches it within the grace period; a named
    sandbox, a signed-in person's, is never deleted for it -- marked nothing, answered the same."""
    from provisa.api.demo_sandbox import VISITOR, mark_closing, visitor_user_id
    from provisa.api.sandbox_tier import is_sandbox_tier_org, is_template_org
    from provisa.core.request_context import require_current_org

    _require_tier()
    org_id = require_current_org()
    if not is_sandbox_tier_org(org_id) or is_template_org(org_id):
        raise ApiError(404, "sandbox.not_a_sandbox", "This organization is not a demo sandbox")
    identity = getattr(request.state, "identity", None)
    if identity is not None and identity.user_id == visitor_user_id(org_id):
        await mark_closing(org_id)
        return
    if await _kind_of(org_id) == VISITOR:
        raise ApiError(403, "sandbox.not_its_visitor", "Only the sandbox's visitor closes it")


async def _kind_of(org_id: str) -> str | None:
    from sqlalchemy import select

    from provisa.api.admin.orgs_router import _admin_pool
    from provisa.core.schema_admin import demo_sandboxes

    async with _admin_pool().acquire() as conn:
        row = (
            await conn.execute_core(
                select(demo_sandboxes.c.kind).where(demo_sandboxes.c.org_id == org_id)
            )
        ).fetchone()
    return None if row is None else row[0]


@named_router.post("/{name}")
async def open_named(name: str, request: Request) -> dict:
    """The signed-in person's own sandbox of the published demo ``name``: the one they already
    have open, from any of their devices, or a new one. Nobody else can be admitted to it -- the
    sandbox role holds no user_management (REQ-1597)."""
    from provisa.api.demo_sandbox import NAMED, named_sandbox_of, open_sandbox
    from provisa.api.demo_warmup import warm_tour

    _require_tier()
    identity = getattr(request.state, "identity", None)
    if identity is None or identity.user_id == "anonymous":
        raise ApiError(401, "sandbox.sign_in_required", "Sign in to open your own sandbox")
    existing = await named_sandbox_of(identity.user_id, name)
    if existing is not None:
        return {"org_id": existing, "demo": name}
    opened = await open_sandbox(name, kind=NAMED, user_id=identity.user_id)
    warm_tour(
        request.app, opened.org_id, opened.demo, {"Authorization": request.headers["authorization"]}
    )
    return {"org_id": opened.org_id, "demo": opened.demo}


@named_router.get("/limits")
async def limits() -> dict:
    """The limits every demo sandbox runs under, read from the operator settings that enforce them
    -- what the sandbox notice and the hosted-sandbox page tell a person, so the two cannot drift
    apart."""
    from provisa.core import settings_registry

    _require_tier()
    return {
        "engine": "duckdb",
        "memory_limit": int(settings_registry.value("sandbox.memory_limit")),
        "disk_limit": int(settings_registry.value("sandbox.disk_limit")),
        "idle_ttl": int(settings_registry.value("sandbox.idle_ttl")),
        "max_concurrent": int(settings_registry.value("sandbox.max_concurrent")),
    }

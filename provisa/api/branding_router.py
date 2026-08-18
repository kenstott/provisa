# Copyright (c) 2026 Kenneth Stott
# Canary: 9fee4a72-0015-4072-b542-696d003b226a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The public read side of per-org branding (REQ-1486).

The surfaces branding exists for — the sign-in page, and the moment just after it — render before
the browser holds a bearer, so these two endpoints answer without one (they are in
``_SKIP_PATHS``). What they expose is what an org chose to show every visitor who reaches its
address: a name, a logo, two colors, a welcome line. Nothing here reads membership, and nothing
here reveals whether an org exists that the caller did not already name by addressing it.

Which org is being addressed follows REQ-1276 exactly: the Host subdomain, except on the
control-plane host, where the org can only come from the ``x-org-provisa`` header. That header is
how the sign-in page asks for the branding of the org it is about to send the user back to — the
org subdomain redirects to ``cloud`` to sign in, so on that page the Host names no org.
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response
from sqlalchemy import select

from provisa.api.errors import ApiError
from provisa.core.org_branding import parse_branding
from provisa.core.org_ids import is_org_id
from provisa.core.schema_admin import orgs
from provisa.security.sni import is_control_plane_host, org_from_host

router = APIRouter(prefix="/orgs/branding", tags=["branding"])


def addressed_org(request: Request) -> str | None:  # REQ-1276, REQ-1486
    """The org this request addresses, or None when it addresses no org.

    None is an answer, not a failure: the control-plane host with no header, an apex host and
    localhost all legitimately name no org, and the caller renders the product's own presentation.
    """
    host = request.headers.get("host", "")
    if is_control_plane_host(host):
        # The header is how a fetch names the org; the query parameter is how an <img> does, since
        # an image request carries no headers of the page's choosing. Both are accepted only here,
        # on the host that has no org of its own, and only to read what that org publishes.
        named = request.headers.get("x-org-provisa") or request.query_params.get("org")
        return named if named and is_org_id(named) else None
    return org_from_host(host)


async def _branding_row(org_id: str):
    from provisa.api.app import state

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(
                orgs.c.id,
                orgs.c.name,
                orgs.c.branding,
                orgs.c.branding_logo_media_type,
            ).where(orgs.c.id == org_id)
        )
        return result.fetchone()


@router.get("")
async def read_branding(request: Request):  # REQ-1486
    """The addressed org's branding, or a null document when no org is addressed."""
    org_id = addressed_org(request)
    if org_id is None:
        return {"org_id": None, "name": None, "branding": {}, "logo": False}
    row = await _branding_row(org_id)
    if row is None:
        # An address that names no org: the same answer as naming none at all, so a probe cannot
        # use this endpoint to enumerate which org ids exist.
        return {"org_id": None, "name": None, "branding": {}, "logo": False}
    record = dict(row._mapping)
    return {
        "org_id": record["id"],
        "name": record["name"],
        "branding": parse_branding(record["branding"]),
        "logo": record["branding_logo_media_type"] is not None,
    }


@router.get("/logo")
async def read_branding_logo(request: Request):  # REQ-1486
    """The addressed org's logo bytes. 404 when the org set none — an <img> that 404s is a
    missing logo, which is exactly the state being reported."""
    org_id = addressed_org(request)
    if org_id is None:
        raise ApiError(404, "branding.no_org", "This address names no org")
    from provisa.api.app import state

    assert state.admin_db is not None
    async with state.admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.branding_logo, orgs.c.branding_logo_media_type).where(
                orgs.c.id == org_id
            )
        )
        row = result.fetchone()
    if row is None or row._mapping["branding_logo"] is None:
        raise ApiError(404, "branding.no_logo", "This org has no logo")
    return Response(
        content=row._mapping["branding_logo"],
        media_type=row._mapping["branding_logo_media_type"],
        headers={
            # An SVG logo is tenant-supplied markup served from the platform's own origin. The
            # sandbox keeps it from running script or reaching anything if it is fetched directly;
            # as an <img> source it cannot execute regardless.
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; sandbox",
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-cache",
        },
    )

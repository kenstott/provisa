# Copyright (c) 2026 Kenneth Stott
# Canary: d6c894e4-3241-4a03-b10f-bdb5732fbca2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Control plane FastAPI router for REQ-073.

REQ-1355: the routes are ``/control-plane/orgs*``. They were ``/control-plane/tenants*`` and had no
clients — ``app.include_router`` sat behind a ``state.multitenancy`` branch that never ran (fixed in
f61d2974), so every one of these paths 404'd for the whole life of the prefix.
"""

# Requirements: REQ-073, REQ-1355

from __future__ import annotations

import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.control_plane.entitlements import UnknownTierError, parse_tier
from provisa.control_plane.models import DataPlane, Org
from provisa.control_plane.store import control_plane_store

router = APIRouter(prefix="/control-plane", tags=["control-plane"])

_store = control_plane_store()


def _require_multitenancy() -> None:
    from provisa.api.app import state

    if not state.multitenancy:
        raise ApiError(403, "control_plane.multitenancy_disabled", "multitenancy is not enabled")


class RegisterOrgRequest(BaseModel):  # REQ-457
    id: str
    name: str
    data_plane_id: str
    tier: str  # REQ-1053: required — the caller names the tier; the platform never assumes one


class RegisterDataPlaneRequest(BaseModel):  # REQ-456
    id: str
    org_id: str
    endpoint: str
    region: str


@router.post("/orgs")
def register_org(body: RegisterOrgRequest) -> dict:  # REQ-073, REQ-592
    _require_multitenancy()
    # REQ-1053: reject an unrecognised tier at registration; an org that reaches the store
    # with an untyped tier fails every later entitlement check instead of this one.
    try:
        parse_tier(body.id, body.tier)
    except UnknownTierError as exc:
        raise ApiError(400, "control_plane.unknown_tier", str(exc)) from exc
    org = Org(
        id=body.id,
        name=body.name,
        data_plane_id=body.data_plane_id,
        created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        tier=body.tier,
    )
    _store.register_org(org)
    return {
        "id": org.id,
        "name": org.name,
        "data_plane_id": org.data_plane_id,
        "created_at": org.created_at,
        "tier": org.tier,
    }


@router.get("/orgs")
def list_orgs() -> list[dict]:  # REQ-073, REQ-592
    _require_multitenancy()
    return [
        {
            "id": o.id,
            "name": o.name,
            "data_plane_id": o.data_plane_id,
            "created_at": o.created_at,
            "tier": o.tier,
        }
        for o in _store.list_orgs()
    ]


@router.get("/orgs/{org_id}/route")
def route_org(org_id: str) -> dict:  # REQ-073
    _require_multitenancy()
    try:
        dp = _store.route_query(org_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"endpoint": dp.endpoint}


@router.post("/data-planes")
def register_data_plane(body: RegisterDataPlaneRequest) -> dict:  # REQ-073, REQ-506
    _require_multitenancy()
    dp = DataPlane(
        id=body.id,
        org_id=body.org_id,
        endpoint=body.endpoint,
        region=body.region,
        active=True,
    )
    _store.register_data_plane(dp)
    return {
        "id": dp.id,
        "org_id": dp.org_id,
        "endpoint": dp.endpoint,
        "region": dp.region,
        "active": dp.active,
    }


@router.get("/data-planes")
def list_data_planes() -> list[dict]:  # REQ-073, REQ-506
    _require_multitenancy()
    return [
        {
            "id": dp.id,
            "org_id": dp.org_id,
            "endpoint": dp.endpoint,
            "region": dp.region,
            "active": dp.active,
        }
        for dp in _store.list_data_planes()
    ]

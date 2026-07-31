# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
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
from provisa.control_plane.models import DataPlane, Org
from provisa.control_plane.store import ControlPlaneStore

router = APIRouter(prefix="/control-plane", tags=["control-plane"])

_store = ControlPlaneStore()


def _require_multitenancy() -> None:
    from provisa.api.app import state

    if not state.multitenancy:
        raise ApiError(403, "control_plane.multitenancy_disabled", "multitenancy is not enabled")


class RegisterOrgRequest(BaseModel):  # REQ-457
    id: str
    name: str
    data_plane_id: str


class RegisterDataPlaneRequest(BaseModel):  # REQ-456
    id: str
    org_id: str
    endpoint: str
    region: str


@router.post("/orgs")
def register_org(body: RegisterOrgRequest) -> dict:  # REQ-073, REQ-592
    _require_multitenancy()
    org = Org(
        id=body.id,
        name=body.name,
        data_plane_id=body.data_plane_id,
        created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )
    _store.register_org(org)
    return {
        "id": org.id,
        "name": org.name,
        "data_plane_id": org.data_plane_id,
        "created_at": org.created_at,
    }


@router.get("/orgs")
def list_orgs() -> list[dict]:  # REQ-073, REQ-592
    _require_multitenancy()
    return [
        {"id": o.id, "name": o.name, "data_plane_id": o.data_plane_id, "created_at": o.created_at}
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

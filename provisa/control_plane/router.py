# Copyright (c) 2026 Kenneth Stott
# Canary: 8f8ec523-0921-4866-889d-9a3f38256e46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Control plane FastAPI router for REQ-073."""

# Requirements: REQ-073

from __future__ import annotations

import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from provisa.control_plane.models import DataPlane, Tenant
from provisa.control_plane.store import ControlPlaneStore

router = APIRouter(prefix="/control-plane", tags=["control-plane"])

_store = ControlPlaneStore()


def _require_multitenancy() -> None:
    from provisa.api.app import state

    if not state.multitenancy:
        raise HTTPException(status_code=403, detail="multitenancy is not enabled")


class RegisterTenantRequest(BaseModel):  # REQ-457
    id: str
    name: str
    data_plane_id: str


class RegisterDataPlaneRequest(BaseModel):  # REQ-456
    id: str
    tenant_id: str
    endpoint: str
    region: str


@router.post("/tenants")
def register_tenant(body: RegisterTenantRequest) -> dict:  # REQ-073, REQ-592
    _require_multitenancy()
    tenant = Tenant(
        id=body.id,
        name=body.name,
        data_plane_id=body.data_plane_id,
        created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )
    _store.register_tenant(tenant)
    return {
        "id": tenant.id,
        "name": tenant.name,
        "data_plane_id": tenant.data_plane_id,
        "created_at": tenant.created_at,
    }


@router.get("/tenants")
def list_tenants() -> list[dict]:  # REQ-073, REQ-592
    _require_multitenancy()
    return [
        {"id": t.id, "name": t.name, "data_plane_id": t.data_plane_id, "created_at": t.created_at}
        for t in _store.list_tenants()
    ]


@router.get("/tenants/{tenant_id}/route")
def route_tenant(tenant_id: str) -> dict:  # REQ-073
    _require_multitenancy()
    try:
        dp = _store.route_query(tenant_id)
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
        tenant_id=body.tenant_id,
        endpoint=body.endpoint,
        region=body.region,
        active=True,
    )
    _store.register_data_plane(dp)
    return {
        "id": dp.id,
        "tenant_id": dp.tenant_id,
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
            "tenant_id": dp.tenant_id,
            "endpoint": dp.endpoint,
            "region": dp.region,
            "active": dp.active,
        }
        for dp in _store.list_data_planes()
    ]

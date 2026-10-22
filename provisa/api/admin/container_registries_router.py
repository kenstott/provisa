# Copyright (c) 2026 Kenneth Stott
# Canary: ad02e221-04cf-4a88-a4d5-6bbf1c9f75b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's container registries, as its administrator sets them (REQ-2098).

``/admin/orgs/{org_id}/container-registries`` is the organisation's own, opened as its package
registries are (``org_settings`` held in THAT organisation). Each field is answered as entered --
the value, or a reference the administrator typed -- so the form shows again what it holds.
"""

# Requirements: REQ-2098
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.secrets_router import _admin_pool, _org_guard
from provisa.api.errors import ApiError
from provisa.core import container_registries
from provisa.core.container_registries import ContainerRegistryRefused, Registry

router = APIRouter(prefix="/admin/orgs/{org_id}/container-registries", tags=["admin"])


class RegistryBody(BaseModel):
    address: str
    username: str
    #: Left out on a replace to keep the credential already entered.
    credential: str | None = None
    default: bool = False
    #: The PEM certificate authority, or a reference to it; empty or left out holds none.
    ca_bundle: str | None = None


def _refusal(refused: ContainerRegistryRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


def _entry(registry: Registry) -> dict:
    return {
        "id": registry.id,
        "address": registry.address,
        "username": registry.username,
        # As entered; read only by who may enter it (_org_guard).
        "credential": registry.credential,
        "default": registry.default,
        "ca_bundle": registry.ca_bundle,
    }


@router.get("")
async def list_registries(request: Request, org_id: str) -> dict:
    """Every container registry the organisation's built images may be pushed to."""
    await _org_guard(request, org_id)
    return {
        "registries": [_entry(r) for r in await container_registries.entries(_admin_pool(), org_id)]
    }


async def _put(request: Request, org_id: str, registry_id: str | None, body: RegistryBody) -> dict:
    actor = await _org_guard(request, org_id)
    try:
        kept = await container_registries.put(
            _admin_pool(),
            org_id,
            registry_id=registry_id,
            address=body.address,
            username=body.username,
            credential=body.credential,
            default=body.default,
            actor=actor,
            ca_bundle=body.ca_bundle,
        )
    except ContainerRegistryRefused as refused:
        raise _refusal(refused) from None
    return _entry(kept)


@router.post("")
async def add_registry(request: Request, org_id: str, body: RegistryBody) -> dict:
    """Add a container registry."""
    return await _put(request, org_id, None, body)


@router.put("/{registry_id}")
async def replace_registry(
    request: Request, org_id: str, registry_id: str, body: RegistryBody
) -> dict:
    """Replace an entry; a credential left out keeps the one entered."""
    return await _put(request, org_id, registry_id, body)


@router.delete("/{registry_id}")
async def delete_registry(request: Request, org_id: str, registry_id: str) -> dict:
    """Remove an entry. A secret it named is the administrator's and is not touched."""
    await _org_guard(request, org_id)
    return {"removed": await container_registries.forget(_admin_pool(), org_id, registry_id)}

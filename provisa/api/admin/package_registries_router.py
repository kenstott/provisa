# Copyright (c) 2026 Kenneth Stott
# Canary: 0f8bb5db-b1cc-456b-9947-c0c6860fa5f0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's package registries, as its administrator sets them (REQ-1984).

``/admin/orgs/{org_id}/package-registries`` is the organisation's own: it is opened by
``org_settings`` held in THAT organisation, the right its vault answers to, and by nothing above
it. Each field is answered as entered -- the value, or a reference the administrator typed -- so
the form shows again what it holds.
"""

# Requirements: REQ-1984, REQ-2088
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin.secrets_router import _admin_pool, _org_guard
from provisa.api.errors import ApiError
from provisa.core import package_registries
from provisa.core.package_registries import PackageRegistryRefused, Registry

router = APIRouter(prefix="/admin/orgs/{org_id}/package-registries", tags=["admin"])


class RegistryBody(BaseModel):
    kind: str
    address: str
    scope: str | None = None
    #: Left out on a replace to keep the credential already entered.
    credential: str | None = None
    mirror: bool = False
    #: REQ-2088: the PEM certificate authority, or a reference to it; empty or left out holds none.
    ca_bundle: str | None = None


def _refusal(refused: PackageRegistryRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


def _entry(registry: Registry) -> dict:
    return {
        "id": registry.id,
        "kind": registry.kind,
        "address": registry.address,
        "scope": registry.scope,
        # As entered; read only by who may enter it (_org_guard).
        "credential": registry.credential,
        "mirror": registry.mirror,
        "ca_bundle": registry.ca_bundle,
    }


@router.get("")
async def list_registries(request: Request, org_id: str) -> dict:
    """Every npm registry and Git host the organisation's Node installs go through."""
    await _org_guard(request, org_id)
    return {
        "registries": [_entry(r) for r in await package_registries.entries(_admin_pool(), org_id)]
    }


async def _put(request: Request, org_id: str, registry_id: str | None, body: RegistryBody) -> dict:
    actor = await _org_guard(request, org_id)
    try:
        kept = await package_registries.put(
            _admin_pool(),
            org_id,
            registry_id=registry_id,
            kind=body.kind,
            address=body.address,
            scope=body.scope,
            credential=body.credential,
            mirror=body.mirror,
            actor=actor,
            ca_bundle=body.ca_bundle,
        )
    except PackageRegistryRefused as refused:
        raise _refusal(refused) from None
    return _entry(kept)


@router.post("")
async def add_registry(request: Request, org_id: str, body: RegistryBody) -> dict:
    """Add an npm registry or a Git host."""
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
    return {"removed": await package_registries.forget(_admin_pool(), org_id, registry_id)}

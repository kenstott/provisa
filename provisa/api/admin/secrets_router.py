# Copyright (c) 2026 Kenneth Stott
# Canary: 71d9c78b-7623-41db-b6a1-62c8836fd38d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The secrets of one organization (REQ-1557, REQ-1558).

NAMES GO IN, VALUES NEVER COME BACK OUT. There is no endpoint here that returns a stored value,
and there is deliberately no way to add one: a value readable through the API is a credential the
browser has already been handed, and every screen that has ever offered a "show" button has ended
up displaying a live credential to whoever was standing behind the person who clicked it. A person
who has lost a secret REPLACES it, which is the same call that created it.

Secrets are the ORG'S. A platform admin operates the control plane and has no read of any org's
secret values (REQ-1361) -- and in fact nobody does, because nothing reads them out by name.
"""

# Requirements: REQ-1361, REQ-1557, REQ-1558

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.core import secrets_store
from provisa.core.database import Database

router = APIRouter(prefix="/admin/orgs/{org_id}/secrets", tags=["admin"])


def _admin_pool() -> Database:
    """Secrets live in the platform control plane, beside the org they belong to."""
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _guard(request: Request, org_id: str) -> str | None:
    """Every act here is an org_admin act, reads included: the LIST of names an org holds is
    itself a statement about what that org connects to."""
    from provisa.api.admin.environments_router import _caller_user_id
    from provisa.api.admin.invites_router import _require_org_admin

    await _require_org_admin(request, org_id)
    return _caller_user_id(request)


async def _audit(org_id: str, actor: str | None, action: str, name: str) -> None:
    """Record the act -- the NAME and who acted, never the value, not even its length."""
    from provisa.api.admin.orgs_router import _org_tenant_db
    from provisa.core.org_membership import record_admin_action

    await record_admin_action(
        await _org_tenant_db(org_id),
        action=action,
        actor_id=actor or "anonymous",
        subject_id=name,
        detail={},
    )


class SecretBody(BaseModel):
    value: str
    description: str | None = None


@router.get("")
async def list_secrets(request: Request, org_id: str) -> dict:
    """What the org holds, by name, and which service is holding it.

    The backend is named because it decides what an org should DO here: when a central secrets
    service is configured, a secret belongs in that service and this page is a read of what it
    already has, not a second place to put things.
    """
    await _guard(request, org_id)
    from provisa.core.secrets_runtime import secrets_backend_spec

    spec = secrets_backend_spec()
    if spec is None:
        raise ApiError(
            500,
            "secrets.provider_unknown",
            "The configured secrets provider is not registered in this deployment.",
            org=org_id,
        )
    writable = spec.writable
    return {
        "provider": {"key": spec.key, "label": spec.label, "writable": writable},
        # A central service owns its own names; Provisa does not enumerate somebody else's store.
        "secrets": [s.as_dict() for s in await secrets_store.listing(_admin_pool(), org_id)]
        if writable
        else [],
    }


def _must_be_writable(org_id: str) -> None:
    from provisa.core.secrets_runtime import secrets_backend_spec

    spec = secrets_backend_spec()
    if spec is None or not spec.writable:
        raise ApiError(
            400,
            "secrets.provider_read_only",
            "This deployment reads its secrets from a central service, which owns creating and "
            "deleting them. Add the secret there.",
            org=org_id,
        )


@router.put("/{name}")
async def put_secret(request: Request, org_id: str, name: str, body: SecretBody) -> dict:
    """Create or replace one secret. The same call for both: the name is the identity."""
    actor = await _guard(request, org_id)
    _must_be_writable(org_id)
    existed = await secrets_store.describe(_admin_pool(), org_id, name) is not None
    try:
        stored = await secrets_store.put(
            _admin_pool(),
            org_id,
            name,
            body.value,
            description=body.description,
            actor=actor,
        )
    except ValueError as exc:
        raise ApiError(400, "secrets.invalid", str(exc), org=org_id) from exc
    except RuntimeError as exc:
        # No master key and nowhere to keep one. The deployment cannot hold a secret at all yet,
        # and saying so is the only correct answer -- storing it in the clear is not (REQ-1557).
        raise ApiError(500, "secrets.unencryptable", str(exc), org=org_id) from exc
    await _audit(org_id, actor, "secret.replaced" if existed else "secret.created", name)
    return stored.as_dict()


@router.delete("/{name}")
async def delete_secret(request: Request, org_id: str, name: str) -> dict:
    """Delete one secret.

    A config somewhere still writing ``${secret:NAME}`` does not block this: the reference is
    text, and what it resolves to is decided when it is used (REQ-1558).
    """
    actor = await _guard(request, org_id)
    _must_be_writable(org_id)
    if not await secrets_store.remove(_admin_pool(), org_id, name):
        raise ApiError(
            404,
            "secrets.not_found",
            f"This organization has no secret named {name!r}.",
            org=org_id,
        )
    await _audit(org_id, actor, "secret.deleted", name)
    return {"deleted": name}

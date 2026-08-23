# Copyright (c) 2026 Kenneth Stott
# Canary: 71d9c78b-7623-41db-b6a1-62c8836fd38d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The secrets of one organization, and of the person acting in it (REQ-1557, REQ-1558, REQ-1560).

NAMES GO IN, VALUES NEVER COME BACK OUT. There is no endpoint here that returns a stored value,
and there is deliberately no way to add one: a value readable through the API is a credential the
browser has already been handed, and every screen that has ever offered a "show" button has ended
up displaying a live credential to whoever was standing behind the person who clicked it. A person
who has lost a secret REPLACES it, which is the same call that created it.

TWO VAULTS, ONE ROUTER (REQ-1560). ``/secrets`` is the ORG vault -- shared, org_settings, and
what an org_admin stores there is what every member resolves. ``/my-secrets`` is the acting
person's own; its owner is taken from the authenticated identity and never from the path, so there
is no request that names another person's vault and therefore nothing to authorize. No right
reaches into a personal vault: not org_settings, not cross_org, not the platform bypass.

Secrets are the ORG'S. A platform admin operates the control plane and has no read of any org's
secret values (REQ-1361) -- and in fact nobody does, because nothing reads them out by name.
"""

# Requirements: REQ-1361, REQ-1557, REQ-1558, REQ-1560

from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.api.admin.environments_router import _caller_user_id
from provisa.core import secrets_store
from provisa.core.database import Database
from provisa.core.secrets_store import ORG_OWNER

router = APIRouter(prefix="/admin/orgs/{org_id}", tags=["admin"])


def _admin_pool() -> Database:
    """Secrets live in the platform control plane, beside the org they belong to."""
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


async def _org_guard(request: Request, org_id: str) -> str | None:
    """The ORG vault. Reads included: the LIST of names an org holds is itself a statement about
    what that org connects to.

    REQ-1560 fixes what this used to ask. It delegated to the invites guard, which reads
    ``user_management`` -- a right about people, not about the org's settings -- so a role holding
    org_settings saw the surface and was refused by the API. The right is ORG_SETTINGS, the same
    one the route and the nav entry are gated on, held in THIS org.

    REQ-1361/REQ-1558: cross_org and the platform bypass do NOT open it. Administering an org's
    lifecycle is not a read of the credentials that org keeps, and a platform operator who could
    list them has the tenant separation the deployment was sold on only on paper.
    """
    from provisa.api.admin.capabilities import _resolved_capabilities
    from provisa.api.app import state as _app_state
    from provisa.security.rights import Capability

    user_id = _caller_user_id(request)
    if user_id is None:
        return None  # dev mode -- no auth configured, matching every other admin guard
    caps = _resolved_capabilities(request.state.identity, _app_state)
    active_org = getattr(request.state, "active_org_id", None)
    if Capability.ORG_SETTINGS.value not in caps or active_org != org_id:
        raise ApiError(
            403,
            "secrets.org_settings_in_org_required",
            f"org_settings in {org_id} required. An organization's secrets answer to the "
            "administrator of that organization and to nobody above it.",
            org_id=org_id,
        )
    return user_id


def _personal_owner(request: Request, org_id: str) -> str:
    """The vault of whoever is calling (REQ-1560).

    There is no ``owner`` parameter anywhere in this router, and that is the security property: the
    owner is read off the authenticated identity, so the only vault any request can address is the
    caller's own. Nothing here consults capabilities -- holding a personal secret is not a
    privilege, and no privilege reaches another person's.
    """
    user_id = _caller_user_id(request)
    if user_id is None:
        raise ApiError(
            403,
            "secrets.identity_required",
            "A personal secret belongs to a person, so this deployment must have authentication "
            "configured before one can be stored.",
            org_id=org_id,
        )
    return user_id


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


def _provider_block(org_id: str):
    """The one secrets service this deployment is wired to, and whether it can be written.

    The backend is named because it decides what a person should DO here: when a central secrets
    service is configured, a secret belongs in that service and this page is a read of what it
    already has, not a second place to put things. Both vaults live in that one service
    (REQ-1560) -- the scope chooses the vault, never a second backend.
    """
    from provisa.core.secrets_runtime import secrets_backend_spec

    spec = secrets_backend_spec()
    if spec is None:
        raise ApiError(
            500,
            "secrets.provider_unknown",
            "The configured secrets provider is not registered in this deployment.",
            org=org_id,
        )
    return spec


async def _list_vault(org_id: str, owner_id: str) -> dict:
    spec = _provider_block(org_id)
    return {
        "provider": {"key": spec.key, "label": spec.label, "writable": spec.writable},
        # A central service owns its own names; Provisa does not enumerate somebody else's store.
        "secrets": [
            s.as_dict()
            for s in await secrets_store.listing(_admin_pool(), org_id, owner_id=owner_id)
        ]
        if spec.writable
        else [],
    }


def _must_be_writable(org_id: str) -> None:
    if not _provider_block(org_id).writable:
        raise ApiError(
            400,
            "secrets.provider_read_only",
            "This deployment reads its secrets from a central service, which owns creating and "
            "deleting them. Add the secret there.",
            org=org_id,
        )


async def _store(
    org_id: str, owner_id: str, name: str, body: "SecretBody", actor: str | None
) -> dict:
    """Create or replace one secret in one vault. The same call for both: the name is the identity."""
    _must_be_writable(org_id)
    existed = (
        await secrets_store.describe(_admin_pool(), org_id, name, owner_id=owner_id) is not None
    )
    try:
        stored = await secrets_store.put(
            _admin_pool(),
            org_id,
            name,
            body.value,
            owner_id=owner_id,
            description=body.description,
            actor=actor,
        )
    except ValueError as exc:
        raise ApiError(400, "secrets.invalid", str(exc), org=org_id) from exc
    except RuntimeError as exc:
        # No master key and nowhere to keep one. The deployment cannot hold a secret at all yet,
        # and saying so is the only correct answer -- storing it in the clear is not (REQ-1557).
        raise ApiError(500, "secrets.unencryptable", str(exc), org=org_id) from exc
    scope = "secret" if owner_id == ORG_OWNER else "user_secret"
    await _audit(org_id, actor, f"{scope}.replaced" if existed else f"{scope}.created", name)
    return stored.as_dict()


async def _drop(org_id: str, owner_id: str, name: str, actor: str | None) -> dict:
    """Delete one secret from one vault.

    A config somewhere still naming it does not block this: the reference is text, and what it
    resolves to is decided when it is used (REQ-1558).
    """
    _must_be_writable(org_id)
    if not await secrets_store.remove(_admin_pool(), org_id, name, owner_id=owner_id):
        raise ApiError(
            404,
            "secrets.not_found",
            f"There is no secret named {name!r} in this vault.",
            org=org_id,
        )
    scope = "secret" if owner_id == ORG_OWNER else "user_secret"
    await _audit(org_id, actor, f"{scope}.deleted", name)
    return {"deleted": name}


# ---------------------------------------------------------------- the ORG vault (org_settings)


@router.get("/secrets")
async def list_secrets(request: Request, org_id: str) -> dict:
    """What the ORG holds, by name, and which service is holding it."""
    await _org_guard(request, org_id)
    return await _list_vault(org_id, ORG_OWNER)


@router.put("/secrets/{name}")
async def put_secret(request: Request, org_id: str, name: str, body: SecretBody) -> dict:
    """Create or replace one org secret. Storing it HERE is the act that shares it."""
    actor = await _org_guard(request, org_id)
    return await _store(org_id, ORG_OWNER, name, body, actor)


@router.delete("/secrets/{name}")
async def delete_secret(request: Request, org_id: str, name: str) -> dict:
    """Delete one org secret."""
    actor = await _org_guard(request, org_id)
    return await _drop(org_id, ORG_OWNER, name, actor)


# ------------------------------------------------------------- the PERSONAL vault (REQ-1560)


@router.get("/my-secrets")
async def list_my_secrets(request: Request, org_id: str) -> dict:
    """What the CALLER holds in this org. Never anyone else's -- there is no way to ask."""
    return await _list_vault(org_id, _personal_owner(request, org_id))


@router.put("/my-secrets/{name}")
async def put_my_secret(request: Request, org_id: str, name: str, body: SecretBody) -> dict:
    """Create or replace one of the caller's own secrets."""
    owner = _personal_owner(request, org_id)
    return await _store(org_id, owner, name, body, owner)


@router.delete("/my-secrets/{name}")
async def delete_my_secret(request: Request, org_id: str, name: str) -> dict:
    """Delete one of the caller's own secrets."""
    owner = _personal_owner(request, org_id)
    return await _drop(org_id, owner, name, owner)

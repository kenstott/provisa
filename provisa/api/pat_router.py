# Copyright (c) 2026 Kenneth Stott
# Canary: 7170de67-0941-4530-a72c-ebdcbf57998c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Self-service personal access tokens (REQ-1263).

Every route acts on the caller's own tokens in their active org. There is no path here to read
or revoke another user's token: a PAT is a personal credential, so administering one belongs to
the user who holds it, and revocation on behalf of someone else happens through org membership
removal (``remove_from_org``), not through this router.
"""

from __future__ import annotations

import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.auth.pat import TokenNameTaken

# Requirements: REQ-1263, REQ-2076

router = APIRouter(prefix="/auth/tokens", tags=["auth"])

# A token issued for longer than a year is a permanent credential in practice. Non-expiring
# tokens remain available by omitting expires_in_days — an explicit choice, not a slip.
_MAX_EXPIRY_DAYS = 366


class IssueTokenBody(BaseModel):
    name: str
    # The roles the token may act as, by name. Required: a token with none is not issued, and
    # each must be a role the issuer holds (checked in issue_token).
    scopes: list[str] = []
    expires_in_days: int | None = None
    # REQ-1503: the environment the token is issued INTO. Absent means the one this request is
    # being served by -- the environment the person issuing it is looking at.
    env_name: str | None = None


def _caller(request: Request) -> tuple[str, str]:
    """The authenticated user id and active org, or 401/409."""
    identity = getattr(request.state, "identity", None)
    if identity is None or getattr(identity, "user_id", "anonymous") == "anonymous":
        raise ApiError(401, "auth.authentication_required", "Authentication required")
    org_id = getattr(request.state, "active_org_id", None)
    if not org_id:
        raise ApiError(
            409,
            "auth.no_active_org",
            "A personal access token is scoped to an org and the request has no active org",
        )
    return identity.user_id, org_id


def _store():
    from provisa.api.app import state
    from provisa.auth.pat import PersonalAccessTokenStore

    admin_db = state.admin_db
    if admin_db is None:
        raise ApiError(
            503,
            "auth.control_plane_unavailable",
            "Personal access tokens require the platform control plane",
        )
    return PersonalAccessTokenStore(admin_db)


async def _issued_into(request: Request, org_id: str, named: str | None) -> str:
    """The environment a new token addresses (REQ-1503): the one named, or the one serving this
    request. A named environment must be one this caller could be served by -- issuing a token is
    not a way into an environment the caller cannot otherwise reach."""
    from provisa.api.app import state
    from provisa.api.env_routing import (
        EnvironmentRightError,
        EnvironmentSelectionError,
        resolve_selected_env,
    )
    from provisa.auth.middleware import org_env_capabilities
    from provisa.core.request_context import active_env

    if named is None or named == active_env():
        return active_env()
    identity = request.state.identity
    try:
        return await resolve_selected_env(
            state.admin_db, org_id, identity, named, await org_env_capabilities(identity, org_id)
        )
    except EnvironmentSelectionError as exc:
        raise ApiError(404, "env.unknown", str(exc), org=org_id, env=named) from exc
    except EnvironmentRightError as exc:
        raise ApiError(403, "env.switch_forbidden", str(exc), org=org_id, env=named) from exc


async def _held_scopes(request: Request, org_id: str, scopes: list[str]) -> list[str]:
    """``scopes`` when the token may be issued for them (REQ-1263): at least one, each a role the
    issuer holds in this org now -- the assignments the auth middleware settled for this request,
    whichever source the deployment reads them from. Issuing under a token cannot widen it: its
    own assignments are already narrowed to its scopes.

    REQ-2076: a scope the issuer does not hold is a role named that is not held. It is refused
    and recorded as that event is on every other request."""
    scopes = list(dict.fromkeys(s.strip() for s in scopes if s.strip()))
    if not scopes:
        raise ApiError(
            400,
            "auth.token_scopes_required",
            "A personal access token needs at least one role it may act as",
        )
    identity = request.state.identity
    held = {a.role_id for a in request.state.assignments}
    for scope in scopes:
        if scope in held:
            continue
        from provisa.auth.middleware import _client_of
        from provisa.security import role_probe

        probe = role_probe.Probe(
            org_id=org_id,
            user_id=identity.user_id,
            credential=identity.credential,
            credential_id=identity.credential_id,
            transport="http",
            endpoint=f"{request.method} {request.url.path}",
            requested=scope,
            held=tuple(sorted(held)),
            client=_client_of(request),
        )
        await role_probe.record(probe)
        raise ApiError(
            403,
            "auth.token_scope_not_held",
            role_probe.refusal(probe, f"Role {scope!r} is not assigned to this user"),
        )
    return scopes


@router.post("")  # REQ-1263, REQ-1503, REQ-2076
async def issue_token(body: IssueTokenBody, request: Request):
    """Mint a token for the caller. The secret is in this response and nowhere else, ever."""
    user_id, org_id = _caller(request)
    expires_at = None
    if body.expires_in_days is not None:
        if not 1 <= body.expires_in_days <= _MAX_EXPIRY_DAYS:
            raise ApiError(
                400,
                "auth.invalid_token_expiry",
                f"expires_in_days must be between 1 and {_MAX_EXPIRY_DAYS}",
            )
        expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            days=body.expires_in_days
        )
    scopes = await _held_scopes(request, org_id, body.scopes)
    env_name = await _issued_into(request, org_id, body.env_name)
    try:
        secret, row = await _store().issue(
            user_id=user_id,
            org_id=org_id,
            name=body.name,
            env_name=env_name,
            scopes=scopes,
            expires_at=expires_at,
        )
    except TokenNameTaken as exc:
        raise ApiError(409, "auth.token_name_taken", str(exc), name=exc.name) from exc
    except ValueError as exc:
        raise ApiError(400, "auth.invalid_token_request", str(exc))
    return {**row, "token": secret}


@router.get("")  # REQ-1263
async def list_tokens(request: Request):
    """The caller's tokens in this org. Carries the hash as the id for revocation, never a secret."""
    user_id, org_id = _caller(request)
    return await _store().list_for_user(user_id, org_id)


@router.delete("/{token_hash}")  # REQ-1263
async def revoke_token(token_hash: str, request: Request):
    """Revoke one of the caller's tokens. 404 when it is not theirs or already revoked."""
    user_id, _org_id = _caller(request)
    if not await _store().revoke(token_hash=token_hash, user_id=user_id):
        raise ApiError(404, "auth.token_not_found", "No such active token for this user")
    return {"revoked": token_hash}

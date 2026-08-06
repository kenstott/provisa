# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-1263

router = APIRouter(prefix="/auth/tokens", tags=["auth"])

# A token issued for longer than a year is a permanent credential in practice. Non-expiring
# tokens remain available by omitting expires_in_days — an explicit choice, not a slip.
_MAX_EXPIRY_DAYS = 366


class IssueTokenBody(BaseModel):
    name: str
    # Null means the token resolves to whatever role its owner holds. A value narrows it.
    role_id: str | None = None
    scopes: list[str] = []
    expires_in_days: int | None = None


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


@router.post("")  # REQ-1263
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
    try:
        secret, row = await _store().issue(
            user_id=user_id,
            org_id=org_id,
            name=body.name,
            role_id=body.role_id,
            scopes=body.scopes,
            expires_at=expires_at,
        )
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

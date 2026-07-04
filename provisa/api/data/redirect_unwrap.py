# Copyright (c) 2026 Kenneth Stott
# Canary: 9c2a6d17-3b84-4e50-9f61-2c7a0d4f8b17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Authenticated unwrap of a redirect payload's role-bound encryption grant (REQ-687)."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter(prefix="/data", tags=["data"])


class UnwrapRequest(BaseModel):  # REQ-687
    grant: str  # base64 role-bound grant from a redirect response's `encryption` block


@router.post("/redirect/unwrap")
async def redirect_unwrap(  # REQ-687
    body: UnwrapRequest,
    raw_request: Request,
    x_provisa_role: str | None = Header(None),
):
    """Open a redirect payload's role-bound grant for an authenticated caller (REQ-687).

    Bulk results are envelope-encrypted before upload, so the S3 object is ciphertext and
    the presigned URL alone (or the bucket admin) cannot read it. The client presents the
    ``grant`` it received in the redirect response; the server opens it under the master key
    (which never leaves the MasterKeyProvider), verifies the caller is the creating role —
    or holds ADMIN/SUPERADMIN — and returns the DEK the client uses to AES-256-GCM decrypt
    the downloaded blob. A grant issued to one role cannot be redeemed by another.
    """
    import base64
    import json

    from cryptography.exceptions import InvalidTag

    from provisa.api.app import state
    from provisa.encryption import EnvelopeEncryption, encryption_service
    from provisa.security.mutation_authz import Capability

    auth_role = getattr(raw_request.state, "role", None)
    role_id = auth_role or x_provisa_role
    if not role_id or role_id not in state.contexts:
        raise HTTPException(status_code=403, detail="No accessible schema for role")
    svc = encryption_service()
    if not isinstance(svc, EnvelopeEncryption):
        raise HTTPException(status_code=409, detail="no envelope encryption provider configured")
    try:
        grant_bytes = base64.b64decode(body.grant, validate=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid grant: {exc}")
    try:
        payload = json.loads(svc.decrypt(grant_bytes))
    except (ValueError, InvalidTag):
        raise HTTPException(status_code=400, detail="grant could not be opened")
    role = state.roles.get(role_id)
    caps = (role or {}).get("capabilities") or []
    is_admin = Capability.ADMIN.value in caps or Capability.SUPERADMIN.value in caps
    if payload.get("role") != role_id and not is_admin:
        raise HTTPException(status_code=403, detail="grant is scoped to another role")
    return JSONResponse({"dek": payload["dek"]})

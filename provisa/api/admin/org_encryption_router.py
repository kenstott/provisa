# Copyright (c) 2026 Kenneth Stott
# Canary: 8687838e-dd39-40ab-a7af-9ab44aebe0e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1574: the acting org's encryption key -- set it, rotate it, never read it.

Two verbs and no third. GET reports the key's FINGERPRINT, id and provenance; PUT sets or rotates
it. There is deliberately no endpoint that returns key material, no query parameter that reveals
it, and no delete: retiring the last key would leave every payload it wrapped unreadable, and
V1 ships no re-key pass that could make that safe (see the requirement).

Gated on ``org_settings`` because the key is the ORG's -- a platform_admin holds no data capability
anywhere (REQ-1337) and so can neither set nor rotate it.
"""

# Requirements: REQ-1574

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from pydantic import BaseModel

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError

log = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


class OrgKeyBody(BaseModel):
    # 32 raw bytes, base64. ``None`` means "generate one here", which is the path that guarantees
    # the key was never anywhere a copy could be taken from -- including the operator's clipboard.
    key_b64: str | None = None


def _admin_pool():
    from provisa.api.app import state

    assert state.admin_db is not None
    return state.admin_db


@router.get("/admin/org-encryption")
async def get_org_encryption(request: Request):
    """What may be told about the org's key: its fingerprint, id, provenance and ring depth."""
    require_org_settings(request)
    from provisa.core.org_encryption import org_key_status

    org_id = require_active_org_id(request)
    status = await org_key_status(_admin_pool(), org_id)
    if status is None:
        # Not an error: an org that has set no key of its own is served the deployment's, which is
        # the state every org starts in.
        return {"org_id": org_id, "configured": False}
    return {"org_id": org_id, "configured": True, **status.as_dict()}


@router.put("/admin/org-encryption")
async def put_org_encryption(body: OrgKeyBody, request: Request):
    """Set the org's key, or rotate it. Returns the new fingerprint -- never the key.

    Rotation is the same call: a ring already holding a key gets a new active entry and keeps the
    old one, so what was written under the old key still reads. It is not re-encryption.
    """
    require_org_settings(request)
    from provisa.core.org_encryption import OrgKeyError, load_org_ring, set_org_key
    from provisa.encryption.runtime import set_org_encryption

    org_id = require_active_org_id(request)
    identity = getattr(request.state, "identity", None)
    actor = getattr(identity, "user_id", None)
    try:
        status = await set_org_key(_admin_pool(), org_id, key_b64=body.key_b64, actor=actor)
    except OrgKeyError as exc:
        raise ApiError(400, "org_encryption.invalid_key", str(exc), org=org_id) from exc

    # Rebind the live ring in the same breath: the next encrypted write in this process must use
    # the key that was just made active, and a ring left stale would keep writing under the old one.
    set_org_encryption(org_id, await load_org_ring(_admin_pool(), org_id))
    log.info(
        "org %s encryption key %s active (fingerprint %s)",
        org_id,
        status.key_id,
        status.fingerprint,
    )
    return {"org_id": org_id, "configured": True, **status.as_dict()}

# Copyright (c) 2026 Kenneth Stott
# Canary: 2744cba8-2af6-451e-be81-f7adca784463
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's mail platforms, as its administrator sets them (REQ-1923).

``/admin/orgs/{org_id}/mail-platforms`` is the organisation's own: it is opened by
``org_settings`` held in THAT organisation, the right its vault answers to, and by nothing above
it -- not ``platform_settings``, which governs the mail the deployment sends. An administrator
enters a platform's client once; the answer says whether one is entered and gives the address
to register with the platform, ready to copy. The client's secret goes to the organisation's
vault and is never answered.
"""

# Requirements: REQ-1923
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel

import provisa.google_workspace  # noqa: F401  (declares the Google Workspace platform)
from provisa.api.admin.secrets_router import _admin_pool, _drop, _org_guard
from provisa.api.errors import ApiError
from provisa.core import mail_platforms, secrets_store, source_sign_in
from provisa.core.mail_platforms import MailPlatformRefused
from provisa.core.source_sign_in import SignInRefused

router = APIRouter(prefix="/admin/orgs/{org_id}/mail-platforms", tags=["admin"])


class PlatformBody(BaseModel):
    client_id: str
    #: Left out to keep the secret already entered.
    client_secret: str | None = None
    settings: dict[str, str] = {}


def _refusal(refused: MailPlatformRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


def _redirect() -> dict:
    """The address a platform is told to return a sign-in to, or why it cannot be stated."""
    from provisa.api.admin.source_sign_in_router import _public_address

    try:
        return {"redirect_address": source_sign_in.redirect_address(_public_address())}
    except SignInRefused as refused:
        return {"redirect_address": None, "redirect_problem": refused.code}


async def _entry(org_id: str, platform: mail_platforms.Platform) -> dict:
    configured = await mail_platforms.read(_admin_pool(), org_id, platform.id)
    return {
        "platform": platform.id,
        "settings_fields": list(platform.settings),
        "configured": configured is not None,
        "client_id": None if configured is None else configured.client_id,
        "settings": {} if configured is None else configured.settings,
    }


@router.get("")
async def list_platforms(request: Request, org_id: str) -> dict:
    """Every mail platform sources can sign in to, and which of them this organisation has
    entered a client for."""
    await _org_guard(request, org_id)
    return {
        **_redirect(),
        "platforms": [await _entry(org_id, p) for p in mail_platforms.platforms()],
    }


@router.put("/{platform}")
async def put_platform(request: Request, org_id: str, platform: str, body: PlatformBody) -> dict:
    """Enter or replace the organisation's client for one platform."""
    actor = await _org_guard(request, org_id)
    try:
        await mail_platforms.put(
            _admin_pool(),
            org_id,
            platform,
            client_id=body.client_id,
            client_secret=body.client_secret or None,
            settings=body.settings,
            actor=actor,
        )
        return await _entry(org_id, mail_platforms.platform(platform))
    except MailPlatformRefused as refused:
        raise _refusal(refused) from None


@router.delete("/{platform}")
async def delete_platform(request: Request, org_id: str, platform: str) -> dict:
    """Remove the organisation's client for one platform, and its secret from the vault. Sources
    already signed in keep their own approvals but cannot renew them until a client is entered
    again."""
    actor = await _org_guard(request, org_id)
    try:
        if await mail_platforms.read(_admin_pool(), org_id, platform) is None:
            return {"removed": False}
    except MailPlatformRefused as refused:
        raise _refusal(refused) from None
    # The secret first: if the vault refuses to let it go, the client stays whole.
    await _drop(org_id, secrets_store.ORG_OWNER, mail_platforms.secret_name(platform), actor)
    return {"removed": await mail_platforms.forget(_admin_pool(), org_id, platform)}

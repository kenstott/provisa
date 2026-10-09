# Copyright (c) 2026 Kenneth Stott
# Canary: b756f246-4511-4412-99e9-a25417604749
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Signing a source in to its issuer from the Sources form (REQ-1923).

Three calls, each open to whoever may add a source in the caller's organisation:

- ``GET  /admin/source-sign-in/status`` -- whether the organisation has entered a client for
  the kind of source, and whether the caller is one who may enter it;
- ``POST /admin/source-sign-in/start`` -- records the sign-in with the ORGANISATION's client
  (``core.mail_platforms``; none is sent by the form) and answers the address the operator's
  browser is sent to;
- ``POST /admin/source-sign-in/complete`` -- takes the issuer's answer, which the page the
  browser came back to handed to the form that started the sign-in.

No answer carries a code, a token or a secret: a completed sign-in answers the reference the
source's settings then hold. The exchange itself is ``provisa.core.source_sign_in``.
"""

# Requirements: REQ-1923
from __future__ import annotations

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

import provisa.google_workspace.sign_in  # noqa: F401  (registers the Google kind)
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.environments_router import _caller_user_id
from provisa.api.errors import ApiError
from provisa.core import mail_platforms, source_sign_in
from provisa.core.database import Database
from provisa.core.mail_platforms import MailPlatformRefused
from provisa.core.source_sign_in import SignInRefused

router = APIRouter(prefix="/admin/source-sign-in", tags=["admin", "sources"])


class StartRequest(BaseModel):
    """What the person adding a source gives. The client is the organisation's and is read
    from its mail-platform setting, never sent here."""

    source_id: str
    kind: str
    account: str
    scopes: list[str]


class CompleteRequest(BaseModel):
    state: str
    code: str | None = None
    error: str | None = None


def _admin_db() -> Database:
    from provisa.api.app import state

    assert state.admin_db is not None, "the platform control plane holds pending sign-ins"
    return state.admin_db


def _public_address() -> str | None:
    from provisa.api.app import state

    config = getattr(state, "config", None)
    return config.mail.base_url if config is not None else None


def _refusal(refused: SignInRefused) -> ApiError:
    return ApiError(refused.status, refused.code, str(refused), None, **refused.params)


def _starter(request: Request) -> str:
    """Who a sign-in is bound to. A deployment run without sign-in has one caller, whom the
    product names anonymous; a sign-in there is bound to that name like any other."""
    return _caller_user_id(request) or "anonymous"


async def _source_exists(source_id: str) -> bool:
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.core.schema_org import sources

    async with (await _get_pool()).acquire() as conn:
        found = await conn.execute_core(select(sources.c.id).where(sources.c.id == source_id))
        return found.fetchone() is not None


def _holds_org_settings(request: Request) -> bool:
    """Whether the caller may enter the organisation's client (Admin > Email)."""
    from provisa.api.admin.capabilities import has_capability_request

    return has_capability_request(request, "org_settings")


def _store_refresh_token(org_id: str, actor: str | None):
    """The refresh token goes to the vault under the lock the source's refreshes take
    (``oauth_store``), so a sign-in and a refresh never write one source's token at once."""

    async def store(source_id: str, secret_name: str, refresh_token: str) -> str:
        from provisa.api_source import oauth_store
        from provisa.core.config_loader import load_control_plane
        from provisa.core.config_location import config_path_str

        await oauth_store.store_refresh_token(
            _admin_db(),
            load_control_plane(config_path_str()).resolved_platform_url(),
            org_id,
            source_id=source_id,
            secret_name=secret_name,
            refresh_token=refresh_token,
            actor=actor,
        )
        return f"${{secret:{secret_name}}}"

    return store


@router.get("/status")
async def status(request: Request, kind: str) -> dict:
    """Whether the caller's organisation has entered a client for ``kind``, so the Sources
    form can offer the sign-in or say what is missing and who sets it."""
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import require_current_org

    try:
        configured = await mail_platforms.read(_admin_db(), require_current_org(), kind)
    except MailPlatformRefused as refused:
        raise ApiError(refused.status, refused.code, str(refused), None, **refused.params) from None
    return {"configured": configured is not None, "may_configure": _holds_org_settings(request)}


@router.post("/start")
async def start(request: Request, body: StartRequest) -> dict:
    require_capability_request(request, "source_registration")
    from provisa.api.admin.schema_common import _forget_source_secret, source_mapping_secret_name
    from provisa.core.request_context import active_env, require_current_org

    org_id, env, user_id = require_current_org(), active_env(), _starter(request)
    try:
        # The organisation's own client, and no other organisation's.
        client = await mail_platforms.require(_admin_db(), org_id, body.kind)
    except MailPlatformRefused as refused:
        raise ApiError(refused.status, refused.code, str(refused), None, **refused.params) from None
    try:
        await source_sign_in.sweep(
            _admin_db(),
            org_id=org_id,
            env=env,
            source_exists=_source_exists,
            forget_secret=_forget_source_secret,
        )
        started = await source_sign_in.start(
            _admin_db(),
            org_id=org_id,
            env=env,
            user_id=user_id,
            source_id=body.source_id.strip(),
            kind_id=body.kind,
            account=body.account.strip(),
            scopes=body.scopes,
            client_id=client.client_id,
            client_secret_name=mail_platforms.secret_name(body.kind),
            refresh_token_name=source_mapping_secret_name(
                body.source_id.strip(), "refresh_token", env
            ),
            public_address=_public_address(),
            settings=client.settings,
        )
    except SignInRefused as refused:
        raise _refusal(refused) from None
    return {
        "authorization_url": started.authorization_url,
        "expires_in": started.expires_in,
        # Where the issuer returns the browser to: the form listens for the page there.
        "return_origin": started.return_origin,
    }


@router.post("/complete")
async def complete(request: Request, body: CompleteRequest) -> dict:
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import require_current_org

    org_id = require_current_org()
    try:
        done = await source_sign_in.complete(
            _admin_db(),
            org_id=org_id,
            user_id=_starter(request),
            state=body.state,
            code=body.code,
            error=body.error,
            store_refresh_token=_store_refresh_token(org_id, _caller_user_id(request)),
        )
    except SignInRefused as refused:
        raise _refusal(refused) from None
    return {
        "source_id": done.source_id,
        "account": done.account,
        "refresh_token": done.refresh_token,
    }

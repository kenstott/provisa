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

- ``GET  /admin/source-sign-in/redirect-address`` -- the address the operator copies into their
  own client at the issuer;
- ``POST /admin/source-sign-in/start`` -- records the sign-in and answers the address the
  operator's browser is sent to;
- ``POST /admin/source-sign-in/complete`` -- takes the issuer's answer, which the page the
  browser came back to handed to the form that started the sign-in.

No answer carries a code, a token or a secret: a completed sign-in answers the references the
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
from provisa.core import source_sign_in
from provisa.core.database import Database
from provisa.core.source_sign_in import SignInRefused

router = APIRouter(prefix="/admin/source-sign-in", tags=["admin", "sources"])


class StartRequest(BaseModel):
    source_id: str
    kind: str
    account: str
    scopes: list[str]
    client_id: str
    client_secret: str


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


def _store_secret(actor: str | None):
    async def store(name: str, value: str, description: str) -> str:
        from provisa.api.admin.schema_common import _store_source_secret

        return await _store_source_secret(actor, name, value, description)

    return store


@router.get("/redirect-address")
async def redirect_address(request: Request) -> dict:
    require_capability_request(request, "source_registration")
    try:
        return {"redirect_address": source_sign_in.redirect_address(_public_address())}
    except SignInRefused as refused:
        raise _refusal(refused) from None


@router.post("/start")
async def start(request: Request, body: StartRequest) -> dict:
    require_capability_request(request, "source_registration")
    from provisa.api.admin.schema_common import _forget_source_secret, source_mapping_secret_name
    from provisa.core.request_context import active_env, require_current_org

    org_id, env, user_id = require_current_org(), active_env(), _starter(request)
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
            client_id=body.client_id.strip(),
            client_secret=body.client_secret,
            public_address=_public_address(),
            secret_names=(
                source_mapping_secret_name(body.source_id.strip(), "client_secret", env),
                source_mapping_secret_name(body.source_id.strip(), "refresh_token", env),
            ),
            store_secret=_store_secret(_caller_user_id(request)),
        )
    except SignInRefused as refused:
        raise _refusal(refused) from None
    return {"authorization_url": started.authorization_url, "expires_in": started.expires_in}


@router.post("/complete")
async def complete(request: Request, body: CompleteRequest) -> dict:
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import require_current_org

    try:
        done = await source_sign_in.complete(
            _admin_db(),
            org_id=require_current_org(),
            user_id=_starter(request),
            state=body.state,
            code=body.code,
            error=body.error,
            store_secret=_store_secret(_caller_user_id(request)),
        )
    except SignInRefused as refused:
        raise _refusal(refused) from None
    return {
        "source_id": done.source_id,
        "account": done.account,
        "client_secret": done.client_secret,
        "refresh_token": done.refresh_token,
    }

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
  browser is sent to. A kind whose client is the source's own (QuickBooks, REQ-2197) is sent
  that client by the form, as the source's settings hold it, and reads no organisation entry;
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

from typing import Any

import provisa.google_workspace.sign_in  # noqa: F401  (registers the Google kind)
import provisa.microsoft365.sign_in  # noqa: F401  (registers the Microsoft kind)
import provisa.quickbooks.sign_in  # noqa: F401  (registers the QuickBooks kind)
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.environments_router import _caller_user_id
from provisa.api.errors import ApiError
from provisa.core import mail_platforms, source_sign_in
from provisa.core.database import Database
from provisa.core.mail_platforms import MailPlatformRefused
from provisa.core.source_sign_in import SignInRefused

router = APIRouter(prefix="/admin/source-sign-in", tags=["admin", "sources"])


class SourceClient(BaseModel):
    """REQ-2197: a source's own client, as its settings hold it: each a value or a
    ``${secret:...}`` reference, resolved in the organisation's vault."""

    client_id: str
    client_secret: str

    def __repr__(self) -> str:  # a client is never printed with its secret
        return f"SourceClient(client_id={self.client_id!r})"


class StartRequest(BaseModel):
    """What the person adding a source gives. The client is the organisation's and is read
    from its mail-platform setting, never sent here -- except for a kind whose client is the
    source's own (``client``)."""

    source_id: str
    kind: str
    account: str
    #: REQ-1994: the source's settings as the form states them; the server asks for the scopes
    #: of what they read (the source kind's ``scopes``), never a list the form makes.
    mapping: dict[str, Any]
    client: SourceClient | None = None


class CompleteRequest(BaseModel):
    state: str
    code: str | None = None
    error: str | None = None
    #: REQ-2197: the company Intuit's return address names (its ``realmId``).
    realm_id: str | None = None


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


def _state_db():
    """The organisation's state store, where an owner's approval is kept beside what the
    source's reads reached."""
    from provisa.api.app import state

    assert state.tenant_db is not None, "a running deployment has its organisation's state store"
    return state.tenant_db


def _keep_grant(org_id: str):
    """The approval goes to the organisation's state store under the lock the source's renewals
    take (``oauth_store``), so a sign-in and a renewal never write one source's at once."""

    async def keep(source_id: str, account: str, refresh_token: str, scopes: list[str]) -> None:
        from provisa.api_source import oauth_store
        from provisa.core.config_loader import load_control_plane
        from provisa.core.config_location import config_path_str

        await oauth_store.store_refresh_token(
            _state_db(),
            load_control_plane(config_path_str()).resolved_platform_url(),
            org_id,
            source_id=source_id,
            account=account,
            refresh_token=refresh_token,
            scopes=scopes,
        )

    return keep


async def _forget_grant(source_id: str, account: str) -> None:
    from provisa.federation import mail_sync_state

    async with _state_db().acquire() as conn:
        await mail_sync_state.forget_grant(conn, source_id, account)


def _holds_org_settings(request: Request) -> bool:
    """Whether the caller may enter the organisation's client (Admin > Email)."""
    from provisa.api.admin.capabilities import has_capability_request

    return has_capability_request(request, "org_settings")


@router.get("/status")
async def status(
    request: Request, kind: str, source_id: str | None = None, account: str | None = None
) -> dict:
    """Whether the caller's organisation has entered a client for ``kind``, so the Sources
    form can offer the sign-in or say what is missing and who sets it. With ``source_id`` and
    ``account``, also whether that mailbox's owner has signed in, and when: the form shows it
    as "Connected as ... on ..." and never holds the approval itself."""
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import require_current_org

    try:
        configured = await mail_platforms.read(_admin_db(), require_current_org(), kind)
    except MailPlatformRefused as refused:
        raise ApiError(refused.status, refused.code, str(refused), None, **refused.params) from None
    connected_at = None
    if source_id and account:
        from provisa.federation import mail_sync_state

        async with _state_db().acquire() as conn:
            connected_at = await mail_sync_state.granted_at(conn, source_id.strip(), account)
    return {
        "connected": connected_at is not None,
        "connected_at": None if connected_at is None else connected_at.isoformat(),
        # A client to sign in with; an entry may stand without one (organisation mailboxes).
        "configured": configured is not None and configured.client_id is not None,
        "may_configure": _holds_org_settings(request),
        # Whether the administrator allows sources that read the organisation's mailboxes.
        "organisation_mailboxes": configured is not None and configured.organisation_mailboxes,
    }


async def _resolved(entered: str) -> str:
    """A value of a source's settings, resolved in the vault of the caller's organisation."""
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_store import bound_to_request_org

    async with bound_to_request_org():
        return resolve_secrets(entered)


async def _client(org_id: str, body: StartRequest) -> tuple[str, str | None, dict[str, str]]:
    """The client a sign-in is started with: its id, its secret as entered where it is the
    source's own, and what the issuer's addresses are made from."""
    kind = source_sign_in.KINDS.get(body.kind)
    if kind is not None and kind.client_of_source:
        given = body.client
        if given is None or not (given.client_id.strip() and given.client_secret.strip()):
            raise SignInRefused(
                "client_missing", "Enter the app's client id and client secret, then sign in"
            )
        # The id is sent to the issuer, so it is resolved now; the secret is resolved only
        # when the code is exchanged.
        client_id = await _resolved(given.client_id.strip())
        return client_id, given.client_secret.strip(), {}
    if body.client is not None:
        raise SignInRefused(
            "client_not_taken", "This kind of source signs in with the organisation's client"
        )
    # The organisation's own client, and no other organisation's.
    entry = await mail_platforms.require(_admin_db(), org_id, body.kind)
    return entry.client, None, entry.settings


@router.post("/start")
async def start(request: Request, body: StartRequest) -> dict:
    require_capability_request(request, "source_registration")
    from provisa.core.request_context import active_env, require_current_org

    org_id, env, user_id = require_current_org(), active_env(), _starter(request)
    try:
        client_id, client_secret, settings = await _client(org_id, body)
    except MailPlatformRefused as refused:
        raise ApiError(refused.status, refused.code, str(refused), None, **refused.params) from None
    except SignInRefused as refused:
        raise _refusal(refused) from None
    try:
        await source_sign_in.sweep(
            _admin_db(),
            org_id=org_id,
            env=env,
            source_exists=_source_exists,
            forget_grant=_forget_grant,
        )
        started = await source_sign_in.start(
            _admin_db(),
            org_id=org_id,
            env=env,
            user_id=user_id,
            source_id=body.source_id.strip(),
            kind_id=body.kind,
            account=body.account.strip(),
            mapping=body.mapping,
            client_id=client_id,
            public_address=_public_address(),
            settings=settings,
            # Only a source's own client is handed over; an organisation's secret is read
            # from its entry when the code is exchanged.
            **({"client_secret": client_secret} if client_secret else {}),
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
            keep_grant=_keep_grant(org_id),
            returned={"realmId": body.realm_id},
        )
    except SignInRefused as refused:
        raise _refusal(refused) from None
    return {
        "source_id": done.source_id,
        "account": done.account,
        # The approval is sign-in state, kept for the source's reads; it is in no answer.
        "connected": True,
    }

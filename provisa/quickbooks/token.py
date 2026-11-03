# Copyright (c) 2026 Kenneth Stott
# Canary: e8a41c27-3f60-4b95-8d1e-7c2a90f5b318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The access token a QuickBooks source is read with (REQ-2197).

Intuit answers every refresh with a refresh token -- the same one, or a new one that replaces it
-- and the one replaced stops working. So the source's approval is kept where a Microsoft 365
source's is (``api_source.oauth_store``): in the organisation's state store under the company's
realm id, one refresh of the source at a time, the replacement written before the access token
is used. The approval entered on the form is read when the store holds none, and kept there; a
save of the source that changes it replaces the one kept (:func:`entered_on_save`), so an owner
who enters a new approval is read with it. A save that leaves it as it was keeps the kept one,
which Intuit has replaced since.

Intuit takes the app's credentials as HTTP Basic authentication (RFC 6749 section 2.3.1). No
token, secret or code is part of a refusal, a log record or an answer.
"""

# Requirements: REQ-2197
from __future__ import annotations

import httpx

from provisa.api_source.oauth_grants import CredentialRefused
from provisa.api_source.oauth_store import Grant
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from provisa.core.auth_models import ApiAuthOAuth2RefreshToken
from provisa.core.secrets import resolve_secrets
from provisa.quickbooks.settings import ACCOUNTING_SCOPE

if TYPE_CHECKING:
    from provisa.core.database import Database

_TIMEOUT = 30.0
# RFC 6749 section 5.2: the statuses a token endpoint refuses a grant with.
_REFUSED = frozenset({400, 401})


def _refusal(resp: httpx.Response) -> str:
    """Intuit's name for its refusal, and nothing else it said."""
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    named = body.get("error") if isinstance(body, dict) else None
    return named if isinstance(named, str) and named.isidentifier() else f"HTTP {resp.status_code}"


def exchange(auth: ApiAuthOAuth2RefreshToken, refresh_token: str) -> Grant:
    """One exchange of ``refresh_token`` at Intuit for the app ``auth`` names. Blocking: run off
    the event loop. What Intuit answers is the caller's to keep."""
    client_id = resolve_secrets(auth.client_id)
    resp = httpx.post(
        auth.token_url,
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        auth=(client_id, resolve_secrets(auth.client_secret or "")),
        headers={"Accept": "application/json"},
        timeout=_TIMEOUT,
    )
    if resp.status_code in _REFUSED:
        raise CredentialRefused(f"The QuickBooks approval for app {client_id}", _refusal(resp))
    resp.raise_for_status()
    body = resp.json()
    return Grant(
        access_token=body["access_token"],
        expires_in=float(body["expires_in"]) if "expires_in" in body else None,
        refresh_token=body.get("refresh_token"),
    )


def _store() -> tuple["Database", str, str]:
    """Where the request's organisation keeps its approvals: its state store, the platform's
    address (whose lock a refresh takes), and the organisation."""
    from provisa.api.app import state
    from provisa.core.config_loader import load_control_plane
    from provisa.core.config_location import config_path_str
    from provisa.core.request_context import require_current_org

    assert state.tenant_db is not None, "a running deployment has its organisation's state store"
    return (
        state.tenant_db,
        load_control_plane(config_path_str()).resolved_platform_url(),
        require_current_org(),
    )


async def access_token(source_id: str, realm_id: str, auth: ApiAuthOAuth2RefreshToken) -> str:
    """An access token for ``source_id``'s company ``realm_id``, from the approval kept in the
    organisation's state store (the one entered on the form where none is kept yet)."""
    from provisa.api_source import oauth_store

    async def entered() -> str | None:
        return resolve_secrets(auth.refresh_token) if auth.refresh_token else None

    return await oauth_store.stored_access_token(
        *_store(),
        source_id=source_id,
        account=realm_id,
        exchange=lambda refresh_token: exchange(auth, refresh_token),
        replaces=True,  # Intuit answers every refresh with the refresh token to send next
        earlier=entered,
    )


def forget(source_id: str, realm_id: str) -> None:
    """Drop the access token this process holds for the source: QuickBooks refused it."""
    from provisa.api_source import oauth_store
    from provisa.core.request_context import require_current_org

    oauth_store.forget(require_current_org(), source_id, realm_id)


async def keep_entered(
    state_db: "Database",
    platform_url: str,
    org_id: str,
    *,
    source_id: str,
    realm_id: str,
    entered: str | None,
    before: str | None,
) -> bool:
    """Keep ``entered``, the refresh token a save of the source carries, in place of the one
    kept for company ``realm_id`` -- when it is not ``before``, the one the source carried until
    the save. Under the lock a refresh takes; the access token held under the one replaced is
    dropped. Answers whether it was kept."""
    from provisa.api_source import oauth_store

    if not entered or entered == before:
        return False
    await oauth_store.store_refresh_token(
        state_db,
        platform_url,
        org_id,
        source_id=source_id,
        account=realm_id,
        refresh_token=entered,
        scopes=[ACCOUNTING_SCOPE],
    )
    return True


async def _resolved(value: str) -> str:
    """A value of the source's settings, resolved in the vault of the request's organisation."""
    from provisa.core.secrets_store import bound_to_request_org

    async with bound_to_request_org():
        return resolve_secrets(value)


async def entered_on_save(
    source_id: str,
    *,
    database: str | None,
    before_mapping: Mapping[str, Any] | None,
    mapping: Mapping[str, Any] | None,
) -> None:
    """A saved QuickBooks source's refresh token, where the save changed it as entered, made the
    one its company (``database``, the realm id) is read with from now on."""
    entered = (mapping or {}).get("refresh_token") or None
    before = (before_mapping or {}).get("refresh_token") or None
    if not entered or entered == before or not database:
        return
    await keep_entered(
        *_store(),
        source_id=source_id,
        realm_id=await _resolved(database.strip()),
        entered=await _resolved(str(entered)),
        before=await _resolved(str(before)) if before else None,
    )

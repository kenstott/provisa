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
is used. The approval entered on the form is only the first: read once, when the store holds
none, and kept there.

Intuit takes the app's credentials as HTTP Basic authentication (RFC 6749 section 2.3.1). No
token, secret or code is part of a refusal, a log record or an answer.
"""

# Requirements: REQ-2197
from __future__ import annotations

import httpx

from provisa.api_source.oauth_grants import CredentialRefused
from provisa.api_source.oauth_store import Grant
from provisa.core.auth_models import ApiAuthOAuth2RefreshToken
from provisa.core.secrets import resolve_secrets

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


async def access_token(source_id: str, realm_id: str, auth: ApiAuthOAuth2RefreshToken) -> str:
    """An access token for ``source_id``'s company ``realm_id``, from the approval kept in the
    organisation's state store (the one entered on the form where none is kept yet)."""
    from provisa.api.app import state
    from provisa.api_source import oauth_store
    from provisa.core.config_loader import load_control_plane
    from provisa.core.config_location import config_path_str
    from provisa.core.request_context import require_current_org

    async def entered() -> str | None:
        return resolve_secrets(auth.refresh_token) if auth.refresh_token else None

    assert state.tenant_db is not None, "a running deployment has its organisation's state store"
    return await oauth_store.stored_access_token(
        state.tenant_db,
        load_control_plane(config_path_str()).resolved_platform_url(),
        require_current_org(),
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

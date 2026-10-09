# Copyright (c) 2026 Kenneth Stott
# Canary: bf2b0a0d-b830-4a38-a839-5e9e615c3202
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Access tokens for the delegated credentials an API source may carry (REQ-320).

Two kinds are exchanged here for the short-lived token a call is sent with:

- an OAuth2 refresh token, a person's standing approval of a client (RFC 6749 section 6);
- a Google service account reading as a named account of a Workspace domain that has delegated
  to it: an assertion the account's key signs is exchanged for the token (RFC 7523).

A token is kept in this process until shortly before the issuer says it lapses, and nowhere
else. A credential the issuer refuses is a :class:`CredentialRefused` carrying the issuer's own
reason -- a lapsed or withdrawn approval is never answered as an empty read.

Some issuers answer an exchange with a new refresh token that replaces the one sent, which then
lapses. :func:`exchange_refresh_token` returns that replacement to its caller and keeps nothing
itself, so one writer can store it where the source's credential is kept before any token is
used. :func:`access_token`, which a plain API call is made with, keeps nothing either: there a
replacement is refused by :class:`ReplacementNotKept`, since a source left holding the old token
stops when it lapses.
"""

# Requirements: REQ-320
from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass

import httpx
import jwt

from provisa.api_source.caller import ApiCallError
from provisa.core.auth_models import ApiAuthGoogleServiceAccount, ApiAuthOAuth2RefreshToken
from provisa.core.secrets import resolve_secrets

_TIMEOUT = 10.0
_EXPIRY_MARGIN = 30.0  # a token this close to lapsing is not sent
_ASSERTION_LIFETIME = 3600  # the longest Google accepts for an assertion
_JWT_BEARER = "urn:ietf:params:oauth:grant-type:jwt-bearer"
# RFC 6749 section 5.2: the statuses a token endpoint refuses a grant with.
_REFUSED = frozenset({400, 401})
_KEY_FIELDS = ("client_email", "private_key", "token_uri")

#: Tokens this process holds: digest of what identifies the credential -> (token, lapses at).
_tokens: dict[str, tuple[str, float]] = {}
_tokens_lock = threading.Lock()


class CredentialRefused(ApiCallError):
    """The issuer refused to exchange a source's credential for an access token."""

    def __init__(self, credential: str, reason: str) -> None:
        self.credential = credential
        self.reason = reason
        super().__init__(f"{credential} was refused by its issuer: {reason}")


class ReplacementNotKept(ApiCallError):
    """The issuer replaced the refresh token and the caller keeps no replacement."""

    def __init__(self, credential: str) -> None:
        super().__init__(
            f"{credential}: its issuer replaces the refresh token at each use, and this source "
            "does not keep the replacement, so it would stop when the token it holds lapses"
        )


class ServiceAccountKeyInvalid(ApiCallError):
    """A service account key that is not one: not JSON, or without a part a key has."""


@dataclass(frozen=True)
class RefreshGrant:
    """What an issuer answered an exchange of a refresh token with."""

    access_token: str
    expires_in: float | None  # seconds; None when the issuer states no life
    # The refresh token the issuer answered with in place of the one sent; None when it sent
    # none, or the same one.
    replacement: str | None


def access_token(auth: ApiAuthOAuth2RefreshToken | ApiAuthGoogleServiceAccount) -> str:
    """The access token a call under ``auth`` is sent with, from this process's hold when it
    has one that has not lapsed, else from the issuer."""
    if isinstance(auth, ApiAuthGoogleServiceAccount):
        return _service_account_grant(auth)
    key = _held_key(auth.type, auth.token_url, auth.client_id, auth.refresh_token, auth.scope or "")
    held = _held(key)
    if held is not None:
        return held
    grant = exchange_refresh_token(auth)
    if grant.replacement is not None:
        raise ReplacementNotKept(_approval(resolve_secrets(auth.client_id)))
    _hold(key, grant.access_token, grant.expires_in)
    return grant.access_token


def exchange_refresh_token(auth: ApiAuthOAuth2RefreshToken) -> RefreshGrant:
    """One exchange of exactly the refresh token ``auth`` states. Nothing is held and nothing
    is stored: what the issuer answered is the caller's to keep."""
    client_id = resolve_secrets(auth.client_id)
    refresh_token = resolve_secrets(auth.refresh_token)
    form = {"grant_type": "refresh_token", "client_id": client_id, "refresh_token": refresh_token}
    if auth.client_secret is not None:
        form["client_secret"] = resolve_secrets(auth.client_secret)
    if auth.scope:
        form["scope"] = auth.scope
    body = _exchange(auth.token_url, form, _approval(client_id))
    answered = body.get("refresh_token")
    return RefreshGrant(
        access_token=body["access_token"],
        expires_in=float(body["expires_in"]) if "expires_in" in body else None,
        replacement=answered if answered and answered != refresh_token else None,
    )


def _approval(client_id: str) -> str:
    return f"The sign-in approved for client {client_id}"


def _held_key(*identity: str) -> str:
    # What identifies a credential may itself be the credential, so only its digest is kept.
    return hashlib.sha256("\x00".join(identity).encode()).hexdigest()


def _held(key: str) -> str | None:
    with _tokens_lock:
        held = _tokens.get(key)
    if held is not None and time.time() < held[1] - _EXPIRY_MARGIN:
        return held[0]
    return None


def _refusal(resp: httpx.Response) -> str:
    """The issuer's reason for refusing a grant: its ``error`` and ``error_description`` (RFC
    6749 section 5.2) where it answered with them, else the status alone."""
    try:
        body = resp.json()
    except ValueError:
        return f"HTTP {resp.status_code}"
    if not isinstance(body, dict) or "error" not in body:
        return f"HTTP {resp.status_code}"
    described = body.get("error_description")
    return f"{body['error']}: {described}" if described else str(body["error"])


def _hold(key: str, token: str, expires_in: float | None) -> None:
    # An issuer that does not say when the token lapses gives nothing to hold it by: it is
    # used for this call and asked for again at the next.
    if expires_in is not None:
        with _tokens_lock:
            _tokens[key] = (token, time.time() + expires_in)


def _exchange(token_url: str, form: dict[str, str], credential: str) -> dict:
    """The issuer's answer to a grant, or its refusal."""
    resp = httpx.post(token_url, data=form, timeout=_TIMEOUT)
    if resp.status_code in _REFUSED:
        raise CredentialRefused(credential, _refusal(resp))
    resp.raise_for_status()
    return resp.json()


def _service_account_key(auth: ApiAuthGoogleServiceAccount) -> dict:
    try:
        key = json.loads(resolve_secrets(auth.key))
    except ValueError:
        # The text is the credential, so the parser's message, which quotes it, is not passed on.
        raise ServiceAccountKeyInvalid("The service account key is not JSON") from None
    if not isinstance(key, dict):
        raise ServiceAccountKeyInvalid("The service account key is not a JSON object")
    missing = [name for name in _KEY_FIELDS if not key.get(name)]
    if missing:
        raise ServiceAccountKeyInvalid(f"The service account key has no {', '.join(missing)}")
    return key


def _service_account_grant(auth: ApiAuthGoogleServiceAccount) -> str:
    held_key = _held_key(auth.type, auth.key, auth.subject, " ".join(sorted(auth.scopes)))
    held = _held(held_key)
    if held is not None:
        return held
    key = _service_account_key(auth)
    now = int(time.time())
    claims = {
        "iss": key["client_email"],
        "sub": auth.subject,
        "scope": " ".join(auth.scopes),
        "aud": key["token_uri"],
        "iat": now,
        "exp": now + _ASSERTION_LIFETIME,
    }
    try:
        assertion = jwt.encode(claims, key["private_key"], algorithm="RS256")
    except (ValueError, TypeError, jwt.PyJWTError):
        # The key is the credential, so the signer's message is not passed on.
        raise ServiceAccountKeyInvalid(
            "The service account key's private_key is not a key an assertion can be signed with"
        ) from None
    body = _exchange(
        key["token_uri"],
        {"grant_type": _JWT_BEARER, "assertion": assertion},
        f"Service account {key['client_email']} reading as {auth.subject}",
    )
    _hold(
        held_key, body["access_token"], float(body["expires_in"]) if "expires_in" in body else None
    )
    return body["access_token"]

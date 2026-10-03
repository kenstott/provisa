# Copyright (c) 2026 Kenneth Stott
# Canary: c0cafd1c-31d1-4d2f-833e-b012631f5f2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The browser session token of a provider whose own credential is not a bearer token.

An LDAP password and a SAML assertion are each presented once. The browser then holds a
short-lived token signed with ``auth.jwt_secret``, which names the account and nothing else:
the provider re-reads what it needs when the token comes back.
"""

from __future__ import annotations

import datetime

import jwt

from provisa.api.errors import ApiError

# Requirements: REQ-1265

# Short because the token is a bearer credential held by the browser; it signs in again when
# the token expires.
SESSION_TTL = datetime.timedelta(hours=8)

_ALGORITHM = "HS256"


class SessionTokens:  # REQ-1265
    """Mints and verifies one provider's session tokens.

    ``audience`` is the provider's name. It keeps a token minted under one provider from being
    accepted by another that shares the signing secret after a provider change.
    """

    def __init__(self, secret: str | None, audience: str) -> None:
        # None when auth.jwt_secret is unset. Issuing then fails with a 503 and verifying
        # refuses every token: there is never a default signing key.
        self._secret = secret
        self._audience = audience

    def issue(self, claims: dict) -> str:
        if not self._secret:
            raise ApiError(
                503,
                "auth.session_secret_missing",
                f"auth.jwt_secret is required to issue browser sessions for provider "
                f"{self._audience!r}",
            )
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {**claims, "aud": self._audience, "iat": now, "exp": now + SESSION_TTL}
        return jwt.encode(payload, self._secret, algorithm=_ALGORITHM)

    def verify(self, token: str) -> dict:
        if not self._secret:
            raise ValueError("Session tokens are not configured")
        return jwt.decode(
            token,
            self._secret,
            algorithms=[_ALGORITHM],
            audience=self._audience,
            options={"require": ["exp", "aud", "sub"]},
        )

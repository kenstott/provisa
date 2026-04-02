# Copyright (c) 2025 Kenneth Stott
# Canary: 152cabb2-0359-4e32-a0d0-28b5698898e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic OIDC/OAuth 2.0 JWT validation provider."""

from __future__ import annotations

import time

import httpx
import jwt

from provisa.auth.models import AuthIdentity, AuthProvider


class OAuthProvider(AuthProvider):
    """Validates JWTs via generic OIDC discovery."""

    def __init__(
        self,
        discovery_url: str,
        client_id: str,
        audience: str | None = None,
        role_claim: str = "roles",
    ) -> None:
        self._discovery_url = discovery_url
        self._client_id = client_id
        self._audience = audience or client_id
        self._role_claim = role_claim
        self._jwks_client: jwt.PyJWKClient | None = None
        self._jwks_fetched_at: float = 0.0
        self._jwks_ttl: float = 3600.0
        self._jwks_uri: str | None = None

    def _fetch_jwks_uri(self) -> str:
        if self._jwks_uri is None:
            resp = httpx.get(self._discovery_url, timeout=10)
            resp.raise_for_status()
            self._jwks_uri = resp.json()["jwks_uri"]
        return self._jwks_uri

    def _get_jwks_client(self) -> jwt.PyJWKClient:
        now = time.monotonic()
        if self._jwks_client is None or (now - self._jwks_fetched_at) > self._jwks_ttl:
            uri = self._fetch_jwks_uri()
            self._jwks_client = jwt.PyJWKClient(uri)
            self._jwks_fetched_at = now
        return self._jwks_client

    async def validate_token(self, token: str) -> AuthIdentity:
        client = self._get_jwks_client()
        signing_key = client.get_signing_key_from_jwt(token)
        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=self._audience,
        )
        roles = decoded.get(self._role_claim, [])
        if isinstance(roles, str):
            roles = [roles]
        return AuthIdentity(
            user_id=decoded["sub"],
            email=decoded.get("email"),
            display_name=decoded.get("name"),
            roles=roles,
            raw_claims=decoded,
        )

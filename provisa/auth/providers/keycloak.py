# Copyright (c) 2026 Kenneth Stott
# Canary: 578b9ac4-1994-49ca-b77b-dbc56b469396
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Keycloak OIDC JWT validation provider."""

from __future__ import annotations

import time

import jwt

from provisa.auth.models import AuthIdentity, AuthProvider

# Requirements: REQ-120, REQ-122


class KeycloakAuthProvider(AuthProvider):  # REQ-120, REQ-122
    """Validates JWTs issued by Keycloak via JWKS."""

    provider_name: str = "keycloak"

    def __init__(
        self,
        server_url: str,
        realm: str,
        client_id: str,
        client_secret: str | None = None,
    ) -> None:
        self._server_url = server_url.rstrip("/")
        self._realm = realm
        self._client_id = client_id
        self._client_secret = client_secret
        self._jwks_uri = f"{self._server_url}/realms/{realm}/protocol/openid-connect/certs"
        self._jwks_client: jwt.PyJWKClient | None = None
        self._jwks_fetched_at: float = 0.0
        self._jwks_ttl: float = 3600.0

    def _get_jwks_client(self) -> jwt.PyJWKClient:
        now = time.monotonic()
        if self._jwks_client is None or (now - self._jwks_fetched_at) > self._jwks_ttl:
            self._jwks_client = jwt.PyJWKClient(self._jwks_uri)
            self._jwks_fetched_at = now
        return self._jwks_client

    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-120, REQ-122
        client = self._get_jwks_client()
        signing_key = client.get_signing_key_from_jwt(token)
        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=self._client_id,
        )
        # REQ-122: map both realm roles (realm_access.roles) and this client's
        # roles (resource_access.<client_id>.roles). Realm roles first, then client
        # roles, de-duplicated with order preserved.
        realm_roles = decoded.get("realm_access", {}).get("roles", [])
        client_roles = decoded.get("resource_access", {}).get(self._client_id, {}).get("roles", [])
        seen: set[str] = set()
        roles = [r for r in [*realm_roles, *client_roles] if not (r in seen or seen.add(r))]
        return AuthIdentity(
            user_id=decoded["sub"],
            email=decoded.get("email"),
            display_name=decoded.get("preferred_username"),
            roles=roles,
            raw_claims=decoded,
        )

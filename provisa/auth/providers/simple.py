# Copyright (c) 2026 Kenneth Stott
# Canary: e7ff290d-852f-48dd-acfe-dd68a1c7a143
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Simple username/password auth with bcrypt + JWT."""

from __future__ import annotations

import base64
import datetime

import bcrypt
import jwt

from provisa.auth.models import AuthIdentity, AuthProvider

# Requirements: REQ-120, REQ-124


class SimpleAuthProvider(AuthProvider):  # REQ-120, REQ-124
    """Bcrypt password validation with JWT issuance for testing/simple deployments."""

    provider_name: str = "simple"

    def __init__(self, users: list[dict], jwt_secret: str, user_ids) -> None:
        self._users = {u["username"]: u for u in users}
        self._jwt_secret = jwt_secret
        # The stored GUID of each user (provisa/auth/simple_user_ids.py); never the username.
        self._user_ids = user_ids

    def _check_password(self, username: str, password: str) -> dict:
        user = self._users.get(username)
        if user is None or not bcrypt.checkpw(
            password.encode("utf-8"), user["password_hash"].encode("utf-8")
        ):
            raise ValueError("Invalid credentials")
        return user

    async def _identity(self, username: str, user: dict) -> AuthIdentity:
        user_id = await self._user_ids.id_for(username)
        roles = user.get("roles", [])
        return AuthIdentity(
            user_id=user_id,
            email=None,
            display_name=username,
            roles=roles,
            raw_claims={"sub": user_id, "username": username, "roles": roles},
        )

    async def login(self, username: str, password: str) -> str:  # REQ-124
        """Verify credentials and return a signed JWT naming the user's stored id."""
        user = self._check_password(username, password)
        user_id = await self._user_ids.id_for(username)
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "sub": user_id,
            "username": username,
            "roles": user.get("roles", []),
            "iat": now,
            "exp": now + datetime.timedelta(minutes=30),
        }
        return jwt.encode(payload, self._jwt_secret, algorithm="HS256")

    async def password_login(self, username: str, password: str) -> str:  # REQ-124
        """The ``POST /auth/login`` exchange (provisa/auth/login_router.py)."""
        return await self.login(username, password)

    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-120, REQ-124
        decoded = jwt.decode(
            token, self._jwt_secret, algorithms=["HS256"], options={"require": ["sub", "username"]}
        )
        return AuthIdentity(
            user_id=decoded["sub"],
            email=None,
            display_name=decoded["username"],
            roles=decoded.get("roles", []),
            raw_claims=decoded,
        )

    @property
    def token_validators(self):
        """Credential presentation → the validator that accepts it (REQ-124).

        This provider already verifies passwords to mint its JWT, so a surface that carries a
        username and password directly — Bolt's ``basic`` scheme, pgwire's startup — can be
        authenticated here without the caller first exchanging them for a token over HTTP.
        """
        return {"bearer": self.validate_credential, "basic": self.validate_basic}

    async def validate_basic(self, token: str) -> AuthIdentity:  # REQ-124
        """Validate a ``Basic`` credential — b64(username:password)."""
        try:
            username, password = base64.b64decode(token).decode("utf-8").split(":", 1)
        except (ValueError, UnicodeDecodeError):
            raise ValueError("Invalid credentials")
        return await self._identity(username, self._check_password(username, password))

# Copyright (c) 2025 Kenneth Stott
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

import datetime

import bcrypt
import jwt
from fastapi import APIRouter

from provisa.auth.models import AuthIdentity, AuthProvider

router = APIRouter(prefix="/auth", tags=["auth"])


class SimpleAuthProvider(AuthProvider):
    """Bcrypt password validation with JWT issuance for testing/simple deployments."""

    def __init__(self, users: list[dict], jwt_secret: str) -> None:
        self._users = {u["username"]: u for u in users}
        self._jwt_secret = jwt_secret

    def login(self, username: str, password: str) -> str:
        """Verify credentials and return a signed JWT."""
        user = self._users.get(username)
        if user is None:
            raise ValueError("Invalid credentials")
        if not bcrypt.checkpw(
            password.encode("utf-8"), user["password_hash"].encode("utf-8")
        ):
            raise ValueError("Invalid credentials")
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "sub": username,
            "roles": user.get("roles", []),
            "iat": now,
            "exp": now + datetime.timedelta(minutes=30),
        }
        return jwt.encode(payload, self._jwt_secret, algorithm="HS256")

    async def validate_token(self, token: str) -> AuthIdentity:
        decoded = jwt.decode(token, self._jwt_secret, algorithms=["HS256"])
        return AuthIdentity(
            user_id=decoded["sub"],
            email=None,
            display_name=decoded["sub"],
            roles=decoded.get("roles", []),
            raw_claims=decoded,
        )

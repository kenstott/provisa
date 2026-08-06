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
from fastapi import APIRouter
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity, AuthProvider

# Requirements: REQ-120, REQ-124

router = APIRouter(prefix="/auth", tags=["auth"])

# Module-level reference set by app.py when provider=simple
_provider_instance: SimpleAuthProvider | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(request: LoginRequest):  # REQ-124, REQ-1393
    """Authenticate with username/password and receive a JWT."""
    if _provider_instance is None:
        raise ApiError(
            503, "auth.simple_provider_not_configured", "Simple auth provider not configured"
        )
    from provisa.auth.throttle import LockedOut, login_attempt

    try:
        with login_attempt(request.username, request.password):
            token = _provider_instance.login(request.username, request.password)
    except LockedOut as locked:
        raise ApiError(429, "auth.too_many_attempts", str(locked))
    except ValueError as e:
        from fastapi import HTTPException

        raise HTTPException(status_code=401, detail=str(e))
    return {"access_token": token, "token_type": "bearer"}


class SimpleAuthProvider(AuthProvider):  # REQ-120, REQ-124
    """Bcrypt password validation with JWT issuance for testing/simple deployments."""

    provider_name: str = "simple"

    def __init__(self, users: list[dict], jwt_secret: str) -> None:
        self._users = {u["username"]: u for u in users}
        self._jwt_secret = jwt_secret

    def login(self, username: str, password: str) -> str:  # REQ-124
        """Verify credentials and return a signed JWT."""
        user = self._users.get(username)
        if user is None:
            raise ValueError("Invalid credentials")
        if not bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
            raise ValueError("Invalid credentials")
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "sub": username,
            "roles": user.get("roles", []),
            "iat": now,
            "exp": now + datetime.timedelta(minutes=30),
        }
        return jwt.encode(payload, self._jwt_secret, algorithm="HS256")

    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-120, REQ-124
        decoded = jwt.decode(token, self._jwt_secret, algorithms=["HS256"])
        return AuthIdentity(
            user_id=decoded["sub"],
            email=None,
            display_name=decoded["sub"],
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
        except Exception:
            raise ValueError("Invalid credentials")
        user = self._users.get(username)
        if user is None or not bcrypt.checkpw(
            password.encode("utf-8"), user["password_hash"].encode("utf-8")
        ):
            raise ValueError("Invalid credentials")
        return AuthIdentity(
            user_id=username,
            email=None,
            display_name=username,
            roles=user.get("roles", []),
            raw_claims={"sub": username, "roles": user.get("roles", [])},
        )

# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP Basic Auth provider backed by the local_users table.

Two credential presentations, both first-class:

* ``Authorization: Basic <b64(user:pass)>`` — API and CLI clients, which resend the credential
  on every call.
* ``Authorization: Bearer <jwt>`` — the browser. ``POST /auth/login`` exchanges the
  username/password for a short-lived signed session token, because the SPA keeps its
  credential in ``localStorage`` and a stored ``b64(user:pass)`` is a stored password.

The scheme in the request header selects the validator (``token_validators``); there is no
try-one-then-the-other chain, so a rejected credential is a 401 rather than a second attempt
under the other scheme.
"""

from __future__ import annotations

import base64
import datetime

import bcrypt
import jwt
from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.core.schema_admin import local_users

# Requirements: REQ-124

# The browser session's lifetime. Short because the token is a bearer credential sitting in
# localStorage; the SPA re-authenticates when it expires.
_SESSION_TTL = datetime.timedelta(hours=8)

router = APIRouter(prefix="/auth", tags=["auth"])

# Set by wiring.wire_auth when provider=basic — the same module-level handoff the simple
# provider's router uses.
_provider_instance: "BasicAuthProvider | None" = None


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(body: LoginRequest):  # REQ-124, REQ-1393
    """Exchange username+password for a session JWT."""
    if _provider_instance is None:
        raise ApiError(
            503, "auth.basic_provider_not_configured", "Basic auth provider not configured"
        )
    from provisa.auth.throttle import LockedOut, login_attempt

    try:
        with login_attempt(body.username, body.password):
            token = await _provider_instance.issue_session_token(body.username, body.password)
    except LockedOut as locked:
        raise ApiError(429, "auth.too_many_attempts", str(locked))
    except ValueError as exc:
        raise ApiError(401, "auth.invalid_credentials", str(exc))
    return {"access_token": token, "token_type": "bearer"}


class BasicAuthProvider(AuthProvider):  # REQ-124
    """Validates HTTP Basic credentials against the local_users DB table."""

    provider_name: str = "basic"

    @property
    def auth_scheme(self) -> str:
        return "basic"

    @property
    def token_validators(self):
        """Credential presentation → the validator that accepts it. Read by AuthMiddleware.

        A personal access token arrives as a bearer credential, so the bearer slot resolves
        either a PAT or a session JWT (REQ-1263). The basic slot stays username:password.
        """
        return {"basic": self.validate_token, "bearer": self.validate_bearer}

    async def validate_bearer(self, token: str) -> AuthIdentity:  # REQ-1263
        """A bearer credential: a personal access token, else this provider's session JWT."""
        return await self._with_pat(token, self.validate_session_token)

    def __init__(self, db_pool, session_secret: str | None = None) -> None:
        self._pool = db_pool
        # None when auth.jwt_secret is unset. Session issuance then fails loudly (503) rather
        # than signing with a guessable key — an unsigned or default-keyed session token is a
        # forgeable admin credential.
        self._session_secret = session_secret

    async def _lookup(self, username: str) -> dict:
        async with self._pool.acquire() as conn:
            result = await conn.execute_core(
                select(
                    local_users.c.id,
                    local_users.c.username,
                    local_users.c.password_hash,
                    local_users.c.email,
                    local_users.c.display_name,
                    local_users.c.attributes,
                ).where(
                    local_users.c.username == username,
                    local_users.c.is_active == True,  # noqa: E712
                )
            )
            fetched = result.fetchone()
        if fetched is None:
            raise ValueError("Invalid credentials")
        return dict(fetched._mapping)

    @staticmethod
    def _identity(row: dict) -> AuthIdentity:
        return AuthIdentity(
            user_id=row["id"],
            email=row["email"],
            display_name=row["display_name"],
            roles=[],
            raw_claims={"username": row["username"], **dict(row["attributes"] or {})},
        )

    async def issue_session_token(self, username: str, password: str) -> str:  # REQ-124
        """Verify a password and mint the browser's session JWT."""
        if not self._session_secret:
            raise ApiError(
                503,
                "auth.session_secret_missing",
                "auth.jwt_secret is required to issue browser sessions for provider 'basic'",
            )
        row = await self._lookup(username)
        if not bcrypt.checkpw(password.encode("utf-8"), row["password_hash"].encode("utf-8")):
            raise ValueError("Invalid credentials")
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            "sub": row["id"],
            "username": row["username"],
            "email": row["email"],
            "display_name": row["display_name"],
            "iat": now,
            "exp": now + _SESSION_TTL,
        }
        return jwt.encode(payload, self._session_secret, algorithm="HS256")

    async def validate_session_token(self, token: str) -> AuthIdentity:  # REQ-124
        """Validate a session JWT minted by ``issue_session_token``.

        The account is re-read on every request rather than trusted from the claims: a
        deactivated or deleted user must lose access at once, not when their token expires.
        """
        if not self._session_secret:
            raise ValueError("Session tokens are not configured")
        decoded = jwt.decode(token, self._session_secret, algorithms=["HS256"])
        row = await self._lookup(decoded["username"])
        return self._identity(row)

    async def identity_for(self, username: str) -> AuthIdentity:  # REQ-1394
        """The identity of an account whose password was proven without being transmitted.

        SCRAM leaves the server holding a proof rather than a credential, so pgwire's SASL path has
        nothing to hand :meth:`validate_token`. The account is still re-read here, which is what
        makes a deactivated user refused after a valid proof exactly as after a valid password.
        """
        return self._identity(await self._lookup(username))

    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-124
        """Validate an ``Authorization: Basic`` credential — b64(username:password)."""
        try:
            decoded = base64.b64decode(token).decode("utf-8")
            username, password = decoded.split(":", 1)
        except Exception:
            raise ValueError("Invalid credentials")

        row = await self._lookup(username)
        if not bcrypt.checkpw(password.encode("utf-8"), row["password_hash"].encode("utf-8")):
            raise ValueError("Invalid credentials")
        return self._identity(row)

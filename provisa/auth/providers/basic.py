# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""HTTP Basic Auth provider backed by the local_users table."""

from __future__ import annotations

import base64

import bcrypt
from sqlalchemy import select

from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.core.schema_admin import local_users

# Requirements: REQ-124


class BasicAuthProvider(AuthProvider):  # REQ-124
    """Validates HTTP Basic credentials against the local_users DB table."""

    provider_name: str = "basic"

    @property
    def auth_scheme(self) -> str:
        return "basic"

    def __init__(self, db_pool) -> None:
        self._pool = db_pool

    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-124
        try:
            decoded = base64.b64decode(token).decode("utf-8")
            username, password = decoded.split(":", 1)
        except Exception:
            raise ValueError("Invalid credentials")

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
            row = dict(fetched._mapping)

            if not bcrypt.checkpw(password.encode("utf-8"), row["password_hash"].encode("utf-8")):
                raise ValueError("Invalid credentials")

        return AuthIdentity(
            user_id=row["id"],
            email=row["email"],
            display_name=row["display_name"],
            roles=[],
            raw_claims={"username": row["username"], **dict(row["attributes"] or {})},
        )

# Copyright (c) 2026 Kenneth Stott
# Canary: 834b8d50-ceef-4cb9-bd83-1bb26d4b836b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``POST /auth/login``: exchange a username and password for a session token.

One route for every provider that signs a user in with a password (basic, simple, ldap). It is
mounted by ``create_app`` whatever the configuration, and reads the deployment's auth config
at request time: the config is only known once the app has started, and it can change at run
time (setup wizard, REQ-1267). A provider that takes no password, or a deployment with no
auth, has no such exchange, which is a 404.
"""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from provisa.api.errors import ApiError

# Requirements: REQ-124, REQ-1265, REQ-1393

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(body: LoginRequest):  # REQ-124, REQ-1265, REQ-1393
    from provisa.api.app import state
    from provisa.auth.throttle import LockedOut, login_attempt
    from provisa.auth.wiring import build_auth_provider

    auth_config = state.auth_config
    if auth_config is None:
        raise ApiError(404, "auth.no_password_sign_in", "This deployment has no sign-in")
    provider = build_auth_provider(auth_config, admin_pool=state.admin_db)
    password_login = getattr(provider, "password_login", None)
    if password_login is None:
        raise ApiError(
            404,
            "auth.no_password_sign_in",
            f"Auth provider {provider.provider_name!r} does not sign in with a password",
        )
    try:
        with login_attempt(body.username, body.password):
            token = await password_login(body.username, body.password)
    except LockedOut as locked:
        raise ApiError(429, "auth.too_many_attempts", str(locked))
    except ValueError as exc:
        raise ApiError(401, "auth.invalid_credentials", str(exc))
    return {"access_token": token, "token_type": "bearer"}

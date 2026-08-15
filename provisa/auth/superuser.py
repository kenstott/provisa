# Copyright (c) 2026 Kenneth Stott
# Canary: cc35b2df-5a26-40da-95a9-9955eea18b4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Superuser check — always gets admin role + all capabilities."""

from __future__ import annotations

import datetime
import hmac

import jwt

from provisa.api.errors import ApiError
from provisa.auth.models import AuthIdentity
from provisa.security.rights import ORG_ADMIN_ROLE, PLATFORM_ADMIN_ROLE
from provisa.core.secrets import resolve_secrets

# Requirements: REQ-125, REQ-1472

# Same lifetime as the basic provider's browser session (providers/basic.py): the token is a
# bearer credential the SPA keeps in localStorage.
_SESSION_TTL = datetime.timedelta(hours=8)
# Marks a token as this deployment's break-glass session. Checked on validate so a JWT signed
# with the same auth.jwt_secret by another issuer (the basic provider's user session) can never
# be presented as the superuser.
_SESSION_CLAIM = "provisa.superuser"


def resolve_superuser_config(config: dict | None) -> dict | None:  # REQ-125
    """Resolve ``${env:...}`` references in the superuser config at startup.

    Returns ``{"username", "password"}`` with secrets resolved, or ``None`` when no
    superuser is configured. Resolution happens once at wiring time so per-request
    checks never touch the secrets backend; an unset secret raises here (fail fast at
    startup) rather than silently disabling the superuser at request time.
    """
    if not config:
        return None
    username = config.get("username")
    password = config.get("password")
    if username is None or password is None:
        return None
    return {"username": resolve_secrets(username), "password": resolve_secrets(password)}


def check_superuser(  # REQ-125
    username: str, password: str, config: dict
) -> AuthIdentity | None:
    """Return an admin AuthIdentity if credentials match the (resolved) superuser config.

    ``config`` is expected to already be resolved via :func:`resolve_superuser_config`.
    A blank configured username or password never matches, so an empty secret cannot
    authenticate.

    Both comparisons are constant-time: the superuser is the platform break-glass account, so
    a timing side channel on its password is a remote credential-recovery oracle.
    """
    su_user = config.get("username")
    su_pass = config.get("password")
    if not su_user or not su_pass:
        return None
    user_ok = hmac.compare_digest(username.encode("utf-8"), su_user.encode("utf-8"))
    pass_ok = hmac.compare_digest(password.encode("utf-8"), su_pass.encode("utf-8"))
    if user_ok and pass_ok:
        return AuthIdentity(
            user_id=su_user,
            email=None,
            display_name="Superuser",
            # Both planes: platform_admin administers the deployment, org_admin is what lets it
            # read a table (REQ-1327 generates no data schema for a control-plane role). The
            # middleware overwrites this list from the assignments it builds; it is set here so a
            # caller using check_superuser directly gets the same two.
            roles=[PLATFORM_ADMIN_ROLE, ORG_ADMIN_ROLE],
            raw_claims={"superuser": True},
        )
    return None


def issue_superuser_session(  # REQ-1472
    username: str, password: str, config: dict, secret: str | None
) -> str:
    """Exchange the break-glass credentials for a session JWT the browser can hold.

    The Basic presentation (middleware.py) is the API/CLI path; a browser signing in on a
    deployment whose provider is an IdP has no way to present it, which left the operator
    account unusable from the login page. This is the same exchange ``/auth/login`` performs
    for provider ``basic``, mounted for every provider.

    Raises ``ValueError`` on a credential mismatch (the caller maps it to 401 so it is
    indistinguishable from any other bad password) and 503 when no signing key is configured —
    signing with a guessable key would make the token a forgeable admin credential.
    """
    if not secret:
        raise ApiError(
            503,
            "auth.session_secret_missing",
            "auth.jwt_secret is required to issue a superuser browser session",
        )
    if check_superuser(username, password, config) is None:
        raise ValueError("Invalid credentials")
    now = datetime.datetime.now(datetime.timezone.utc)
    return jwt.encode(
        {"sub": config["username"], "typ": _SESSION_CLAIM, "iat": now, "exp": now + _SESSION_TTL},
        secret,
        algorithm="HS256",
    )


def validate_superuser_session(  # REQ-1472
    token: str, config: dict, secret: str | None
) -> AuthIdentity | None:
    """The identity behind a superuser session JWT, or ``None`` when the token is not one.

    ``None`` is not a swallowed error: every provider that takes a bearer credential shares the
    scheme, so the middleware asks this first and hands anything that is not ours to the
    configured provider, which then decides the request. A token that IS ours but expired or
    re-signed decodes to ``None`` here and is refused by that provider as an unknown credential.
    """
    if not secret:
        return None
    try:
        decoded = jwt.decode(token, secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None
    if decoded.get("typ") != _SESSION_CLAIM:
        return None
    su_user = config.get("username")
    if not su_user or not hmac.compare_digest(str(decoded.get("sub", "")), su_user):
        return None
    return AuthIdentity(
        user_id=su_user,
        email=None,
        display_name="Superuser",
        roles=[PLATFORM_ADMIN_ROLE, ORG_ADMIN_ROLE],
        raw_claims={"superuser": True},
    )

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

from provisa.auth.models import AuthIdentity
from provisa.core.secrets import resolve_secrets

# Requirements: REQ-125


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
    """
    su_user = config.get("username")
    su_pass = config.get("password")
    if not su_user or not su_pass:
        return None
    if username == su_user and password == su_pass:
        return AuthIdentity(
            user_id=su_user,
            email=None,
            display_name="Superuser",
            roles=["admin"],
            raw_claims={"superuser": True},
        )
    return None

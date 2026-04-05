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


def check_superuser(
    username: str, password: str, config: dict
) -> AuthIdentity | None:
    """Return an admin AuthIdentity if credentials match superuser config."""
    su_user = config.get("username")
    su_pass = config.get("password")
    if su_user is None or su_pass is None:
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

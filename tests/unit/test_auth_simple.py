# Copyright (c) 2025 Kenneth Stott
# Canary: 4854b3cb-9bbe-4ddd-8232-acda6ee6842d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for SimpleAuthProvider — login and token validation."""

from __future__ import annotations

import datetime
import bcrypt
import jwt
import pytest

from provisa.auth.providers.simple import SimpleAuthProvider


def _hash_pw(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


@pytest.fixture
def provider():
    users = [
        {"username": "alice", "password_hash": _hash_pw("pass123"), "roles": ["analyst"]},
        {"username": "bob", "password_hash": _hash_pw("bobpass"), "roles": ["admin"]},
    ]
    return SimpleAuthProvider(users=users, jwt_secret="test-secret-key")


def test_login_valid(provider):
    token = provider.login("alice", "pass123")
    assert isinstance(token, str)
    decoded = jwt.decode(token, "test-secret-key", algorithms=["HS256"])
    assert decoded["sub"] == "alice"
    assert decoded["roles"] == ["analyst"]


def test_login_wrong_password(provider):
    with pytest.raises(ValueError, match="Invalid credentials"):
        provider.login("alice", "wrongpass")


def test_login_unknown_user(provider):
    with pytest.raises(ValueError, match="Invalid credentials"):
        provider.login("nobody", "pass123")


@pytest.mark.asyncio
async def test_validate_token_valid(provider):
    token = provider.login("bob", "bobpass")
    identity = await provider.validate_token(token)
    assert identity.user_id == "bob"
    assert identity.display_name == "bob"
    assert identity.roles == ["admin"]


@pytest.mark.asyncio
async def test_validate_token_expired(provider):
    now = datetime.datetime.now(datetime.timezone.utc)
    payload = {
        "sub": "alice",
        "roles": ["analyst"],
        "iat": now - datetime.timedelta(hours=2),
        "exp": now - datetime.timedelta(hours=1),
    }
    token = jwt.encode(payload, "test-secret-key", algorithm="HS256")
    with pytest.raises(jwt.ExpiredSignatureError):
        await provider.validate_token(token)


@pytest.mark.asyncio
async def test_validate_token_bad_secret():
    provider = SimpleAuthProvider(users=[], jwt_secret="real-secret")
    token = jwt.encode({"sub": "x", "roles": []}, "wrong-secret", algorithm="HS256")
    with pytest.raises(jwt.InvalidSignatureError):
        await provider.validate_token(token)

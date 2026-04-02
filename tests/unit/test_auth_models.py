# Copyright (c) 2025 Kenneth Stott
# Canary: 6ff81ad8-8ac5-449b-b615-05aed3e867ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Tests for auth identity, role mapping, and superuser."""

from provisa.auth.models import AuthIdentity
from provisa.auth.role_mapping import resolve_role
from provisa.auth.superuser import check_superuser


def test_auth_identity_creation():
    identity = AuthIdentity(
        user_id="u1",
        email="u@example.com",
        display_name="User One",
        roles=["editor"],
        raw_claims={"sub": "u1"},
    )
    assert identity.user_id == "u1"
    assert identity.email == "u@example.com"
    assert identity.display_name == "User One"
    assert identity.roles == ["editor"]
    assert identity.raw_claims == {"sub": "u1"}


def test_auth_identity_defaults():
    identity = AuthIdentity(user_id="u2", email=None, display_name=None, roles=[])
    assert identity.raw_claims == {}


def test_role_mapping_exact_match():
    identity = AuthIdentity(
        user_id="u1",
        email=None,
        display_name=None,
        roles=[],
        raw_claims={"department": "engineering"},
    )
    rules = [{"type": "exact", "claim": "department", "value": "engineering", "role": "engineer"}]
    assert resolve_role(identity, rules, "analyst") == "engineer"


def test_role_mapping_contains():
    identity = AuthIdentity(
        user_id="u1",
        email=None,
        display_name=None,
        roles=["admin", "viewer"],
        raw_claims={"groups": ["admin", "viewer"]},
    )
    rules = [{"type": "contains", "claim": "groups", "value": "admin", "role": "admin"}]
    assert resolve_role(identity, rules, "analyst") == "admin"


def test_role_mapping_default_fallback():
    identity = AuthIdentity(
        user_id="u1",
        email=None,
        display_name=None,
        roles=[],
        raw_claims={"department": "marketing"},
    )
    rules = [{"type": "exact", "claim": "department", "value": "engineering", "role": "engineer"}]
    assert resolve_role(identity, rules, "analyst") == "analyst"


def test_role_mapping_empty_rules():
    identity = AuthIdentity(user_id="u1", email=None, display_name=None, roles=[])
    assert resolve_role(identity, [], "viewer") == "viewer"


def test_superuser_valid():
    config = {"username": "root", "password": "secret"}
    result = check_superuser("root", "secret", config)
    assert result is not None
    assert result.user_id == "root"
    assert "admin" in result.roles
    assert result.raw_claims["superuser"] is True


def test_superuser_wrong_password():
    config = {"username": "root", "password": "secret"}
    result = check_superuser("root", "wrong", config)
    assert result is None


def test_superuser_no_config():
    result = check_superuser("root", "secret", {})
    assert result is None

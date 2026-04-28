# Copyright (c) 2026 Kenneth Stott
# Canary: 1826bcab-99fb-4b5c-b445-4b76c0b583c6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for rights enforcement."""

import pytest

from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
    has_capability,
)


class TestCheckCapability:
    def test_has_exact_capability(self):
        role = {"id": "dev", "capabilities": ["query_development"]}
        check_capability(role, Capability.QUERY_DEVELOPMENT)  # no raise

    def test_missing_capability_raises(self):
        role = {"id": "viewer", "capabilities": []}
        with pytest.raises(InsufficientRightsError, match="query_development"):
            check_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_admin_has_all_capabilities(self):
        role = {"id": "admin", "capabilities": ["admin"]}
        check_capability(role, Capability.QUERY_DEVELOPMENT)
        check_capability(role, Capability.SOURCE_REGISTRATION)
        check_capability(role, Capability.SECURITY_CONFIG)

    def test_each_capability_independent(self):
        role = {"id": "reg", "capabilities": ["source_registration"]}
        check_capability(role, Capability.SOURCE_REGISTRATION)
        with pytest.raises(InsufficientRightsError):
            check_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_multiple_capabilities(self):
        role = {"id": "steward", "capabilities": ["query_development", "query_approval"]}
        check_capability(role, Capability.QUERY_DEVELOPMENT)
        check_capability(role, Capability.QUERY_APPROVAL)
        with pytest.raises(InsufficientRightsError):
            check_capability(role, Capability.ADMIN)


class TestHasCapability:
    def test_returns_true(self):
        role = {"id": "dev", "capabilities": ["query_development"]}
        assert has_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_returns_false(self):
        role = {"id": "viewer", "capabilities": []}
        assert not has_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_admin_always_true(self):
        role = {"id": "admin", "capabilities": ["admin"]}
        assert has_capability(role, Capability.SOURCE_REGISTRATION)


class TestInsufficientRightsError:
    def test_error_message(self):
        e = InsufficientRightsError("viewer", Capability.QUERY_DEVELOPMENT)
        assert "viewer" in str(e)
        assert "query_development" in str(e)

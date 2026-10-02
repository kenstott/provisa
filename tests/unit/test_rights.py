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
        result = check_capability(role, Capability.QUERY_DEVELOPMENT)  # no raise
        assert result is None  # check_capability returns None on success
        assert has_capability(role, Capability.QUERY_DEVELOPMENT) is True

    def test_missing_capability_raises(self):
        role = {"id": "viewer", "capabilities": []}
        with pytest.raises(InsufficientRightsError, match="query_development"):
            check_capability(role, Capability.QUERY_DEVELOPMENT)

    @pytest.mark.parametrize(
        "held", [["admin"], ["superadmin"], ["platform_settings", "cross_org"]]
    )
    def test_no_string_stands_in_for_a_capability(self, held):
        # REQ-1327: there is no capability that means "every right".
        role = {"id": "admin", "capabilities": held}
        for cap in (
            Capability.QUERY_DEVELOPMENT,
            Capability.SOURCE_REGISTRATION,
            Capability.ACCESS_CONFIG,
        ):
            with pytest.raises(InsufficientRightsError, match=cap.value):
                check_capability(role, cap)
            assert has_capability(role, cap) is False

    def test_the_retired_strings_are_not_capabilities(self):
        values = {c.value for c in Capability}
        assert "admin" not in values
        assert "superadmin" not in values

    def test_each_capability_independent(self):
        role = {"id": "reg", "capabilities": ["source_registration"]}
        check_capability(role, Capability.SOURCE_REGISTRATION)
        with pytest.raises(InsufficientRightsError):
            check_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_multiple_capabilities(self):
        role = {"id": "steward", "capabilities": ["query_development", "approve_view"]}
        check_capability(role, Capability.QUERY_DEVELOPMENT)
        check_capability(role, Capability.APPROVE_VIEW)
        with pytest.raises(InsufficientRightsError):
            check_capability(role, Capability.SOURCE_REGISTRATION)


class TestHasCapability:
    def test_returns_true(self):
        role = {"id": "dev", "capabilities": ["query_development"]}
        assert has_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_returns_false(self):
        role = {"id": "viewer", "capabilities": []}
        assert not has_capability(role, Capability.QUERY_DEVELOPMENT)

    def test_a_role_named_admin_holds_only_what_it_is_given(self):
        role = {"id": "admin", "capabilities": ["admin"]}
        assert not has_capability(role, Capability.SOURCE_REGISTRATION)
        role = {"id": "admin", "capabilities": ["source_registration"]}
        assert has_capability(role, Capability.SOURCE_REGISTRATION)


class TestInsufficientRightsError:
    def test_error_message(self):
        e = InsufficientRightsError("viewer", Capability.QUERY_DEVELOPMENT)
        assert "viewer" in str(e)
        assert "query_development" in str(e)

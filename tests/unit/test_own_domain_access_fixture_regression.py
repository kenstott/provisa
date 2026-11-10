# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7f9a12-4b6d-4c81-b0e5-9d2f8a7c5e14
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit regression: integration test fixtures that set state.roles must include own_domain_access.

REQ-2153 (own-domain column scope) added ``own_domain_access`` to every role dict the runtime
carries in ``state.roles``. The production build calls
``provisa.security.inheritance.with_own_domains`` before folding the domain tree, so every role
in ``state.roles`` carries the key after a live build.

Three integration test fixtures bypassed the build and set ``state.roles`` from raw
``role_repo.list_all`` rows or manually-constructed dicts — neither source applies
``with_own_domains``, so the roles they put into ``state.roles`` lacked ``own_domain_access``.
When ``catalog_scope._subjects`` read the key (line 196 of
``provisa/api/admin/catalog_scope.py``), it raised::

    KeyError: 'own_domain_access'

Affected tests (release/v0.1.0-alpha.508 CI run 42821626805):
- tests/integration/test_relationship_request_domain_approvals.py (6 failures)
- tests/integration/test_sales_steward_domain_scope.py (1 failure)
- tests/integration/test_org_member_admin_surface.py (4 failures)

Root cause: test fixture bug — roles put into state.roles must have own_domain_access.
Fix: apply with_own_domains (or add the key manually) in every fixture that sets state.roles.

This file reproduces the fault as a container-free unit test.
"""

# Requirements: REQ-2153

from __future__ import annotations

import types

import pytest

from provisa.api.admin.catalog_scope import _subjects
from provisa.security.inheritance import OWN_DOMAIN_ACCESS, with_own_domains


def _state(roles: dict) -> types.SimpleNamespace:
    """Minimal state stub used by catalog_scope._subjects."""
    return types.SimpleNamespace(roles=roles)


# A single-member role (not a meta-role): acting_roles returns [role_id].
_ROLE_ID = "analyst"
_DOMAIN_ACCESS = ["sales"]


class TestSubjectsRequiresOwnDomainAccess:
    """_subjects raises KeyError when state.roles lacks own_domain_access — the bug."""

    def test_role_without_own_domain_access_raises_key_error(self):
        """Reproduces the fault: a role dict missing own_domain_access causes KeyError."""
        buggy_role = {
            "id": _ROLE_ID,
            "capabilities": ["query_development"],
            "domain_access": _DOMAIN_ACCESS,
            # own_domain_access is intentionally absent — this is the bug
        }
        state = _state({_ROLE_ID: buggy_role})
        with pytest.raises(KeyError, match="own_domain_access"):
            _subjects(state, _ROLE_ID)

    def test_role_repo_rows_without_with_own_domains_raises_key_error(self):
        """Rows from role_repo.list_all have no own_domain_access until with_own_domains runs."""
        # Simulate what role_repo.list_all returns: no own_domain_access key.
        raw_repo_row = {
            "id": _ROLE_ID,
            "capabilities": ["query_development"],
            "domain_access": _DOMAIN_ACCESS,
            "parents": [],
            "rate_limit": None,
        }
        state = _state({_ROLE_ID: raw_repo_row})
        with pytest.raises(KeyError, match="own_domain_access"):
            _subjects(state, _ROLE_ID)

    def test_manually_constructed_role_without_own_domain_access_raises(self):
        """A manually-constructed role dict (as in test_org_member_admin_surface) raises too."""
        # This is exactly what test_org_member_admin_surface.py set before the fix:
        manual_role = {"capabilities": ["query_development"], "domain_access": ["*"]}
        state = _state({"org_admin": manual_role})
        with pytest.raises(KeyError, match="own_domain_access"):
            _subjects(state, "org_admin")


class TestSubjectsAfterApplyingWithOwnDomains:
    """After with_own_domains is applied, _subjects succeeds and carries the key."""

    def test_with_own_domains_adds_own_domain_access(self):
        raw = [
            {"id": _ROLE_ID, "capabilities": ["query_development"], "domain_access": _DOMAIN_ACCESS}
        ]
        enriched = with_own_domains(raw)
        assert OWN_DOMAIN_ACCESS in enriched[0]
        assert enriched[0][OWN_DOMAIN_ACCESS] == _DOMAIN_ACCESS

    def test_role_with_own_domain_access_does_not_raise(self):
        enriched_roles = with_own_domains(
            [
                {
                    "id": _ROLE_ID,
                    "capabilities": ["query_development"],
                    "domain_access": _DOMAIN_ACCESS,
                }
            ]
        )
        state = _state({_ROLE_ID: enriched_roles[0]})
        result = _subjects(state, _ROLE_ID)
        assert len(result) == 1
        assert result[0]["id"] == _ROLE_ID
        assert OWN_DOMAIN_ACCESS in result[0]

    def test_subjects_carries_own_domain_access_in_output(self):
        raw = [
            {"id": _ROLE_ID, "capabilities": ["query_development"], "domain_access": _DOMAIN_ACCESS}
        ]
        state = _state({_ROLE_ID: with_own_domains(raw)[0]})
        result = _subjects(state, _ROLE_ID)
        assert result[0][OWN_DOMAIN_ACCESS] == _DOMAIN_ACCESS

    def test_manually_constructed_role_with_own_domain_access_works(self):
        """The correct form for test_org_member_admin_surface: include own_domain_access."""
        fixed_role = {
            "capabilities": ["query_development"],
            "domain_access": ["*"],
            OWN_DOMAIN_ACCESS: ["*"],
        }
        state = _state({"org_admin": fixed_role})
        result = _subjects(state, "org_admin")
        assert result[0][OWN_DOMAIN_ACCESS] == ["*"]

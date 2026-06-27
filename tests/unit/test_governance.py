# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Rights-based governance tests (REQ-001/003, REQ-042).

Access is governed solely by independently-assignable rights — there is no query
registry. REQ-042 requires the seven named rights to be distinct and independently
configured; this module pins that contract.
"""

from __future__ import annotations

import pytest

from provisa.security.rights import (
    Capability,
    InsufficientRightsError,
    check_capability,
    has_capability,
)

# REQ-042: the seven distinct rights, each mapped to a Capability member.
NAMED_RIGHTS = {
    "source registration": Capability.SOURCE_REGISTRATION,
    "table registration": Capability.TABLE_REGISTRATION,
    "relationship definition": Capability.CREATE_RELATIONSHIP,
    "security configuration": Capability.ACCESS_CONFIG,
    "query development": Capability.QUERY_DEVELOPMENT,
    "query authorization": Capability.APPROVE_VIEW,
    "ignore relationships": Capability.IGNORE_RELATIONSHIPS,
}


def _role(*caps: Capability) -> dict:
    return {"id": "r", "capabilities": [c.value for c in caps]}


class TestNamedRightsDistinct:
    def test_seven_named_rights_are_distinct_capabilities(self):
        values = [c.value for c in NAMED_RIGHTS.values()]
        assert len(set(values)) == 7  # all distinct

    def test_each_named_right_is_independently_granted(self):
        # A role holding exactly one named right has that one and none of the others.
        for name, cap in NAMED_RIGHTS.items():
            role = _role(cap)
            assert has_capability(role, cap), name
            for other_name, other_cap in NAMED_RIGHTS.items():
                if other_cap is not cap:
                    assert not has_capability(role, other_cap), f"{name} leaked {other_name}"


class TestCapabilityEnforcement:
    def test_check_capability_raises_when_missing(self):
        role = _role(Capability.QUERY_DEVELOPMENT)
        with pytest.raises(InsufficientRightsError):
            check_capability(role, Capability.SOURCE_REGISTRATION)

    def test_check_capability_passes_when_present(self):
        role = _role(Capability.SOURCE_REGISTRATION)
        result = check_capability(role, Capability.SOURCE_REGISTRATION)  # no raise
        assert result is None

    def test_admin_bypasses_all_rights(self):
        # REQ-002/125: admin override — admin satisfies any capability check.
        role = _role(Capability.ADMIN)
        for cap in NAMED_RIGHTS.values():
            assert has_capability(role, cap)
            check_capability(role, cap)  # no raise

    def test_no_capabilities_grants_nothing(self):
        role = {"id": "r", "capabilities": []}
        for cap in NAMED_RIGHTS.values():
            assert not has_capability(role, cap)

    def test_malformed_capabilities_treated_as_empty(self):
        role: dict = {"id": "r", "capabilities": "not-a-list"}
        assert not has_capability(role, Capability.QUERY_DEVELOPMENT)


class TestNoRegistryConcept:
    def test_no_approval_or_registry_capability_exists(self):
        # REQ-001/003: access is rights-based; there is no query-registry / query-approval
        # capability (approval relates to view/relationship creation only).
        names = {c.name for c in Capability}
        assert "APPROVE_QUERY" not in names
        assert "REGISTRY" not in names

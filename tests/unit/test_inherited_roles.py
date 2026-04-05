# Copyright (c) 2026 Kenneth Stott
# Canary: 35a52efd-9998-4355-a2e3-9ed42c7408e0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for inherited role flattening (REQ-215)."""

from provisa.core.models import Role, flatten_roles


class TestFlattenRoles:
    def test_no_inheritance(self):
        roles = [
            Role(id="admin", capabilities=["admin"], domain_access=["*"]),
            Role(id="analyst", capabilities=["query_development"], domain_access=["sales"]),
        ]
        flat = flatten_roles(roles)
        admin = next(r for r in flat if r.id == "admin")
        analyst = next(r for r in flat if r.id == "analyst")
        assert admin.capabilities == ["admin"]
        assert analyst.capabilities == ["query_development"]

    def test_single_level_inheritance(self):
        roles = [
            Role(id="admin", capabilities=["admin", "query_approval"], domain_access=["*"]),
            Role(
                id="senior_analyst",
                capabilities=["query_development"],
                domain_access=["sales"],
                parent_role_id="admin",
            ),
        ]
        flat = flatten_roles(roles)
        senior = next(r for r in flat if r.id == "senior_analyst")
        assert "admin" in senior.capabilities
        assert "query_development" in senior.capabilities
        assert "query_approval" in senior.capabilities
        assert senior.domain_access == ["*"]  # inherits wildcard

    def test_two_level_inheritance(self):
        roles = [
            Role(id="base", capabilities=["read"], domain_access=["public"]),
            Role(
                id="mid", capabilities=["write"],
                domain_access=["internal"], parent_role_id="base",
            ),
            Role(
                id="top", capabilities=["admin"],
                domain_access=["secret"], parent_role_id="mid",
            ),
        ]
        flat = flatten_roles(roles)
        top = next(r for r in flat if r.id == "top")
        assert set(top.capabilities) == {"read", "write", "admin"}
        assert set(top.domain_access) == {"public", "internal", "secret"}

    def test_wildcard_domain_propagates(self):
        roles = [
            Role(id="parent", capabilities=["a"], domain_access=["*"]),
            Role(id="child", capabilities=["b"], domain_access=["sales"], parent_role_id="parent"),
        ]
        flat = flatten_roles(roles)
        child = next(r for r in flat if r.id == "child")
        assert child.domain_access == ["*"]

    def test_parent_not_found_ignored(self):
        """Missing parent_role_id reference doesn't crash — just uses own capabilities."""
        roles = [
            Role(id="orphan", capabilities=["read"], domain_access=["sales"], parent_role_id="nonexistent"),
        ]
        flat = flatten_roles(roles)
        assert flat[0].capabilities == ["read"]

    def test_parent_role_id_preserved(self):
        roles = [
            Role(id="parent", capabilities=["a"], domain_access=["*"]),
            Role(id="child", capabilities=["b"], domain_access=["x"], parent_role_id="parent"),
        ]
        flat = flatten_roles(roles)
        child = next(r for r in flat if r.id == "child")
        assert child.parent_role_id == "parent"

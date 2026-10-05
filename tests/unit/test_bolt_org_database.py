# Copyright (c) 2026 Kenneth Stott
# Canary: ea469807-4344-49f3-8e22-a9b2c7247c77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1235: on Bolt, the database name names the org: ``<org>.provisa_<role>``.

Single-tenant deployments keep today's names (``provisa_<role>``, ``provisa_ops_<role>``) and have
no org part. Under multi-tenancy the org prefixes the name, SHOW DATABASES lists one set per org
the user belongs to, and a bare ``provisa_<role>`` is refused, naming the form to use.
"""

from __future__ import annotations

import pytest

from provisa.api.org_resolve import OrgResolutionError
from provisa.bolt.session import _show_databases_rows, select_database


class TestSelectDatabase:
    def test_multi_tenant_takes_the_org_from_the_prefix(self):
        assert select_database("acme.provisa_analyst", multitenancy=True) == (
            "acme",
            "provisa_analyst",
        )
        assert select_database("acme.provisa_ops_analyst", multitenancy=True) == (
            "acme",
            "provisa_ops_analyst",
        )

    def test_multi_tenant_refuses_a_bare_role_database_naming_the_form(self):
        with pytest.raises(OrgResolutionError, match=r"<org>\.provisa_analyst"):
            select_database("provisa_analyst", multitenancy=True)

    @pytest.mark.parametrize("db", [None, "", "system"])
    def test_the_system_database_needs_no_org(self, db):
        assert select_database(db, multitenancy=True) == (None, db)

    def test_single_tenant_keeps_todays_names(self):
        assert select_database("provisa_analyst", multitenancy=False) == (None, "provisa_analyst")

    def test_single_tenant_has_no_org_part(self):
        with pytest.raises(OrgResolutionError, match="no org"):
            select_database("acme.provisa_analyst", multitenancy=False)


class TestShowDatabases:
    def test_multi_tenant_lists_each_org_the_user_belongs_to(self):
        _, rows = _show_databases_rows(["analyst"], orgs=["acme", "beta"])
        assert [r[0] for r in rows] == [
            "acme.provisa_analyst",
            "acme.provisa_ops_analyst",
            "beta.provisa_analyst",
            "beta.provisa_ops_analyst",
        ]

    def test_single_tenant_lists_todays_names(self):
        _, rows = _show_databases_rows(["analyst"])
        assert [r[0] for r in rows] == ["provisa_analyst", "provisa_ops_analyst"]


class _Conn:
    def __init__(self, orgs):
        self._orgs = orgs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def execute_core(self, _stmt):
        class _R:
            def __init__(self, orgs):
                self._orgs = orgs

            def fetchall(self):
                class _Row:
                    def __init__(self, o):
                        self._mapping = {"org_id": o}

                return [_Row(o) for o in self._orgs]

        return _R(self._orgs)


class _AdminDb:
    def acquire(self):
        return _Conn(["acme", "beta"])


class _State:
    multitenancy = True
    org_id = "default"
    admin_db = _AdminDb()
    roles: dict = {}
    platform_roles: dict = {}


class TestTheDatabaseOrgBindsTheSession:
    def _session(self, monkeypatch, sni_org=None):
        from provisa.bolt.session import BoltSession

        monkeypatch.setattr("provisa.api.app.state", _State(), raising=False)

        async def _noop(_org, _env=None):
            return None

        monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _noop, raising=False)
        session = BoltSession.__new__(BoltSession)
        session._org_resolved = False
        session.org_id = None
        session.user_id = "u1"
        session.roles = ["analyst"]
        session._credential_org = None
        monkeypatch.setattr(session, "_requested_org", lambda: sni_org)
        return session

    async def test_the_database_org_is_bound(self, monkeypatch):
        session = self._session(monkeypatch)
        await session._ensure_org("beta")
        assert session.org_id == "beta"

    async def test_a_hostname_and_database_naming_different_orgs_are_refused(self, monkeypatch):
        session = self._session(monkeypatch, sni_org="acme")
        with pytest.raises(OrgResolutionError, match="'acme'.*'beta'"):
            await session._ensure_org("beta")

    async def test_a_later_database_naming_another_org_is_refused(self, monkeypatch):
        session = self._session(monkeypatch)
        await session._ensure_org("acme")
        with pytest.raises(OrgResolutionError, match="bound to org 'acme'"):
            await session._ensure_org("beta")

    async def test_member_orgs_are_listed(self, monkeypatch):
        session = self._session(monkeypatch)
        assert await session._member_orgs() == ["acme", "beta"]


class TestRoleSetDatabase:
    """REQ-1620: ``provisa_<a>,<b>`` names a set of held roles, which acts as their meta-role."""

    @staticmethod
    def _session(roles, org_id=None):
        from types import SimpleNamespace

        from provisa.bolt.session import BoltSession

        session = BoltSession(SimpleNamespace(), (5, 4))  # type: ignore[arg-type]
        session.roles = roles
        session.org_id = org_id
        return session

    def test_a_set_of_held_roles_is_named_whole(self):
        session = self._session(["analyst", "auditor"])
        assert session._resolve_db("provisa_analyst,auditor") == ("analyst,auditor", False)
        assert session._resolve_db("provisa_ops_auditor, analyst") == ("auditor,analyst", True)

    def test_a_set_with_a_role_not_held_is_not_accessible(self):
        session = self._session(["analyst"])
        assert session._resolve_db("provisa_analyst,org_admin") is None

    def test_a_meta_role_named_directly_is_not_accessible(self):
        session = self._session(["analyst", "auditor"])
        assert session._resolve_db("provisa_meta:analyst+auditor") is None

    def test_the_set_acts_as_its_meta_role_built_in_the_sessions_org(self, monkeypatch):
        import provisa.security.meta_role as meta_role
        from provisa.core.request_context import current_org

        built = []

        def _ensure(state, members):
            built.append((current_org.get(), members))
            return meta_role.meta_role_id(members)

        monkeypatch.setattr(meta_role, "ensure_meta_role", _ensure)
        session = self._session(["analyst", "auditor"], org_id="acme")
        assert session._meta_role(object(), "auditor,analyst") == "meta:analyst+auditor"
        assert built == [("acme", ["analyst", "auditor"])]

    def test_a_role_not_held_is_refused_by_name(self):
        session = self._session(["analyst"], org_id="acme")  # resolved before any RUN (REQ-1266)
        with pytest.raises(PermissionError, match="'org_admin'"):
            session._meta_role(object(), "analyst,org_admin")

# Copyright (c) 2026 Kenneth Stott
# Canary: 834b3643-4182-4783-a8c7-262978860609
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: Provisa's standard roles -- named collections of capabilities composed by union over
several parents, maintained by Provisa and locked; an organisation extends them with child roles.

The sandbox a "Try it Out" visitor holds is composed from standard roles like any role (REQ-2155,
superseding REQ-1597's org_admin-minus-a-denylist): it inherits what the product does and never the
rights that reach past the environment it was minted in.
"""

from __future__ import annotations

import pytest

from provisa.core.db import STANDARD_ROLES, STANDARD_ROLE_RIGHTS, sandbox_demonstrated
from provisa.security.inheritance import role_chains

SANDBOX = "sandbox"


def _rights(role_id: str) -> frozenset[str]:
    return STANDARD_ROLE_RIGHTS[role_id]


class TestTheSet:
    def test_the_roster(self):
        assert set(STANDARD_ROLES) == {
            "viewer",
            "analyst",
            "developer",
            "modeler",
            "data_product_owner",
            "security_admin",
            "data_steward",
            "dq_reader",
            "dq_steward",
            "source_admin",
            "org_operator",
            "org_admin",
            "sandbox",
            "platform_admin",
        }

    def test_every_parent_is_a_standard_role_and_there_is_no_cycle(self):
        for spec in STANDARD_ROLES.values():
            assert set(spec.parents) <= set(STANDARD_ROLES)
        role_chains([{"id": rid, "parents": list(s.parents)} for rid, s in STANDARD_ROLES.items()])

    def test_every_data_role_inherits_viewer(self):
        for rid in set(STANDARD_ROLES) - {"viewer", "platform_admin"}:
            assert _rights("viewer") <= _rights(rid), rid

    def test_every_standard_role_has_a_description(self):
        assert all(spec.description.strip() for spec in STANDARD_ROLES.values())

    def test_a_right_is_held_where_it_first_enters_and_not_restated(self):
        """Composed, not duplicated: no role lists a right an ancestor already gives it."""
        for rid, spec in STANDARD_ROLES.items():
            inherited = frozenset().union(*(_rights(p) for p in spec.parents))
            assert not set(spec.capabilities) & inherited, rid


class TestWhatEachHolds:
    def test_viewer_reads_the_glossary_and_data_products(self):
        assert _rights("viewer") == {"glossary_read", "data_product_read"}

    def test_analyst_queries(self):
        assert _rights("analyst") == _rights("viewer") | {"usage", "query_development"}

    def test_developer_builds_and_switches_but_never_manages_environments(self):
        assert {"create_view", "create_relationship", "write", "environment_switch"} <= _rights(
            "developer"
        )
        assert "environment_management" not in _rights("developer")

    def test_modeler_is_developer_plus_joining_outside_the_model(self):
        assert _rights("modeler") == _rights("developer") | {"ignore_relationships"}

    def test_security_admin_configures_access_but_manages_no_users(self):
        assert {
            "access_config",
            "column_grant",
            "masking_config",
            "sensitive_data",
            "view_governance",
        } <= _rights("security_admin")
        assert "user_management" not in _rights("security_admin")

    def test_data_steward_is_meaning_plus_access(self):
        """REQ-1944's governance and its delegation of read, composed from security_admin and
        analyst, with the glossary, relationships and data products on top."""
        assert STANDARD_ROLES["data_steward"].parents == ("security_admin", "analyst")
        assert _rights("security_admin") | _rights("analyst") <= _rights("data_steward")
        assert {"glossary_rw", "create_relationship", "data_product_rw", "full_results"} <= (
            _rights("data_steward")
        )

    def test_data_quality_needs_no_table_registration(self):
        assert _rights("dq_steward") == _rights("viewer") | {"dq_read", "dq_rw"}
        assert _rights("dq_reader") == _rights("viewer") | {"dq_read"}

    def test_a_data_product_owner_edits_only_what_it_owns(self):
        assert _rights("data_product_owner") == _rights("viewer") | {"data_product_owned_rw"}

    def test_org_admin_holds_every_other_standard_role_and_its_own(self):
        for rid in set(STANDARD_ROLES) - {"org_admin", "modeler", "sandbox", "platform_admin"}:
            assert _rights(rid) <= _rights("org_admin"), rid
        assert {
            "environment_management",
            "environment_data",
            "user_management",
            "org_glossary_rw",
        } <= _rights("org_admin")

    def test_platform_admin_holds_the_platform_rights_alone(self):
        assert _rights("platform_admin") == {"platform_settings", "cross_org"}


class TestTheSandbox:
    def test_it_is_a_union_of_standard_roles_and_developers_rights_bar_owned_environments(self):
        # REQ-2053, REQ-2155: not developer's child, which manages the environments it owns; it
        # holds every other right developer holds.
        assert set(STANDARD_ROLES[SANDBOX].capabilities) == {
            "create_view",
            "write",
            "environment_switch",
        }
        assert _rights("developer") - _rights(SANDBOX) == {"environment_owned_management"}
        assert set(STANDARD_ROLES[SANDBOX].parents) == {
            "analyst",
            "data_product_owner",
            "dq_steward",
            "source_admin",
            "security_admin",
            "data_steward",
            "org_operator",
        }

    @pytest.mark.parametrize(
        "right",
        ["environment_management", "user_management", "org_glossary_rw", "environment_data"],
    )
    def test_what_reaches_past_its_environment_is_never_held(self, right):
        assert right not in _rights(SANDBOX)

    def test_it_carries_nothing_from_the_platform_plane(self):
        assert not _rights(SANDBOX) & {"cross_org", "platform_settings"}

    @pytest.mark.parametrize(
        "right",
        [
            "source_registration",
            "table_registration",
            "create_relationship",
            "write",
            "org_settings",
            "approve_view",
            "glossary_rw",
        ],
    )
    def test_the_visitor_can_actually_build_something(self, right):
        # A sandbox that only reads is a screenshot; these are the rights that make it a trial.
        assert right in _rights(SANDBOX)

    @pytest.mark.asyncio
    async def test_the_visitor_cannot_leave_the_environment_they_were_seated_in(self):
        """REQ-1596: the membership pin confines them though the role holds environment_switch
        (REQ-2053): the pin is checked before the right, and before any lookup."""
        from provisa.api.env_routing import EnvironmentRightError, select_environment

        for named in ("prod", "dev"):
            with pytest.raises(EnvironmentRightError):
                await select_environment(
                    None, "acme", named, set(_rights(SANDBOX)), pinned="sbx_ab12"
                )

    def test_no_other_standard_role_demonstrates_anything(self):
        """Demonstration is the sandbox's whole purpose; anywhere else it would be a surface that
        lies about what the caller may do."""
        import inspect

        from provisa.core import db

        source = inspect.getsource(db.assert_standard_roles)
        assert 'sandbox_demonstrated() if role_id == "sandbox" else []' in source

    def test_it_demonstrates_what_org_admin_holds_and_it_does_not_except_user_management(self):
        """REQ-1602/REQ-1608: the surfaces of a right the visitor lacks stay on the page, disabled
        and badged -- all but user management, which stays a hard block."""
        assert set(sandbox_demonstrated()) == (
            _rights("org_admin") - _rights(SANDBOX) - {"user_management"}
        )
        assert not set(sandbox_demonstrated()) & _rights(SANDBOX)


class TestTheSeed:
    """init_schema writes every standard role as its definition says, and a standard role edited in
    the store is written back on the next run: it is Provisa's, and locked."""

    async def _rows(self, db) -> dict[str, dict]:
        from sqlalchemy import select

        from provisa.core.schema_org import roles

        async with db.acquire() as conn:
            result = await conn.execute_core(select(roles))
            return {r._mapping["id"]: dict(r._mapping) for r in result.fetchall()}

    async def test_every_standard_role_is_in_the_store_as_defined(self, control_plane):
        rows = await self._rows(control_plane.org())
        for rid, spec in STANDARD_ROLES.items():
            assert rows[rid]["parents"] == list(spec.parents), rid
            assert set(rows[rid]["capabilities"]) >= set(spec.capabilities), rid
            assert rows[rid]["description"] == spec.description, rid
            assert rows[rid]["domain_access"] == ([] if rid == "platform_admin" else ["*"]), rid

    async def test_an_edited_standard_role_is_written_back(self, control_plane):
        from sqlalchemy import update

        from provisa.core.db import assert_standard_roles
        from provisa.core.environments import org_schema
        from provisa.core.schema_org import roles

        db = control_plane.org()
        async with db.acquire() as conn:
            await conn.execute_core(
                update(roles)
                .where(roles.c.id == "analyst")
                .values(capabilities=["write"], parents=[], description="changed")
            )
        with db.engine.begin() as sa_conn:
            assert_standard_roles(sa_conn, org_schema("default"))
        analyst = (await self._rows(db))["analyst"]
        assert analyst["capabilities"] == ["usage", "query_development"]
        assert analyst["parents"] == ["viewer"]
        assert analyst["description"] == STANDARD_ROLES["analyst"].description

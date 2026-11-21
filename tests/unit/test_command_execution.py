# Copyright (c) 2026 Kenneth Stott
# Canary: c82fc5d4-aa70-4974-9658-c321fb10cfba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-870 (amended 2026-11-09): a role runs a command only when it holds command_execution, is
assigned the command and reaches the command's domain."""

# Requirements: REQ-870

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.db import STANDARD_ROLE_RIGHTS
from provisa.security.effective_rights import data_view_from_state
from provisa.security.inheritance import (
    OWN_DOMAIN_ACCESS,
    expand_meta_grants,
    flatten_role_dicts,
)
from provisa.security.meta_role import _role as meta_role_definition
from provisa.security.mutation_authz import CommandNotFound, admit_command, command_reachable
from provisa.security.rights import (
    Capability,
    domain_access_for_claims,
    served_roles,
)
from provisa.security.visibility import visible_tables

RUN = Capability.COMMAND_EXECUTION.value


def _role(rid, caps=(), domains=("a",)):
    return {
        "id": rid,
        "description": rid,
        "capabilities": list(caps),
        "domain_access": list(domains),
        "parents": [],
        OWN_DOMAIN_ACCESS: list(domains),
    }


def _command(kind: str | None = "mutation", domain="a", assigned=("runner",)):
    return {"name": "archive", "kind": kind, "domain_id": domain, "visible_to": list(assigned)}


class TestTheRule:
    def test_a_role_with_no_capabilities_reaches_no_command(self):
        assert not command_reachable(_command(), _role("runner", [], ["a"]))
        with pytest.raises(CommandNotFound):
            admit_command(_command(), _role("runner", [], ["a"]), "archive")

    def test_flattened_a_role_with_no_capabilities_reaches_no_command(self):
        flat = flatten_role_dicts([_role("runner", [], ["a"])])[0]
        assert not command_reachable(_command(), flat)

    def test_another_capability_does_not_stand_in_for_it(self):
        assert not command_reachable(_command(), _role("runner", ["usage", "write"], ["a"]))

    def test_holding_it_without_the_assignment_is_not_enough(self):
        assert not command_reachable(_command(assigned=["other"]), _role("runner", [RUN], ["a"]))

    def test_holding_it_and_assigned_outside_the_domain_is_not_enough(self):
        assert not command_reachable(_command(domain="b"), _role("runner", [RUN], ["a"]))

    def test_holding_it_assigned_and_in_the_domain_reaches_it(self):
        assert command_reachable(_command(), _role("runner", [RUN], ["a"]))

    def test_a_role_holding_only_it_runs_a_mutation_it_is_assigned(self):
        admit_command(_command("mutation"), _role("runner", [RUN], ["a"]), "archive")

    def test_a_role_holding_other_rights_runs_a_mutation_it_is_assigned_without_write(self):
        # REQ-870, THE ASSIGNMENT IS THE GRANT: whether the command writes or not.
        admit_command(_command("mutation"), _role("runner", [RUN, "usage"], ["a"]), "archive")

    def test_a_command_of_unknown_kind_needs_no_write_either(self):
        admit_command(_command(None), _role("runner", [RUN, "usage"], ["a"]), "archive")

    def test_the_write_capability_opens_no_command(self):
        with pytest.raises(CommandNotFound):
            admit_command(_command(), _role("runner", ["usage", "write"], ["a"]), "archive")


class TestCatalogCommands:
    APOC = {"name": "apoc_meta_data", "kind": "query", "domain_id": "meta", "visible_to": ["r"]}

    def test_a_catalog_command_runs_under_catalog_read(self):
        role = flatten_role_dicts([_role("r", ["catalog_read"], ["a"])])[0]
        assert command_reachable(self.APOC, role)

    def test_command_execution_does_not_open_a_catalog_command(self):
        role = flatten_role_dicts([_role("r", [RUN], ["a"])])[0]
        assert not command_reachable(self.APOC, role)

    def test_a_role_with_no_capabilities_reaches_no_catalog_command(self):
        role = flatten_role_dicts([_role("r", [], ["a"])])[0]
        assert not command_reachable(self.APOC, role)

    def test_a_mutation_in_meta_is_no_catalog_command(self):
        role = flatten_role_dicts([_role("r", ["catalog_read"], ["a"])])[0]
        assert not command_reachable({**self.APOC, "kind": "mutation"}, role)


class TestHoldingOnlyItReadsNoData:
    COLUMN = {"column_name": "id", "visible_to": ["runner", "p"], "scope": "domain"}

    def test_it_keeps_its_domains(self):
        flat = flatten_role_dicts([_role("runner", [RUN], ["a"])])[0]
        assert flat["domain_access"] == ["a"]

    def test_it_is_served_no_column_in_a_domain_it_lists(self):
        roles = {
            r["id"]: r
            for r in flatten_role_dicts([_role("runner", [RUN], ["a"]), _role("p", ["usage"])])
        }
        chains = {"runner": ["runner"], "p": ["p"]}
        assert served_roles(roles, chains, "a", self.COLUMN) == {"p"}

    def test_its_data_access_is_empty(self):
        table = {"id": 1, "domain_id": "a", "table_name": "t", "columns": [self.COLUMN]}
        state = SimpleNamespace(
            roles={"runner": _role("runner", [RUN], ["a"])},
            tables=[table],
            rls_contexts={},
            masking_rules={},
        )
        view = data_view_from_state(
            state,
            "runner",
            parents={"runner": []},
            stored={},
            readable=lambda *_: True,
            governance=True,
        )
        assert view == []

    def test_the_graph_and_catalog_read_of_its_tables_is_empty(self):
        table = {"id": 1, "domain_id": "a", "table_name": "t", "columns": [self.COLUMN]}
        assert visible_tables([table], _role("runner", [RUN], ["a"])) == []

    def test_its_domains_scope_no_catalog_or_glossary_read(self):
        roles = {"runner": _role("runner", [RUN], ["a"]), "p": _role("p", ["usage"], ["b"])}
        assert domain_access_for_claims(["runner", "p"], roles) == {"b"}

    def test_it_is_given_no_core_metadata(self):
        column = {"column_name": "id", "visible_to": ["viewer"]}
        table = {"domain_id": "meta", "table_name": "registered_tables_meta", "columns": [column]}
        expand_meta_grants([table], [_role("runner", [RUN], ["a"]), _role("p", ["usage"])])
        assert "runner" not in column["visible_to"] and "p" in column["visible_to"]


class TestAsAMemberOfASet:
    """A capability never widens grants (REQ-2152): the runner's domain reaches its commands only."""

    def _meta(self):
        return meta_role_definition(
            [_role("runner", [RUN], ["b"]), _role("reader", ["usage"], ["a"])], "meta:reader+runner"
        )

    def test_a_command_only_member_adds_no_read_reach(self):
        assert self._meta()["domain_access"] == ["a"]

    def test_the_set_runs_the_member_s_command_in_the_member_s_domain(self):
        role = self._meta()
        assert command_reachable(_command(domain="b", assigned=[role["id"]]), role)

    def test_a_member_without_it_gives_its_domain_no_command(self):
        role = self._meta()
        assert not command_reachable(_command(domain="a", assigned=[role["id"]]), role)


def test_the_standard_roles_that_run_commands_hold_it():
    for role in ("analyst", "developer", "modeler", "data_steward", "org_admin", "sandbox"):
        assert "command_execution" in STANDARD_ROLE_RIGHTS[role], role


def test_the_standard_roles_that_do_not_run_commands_do_not():
    for role in ("viewer", "platform_admin"):
        assert "command_execution" not in STANDARD_ROLE_RIGHTS[role], role

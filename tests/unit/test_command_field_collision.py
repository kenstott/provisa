# Copyright (c) 2026 Kenneth Stott
# Canary: 1d9e4b62-8a37-4f50-b2c6-7e0a5d3f9c18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command or webhook never shares a GraphQL root field (REQ-205, REQ-209): two named for one
field (add_pet and addPet are both addPet under apollo_graphql), or a read command named for a
table's own query field, used to leave whichever was added last in the schema with nothing said.
The build refuses it, naming both."""

# Requirements: REQ-205, REQ-209

from __future__ import annotations

import pytest

from provisa.compiler import naming
from provisa.compiler.actions_schema import CommandFieldTaken, _build_action_fields
from provisa.compiler.schema_gen import generate_schema
from provisa.compiler.schema_types import SchemaInput
from tests.unit.test_group_by import _col


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def _command(name: str, kind: str = "mutation") -> dict:
    return {
        "name": name,
        "arguments": [],
        "returns": "",
        "domain_id": "pets",
        "kind": kind,
        "visible_to": ["admin"],  # REQ-2153: a command's list names its roles
    }


def _input(functions: list[dict], webhooks: list[dict] | None = None, tables=None) -> SchemaInput:
    return SchemaInput(
        tables=tables or [],
        relationships=[],
        column_types={1: [_col("id", "integer")]} if tables else {},
        naming_rules=[],
        role={"id": "admin", "capabilities": [], "domain_access": ["*"]},
        domains=[{"id": "pets", "description": "Pets"}],
        domain_prefix=False,
        functions=functions,
        webhooks=webhooks or [],
    )


def test_two_commands_named_for_one_field_are_refused_naming_both():
    with pytest.raises(CommandFieldTaken) as err:
        _build_action_fields(_input([_command("add_pet"), _command("addPet")]), {}, [])
    assert (err.value.root, err.value.field, err.value.command) == ("Mutation", "addPet", "addPet")
    assert err.value.holder == "command 'add_pet'"


def test_a_function_and_a_webhook_named_for_one_field_are_refused():
    webhook = {
        "name": "add_pet",
        "url": "http://x",
        "arguments": [],
        "inline_return_type": [],
        "domain_id": "pets",
        "kind": "mutation",
        "visible_to": ["admin"],
    }
    with pytest.raises(CommandFieldTaken):
        _build_action_fields(_input([_command("add_pet")], [webhook]), {}, [])


def test_a_read_command_named_for_a_tables_field_is_refused():
    pets = {
        "id": 1,
        "source_id": "pg",
        "domain_id": "pets",
        "schema_name": "public",
        "table_name": "pets",
        "columns": [{"column_name": "id", "visible_to": ["admin"]}],
        "write_ops": [],
        "write_returns_rows": False,
        "write_refused_forms": [],
    }
    with pytest.raises(CommandFieldTaken) as err:
        generate_schema(_input([_command("pets", kind="query")], tables=[pets]))
    assert (err.value.root, err.value.field, err.value.command) == ("Query", "pets", "pets")
    assert err.value.holder == "a table's field"


def test_commands_with_their_own_fields_are_all_in_the_schema():
    query, mutation = _build_action_fields(
        _input([_command("add_pet"), _command("find_pet", kind="query")]), {}, []
    )
    assert "addPet" in mutation and "findPet" in query

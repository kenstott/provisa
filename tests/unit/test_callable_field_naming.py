# Copyright (c) 2026 Kenneth Stott
# Canary: 82953bb0-2403-4465-b1e8-32f62aa495bc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1172: registered callable field names (tracked functions and webhooks) apply the active
naming convention at the GraphQL emit site, exactly like table fields — apollo_graphql camelCases
(add_pet -> addPet), hasura_graphql snake_cases. The reverse lookup applies the SAME transform,
so the emitted field routes back to the raw registered command name."""

from __future__ import annotations

import pytest

from provisa.compiler import naming
from provisa.compiler.actions_schema import _build_action_fields
from provisa.compiler.schema_types import SchemaInput


def _schema_input(convention: str, *, kind: str = "mutation") -> SchemaInput:
    naming.configure(gql=convention, sql="snake")
    return SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"]},
        domains=[],
        domain_prefix=False,
        functions=[
            {
                "name": "add_pet",
                "arguments": [{"name": "p_name", "type": "String"}],
                "returns": "",
                "domain_id": "pets",  # a command sits in a domain (REQ-1531)
                "kind": kind,
            }
        ],
        webhooks=[],
    )


@pytest.fixture(autouse=True)
def _restore_convention():
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def test_apollo_camelcases_callable_field():
    si = _schema_input("apollo_graphql")
    _query, mutation = _build_action_fields(si, {}, [])
    assert "addPet" in mutation
    assert "add_pet" not in mutation


def test_hasura_keeps_snake_case_callable_field():
    si = _schema_input("hasura_graphql")
    _query, mutation = _build_action_fields(si, {}, [])
    assert "add_pet" in mutation
    assert "addPet" not in mutation


def test_query_kind_callable_also_cased():
    si = _schema_input("apollo_graphql", kind="query")
    query, _mutation = _build_action_fields(si, {}, [])
    assert "addPet" in query


def test_reverse_lookup_transform_matches_emit_site():
    # The loader registers the convention-cased key using apply_convention with the SAME
    # convention the emitter uses, so the emitted GraphQL field name routes back to the raw
    # registered "add_pet" command.
    naming.configure(gql="apollo_graphql", sql="snake")
    emitted = naming.apply_gql_name("add_pet")
    registered_key = naming.apply_convention("add_pet", "apollo_graphql")
    assert emitted == registered_key == "addPet"


def test_a_source_operation_command_named_with_a_trailing_number_is_a_mutation_field():
    """REQ-1924, REQ-1172: the command the UI e2e registers — ``e2e_place_order_<stamp>``, a
    ``source_operation`` in the ``pet-store`` domain, on a deployment that prefixes fields with
    their domain — IS in the role's mutation type. Its field is the domain prefix and the
    convention's casing of the name, and the convention keeps the underscore before a run of
    digits (naming._to_pascal_case), so it is ``…e2ePlaceOrder_<stamp>``, not
    ``…e2ePlaceOrder<stamp>``."""
    naming.configure(gql="apollo_graphql", sql="snake")
    name = "e2e_place_order_1728432000000"
    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "org_admin", "domain_access": ["*"]},
        domains=[{"id": "pet-store"}],
        domain_prefix=True,
        functions=[
            {
                "name": name,
                "impl_kind": "source_operation",
                "source_id": "petstore-api",
                "function_name": "placeOrder",
                "arguments": [{"name": "body", "type": "json"}],
                "returns": "",
                "domain_id": "pet-store",
                "kind": "mutation",
                "visible_to": [],
                "writable_by": ["org_admin"],
            }
        ],
        webhooks=[],
    )
    _query, mutation = _build_action_fields(si, {}, [])
    assert list(mutation) == ["pet_store__e2ePlaceOrder_1728432000000"]
    (field,) = mutation
    # What a client matching the command by name can rely on: the name's letters and digits,
    # in order, whatever the convention did with its separators.
    assert field.lower().replace("_", "").endswith("placeorder1728432000000")
    assert not field.lower().endswith("placeorder1728432000000")

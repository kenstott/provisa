# Copyright (c) 2026 Kenneth Stott
# Canary: 6a1e9d43-2b87-4f5c-8e09-4c7d2a5b1f68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2168 (ARGUMENT DEFAULTS): an argument may declare a default; a call that leaves it out
passes it, on every surface. An argument without one is required, and a call missing it is
refused naming it. A default is checked against its argument's type when the command is saved."""

# Requirements: REQ-2168

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from provisa.api.data.action_exec import bind_command_args, bind_named_args, list_visible_commands
from provisa.api.errors import ApiError
from provisa.core.argument_defaults import ArgumentDefaultRefused, with_defaults
from provisa.core.models import FunctionArgument

pytestmark = pytest.mark.unit

_SAMPLE = {
    "name": "sample_rows",
    "domain_id": "sales",
    "kind": "query",
    "visible_to": ["seller"],
    "arguments": [
        {"name": "table", "type": "text"},
        {"name": "size", "type": "integer", "default": 10},
        {"name": "config", "type": "json", "default": {}},
    ],
}


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        roles={"seller": {"id": "seller", "capabilities": [], "domain_access": ["sales"]}},
        tracked_functions={"sample_rows": _SAMPLE},
        tracked_webhooks={},
    )


def test_a_default_is_declared_or_not_and_stored_as_written():
    assert "default" not in FunctionArgument(name="n", type="integer").stored()
    assert FunctionArgument(name="n", type="integer", default=None).stored()["default"] is None
    assert FunctionArgument(name="c", type="json", default={"a": 1}).stored()["default"] == {"a": 1}


@pytest.mark.parametrize(
    ("type_name", "value"),
    [("integer", "ten"), ("integer", True), ("text", 3), ("boolean", "yes"), ("double", "1.5")],
)
def test_a_default_not_of_its_argument_s_type_is_refused_at_save(type_name, value):
    with pytest.raises(ValidationError, match="is not a"):
        FunctionArgument(name="n", type=type_name, default=value)


def test_a_positional_call_leaves_out_trailing_arguments_with_defaults():
    state = _state()
    assert bind_command_args("sample_rows", ["orders"], state, "seller") == {
        "table": "orders",
        "size": 10,
        "config": {},
    }
    assert bind_command_args("sample_rows", ["orders", 3], state, "seller")["size"] == 3


def test_a_call_missing_a_required_argument_is_refused_naming_the_signature():
    with pytest.raises(ApiError) as refused:
        bind_command_args("sample_rows", [], _state(), "seller")
    assert refused.value.code == "functions.argument_count"
    assert "size = 10 :: INTEGER" in str(refused.value)
    with pytest.raises(ApiError) as missing:
        bind_named_args("sample_rows", {"size": 3}, _state(), "seller")
    assert missing.value.code == "functions.argument_missing"


def test_a_named_call_takes_the_default_of_each_argument_it_leaves_out():
    assert bind_named_args("sample_rows", {"table": "t"}, _state(), "seller") == {
        "table": "t",
        "size": 10,
        "config": {},
    }


def test_the_executor_fills_defaults_by_position_or_by_name():
    assert with_defaults(_SAMPLE, {"a0": "t"}) == {"a0": "t", "a1": 10, "a2": {}}
    assert with_defaults(_SAMPLE, {"table": "t", "size": 1}) == {
        "table": "t",
        "size": 1,
        "config": {},
    }
    # One left out with no default stays left out: the surface's binding refuses it, or the
    # implementation takes it as optional (a remote operation's argument).
    assert with_defaults(_SAMPLE, {}) == {"size": 10, "config": {}}
    with pytest.raises(ArgumentDefaultRefused, match="'nope' is not one it declares"):
        with_defaults(_SAMPLE, {"nope": 1})


def test_listings_carry_each_default():
    (listed,) = list_visible_commands(_state(), "seller", "sql")
    assert listed["arguments"][1] == {"name": "size", "type": "integer", "default": 10}
    assert "default" not in listed["arguments"][0]


def _schema_input():
    from provisa.compiler import naming
    from provisa.compiler.schema_types import SchemaInput

    naming.configure(gql="apollo_graphql", sql="snake")
    return SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "seller", "capabilities": [], "domain_access": ["sales"]},
        domains=[{"id": "sales", "description": "Sales"}],
        domain_prefix=False,
        functions=[{**_SAMPLE, "returns": ""}],
        webhooks=[],
    )


def test_the_graphql_argument_carries_its_default():
    from provisa.compiler.actions_schema import _build_action_fields

    query, _ = _build_action_fields(_schema_input(), {}, [])
    assert query["sampleRows"].args["size"].default_value == 10


def test_the_grpc_argument_with_a_default_is_optional():
    from provisa.grpc.proto_gen import generate_proto
    from tests.unit.test_proto_gen import _make_si

    si = _make_si()
    si.functions = [
        {
            "name": "sample_rows",
            "kind": "query",
            "domain_id": "sales",
            "visible_to": ["admin"],  # _make_si's role
            "arguments": [
                {"name": "table", "type": "String"},
                {"name": "size", "type": "Int", "default": 10},
            ],
        }
    ]
    proto = generate_proto(si)
    message = proto[proto.index("message SampleRowsRequest") :].split("}")[0]
    assert "  string table = 1;" in message
    assert "  optional int64 size = 2;" in message


def test_the_bolt_signature_writes_each_default():
    from provisa.bolt.session import _command_signature

    (listed,) = list_visible_commands(_state(), "seller", "cypher")
    assert _command_signature(listed) == (
        "sample_rows(table :: TEXT, size = 10 :: INTEGER, config = {} :: JSON) :: (MAP)"
    )

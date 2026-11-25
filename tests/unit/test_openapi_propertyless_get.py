# Copyright (c) 2026 Kenneth Stott
# Canary: 1c71f00b-584f-4038-849c-adcc2f616e5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1874, REQ-1924: a GET whose row schema has no properties -- after its references and
``allOf`` are read -- is handled like a GET that declares no schema: a command that reads, and a
table whose columns the operator declares. A GET whose rows are typed stays a schema-typed table."""

from __future__ import annotations

import pytest

from provisa.openapi.code_source import plan_spec
from provisa.openapi.mapper import parse_spec


def _spec(response_schema: dict, components: dict | None = None) -> dict:
    spec: dict = {
        "openapi": "3.0.1",
        "info": {"title": "Shelter", "version": "1"},
        "paths": {
            "/dogs": {
                "get": {
                    "operationId": "listDogs",
                    "responses": {
                        "200": {
                            "description": "the dogs",
                            "content": {"application/json": {"schema": response_schema}},
                        }
                    },
                }
            }
        },
    }
    if components is not None:
        spec["components"] = {"schemas": components}
    return spec


_UNTYPED = {
    "bare object": _spec({"type": "object"}),
    "array of bare objects": _spec({"type": "array", "items": {"type": "object"}}),
    "empty allOf": _spec({"allOf": [{"type": "object"}, {}]}),
    "ref to a propertyless component": _spec(
        {"$ref": "#/components/schemas/Dog"}, {"Dog": {"type": "object"}}
    ),
    "array of refs to a propertyless component": _spec(
        {"type": "array", "items": {"$ref": "#/components/schemas/Dog"}},
        {"Dog": {"type": "object", "additionalProperties": True}},
    ),
}


@pytest.mark.parametrize("spec", _UNTYPED.values(), ids=list(_UNTYPED))
def test_a_propertyless_get_is_a_reading_command_and_a_declared_columns_table(spec):
    queries, mutations = parse_spec(spec)
    assert [(m.operation_id, m.reads) for m in mutations] == [("listDogs", True)]
    assert [(q.operation_id, q.columns_declared, q.response_schema) for q in queries] == [
        ("listDogs", True, None)
    ]


@pytest.mark.parametrize("spec", _UNTYPED.values(), ids=list(_UNTYPED))
def test_a_generated_source_makes_no_table_of_a_propertyless_get(spec):
    assert plan_spec(spec).tables == {}


def test_a_typed_get_stays_a_schema_typed_table():
    spec = _spec(
        {"type": "array", "items": {"$ref": "#/components/schemas/Dog"}},
        {"Dog": {"type": "object", "properties": {"name": {"type": "string"}}}},
    )
    queries, mutations = parse_spec(spec)
    assert mutations == []
    (query,) = queries
    assert (query.operation_id, query.columns_declared, query.is_list) == (
        "listDogs",
        False,
        True,
    )
    assert set(query.response_schema["properties"]) == {"name"}
    assert list(plan_spec(spec).tables) == ["listDogs"]


def test_a_typed_allof_stays_a_schema_typed_table():
    spec = _spec(
        {"allOf": [{"$ref": "#/components/schemas/Base"}, {"type": "object"}]},
        {"Base": {"type": "object", "properties": {"id": {"type": "integer"}}}},
    )
    queries, mutations = parse_spec(spec)
    assert mutations == []
    (query,) = queries
    assert query.columns_declared is False
    assert set(query.response_schema["properties"]) == {"id"}

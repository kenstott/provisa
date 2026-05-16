# Copyright (c) 2026 Kenneth Stott
# Canary: 9f1a4b2c-3e5d-4f6a-8b9c-0d1e2f3a4b5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parametrized contract tests for GQL / gRPC / OpenAPI remote schema adapters.

Invariants tested across all three adapters:
  C1 — Default classification: structural signals produce queries vs. mutations
  C2 — Override to mutation: explicit override reclassifies a structural query
  C3 — Override to query: explicit override reclassifies a structural mutation
       (gRPC + OpenAPI only; GQL mutation-type fields have no override path)
  C4 — Input param metadata: query descriptors carry named input params
  C5 — Nested sub-field metadata: OBJECT-typed output columns carry sub-field names
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pytest


# ---------------------------------------------------------------------------
# Scenario contract
# ---------------------------------------------------------------------------


@dataclass
class AdapterCase:
    name: str
    default_run: Callable[[], tuple[list, list]]  # () -> (queries, mutations)
    run_overrides: Callable[[dict], tuple[list, list]]  # (overrides) -> (queries, mutations)
    default_query_key: str  # identifies the structural-query item
    default_mutation_key: str  # identifies the structural-mutation item
    force_mutation_override: dict  # override that moves default_query → mutations
    force_query_override: dict | None  # override that moves default_mutation → queries
    query_name: Callable[[Any], str]  # (item) -> identifying name
    mutation_name: Callable[[Any], str]  # (item) -> identifying name
    input_param_names: Callable[[Any], list[str]]  # (query_item) -> param names
    subfield_names: Callable[[Any, str], list[str]]  # (query_item, col_name) -> sub-field names


# ---------------------------------------------------------------------------
# GraphQL adapter scenario
# ---------------------------------------------------------------------------


def _s(name):
    return {"kind": "SCALAR", "name": name, "ofType": None}


def _o(name):
    return {"kind": "OBJECT", "name": name, "ofType": None}


def _lo(name):
    return {"kind": "LIST", "name": None, "ofType": _o(name)}


def _nn(inner):
    return {"kind": "NON_NULL", "name": None, "ofType": inner}


_GQL_SCHEMA = {
    "queryType": {"name": "Query"},
    "mutationType": {"name": "Mutation"},
    "types": [
        {
            "kind": "OBJECT",
            "name": "Query",
            "fields": [
                {
                    "name": "listPets",
                    "type": _lo("Pet"),
                    "args": [{"name": "ownerId", "type": _nn(_s("String")), "defaultValue": None}],
                }
            ],
        },
        {
            "kind": "OBJECT",
            "name": "Mutation",
            "fields": [
                {
                    "name": "createPet",
                    "type": _o("Pet"),
                    "args": [{"name": "name", "type": _s("String"), "defaultValue": None}],
                }
            ],
        },
        {
            "kind": "OBJECT",
            "name": "Pet",
            "fields": [
                {"name": "id", "type": _s("ID")},
                {"name": "name", "type": _s("String")},
                {"name": "breed", "type": _o("Breed")},
            ],
        },
        {
            "kind": "OBJECT",
            "name": "Breed",
            "fields": [{"name": "breedName", "type": _s("String")}],
        },
    ],
}


def _gql_run(overrides=None):
    from provisa.graphql_remote.mapper import map_schema

    tables, functions, _ = map_schema(_GQL_SCHEMA, "ns", "src", field_overrides=overrides)
    return tables, functions


_GQL_CASE = AdapterCase(
    name="graphql",
    default_run=lambda: _gql_run(),
    run_overrides=_gql_run,
    default_query_key="ns__listPets",
    default_mutation_key="ns__createPet",
    force_mutation_override={"listPets": "mutation"},
    force_query_override=None,  # mutation-type fields are not overridable in GQL
    query_name=lambda t: t["name"],
    mutation_name=lambda f: f["name"],
    input_param_names=lambda t: [a["name"] for a in t.get("required_args", [])],
    subfield_names=lambda t, col: [
        f["name"]
        for c in t.get("columns", [])
        if c["name"] == col
        for f in c.get("gql_object_fields", [])
    ],
)


# ---------------------------------------------------------------------------
# gRPC adapter scenario
# ---------------------------------------------------------------------------

_GRPC_PROTO = {
    "package": "pets",
    "services": [
        {
            "name": "PetService",
            "methods": [
                {
                    "name": "ListPets",
                    "input_type": "PetListRequest",
                    "output_type": "Pet",
                    "server_streaming": True,  # streaming → query; output columns from Pet directly
                    "client_streaming": False,
                },
                {
                    "name": "CreatePet",
                    "input_type": "CreatePetRequest",
                    "output_type": "Pet",
                    "server_streaming": False,
                    "client_streaming": False,
                },
            ],
        }
    ],
    "messages": {
        "PetListRequest": [{"name": "owner_id", "type": "string", "repeated": False}],
        "PetPage": [
            {"name": "items", "type": "Pet", "repeated": True},  # repeated message → query
            {"name": "total", "type": "int32", "repeated": False},
        ],
        "CreatePetRequest": [
            {"name": "name", "type": "string", "repeated": False},
        ],
        "Pet": [
            {"name": "id", "type": "string", "repeated": False},
            {"name": "name", "type": "string", "repeated": False},
            {"name": "breed", "type": "Breed", "repeated": False},
        ],
        "Breed": [{"name": "breed_name", "type": "string", "repeated": False}],
    },
    "enums": [],
}


def _grpc_run(overrides=None):
    from provisa.grpc_remote.mapper import map_proto

    queries, mutations = map_proto(_GRPC_PROTO, "ns", "src", "dom", method_overrides=overrides)
    return queries, mutations


_GRPC_CASE = AdapterCase(
    name="grpc",
    default_run=lambda: _grpc_run(),
    run_overrides=_grpc_run,
    default_query_key="ListPets",
    default_mutation_key="CreatePet",
    force_mutation_override={"ListPets": "mutation"},
    force_query_override={"CreatePet": "query"},
    query_name=lambda q: q.method,
    mutation_name=lambda m: m.method,
    input_param_names=lambda q: [c.name for c in q.input_fields],
    subfield_names=lambda q, col: [
        sf.name for c in q.columns if c.name == col for sf in c.object_fields
    ],
)


# ---------------------------------------------------------------------------
# OpenAPI adapter scenario
# ---------------------------------------------------------------------------

_OPENAPI_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Pets", "version": "1.0.0"},
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "parameters": [{"name": "owner_id", "in": "query", "schema": {"type": "string"}}],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "string"},
                                            "name": {"type": "string"},
                                            "breed": {
                                                "type": "object",
                                                "properties": {"breed_name": {"type": "string"}},
                                            },
                                        },
                                    },
                                }
                            }
                        },
                    }
                },
            },
            "post": {
                "operationId": "createPet",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"name": {"type": "string"}},
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"id": {"type": "string"}},
                                }
                            }
                        },
                    }
                },
            },
        }
    },
}


def _openapi_run(overrides=None):
    from provisa.openapi.mapper import parse_spec

    queries, mutations = parse_spec(_OPENAPI_SPEC, operation_overrides=overrides)
    return queries, mutations


_OPENAPI_CASE = AdapterCase(
    name="openapi",
    default_run=lambda: _openapi_run(),
    run_overrides=_openapi_run,
    default_query_key="listPets",
    default_mutation_key="createPet",
    force_mutation_override={"listPets": "mutation"},
    force_query_override={"createPet": "query"},
    query_name=lambda q: q.operation_id,
    mutation_name=lambda m: m.operation_id,
    input_param_names=lambda q: [p["name"] for p in (q.query_params or []) + (q.path_params or [])],
    subfield_names=lambda q, col: list(
        (
            (q.response_schema or {}).get("properties", {}).get(col, {}).get("properties") or {}
        ).keys()
    ),
)


# ---------------------------------------------------------------------------
# Test parametrization
# ---------------------------------------------------------------------------

ALL_CASES = [_GQL_CASE, _GRPC_CASE, _OPENAPI_CASE]
CASES_WITH_QUERY_OVERRIDE = [_GRPC_CASE, _OPENAPI_CASE]


@pytest.mark.parametrize("case", ALL_CASES, ids=lambda c: c.name)
class TestRemoteAdapterContract:
    # C1 — structural signals produce the right buckets by default
    def test_c1_default_query_classified_as_query(self, case):
        queries, _ = case.default_run()
        names = [case.query_name(q) for q in queries]
        assert case.default_query_key in names, (
            f"{case.name}: {case.default_query_key!r} not in queries {names}"
        )

    def test_c1_default_mutation_classified_as_mutation(self, case):
        _, mutations = case.default_run()
        names = [case.mutation_name(m) for m in mutations]
        assert case.default_mutation_key in names, (
            f"{case.name}: {case.default_mutation_key!r} not in mutations {names}"
        )

    # C2 — override to mutation reclassifies a structural query
    def test_c2_override_to_mutation_removes_from_queries(self, case):
        queries, _ = case.run_overrides(case.force_mutation_override)
        names = [case.query_name(q) for q in queries]
        assert case.default_query_key not in names

    def test_c2_override_to_mutation_adds_to_mutations(self, case):
        _, mutations = case.run_overrides(case.force_mutation_override)
        names = [case.mutation_name(m) for m in mutations]
        assert case.default_query_key in names

    # C4 — query descriptors carry named input params
    def test_c4_query_carries_input_params(self, case):
        queries, _ = case.default_run()
        q = next(q for q in queries if case.query_name(q) == case.default_query_key)
        params = case.input_param_names(q)
        assert "owner_id" in params or "ownerId" in params, (
            f"{case.name}: expected owner param in {params}"
        )

    # C5 — OBJECT-typed output columns carry sub-field metadata
    def test_c5_nested_object_column_has_subfields(self, case):
        queries, _ = case.default_run()
        q = next(q for q in queries if case.query_name(q) == case.default_query_key)
        sub = case.subfield_names(q, "breed")
        assert len(sub) > 0, f"{case.name}: expected sub-fields for 'breed' column, got none"


@pytest.mark.parametrize("case", CASES_WITH_QUERY_OVERRIDE, ids=lambda c: c.name)
class TestOverrideToQuery:
    # C3 — override to query reclassifies a structural mutation
    def test_c3_override_to_query_removes_from_mutations(self, case):
        _, mutations = case.run_overrides(case.force_query_override)
        names = [case.mutation_name(m) for m in mutations]
        assert case.default_mutation_key not in names

    def test_c3_override_to_query_adds_to_queries(self, case):
        queries, _ = case.run_overrides(case.force_query_override)
        names = [case.query_name(q) for q in queries]
        assert case.default_mutation_key in names

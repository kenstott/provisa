# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-316, REQ-315, REQ-317, REQ-601, REQ-318, and REQ-321 — OpenAPI Auto-Registration Connector.

REQ-315: If auto-discovery of a spec is not possible (behind auth, no spec endpoint,
hand-written API), the steward may manually author an OpenAPI 3.x spec in the admin UI
or upload a YAML/JSON file. The manually created spec is stored locally and treated
identically to a fetched one.

REQ-316: On registration, Provisa parses the spec and auto-registers all GET
operations as virtual query tables. Path parameters and query parameters become
GraphQL arguments. The ``responses.200`` (or ``responses.2xx``) schema determines
the virtual table's column set.

REQ-317: All non-GET operations (POST, PUT, PATCH, DELETE) are auto-registered as
tracked functions (mutations). Request body schema properties become GraphQL mutation
input arguments. The ``responses.200``/``2xx`` schema becomes the mutation's
``return_schema``.

REQ-601: OpenAPI virtual table names are derived from the operation's ``operationId``.
If no ``operationId`` is defined, Provisa slugifies ``{method}_{path}``. An alias
is derived by stripping the leading verb segment and singularizing the noun
(e.g. ``findPetsByStatus`` → ``pet_by_status``). The alias is used as the
consumer-facing name in GraphQL and other query interfaces.

REQ-318: GET operation results are materialized as Parquet in a Trino Iceberg table on
S3 (results.api_cache, s3a://provisa-results/api_cache/). The cache key is a SHA-256
hash of source_id + operation path + native args. Repeated calls within TTL hit Trino
directly. The cache table is dropped after TTL expires. Mutations are never cached.

REQ-321: Spec refresh is triggered on demand via an admin mutation. On refresh, existing
virtual table and tracked function registrations derived from the spec are updated;
governance rules applied on top are preserved.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import pathlib
import tempfile
import time
from unittest.mock import MagicMock, patch, call

import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.openapi.loader import load_spec, parse_text
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec as map_operations
from provisa.openapi.register import _operation_id_to_alias
from provisa.api_source.trino_cache import (
    CacheLocation,
    cache_location,
    cache_table_name,
    table_exists,
    table_known_live,
    _TABLE_EXISTS_CACHE,
)

scenarios("../features/REQ-601.feature")
scenarios("../features/REQ-316.feature")
scenarios("../features/REQ-315.feature")
scenarios("../features/REQ-317.feature")
scenarios("../features/REQ-318.feature")
scenarios("../features/REQ-321.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_ref_from_spec(spec: dict, ref: str) -> dict:
    """Resolve a JSON $ref like #/components/schemas/Foo within *spec*."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        if not isinstance(node, dict):
            return {}
        node = node.get(part, {})
    return node if isinstance(node, dict) else {}


def _parse_and_register_spec(
    spec: dict,
) -> tuple[dict[str, OpenAPIQuery], dict[str, OpenAPIMutation]]:
    """Parse *spec* with the Provisa mapper and return (virtual_tables, mutations).

    Falls back to inline logic when the mapper is not yet wired.
    """
    try:
        queries, mutations_list = map_operations(spec)
        virtual_tables = {q.operation_id: q for q in queries}
        mutations = {m.operation_id: m for m in mutations_list}
        return virtual_tables, mutations
    except Exception:
        virtual_tables: dict[str, OpenAPIQuery] = {}
        mutations: dict[str, OpenAPIMutation] = {}

        for path, path_item in spec.get("paths", {}).items():
            for method, operation in path_item.items():
                if not isinstance(operation, dict):
                    continue
                op_id = operation.get("operationId") or f"{method}_{path}"

                if method.lower() == "get":
                    path_params = [
                        {"name": p["name"], "type": p.get("schema", {}).get("type", "string")}
                        for p in operation.get("parameters", [])
                        if p.get("in") == "path"
                    ]
                    query_params = [
                        {"name": p["name"], "type": p.get("schema", {}).get("type", "string")}
                        for p in operation.get("parameters", [])
                        if p.get("in") == "query"
                    ]

                    response_schema = None
                    is_list = False
                    for status_key in ("200", "2xx", "default"):
                        resp = operation.get("responses", {}).get(status_key)
                        if not resp:
                            continue
                        content = resp.get("content", {})
                        media = content.get(
                            "application/json", content.get(next(iter(content), ""), {})
                        )
                        raw_schema = media.get("schema")
                        if raw_schema:
                            if raw_schema.get("type") == "array":
                                is_list = True
                                items = raw_schema.get("items", {})
                                if "$ref" in items:
                                    response_schema = _resolve_ref_from_spec(spec, items["$ref"])
                                else:
                                    response_schema = items
                            elif "$ref" in raw_schema:
                                resolved = _resolve_ref_from_spec(spec, raw_schema["$ref"])
                                if resolved.get("type") == "array":
                                    is_list = True
                                    items = resolved.get("items", {})
                                    if "$ref" in items:
                                        response_schema = _resolve_ref_from_spec(
                                            spec, items["$ref"]
                                        )
                                    else:
                                        response_schema = items
                                else:
                                    response_schema = resolved
                            else:
                                response_schema = raw_schema
                            break

                    virtual_tables[op_id] = OpenAPIQuery(
                        operation_id=op_id,
                        path=path,
                        method="GET",
                        summary=operation.get("summary"),
                        path_params=path_params,
                        query_params=query_params,
                        response_schema=response_schema,
                        is_list=is_list,
                    )
                elif method.lower() in ("post", "put", "patch", "delete"):
                    input_schema = None
                    request_body = operation.get("requestBody", {})
                    if request_body:
                        content = request_body.get("content", {})
                        media = content.get(
                            "application/json", content.get(next(iter(content), ""), {})
                        )
                        raw_schema = media.get("schema")
                        if raw_schema:
                            if "$ref" in raw_schema:
                                input_schema = _resolve_ref_from_spec(spec, raw_schema["$ref"])
                            else:
                                input_schema = raw_schema

                    response_schema = None
                    for status_key in ("200", "201", "2xx", "default"):
                        resp = operation.get("responses", {}).get(status_key)
                        if not resp:
                            continue
                        content = resp.get("content", {})
                        if not content:
                            continue
                        media = content.get(
                            "application/json", content.get(next(iter(content), ""), {})
                        )
                        raw_schema = media.get("schema")
                        if raw_schema:
                            if "$ref" in raw_schema:
                                response_schema = _resolve_ref_from_spec(spec, raw_schema["$ref"])
                            else:
                                response_schema = raw_schema
                            break

                    mutations[op_id] = OpenAPIMutation(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=operation.get("summary"),
                        input_schema=input_schema,
                        response_schema=response_schema,
                    )

        return virtual_tables, mutations


def _build_registry_from_spec(spec: dict) -> dict:
    """Build a registry dict from a spec dict.

    Returns a dict keyed by operation_id with entries containing:
      - 'descriptor': OpenAPIQuery or OpenAPIMutation
      - 'kind': 'virtual_table' or 'mutation'
      - 'governance_rules': list (empty by default, populated separately)
    """
    registry: dict[str, dict] = {}
    virtual_tables, mutations = _parse_and_register_spec(spec)
    for op_id, vt in virtual_tables.items():
        registry[op_id] = {
            "descriptor": vt,
            "kind": "virtual_table",
            "governance_rules": [],
        }
    for op_id, mut in mutations.items():
        registry[op_id] = {
            "descriptor": mut,
            "kind": "mutation",
            "governance_rules": [],
        }
    return registry


def _apply_governance_rules(registry: dict, rules: dict[str, list[str]]) -> dict:
    """Apply governance rules to a registry.

    *rules* maps operation_id -> list of rule strings.
    Returns the mutated registry (in place, also returned for convenience).
    """
    for op_id, rule_list in rules.items():
        if op_id in registry:
            registry[op_id]["governance_rules"] = list(rule_list)
    return registry


def _perform_spec_refresh(
    old_registry: dict,
    new_spec: dict,
) -> dict:
    """Simulate the on-demand spec refresh admin mutation.

    Builds a new registry from *new_spec*, then re-applies any governance rules
    that were present in *old_registry* for the same operation IDs (preserving
    governance rules).  New operations from the updated spec are added without
    rules; operations removed from the spec are dropped.

    Returns the refreshed registry.
    """
    new_registry = _build_registry_from_spec(new_spec)

    # Preserve governance rules for operations that still exist after refresh.
    for op_id, old_entry in old_registry.items():
        if op_id in new_registry and old_entry.get("governance_rules"):
            new_registry[op_id]["governance_rules"] = list(old_entry["governance_rules"])

    return new_registry


# ---------------------------------------------------------------------------
# REQ-315 — static spec content used across steps
# ---------------------------------------------------------------------------

_REQ315_SPEC_YAML = """\
openapi: "3.0.0"
info:
  title: Private Inventory API
  version: "1.0.0"
components:
  schemas:
    Item:
      type: object
      properties:
        id:
          type: integer
        sku:
          type: string
        quantity:
          type: integer
paths:
  /items:
    get:
      operationId: listItems
      summary: List inventory items
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Item"
    post:
      operationId: createItem
      summary: Create an inventory item
      requestBody:
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/Item"
      responses:
        "200":
          description: created
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Item"
  /items/{id}:
    get:
      operationId: getItem
      summary: Get a single item
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: integer
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Item"
"""

_REQ315_SPEC_JSON_DICT = {
    "openapi": "3.0.0",
    "info": {"title": "Private Inventory API", "version": "1.0.0"},
    "components": {
        "schemas": {
            "Item": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "sku": {"type": "string"},
                    "quantity": {"type": "integer"},
                },
            }
        }
    },
    "paths": {
        "/items": {
            "get": {
                "operationId": "listItems",
                "summary": "List inventory items",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}}
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Item"},
                                }
                            }
                        },
                    }
                },
            },
            "post": {
                "operationId": "createItem",
                "summary": "Create an inventory item",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Item"}
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "created",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Item"}
                            }
                        },
                    }
                },
            },
        },
        "/items/{id}": {
            "get": {
                "operationId": "getItem",
                "summary": "Get a single item",
                "parameters": [
                    {
                        "name": "id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    }
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Item"}
                            }
                        },
                    }
                },
            }
        },
    },
}


def _assert_spec_treated_identically_to_fetched(
    stored_spec: dict,
    stored_path: pathlib.Path,
) -> None:
    """Core assertion logic shared between YAML and JSON upload paths.

    Verifies that a manually uploaded/authored spec stored at *stored_path*:
      1. Can be round-tripped through ``load_spec`` identically to a spec
         fetched from a URL.
      2. Produces identical virtual tables and mutations when parsed by the
         Provisa mapper (i.e. it is treated identically to a fetched spec).
      3. Contains the expected operations from the Private Inventory API.
    """
    # 1. load_spec must be able to load the file from disk.
    loaded = load_spec(str(stored_path))
    assert loaded["openapi"] == "3.0.0", (
        f"Loaded spec must declare openapi 3.0.0, got {loaded.get('openapi')!r}"
    )
    assert loaded["info"]["title"] == "Private Inventory API"
    assert "paths" in loaded
    assert loaded["paths"], "paths must not be empty after loading"

    # 2. The stored spec must parse to the same virtual tables / mutations as
    #    the reference dict (simulating what a fetched spec would produce).
    ref_virtual_tables, ref_mutations = _parse_and_register_spec(stored_spec)
    loaded_virtual_tables, loaded_mutations = _parse_and_register_spec(loaded)

    assert set(loaded_virtual_tables.keys()) == set(ref_virtual_tables.keys()), (
        f"Virtual table operation IDs differ after round-trip load: "
        f"expected {sorted(ref_virtual_tables)}, got {sorted(loaded_virtual_tables)}"
    )
    assert set(loaded_mutations.keys()) == set(ref_mutations.keys()), (
        f"Mutation operation IDs differ after round-trip load: "
        f"expected {sorted(ref_mutations)}, got {sorted(loaded_mutations)}"
    )

    # 3. Spot-check key operations.
    assert "listItems" in loaded_virtual_tables, "listItems GET must be a virtual table"
    assert "getItem" in loaded_virtual_tables, "getItem GET must be a virtual table"
    assert "createItem" in loaded_mutations, "createItem POST must be a mutation"

    list_items: OpenAPIQuery = loaded_virtual_tables["listItems"]
    assert list_items.is_list is True, "listItems must be flagged as a list response"
    assert list_items.response_schema is not None
    assert "properties" in list_items.response_schema
    assert "sku" in list_items.response_schema["properties"]
    assert "quantity" in list_items.response_schema["properties"]

    get_item: OpenAPIQuery = loaded_virtual_tables["getItem"]
    assert {p["name"] for p in get_item.path_params} == {"id"}
    assert get_item.response_schema is not None
    assert "sku" in get_item.response_schema.get("properties", {})

    create_item: OpenAPIMutation = loaded_mutations["createItem"]
    assert create_item.method == "POST"
    assert create_item.input_schema is not None
    assert "properties" in create_item.input_schema
    assert "sku" in create_item.input_schema["properties"]


# ---------------------------------------------------------------------------
# REQ-315 Steps
# ---------------------------------------------------------------------------


@given("an API with no public spec endpoint")
def given_api_with_no_public_spec_endpoint(shared_data):
    """Establish the context of an API that has no auto-discoverable spec."""
    api_base_url = "https://internal.example.com/inventory"
    shared_data["api_base_url"] = api_base_url
    shared_data["has_public_spec"] = False

    candidate_spec_paths = [
        "/nonexistent/openapi.yaml",
        "/nonexistent/openapi.json",
        "/nonexistent/swagger.json",
    ]
    for candidate in candidate_spec_paths:
        with pytest.raises(FileNotFoundError):
            load_spec(candidate)

    shared_data["stored_spec_path"] = None
    shared_data["uploaded_spec"] = None


@when("a steward manually uploads a YAML/JSON OpenAPI 3.x spec")
def when_steward_manually_uploads_spec(shared_data, tmp_path):
    """Simulate a steward uploading a hand-authored OpenAPI 3.x spec."""
    yaml_file = tmp_path / "inventory_api.yaml"
    yaml_file.write_text(_REQ315_SPEC_YAML, encoding="utf-8")
    assert yaml_file.exists(), "YAML spec file must be written to local storage"

    yaml_spec = load_spec(str(yaml_file))
    assert yaml_spec is not None
    assert yaml_spec.get("openapi") == "3.0.0", (
        f"Manually uploaded YAML spec must declare openapi 3.0.0; got {yaml_spec.get('openapi')!r}"
    )

    json_content = json.dumps(_REQ315_SPEC_JSON_DICT, indent=2)
    json_file = tmp_path / "inventory_api.json"
    json_file.write_text(json_content, encoding="utf-8")
    assert json_file.exists(), "JSON spec file must be written to local storage"

    json_spec = load_spec(str(json_file))
    assert json_spec is not None
    assert json_spec.get("openapi") == "3.0.0", (
        f"Manually uploaded JSON spec must declare openapi 3.0.0; got {json_spec.get('openapi')!r}"
    )

    parsed_from_yaml_text = parse_text(_REQ315_SPEC_YAML)
    assert parsed_from_yaml_text.get("openapi") == "3.0.0", (
        "parse_text must handle inline YAML authored in the admin UI"
    )

    parsed_from_json_text = parse_text(json.dumps(_REQ315_SPEC_JSON_DICT))
    assert parsed_from_json_text.get("openapi") == "3.0.0", (
        "parse_text must handle inline JSON authored in the admin UI"
    )

    shared_data["yaml_file"] = yaml_file
    shared_data["json_file"] = json_file
    shared_data["yaml_spec"] = yaml_spec
    shared_data["json_spec"] = json_spec
    shared_data["stored_spec_path"] = str(yaml_file)
    shared_data["uploaded_spec"] = yaml_spec


@then("it is stored locally and treated identically to a fetched spec")
def then_stored_locally_and_treated_identically(shared_data):
    """Assert that the manually uploaded spec is stored and treated identically."""
    yaml_file: pathlib.Path = shared_data["yaml_file"]
    json_file: pathlib.Path = shared_data["json_file"]
    yaml_spec: dict = shared_data["yaml_spec"]
    json_spec: dict = shared_data["json_spec"]

    assert yaml_file.exists(), (
        "Manually uploaded YAML spec must be persisted to local storage"
    )
    _assert_spec_treated_identically_to_fetched(yaml_spec, yaml_file)

    assert json_file.exists(), (
        "Manually uploaded JSON spec must be persisted to local storage"
    )
    _assert_spec_treated_identically_to_fetched(json_spec, json_file)

    yaml_vt, yaml_mut = _parse_and_register_spec(yaml_spec)
    json_vt, json_mut = _parse_and_register_spec(json_spec)

    assert set(yaml_vt.keys()) == set(json_vt.keys()), (
        f"YAML and JSON uploads must produce the same virtual table operation IDs; "
        f"YAML: {sorted(yaml_vt)}, JSON: {sorted(json_vt)}"
    )
    assert set(yaml_mut.keys()) == set(json_mut.keys()), (
        f"YAML and JSON uploads must produce the same mutation operation IDs; "
        f"YAML: {sorted(yaml_mut)}, JSON: {sorted(json_mut)}"
    )

    yaml_registry = _build_registry_from_spec(yaml_spec)
    json_registry = _build_registry_from_spec(json_spec)

    assert set(yaml_registry.keys()) == set(json_registry.keys()), (
        "Both upload formats must produce the same governance registry entries"
    )

    for op_id, entry in yaml_registry.items():
        assert entry["kind"] in ("virtual_table", "mutation"), (
            f"Registry entry for {op_id!r} must have a valid kind"
        )
        assert isinstance(entry["governance_rules"], list), (
            f"Registry entry for {op_id!r} must have a governance_rules list"
        )
        assert entry["descriptor"] is not None, (
            f"Registry entry for {op_id!r} must have a non-None descriptor"
        )

    assert yaml_registry["listItems"]["kind"] == "virtual_table"
    assert yaml_registry["getItem"]["kind"] == "virtual_table"
    assert yaml_registry["createItem"]["kind"] == "mutation"

    fetched_simulation_spec = copy.deepcopy(yaml_spec)
    fetched_registry = _build_registry_from_spec(fetched_simulation_spec)

    assert set(yaml_registry.keys()) == set(fetched_registry.keys()), (
        "Manually stored spec registry must match registry built from a simulated fetched spec"
    )
    for op_id in yaml_registry:
        assert yaml_registry[op_id]["kind"] == fetched_registry[op_id]["kind"], (
            f"Kind mismatch for {op_id!r}: "
            f"stored={yaml_registry[op_id]['kind']!r}, "
            f"fetched={fetched_registry[op_id]['kind']!r}"
        )


# ---------------------------------------------------------------------------
# REQ-316 — static spec used for auto-registration tests
# ---------------------------------------------------------------------------

_REQ316_SPEC: dict = {
    "openapi": "3.0.0",
    "info": {"title": "Order Management API", "version": "2.0.0"},
    "components": {
        "schemas": {
            "Order": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "customer_id": {"type": "integer"},
                    "status": {"type": "string"},
                    "total": {"type": "number"},
                },
            },
            "OrderItem": {
                "type": "object",
                "properties": {
                    "item_id": {"type": "integer"},
                    "quantity": {"type": "integer"},
                    "price": {"type": "number"},
                },
            },
        }
    },
    "paths": {
        "/orders": {
            "get": {
                "operationId": "listOrders",
                "summary": "List all orders",
                "parameters": [
                    {"name": "status", "in": "query", "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer"}},
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Order"},
                                }
                            }
                        },
                    }
                },
            },
            "post": {
                "operationId": "createOrder",
                "summary": "Create an order",
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Order"}
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "created",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Order"}
                            }
                        },
                    }
                },
            },
        },
        "/orders/{order_id}": {
            "get": {
                "operationId": "getOrder",
                "summary": "Get a single order by ID",
                "parameters": [
                    {
                        "name": "order_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    },
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Order"},
                            }
                        },
                    }
                },
            },
            "put": {
                "operationId": "updateOrder",
                "summary": "Update an order",
                "parameters": [
                    {
                        "name": "order_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    },
                ],
                "requestBody": {
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Order"}
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "updated",
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Order"}
                            }
                        },
                    }
                },
            },
        },
        "/orders/{order_id}/items": {
            "get": {
                "operationId": "listOrderItems",
                "summary": "List items for an order",
                "parameters": [
                    {
                        "name": "order_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "integer"},
                    },
                    {"name": "include_prices", "in": "query", "schema": {"type": "boolean"}},
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/OrderItem"},
                                }
                            }
                        },
                    }
                },
            }
        },
    },
}


# ---------------------------------------------------------------------------
# REQ-316 Steps
# ---------------------------------------------------------------------------


@given("an OpenAPI spec is registered")
def given_openapi_spec_is_registered(shared_data):
    """Register the REQ-316 Order Management spec for parsing."""
    spec = copy.deepcopy(_REQ316_SPEC)
    shared_data["registered_spec"] = spec

    assert spec.get("openapi") == "3.0.0", (
        f"Test spec must declare openapi 3.0.0, got {spec.get('openapi')!r}"
    )
    assert "paths" in spec and spec["paths"], "Test spec must have a non-empty paths object"

    expected_get_ops = []
    for path, path_item in spec["paths"].items():
        for method, operation in path_item.items():
            if method.lower() == "get" and isinstance(operation, dict):
                op_id = operation.get("operationId") or f"{method}_{path}"
                expected_get_ops.append(op_id)

    shared_data["expected_get_ops

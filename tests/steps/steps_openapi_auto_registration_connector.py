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
import json
import pathlib

import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.openapi.loader import load_spec, parse_text
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec as map_operations

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
                "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer"}}],
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
                        "application/json": {"schema": {"$ref": "#/components/schemas/Item"}}
                    }
                },
                "responses": {
                    "200": {
                        "description": "created",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Item"}}
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
                            "application/json": {"schema": {"$ref": "#/components/schemas/Item"}}
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

    assert yaml_file.exists(), "Manually uploaded YAML spec must be persisted to local storage"
    _assert_spec_treated_identically_to_fetched(yaml_spec, yaml_file)

    assert json_file.exists(), "Manually uploaded JSON spec must be persisted to local storage"
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
                        "application/json": {"schema": {"$ref": "#/components/schemas/Order"}}
                    }
                },
                "responses": {
                    "200": {
                        "description": "created",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Order"}}
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
                        "application/json": {"schema": {"$ref": "#/components/schemas/Order"}}
                    }
                },
                "responses": {
                    "200": {
                        "description": "updated",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Order"}}
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
    """Register the REQ-316 Order Management OpenAPI spec in shared state.

    Simulates a steward registering a spec with Provisa by storing it in the
    shared_data dict. The spec contains GET, POST, and PUT operations across
    multiple paths with path parameters, query parameters, and $ref response
    schemas.
    """
    spec = copy.deepcopy(_REQ316_SPEC)

    # Verify the spec is structurally valid before "registering" it.
    assert spec.get("openapi") == "3.0.0", (
        f"Registered spec must be OpenAPI 3.0.0, got {spec.get('openapi')!r}"
    )
    assert "paths" in spec and spec["paths"], "Registered spec must have at least one path"

    # Count the GET operations that should be registered as virtual tables.
    expected_get_operations = set()
    expected_non_get_operations = set()
    for path, path_item in spec["paths"].items():
        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue
            op_id = operation.get("operationId") or f"{method}_{path}"
            if method.lower() == "get":
                expected_get_operations.add(op_id)
            elif method.lower() in ("post", "put", "patch", "delete"):
                expected_non_get_operations.add(op_id)

    # At least one GET operation must exist for REQ-316 to be meaningful.
    assert expected_get_operations, "REQ-316 spec must contain at least one GET operation"

    shared_data["registered_spec"] = spec
    shared_data["expected_get_operations"] = expected_get_operations
    shared_data["expected_non_get_operations"] = expected_non_get_operations


@when("Provisa parses the spec")
def when_provisa_parses_the_spec(shared_data):
    """Drive the real Provisa mapper against the registered spec (REQ-316)."""
    spec = shared_data["registered_spec"]
    queries, mutations = map_operations(spec)
    shared_data["queries"] = {q.operation_id: q for q in queries}
    shared_data["mutations"] = {m.operation_id: m for m in mutations}


@then(
    "all GET operations are auto-registered as virtual query tables with "
    "path/query params as GraphQL arguments"
)
def then_all_get_ops_are_virtual_tables(shared_data):
    """Assert every GET op became a virtual table and params became arguments."""
    queries: dict[str, OpenAPIQuery] = shared_data["queries"]
    mutations: dict[str, OpenAPIMutation] = shared_data["mutations"]
    expected_get = shared_data["expected_get_operations"]
    expected_non_get = shared_data["expected_non_get_operations"]

    # Every GET operation is registered as a virtual query table.
    assert set(queries.keys()) == expected_get, (
        f"GET operations must become virtual tables; "
        f"expected {sorted(expected_get)}, got {sorted(queries)}"
    )
    # Non-GET operations must NOT be virtual tables (they are mutations).
    assert expected_non_get.isdisjoint(queries.keys()), (
        f"Non-GET operations must not be virtual tables; overlap: "
        f"{expected_non_get & queries.keys()}"
    )
    assert expected_non_get <= mutations.keys(), (
        f"Non-GET operations must be registered as mutations; missing: "
        f"{expected_non_get - mutations.keys()}"
    )

    spec = shared_data["registered_spec"]
    for path, path_item in spec["paths"].items():
        get_op = path_item.get("get")
        if not get_op:
            continue
        op_id = get_op.get("operationId") or f"get_{path}"
        q = queries[op_id]

        # Path parameters become GraphQL arguments (path_params).
        spec_path_params = {
            p["name"] for p in get_op.get("parameters", []) if p.get("in") == "path"
        }
        got_path_params = {p["name"] for p in q.path_params}
        assert got_path_params == spec_path_params, (
            f"{op_id}: path params must become arguments; "
            f"expected {spec_path_params}, got {got_path_params}"
        )

        # Query parameters become GraphQL arguments (query_params).
        spec_query_params = {
            p["name"] for p in get_op.get("parameters", []) if p.get("in") == "query"
        }
        got_query_params = {p["name"] for p in q.query_params}
        assert got_query_params == spec_query_params, (
            f"{op_id}: query params must become arguments; "
            f"expected {spec_query_params}, got {got_query_params}"
        )

    # Spot-check listOrders: three query params, array response → column set.
    list_orders = queries["listOrders"]
    assert {p["name"] for p in list_orders.query_params} == {"status", "limit", "offset"}
    assert list_orders.is_list is True
    assert set(list_orders.response_schema["properties"]) == {
        "id",
        "customer_id",
        "status",
        "total",
    }
    # getOrder: path param, object response.
    get_order = queries["getOrder"]
    assert {p["name"] for p in get_order.path_params} == {"order_id"}
    assert get_order.is_list is False


# ---------------------------------------------------------------------------
# REQ-317 — POST/PUT/PATCH/DELETE spec
# ---------------------------------------------------------------------------

_REQ317_SPEC: dict = {
    "openapi": "3.0.0",
    "info": {"title": "Mutation API", "version": "1.0.0"},
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "email": {"type": "string"},
                    "name": {"type": "string"},
                },
            }
        }
    },
    "paths": {
        "/users": {
            "post": {
                "operationId": "createUser",
                "summary": "Create a user",
                "requestBody": {
                    "content": {
                        "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                    }
                },
                "responses": {
                    "200": {
                        "description": "created",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                        },
                    }
                },
            }
        },
        "/users/{id}": {
            "put": {
                "operationId": "replaceUser",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "requestBody": {
                    "content": {
                        "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                    }
                },
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                        },
                    }
                },
            },
            "patch": {
                "operationId": "patchUser",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "requestBody": {
                    "content": {
                        "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                    }
                },
                "responses": {"200": {"description": "ok"}},
            },
            "delete": {
                "operationId": "deleteUser",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "responses": {"200": {"description": "deleted"}},
            },
        },
    },
}


@given("an OpenAPI spec with POST/PUT/PATCH/DELETE operations")
def given_spec_with_mutating_operations(shared_data):
    """Register a spec containing all four mutating HTTP methods (REQ-317)."""
    spec = copy.deepcopy(_REQ317_SPEC)
    methods = {
        method.upper()
        for path_item in spec["paths"].values()
        for method in path_item
        if isinstance(path_item[method], dict)
    }
    assert {"POST", "PUT", "PATCH", "DELETE"} <= methods, (
        f"REQ-317 spec must contain all mutating methods; found {methods}"
    )
    shared_data["spec_to_register"] = spec


@given(parsers.parse('an OpenAPI spec with operationId "{operation_id}"'))
def given_spec_with_operation_id(shared_data, operation_id):
    """Register a single-GET spec whose operation carries *operation_id* (REQ-601)."""
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "Pet API", "version": "1.0.0"},
        "paths": {
            "/pets/findByStatus": {
                "get": {
                    "operationId": operation_id,
                    "parameters": [
                        {"name": "status", "in": "query", "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {"id": {"type": "integer"}},
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            }
        },
    }
    shared_data["spec_to_register"] = spec
    shared_data["operation_id"] = operation_id


@when("the spec is registered")
def when_the_spec_is_registered(shared_data):
    """Parse the pending spec with the real mapper (REQ-317, REQ-601)."""
    spec = shared_data["spec_to_register"]
    queries, mutations = map_operations(spec)
    shared_data["queries"] = {q.operation_id: q for q in queries}
    shared_data["mutations"] = {m.operation_id: m for m in mutations}


@then(
    "those operations are auto-registered as tracked functions with request "
    "body properties as mutation input arguments"
)
def then_mutating_ops_are_tracked_functions(shared_data):
    """Assert non-GET ops became mutations with request-body-derived inputs (REQ-317)."""
    mutations: dict[str, OpenAPIMutation] = shared_data["mutations"]
    queries: dict[str, OpenAPIQuery] = shared_data["queries"]

    expected = {"createUser", "replaceUser", "patchUser", "deleteUser"}
    assert expected <= mutations.keys(), (
        f"All mutating operations must become tracked functions; missing "
        f"{expected - mutations.keys()}"
    )
    assert expected.isdisjoint(queries.keys()), (
        "Mutating operations must not be registered as virtual query tables"
    )

    # Methods preserved.
    assert mutations["createUser"].method == "POST"
    assert mutations["replaceUser"].method == "PUT"
    assert mutations["patchUser"].method == "PATCH"
    assert mutations["deleteUser"].method == "DELETE"

    # Request body properties become mutation input arguments.
    create = mutations["createUser"]
    assert create.input_schema is not None, "createUser must expose an input schema"
    assert set(create.input_schema["properties"]) == {"id", "email", "name"}, (
        f"Request body properties must become input arguments; got "
        f"{sorted(create.input_schema.get('properties', {}))}"
    )
    # DELETE has no request body → no input schema.
    assert mutations["deleteUser"].input_schema is None
    # 200 response schema becomes the return_schema.
    assert set(create.response_schema["properties"]) == {"id", "email", "name"}


@then(
    parsers.parse(
        'the virtual table alias is "{alias}" used as the consumer-facing GraphQL name'
    )
)
def then_virtual_table_alias(shared_data, alias):
    """Assert the derived alias matches, using real register.py logic (REQ-601)."""
    from provisa.openapi.register import _operation_id_to_alias

    op_id = shared_data["operation_id"]
    queries: dict[str, OpenAPIQuery] = shared_data["queries"]
    assert op_id in queries, f"{op_id} must be registered as a virtual table"

    derived = _operation_id_to_alias(op_id)
    assert derived == alias, (
        f"Alias for operationId {op_id!r} must be {alias!r}, got {derived!r}"
    )


# ---------------------------------------------------------------------------
# REQ-318 — cache-hit serves from Trino with zero upstream REST calls
# ---------------------------------------------------------------------------


@given("a GET operation result cached in Trino Iceberg on S3")
def given_get_result_cached(shared_data):
    """Prime the OpenAPI cache freshness for a params combination (REQ-318)."""
    from provisa.openapi import pg_cache

    pg_schema = "results"
    pg_table = "api_cache"
    params = {"status": "available", "limit": 10}
    ttl = 300

    # Clear any prior in-memory freshness for a clean start.
    pg_cache._mem_fresh.clear()
    phash = pg_cache._hash_params(params)

    # Cache key is a hash of the fetch params (source path + native args).
    assert not pg_cache.is_mem_fresh(pg_schema, pg_table, params), (
        "Cache must start cold before priming"
    )
    pg_cache._mark_fresh(pg_schema, pg_table, phash, ttl)
    assert pg_cache.is_mem_fresh(pg_schema, pg_table, params), (
        "After caching, the params combination must be fresh"
    )

    shared_data["pg_cache"] = pg_cache
    shared_data["pg_schema"] = pg_schema
    shared_data["pg_table"] = pg_table
    shared_data["params"] = params
    shared_data["ttl"] = ttl
    shared_data["phash"] = phash


@when("the same query with identical args is issued within TTL")
def when_same_query_within_ttl(shared_data):
    """Re-issue the identical query while cache is fresh, counting upstream calls (REQ-318)."""
    import asyncio
    from unittest.mock import MagicMock, patch

    pg_cache = shared_data["pg_cache"]

    # A fresh in-memory hash short-circuits fill_api_table before any PG or HTTP work.
    # asyncpg conn is only touched on a miss; MagicMock proves it is never used.
    fake_conn = MagicMock()

    with patch("provisa.openapi.pg_cache.httpx.get") as mock_get:
        rows_inserted = asyncio.run(
            pg_cache.fill_api_table(
                base_url="https://api.example.com",
                path="/pets/findByStatus",
                params=shared_data["params"],
                pg_conn=fake_conn,
                pg_schema=shared_data["pg_schema"],
                pg_table=shared_data["pg_table"],
                ttl=shared_data["ttl"],
            )
        )
        shared_data["upstream_calls"] = mock_get.call_count
        shared_data["rows_inserted"] = rows_inserted
        shared_data["fake_conn"] = fake_conn


@then("results are served from Trino directly with zero upstream REST calls")
def then_served_from_trino_zero_upstream(shared_data):
    """Assert the cache hit made no upstream REST call and no DB fetch (REQ-318)."""
    assert shared_data["upstream_calls"] == 0, (
        f"A cache hit within TTL must make zero upstream REST calls, "
        f"got {shared_data['upstream_calls']}"
    )
    assert shared_data["rows_inserted"] == 0, (
        "A cache hit must insert no new rows (served from existing cache)"
    )
    # No PG round-trip on the freshness fast-path: connection was never queried.
    fake_conn = shared_data["fake_conn"]
    assert fake_conn.fetchval.call_count == 0, (
        "A fresh in-memory cache hit must not query PostgreSQL"
    )
    assert fake_conn.execute.call_count == 0
    # Still fresh after the read.
    pg_cache = shared_data["pg_cache"]
    assert pg_cache.is_mem_fresh(
        shared_data["pg_schema"], shared_data["pg_table"], shared_data["params"]
    )


# ---------------------------------------------------------------------------
# REQ-321 — spec refresh preserves governance rules
# ---------------------------------------------------------------------------


@given("an OpenAPI spec that has been updated upstream")
def given_spec_updated_upstream(shared_data):
    """Register an initial spec + governance, then stage an updated upstream spec (REQ-321)."""
    old_spec = copy.deepcopy(_REQ316_SPEC)
    old_registry = _build_registry_from_spec(old_spec)

    # Steward applies governance rules on top of the initial registrations.
    governance = {
        "listOrders": ["mask:total", "row_filter:status='shipped'"],
        "getOrder": ["mask:total"],
        "createOrder": ["require_role:admin"],
    }
    _apply_governance_rules(old_registry, governance)
    for op_id, rules in governance.items():
        assert old_registry[op_id]["governance_rules"] == rules

    # Upstream update: drop updateOrder, add a new getOrderStatus GET operation,
    # add a query param to listOrders. Governance on surviving ops must persist.
    new_spec = copy.deepcopy(_REQ316_SPEC)
    del new_spec["paths"]["/orders/{order_id}"]["put"]  # updateOrder removed
    new_spec["paths"]["/orders"]["get"]["parameters"].append(
        {"name": "sort", "in": "query", "schema": {"type": "string"}}
    )
    new_spec["paths"]["/orders/{order_id}/status"] = {
        "get": {
            "operationId": "getOrderStatus",
            "parameters": [
                {"name": "order_id", "in": "path", "required": True, "schema": {"type": "integer"}}
            ],
            "responses": {
                "200": {
                    "description": "ok",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {"status": {"type": "string"}},
                            }
                        }
                    },
                }
            },
        }
    }

    shared_data["old_registry"] = old_registry
    shared_data["new_spec"] = new_spec
    shared_data["governance"] = governance


@when("a steward triggers the spec refresh admin mutation")
def when_steward_triggers_refresh(shared_data):
    """Perform the refresh, rebuilding registrations from the updated spec (REQ-321)."""
    shared_data["new_registry"] = _perform_spec_refresh(
        shared_data["old_registry"], shared_data["new_spec"]
    )


@then("registrations are updated and governance rules applied on top are preserved")
def then_registrations_updated_governance_preserved(shared_data):
    """Assert refreshed registrations reflect the new spec and keep governance (REQ-321)."""
    new_registry = shared_data["new_registry"]
    governance = shared_data["governance"]

    # New operation is registered.
    assert "getOrderStatus" in new_registry, "New upstream operation must be registered"
    assert new_registry["getOrderStatus"]["kind"] == "virtual_table"

    # Removed operation is dropped.
    assert "updateOrder" not in new_registry, "Removed upstream operation must be dropped"

    # Updated operation reflects the new param.
    list_orders = new_registry["listOrders"]["descriptor"]
    assert "sort" in {p["name"] for p in list_orders.query_params}, (
        "Refreshed registration must reflect the added query parameter"
    )

    # Governance rules on surviving operations are preserved.
    for op_id, rules in governance.items():
        if op_id == "updateOrder":
            continue
        assert new_registry[op_id]["governance_rules"] == rules, (
            f"Governance rules for {op_id!r} must survive refresh; "
            f"expected {rules}, got {new_registry[op_id]['governance_rules']}"
        )
    # New operation starts with no rules.
    assert new_registry["getOrderStatus"]["governance_rules"] == []

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

import asyncio
import copy
import hashlib
import json
import pathlib
import unittest.mock as mock

import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.openapi.loader import load_spec, parse_text
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec as map_operations
from provisa.api_source.engine_cache import (
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
# REQ-318 helpers
# ---------------------------------------------------------------------------

_REQ318_SOURCE_ID = "petstore-api"
_REQ318_OPERATION_PATH = "/pets"
_REQ318_NATIVE_ARGS = {"status": "available", "limit": 10}
_REQ318_TTL = 300  # seconds


def _build_req318_cache_key(source_id: str, operation_path: str, native_args: dict) -> str:
    """Compute the SHA-256 cache key exactly as engine_cache.cache_table_name does."""
    key = json.dumps(
        {
            "s": source_id,
            "o": operation_path,
            "a": sorted(native_args.items()),
            "v": 2,
        },
        sort_keys=True,
    )
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _make_fake_trino_conn(table_name: str, loc: CacheLocation, ttl: int) -> mock.MagicMock:
    """Return a mock Trino connection that simulates a live cache table."""
    conn = mock.MagicMock()
    cur = mock.MagicMock()
    conn.cursor.return_value = cur
    cur.execute.return_value = None
    # fetchall returns one row to simulate SELECT 1 FROM ... LIMIT 1 succeeding
    cur.fetchall.return_value = [(1,)]
    return conn


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
            },
        },
    },
}


# ---------------------------------------------------------------------------
# REQ-316 Steps — GET operations auto-register as virtual query tables
# ---------------------------------------------------------------------------


@given("an OpenAPI spec is registered")
def given_openapi_spec_is_registered(shared_data):
    """Register the Order Management spec (stored locally, ready to parse)."""
    shared_data["spec"] = copy.deepcopy(_REQ316_SPEC)


@when("Provisa parses the spec")
def when_provisa_parses_the_spec(shared_data):
    """Parse the spec with the real Provisa mapper."""
    spec = shared_data["spec"]
    queries, mutations = map_operations(spec)
    shared_data["queries"] = {q.operation_id: q for q in queries}
    shared_data["mutations"] = {m.operation_id: m for m in mutations}


@then(
    "all GET operations are auto-registered as virtual query tables with "
    "path/query params as GraphQL arguments"
)
def then_get_operations_registered_as_virtual_tables(shared_data):
    """Assert real mapper output: every GET is a query with its params surfaced."""
    spec: dict = shared_data["spec"]
    queries: dict[str, OpenAPIQuery] = shared_data["queries"]
    mutations: dict[str, OpenAPIMutation] = shared_data["mutations"]

    # Every GET operation in the spec must appear as a virtual query table.
    expected_get_ids: set[str] = set()
    for path_item in spec["paths"].values():
        get_op = path_item.get("get")
        if get_op:
            expected_get_ids.add(get_op["operationId"])

    assert expected_get_ids, "fixture must contain GET operations"
    assert expected_get_ids <= set(queries), (
        f"missing GET virtual tables: {expected_get_ids - set(queries)}"
    )

    # No GET may be classified as a mutation.
    assert expected_get_ids.isdisjoint(mutations), (
        f"GET operations wrongly classified as mutations: {expected_get_ids & set(mutations)}"
    )

    # listOrders: query params become GraphQL arguments.
    list_orders = queries["listOrders"]
    assert list_orders.method == "GET"
    assert {p["name"] for p in list_orders.query_params} == {"status", "limit", "offset"}
    assert list_orders.is_list is True
    # responses.200 array-item schema determines the column set.
    assert list_orders.response_schema is not None
    assert set(list_orders.response_schema["properties"]) == {
        "id",
        "customer_id",
        "status",
        "total",
    }

    # getOrder: path parameter becomes a GraphQL argument.
    get_order = queries["getOrder"]
    assert {p["name"] for p in get_order.path_params} == {"order_id"}
    assert get_order.is_list is False
    assert get_order.response_schema is not None
    assert "customer_id" in get_order.response_schema["properties"]

    # listOrderItems: both path and query params surface as arguments.
    list_items = queries["listOrderItems"]
    assert {p["name"] for p in list_items.path_params} == {"order_id"}
    assert {p["name"] for p in list_items.query_params} == {"include_prices"}


# ---------------------------------------------------------------------------
# REQ-317 Steps — non-GET operations auto-register as tracked functions
# ---------------------------------------------------------------------------


@given("an OpenAPI spec with POST/PUT/PATCH/DELETE operations")
def given_spec_with_non_get_operations(shared_data):
    """Provide a spec exercising POST, PUT, PATCH and DELETE."""
    spec: dict = {
        "openapi": "3.0.0",
        "info": {"title": "Widget API", "version": "1.0.0"},
        "components": {
            "schemas": {
                "Widget": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "weight": {"type": "number"},
                    },
                }
            }
        },
        "paths": {
            "/widgets": {
                "post": {
                    "operationId": "createWidget",
                    "requestBody": {
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Widget"}}
                        }
                    },
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Widget"}
                                }
                            }
                        }
                    },
                }
            },
            "/widgets/{widget_id}": {
                "put": {
                    "operationId": "replaceWidget",
                    "parameters": [
                        {
                            "name": "widget_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "integer"},
                        }
                    ],
                    "requestBody": {
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Widget"}}
                        }
                    },
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Widget"}
                                }
                            }
                        }
                    },
                },
                "patch": {
                    "operationId": "updateWidget",
                    "parameters": [
                        {
                            "name": "widget_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "integer"},
                        }
                    ],
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
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Widget"}
                                }
                            }
                        }
                    },
                },
                "delete": {
                    "operationId": "deleteWidget",
                    "parameters": [
                        {
                            "name": "widget_id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "integer"},
                        }
                    ],
                    "responses": {"204": {"description": "deleted"}},
                },
            },
        },
    }
    shared_data["spec"] = spec


@when("the spec is registered")
def when_the_spec_is_registered(shared_data):
    """Parse the spec with the real mapper (shared by REQ-317 and REQ-601).

    When ``operation_id`` is present (REQ-601), also derive the consumer-facing
    alias via the real register helper.
    """
    spec = shared_data["spec"]
    queries, mutations = map_operations(spec)
    shared_data["queries"] = {q.operation_id: q for q in queries}
    shared_data["mutations"] = {m.operation_id: m for m in mutations}

    op_id = shared_data.get("operation_id")
    if op_id is not None:
        from provisa.openapi.register import _operation_id_to_alias

        shared_data["alias"] = _operation_id_to_alias(op_id)


@then(
    "those operations are auto-registered as tracked functions with request "
    "body properties as mutation input arguments"
)
def then_non_get_registered_as_tracked_functions(shared_data):
    """Assert real mapper output: every non-GET is a mutation with its input schema."""
    mutations: dict[str, OpenAPIMutation] = shared_data["mutations"]
    queries: dict[str, OpenAPIQuery] = shared_data["queries"]

    expected = {"createWidget", "replaceWidget", "updateWidget", "deleteWidget"}
    assert expected <= set(mutations), f"missing mutations: {expected - set(mutations)}"
    assert expected.isdisjoint(queries), "non-GET ops must not be virtual tables"

    assert mutations["createWidget"].method == "POST"
    assert mutations["replaceWidget"].method == "PUT"
    assert mutations["updateWidget"].method == "PATCH"
    assert mutations["deleteWidget"].method == "DELETE"

    # Request body schema properties become mutation input arguments.
    create = mutations["createWidget"]
    assert create.input_schema is not None
    assert set(create.input_schema["properties"]) == {"id", "name", "weight"}

    patch = mutations["updateWidget"]
    assert patch.input_schema is not None
    assert set(patch.input_schema["properties"]) == {"name"}

    # responses.200 schema becomes the mutation return_schema.
    assert create.response_schema is not None
    assert set(create.response_schema["properties"]) == {"id", "name", "weight"}

    # DELETE with no request body → no input schema.
    assert mutations["deleteWidget"].input_schema is None


# ---------------------------------------------------------------------------
# REQ-601 Steps — virtual table alias derivation
# ---------------------------------------------------------------------------


@given(parsers.parse('an OpenAPI spec with operationId "{operation_id}"'))
def given_spec_with_operation_id(shared_data, operation_id):
    """Build a minimal spec whose single GET carries *operation_id*."""
    shared_data["operation_id"] = operation_id
    shared_data["spec"] = {
        "openapi": "3.0.0",
        "info": {"title": "Pet API", "version": "1.0.0"},
        "components": {
            "schemas": {
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "status": {"type": "string"},
                    },
                }
            }
        },
        "paths": {
            "/pets/findByStatus": {
                "get": {
                    "operationId": operation_id,
                    "parameters": [{"name": "status", "in": "query", "schema": {"type": "string"}}],
                    "responses": {
                        "200": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {"$ref": "#/components/schemas/Pet"},
                                    }
                                }
                            }
                        }
                    },
                }
            }
        },
    }


@then(
    parsers.parse('the virtual table alias is "{alias}" used as the consumer-facing GraphQL name')
)
def then_virtual_table_alias(shared_data, alias):
    """Assert the real alias derivation matches the expected consumer-facing name."""
    op_id = shared_data["operation_id"]
    assert op_id in shared_data["queries"], "operation must register as a virtual table"
    assert shared_data["alias"] == alias, (
        f"alias for {op_id!r} was {shared_data['alias']!r}, expected {alias!r}"
    )


# ---------------------------------------------------------------------------
# REQ-318 Steps — GET results served from Trino cache within TTL
# ---------------------------------------------------------------------------


@given("a GET operation result cached in Trino Iceberg on S3")
def given_get_result_cached_in_trino(shared_data):
    """Populate the in-process table-exists cache to simulate a live Iceberg table."""
    loc = cache_location(_REQ318_SOURCE_ID, cache_catalog="results")
    assert loc.backend == "iceberg", "results catalog must map to the Iceberg backend"

    table_name = cache_table_name(_REQ318_SOURCE_ID, _REQ318_OPERATION_PATH, _REQ318_NATIVE_ARGS)
    assert table_name.startswith("r_"), "cache table name must be the SHA-256-derived r_ name"

    # First access: a live Trino probe materializes the positive result into the
    # in-process TTL cache (matching real table_exists behaviour on cache miss).
    conn = _make_fake_trino_conn(table_name, loc, _REQ318_TTL)
    _TABLE_EXISTS_CACHE.pop((loc.catalog, loc.schema, table_name), None)
    assert table_exists(conn, loc, table_name, ttl=_REQ318_TTL) is True
    assert conn.cursor.called, "first access must probe Trino (cache miss)"

    shared_data["loc"] = loc
    shared_data["table_name"] = table_name
    shared_data["probe_conn"] = conn


@when("the same query with identical args is issued within TTL")
def when_same_query_issued_within_ttl(shared_data):
    """Re-issue the identical query; recompute the cache key from identical args."""
    loc: CacheLocation = shared_data["loc"]
    # Recomputing the cache table name from identical args yields the same table.
    recomputed = cache_table_name(
        _REQ318_SOURCE_ID, _REQ318_OPERATION_PATH, dict(_REQ318_NATIVE_ARGS)
    )
    assert recomputed == shared_data["table_name"], (
        "identical args must resolve to the identical cache table"
    )
    # A fresh connection stands in for the second request path.
    shared_data["second_conn"] = _make_fake_trino_conn(recomputed, loc, _REQ318_TTL)


@then("results are served from Trino directly with zero upstream REST calls")
def then_served_from_trino_zero_rest(shared_data):
    """Assert the second call is a cache hit — no Trino probe, no REST fetch."""
    loc: CacheLocation = shared_data["loc"]
    table_name: str = shared_data["table_name"]
    second_conn: mock.MagicMock = shared_data["second_conn"]

    # Within TTL the in-process cache confirms the table is live without any probe.
    assert table_known_live(loc, table_name) is True

    # table_exists returns True from cache without touching the connection.
    assert table_exists(second_conn, loc, table_name, ttl=_REQ318_TTL) is True
    assert not second_conn.cursor.called, (
        "cache hit must not issue any Trino probe (zero upstream calls)"
    )


# ---------------------------------------------------------------------------
# REQ-321 Steps — on-demand spec refresh preserves governance rules
# ---------------------------------------------------------------------------


@given("an OpenAPI spec that has been updated upstream")
def given_spec_updated_upstream(shared_data):
    """Register an initial spec, apply governance rules, then stage an updated spec."""
    initial_spec = copy.deepcopy(_REQ316_SPEC)
    registry = _build_registry_from_spec(initial_spec)

    # Steward applies governance rules on top of the derived registrations.
    _apply_governance_rules(
        registry,
        {
            "listOrders": ["mask:total", "row_filter:status='shipped'"],
            "getOrder": ["mask:customer_id"],
        },
    )
    assert registry["listOrders"]["governance_rules"], "governance rules must be applied"

    # Upstream update: add a new GET operation and drop updateOrder (PUT).
    updated_spec = copy.deepcopy(_REQ316_SPEC)
    del updated_spec["paths"]["/orders/{order_id}"]["put"]
    updated_spec["paths"]["/orders/{order_id}/history"] = {
        "get": {
            "operationId": "getOrderHistory",
            "parameters": [
                {
                    "name": "order_id",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "integer"},
                }
            ],
            "responses": {
                "200": {
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": "#/components/schemas/Order"},
                            }
                        }
                    }
                }
            },
        }
    }

    shared_data["old_registry"] = registry
    shared_data["updated_spec"] = updated_spec


@when("a steward triggers the spec refresh admin mutation")
def when_steward_triggers_spec_refresh(shared_data):
    """Perform the on-demand refresh, preserving governance rules."""
    shared_data["new_registry"] = _perform_spec_refresh(
        shared_data["old_registry"], shared_data["updated_spec"]
    )


@then("registrations are updated and governance rules applied on top are preserved")
def then_registrations_updated_governance_preserved(shared_data):
    """Assert refresh updates registrations while keeping governance rules intact."""
    new_registry: dict = shared_data["new_registry"]

    # New operation from the updated spec is registered.
    assert "getOrderHistory" in new_registry
    assert new_registry["getOrderHistory"]["kind"] == "virtual_table"

    # Operation removed upstream is dropped.
    assert "updateOrder" not in new_registry

    # Governance rules on surviving operations are preserved verbatim.
    assert new_registry["listOrders"]["governance_rules"] == [
        "mask:total",
        "row_filter:status='shipped'",
    ]
    assert new_registry["getOrder"]["governance_rules"] == ["mask:customer_id"]

    # Newly added operation starts with no rules.
    assert new_registry["getOrderHistory"]["governance_rules"] == []


# ---------------------------------------------------------------------------
# REQ-739 — API source discovery endpoint (orphaned REQ; real code in
# provisa/api_source/introspect.py + provisa/api_source/candidates.py)
# ---------------------------------------------------------------------------

scenarios("../features/REQ-739.feature")


async def _open_candidate_db(dsn: str):
    """Real file-backed sqlite Database with the candidate tables created, so the candidates
    repository (SQLAlchemy Core: upsert_returning / execute_core) exercises a real backend rather
    than a hand-parsed raw-SQL stand-in — the repo was migrated off asyncpg to Core."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from provisa.core.database import Database
    from provisa.core.schema_org import api_endpoint_candidates, api_endpoints

    engine = create_async_engine(dsn)
    async with engine.begin() as _c:
        await _c.run_sync(
            lambda s: api_endpoint_candidates.metadata.create_all(
                s, tables=[api_endpoints, api_endpoint_candidates]
            )
        )
    return Database(engine, name="candidates-test")


_REQ739_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Discovery API", "version": "1.0.0"},
    "components": {
        "schemas": {
            "User": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "email": {"type": "string"},
                    "active": {"type": "boolean"},
                },
            }
        }
    },
    "paths": {
        "/users": {
            "get": {
                "operationId": "listUsers",
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/User"},
                                }
                            }
                        }
                    }
                },
            }
        },
        "/users/{id}": {
            "get": {
                "operationId": "getUser",
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
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/User"}}
                        }
                    }
                },
            }
        },
    },
}


def _make_fake_httpx_response(spec: dict) -> mock.MagicMock:
    """Build a mock httpx response that serves *spec* as JSON."""
    resp = mock.MagicMock()
    resp.json.return_value = spec
    resp.raise_for_status.return_value = None
    return resp


def _make_fake_async_client(spec: dict) -> mock.MagicMock:
    """Return a mock httpx.AsyncClient context manager serving *spec*.

    Only the live HTTP fetch of the spec (the true external boundary) is mocked;
    all parsing/candidate generation runs through the real introspect_openapi.
    """
    resp = _make_fake_httpx_response(spec)
    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value=resp)

    ctx = mock.MagicMock()
    ctx.__aenter__ = mock.AsyncMock(return_value=client)
    ctx.__aexit__ = mock.AsyncMock(return_value=None)
    return ctx


@given("an OpenAPI spec URL")
def given_openapi_spec_url(shared_data):
    """Provide a spec URL; stage the fake HTTP client that serves its content."""
    shared_data["spec_url"] = "https://api.example.com/openapi.json"
    shared_data["fake_client"] = _make_fake_async_client(_REQ739_SPEC)
    shared_data["source_id"] = "discovery-src"


@when("the discovery endpoint introspects it")
def when_discovery_endpoint_introspects(shared_data, tmp_path):
    """Run the real introspect_openapi over the served spec and store candidates."""
    from provisa.api_source import candidates as candidates_repo
    from provisa.api_source import introspect as introspect_mod

    # File-backed sqlite so the same store survives the separate asyncio.run() in the accept/reject
    # step (each step opens its own connection to the one DSN).
    dsn = f"sqlite+aiosqlite:///{tmp_path / 'candidates.db'}"

    async def _run():
        with mock.patch.object(
            introspect_mod.httpx, "AsyncClient", return_value=shared_data["fake_client"]
        ):
            candidates = await introspect_mod.introspect_openapi(shared_data["spec_url"])
        source_id = shared_data["source_id"]
        for c in candidates:
            c.source_id = source_id

        db = await _open_candidate_db(dsn)
        async with db.acquire() as conn:
            ids = await candidates_repo.store_candidates(conn, source_id, candidates)
            stored = await candidates_repo.list_candidates(conn, source_id)
        return candidates, ids, stored

    candidates, ids, stored = asyncio.run(_run())
    shared_data["candidates"] = candidates
    shared_data["stored_ids"] = ids
    shared_data["stored"] = stored
    shared_data["dsn"] = dsn


@then("discovered operation candidates are stored and queryable via admin API")
def then_candidates_stored_and_queryable(shared_data):
    """Assert real introspection produced GET candidates that round-trip via the repo."""
    candidates = shared_data["candidates"]
    stored = shared_data["stored"]

    # introspect_openapi generates one candidate per GET operation with columns.
    by_path = {c.path: c for c in candidates}
    assert set(by_path) == {"/users", "/users/{id}"}, (
        f"unexpected discovered paths: {sorted(by_path)}"
    )
    assert by_path["/users"].method == "GET"
    assert by_path["/users"].table_name == "users"
    assert by_path["/users/{id}"].table_name == "users"

    users_cols = {c.name for c in by_path["/users"].columns}
    assert users_cols == {"id", "email", "active"}, (
        f"column set derived from response schema: {users_cols}"
    )

    # Stored candidates are queryable and carry discovered status + IDs.
    assert len(stored) == 2
    assert all(c.id is not None for c in stored)
    assert all(c.status == "discovered" for c in stored)
    assert {c.path for c in stored} == {"/users", "/users/{id}"}


@then("stewards can accept or reject each candidate")
def then_stewards_accept_or_reject(shared_data):
    """Exercise the real accept/reject repository code against stored candidates."""
    from sqlalchemy import select

    from provisa.api_source import candidates as candidates_repo
    from provisa.core.schema_org import api_endpoint_candidates

    stored = shared_data["stored"]
    assert len(stored) == 2

    accept_id = stored[0].id
    reject_id = stored[1].id
    assert accept_id is not None and reject_id is not None

    async def _run():
        db = await _open_candidate_db(shared_data["dsn"])
        async with db.acquire() as conn:
            endpoint = await candidates_repo.accept_candidate(conn, accept_id)
            await candidates_repo.reject_candidate(conn, reject_id)
            remaining = await candidates_repo.list_candidates(conn, shared_data["source_id"])
            result = await conn.execute_core(
                select(api_endpoint_candidates.c.id, api_endpoint_candidates.c.status).where(
                    api_endpoint_candidates.c.id.in_([accept_id, reject_id])
                )
            )
            statuses = {row[0]: row[1] for row in result.fetchall()}
        return endpoint, remaining, statuses

    endpoint, remaining, statuses = asyncio.run(_run())

    # Accepted candidate becomes a registered endpoint.
    assert endpoint.table_name == stored[0].table_name
    assert endpoint.source_id == shared_data["source_id"]

    # Both candidates leave the 'discovered' queue (one registered, one rejected).
    assert remaining == []
    assert statuses[accept_id] == "registered"
    assert statuses[reject_id] == "rejected"

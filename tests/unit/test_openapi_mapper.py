# Copyright (c) 2026 Kenneth Stott
# Canary: 7247227d-e9bb-4e38-b0f0-bed46287ee62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.openapi.mapper."""

import pytest
from provisa.openapi.mapper import parse_spec, OpenAPIQuery, OpenAPIMutation


def _spec(paths: dict) -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "paths": paths,
    }


def test_get_operation_produces_query():
    spec = _spec({
        "/users": {
            "get": {
                "operationId": "listUsers",
                "summary": "List users",
                "parameters": [],
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, mutations = parse_spec(spec)
    assert len(queries) == 1
    assert len(mutations) == 0
    q = queries[0]
    assert isinstance(q, OpenAPIQuery)
    assert q.operation_id == "listUsers"
    assert q.path == "/users"
    assert q.method == "GET"
    assert q.summary == "List users"


def test_post_operation_produces_mutation():
    spec = _spec({
        "/users": {
            "post": {
                "operationId": "createUser",
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, mutations = parse_spec(spec)
    assert len(queries) == 0
    assert len(mutations) == 1
    m = mutations[0]
    assert isinstance(m, OpenAPIMutation)
    assert m.operation_id == "createUser"
    assert m.method == "POST"


def test_path_params_extracted():
    spec = _spec({
        "/users/{id}": {
            "get": {
                "operationId": "getUser",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "string"}},
                ],
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, _ = parse_spec(spec)
    assert len(queries) == 1
    q = queries[0]
    assert q.path_params == [{"name": "id", "type": "string"}]
    assert q.query_params == []


def test_query_params_extracted():
    spec = _spec({
        "/items": {
            "get": {
                "operationId": "listItems",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer"}},
                ],
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.query_params == [{"name": "limit", "type": "integer"}, {"name": "offset", "type": "integer"}]


def test_array_response_unwrapped():
    spec = _spec({
        "/users": {
            "get": {
                "operationId": "listUsers",
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
                                            "id": {"type": "integer"},
                                            "name": {"type": "string"},
                                        },
                                    },
                                }
                            }
                        },
                    }
                },
            }
        }
    })
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.response_schema is not None
    assert "id" in q.response_schema.get("properties", {})
    assert "name" in q.response_schema.get("properties", {})


def test_object_response_kept():
    spec = _spec({
        "/status": {
            "get": {
                "operationId": "getStatus",
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "status": {"type": "string"},
                                    },
                                }
                            }
                        },
                    }
                },
            }
        }
    })
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.response_schema is not None
    assert "status" in q.response_schema.get("properties", {})


def test_operation_id_absent_slugified():
    spec = _spec({
        "/my-resource/{id}/details": {
            "get": {
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.operation_id == "get_my_resource_id_details"


def test_operation_id_present_used():
    spec = _spec({
        "/foo": {
            "get": {
                "operationId": "myOp",
                "responses": {"200": {"description": "ok"}},
            }
        }
    })
    queries, _ = parse_spec(spec)
    assert queries[0].operation_id == "myOp"


def test_ref_resolution_in_response():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "email": {"type": "string"},
                    },
                }
            }
        },
        "paths": {
            "/users/{id}": {
                "get": {
                    "operationId": "getUser",
                    "parameters": [
                        {"name": "id", "in": "path", "schema": {"type": "integer"}},
                    ],
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/User"}
                                }
                            },
                        }
                    },
                }
            }
        },
    }
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.response_schema is not None
    props = q.response_schema.get("properties", {})
    assert "id" in props
    assert "email" in props


def test_mutation_with_request_body_schema():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "paths": {
            "/users": {
                "post": {
                    "operationId": "createUser",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "email": {"type": "string"},
                                    },
                                }
                            }
                        }
                    },
                    "responses": {"200": {"description": "ok"}},
                }
            }
        },
    }
    _, mutations = parse_spec(spec)
    m = mutations[0]
    assert m.input_schema is not None
    props = m.input_schema.get("properties", {})
    assert "name" in props
    assert "email" in props

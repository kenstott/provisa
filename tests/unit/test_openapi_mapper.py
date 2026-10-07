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

from provisa.openapi.mapper import parse_spec, OpenAPIQuery, OpenAPIMutation


# A response that declares the rows it answers with.
_ROWS = {
    "200": {
        "description": "ok",
        "content": {
            "application/json": {
                "schema": {"type": "object", "properties": {"id": {"type": "integer"}}}
            }
        },
    }
}


def _spec(paths: dict) -> dict:
    return {
        "openapi": "3.0.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "paths": paths,
    }


def test_get_operation_produces_query():
    spec = _spec(
        {
            "/users": {
                "get": {
                    "operationId": "listUsers",
                    "summary": "List users",
                    "parameters": [],
                    "responses": _ROWS,
                }
            }
        }
    )
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
    spec = _spec(
        {
            "/users": {
                "post": {
                    "operationId": "createUser",
                    "responses": _ROWS,
                }
            }
        }
    )
    queries, mutations = parse_spec(spec)
    assert len(queries) == 0
    assert len(mutations) == 1
    m = mutations[0]
    assert isinstance(m, OpenAPIMutation)
    assert m.operation_id == "createUser"
    assert m.method == "POST"


def test_path_params_extracted():
    spec = _spec(
        {
            "/users/{id}": {
                "get": {
                    "operationId": "getUser",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": True,
                            "schema": {"type": "string"},
                        },
                    ],
                    "responses": _ROWS,
                }
            }
        }
    )
    queries, _ = parse_spec(spec)
    assert len(queries) == 1
    q = queries[0]
    assert q.path_params == [{"name": "id", "type": "string"}]
    assert q.query_params == []


def test_query_params_extracted():
    spec = _spec(
        {
            "/items": {
                "get": {
                    "operationId": "listItems",
                    "parameters": [
                        {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                        {"name": "offset", "in": "query", "schema": {"type": "integer"}},
                    ],
                    "responses": _ROWS,
                }
            }
        }
    )
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.query_params == [
        {"name": "limit", "type": "integer"},
        {"name": "offset", "type": "integer"},
    ]


def test_array_response_unwrapped():
    spec = _spec(
        {
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
        }
    )
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.response_schema is not None
    assert "id" in q.response_schema.get("properties", {})
    assert "name" in q.response_schema.get("properties", {})


def test_object_response_kept():
    spec = _spec(
        {
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
        }
    )
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.response_schema is not None
    assert "status" in q.response_schema.get("properties", {})


def test_operation_id_absent_slugified():
    spec = _spec(
        {
            "/my-resource/{id}/details": {
                "get": {
                    "responses": _ROWS,
                }
            }
        }
    )
    queries, _ = parse_spec(spec)
    q = queries[0]
    assert q.operation_id == "get_my_resource_id_details"


def test_operation_id_present_used():
    spec = _spec(
        {
            "/foo": {
                "get": {
                    "operationId": "myOp",
                    "responses": _ROWS,
                }
            }
        }
    )
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
                    "responses": _ROWS,
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


# -- composed schemas and referenced parameters, as a published Swagger 2.0 spec writes them --------

_COMPOSED = {
    "swagger": "2.0",
    "info": {"title": "Test", "version": "1.0.0"},
    "parameters": {"Slug": {"name": "slug", "in": "path", "type": "string", "required": True}},
    "definitions": {
        "object": {
            "type": "object",
            "properties": {"type": {"type": "string"}},
        },
        "account": {
            "allOf": [
                {"$ref": "#/definitions/object"},
                {"type": "object", "properties": {"uuid": {"type": "string"}}},
            ]
        },
        "repository": {
            "allOf": [
                {"$ref": "#/definitions/object"},
                {
                    "type": "object",
                    "properties": {
                        "full_name": {"type": "string"},
                        "size": {"type": "integer"},
                        "owner": {"$ref": "#/definitions/account"},
                        "parent": {"$ref": "#/definitions/repository"},
                    },
                },
            ]
        },
    },
    "paths": {
        "/repositories/{slug}": {
            "parameters": [{"$ref": "#/parameters/Slug"}],
            "get": {
                "operationId": "getRepository",
                "responses": {
                    "200": {"description": "ok", "schema": {"$ref": "#/definitions/repository"}}
                },
            },
        }
    },
}


def test_allof_members_are_one_set_of_properties():
    (query,), _ = parse_spec(_COMPOSED)
    props = query.response_schema["properties"]
    assert set(props) == {"type", "full_name", "size", "owner", "parent"}
    assert props["size"]["type"] == "integer"


def test_a_composed_property_is_an_object_with_its_fields():
    (query,), _ = parse_spec(_COMPOSED)
    owner = query.response_schema["properties"]["owner"]
    assert owner["type"] == "object"
    assert set(owner["properties"]) == {"type", "uuid"}


def test_a_schema_that_refers_to_itself_is_read():
    (query,), _ = parse_spec(_COMPOSED)
    parent = query.response_schema["properties"]["parent"]
    assert parent["type"] == "object"
    assert "full_name" in parent["properties"]


def test_a_referenced_parameter_is_read():
    (query,), _ = parse_spec(_COMPOSED)
    assert query.path_params == [{"name": "slug", "type": "string"}]


# -- a GET that declares no rows, and an operation that answers with a file (REQ-1924) ------------


def _get(operation: dict) -> dict:
    return _spec({"/repo/diff": {"get": {"operationId": "getDiff", **operation}}})


def test_a_get_that_declares_no_response_schema_is_a_command_that_reads():
    queries, (command,) = parse_spec(_get({"responses": {"200": {"description": "the diff"}}}))
    assert queries == []
    assert (command.operation_id, command.method, command.reads) == ("getDiff", "GET", True)
    assert command.binary is False


def test_a_get_the_spec_marks_a_mutation_does_not_read():
    _, (command,) = parse_spec(
        _get({"x-provisa-kind": "mutation", "responses": {"200": {"description": "ok"}}})
    )
    assert command.reads is False


def test_a_get_marked_a_query_is_a_table_whatever_it_declares():
    (query,), commands = parse_spec(
        _get({"x-provisa-kind": "query", "responses": {"200": {"description": "ok"}}})
    )
    assert (query.operation_id, commands) == ("getDiff", [])


def test_an_operation_that_answers_with_a_file_is_a_command_that_answers_binary():
    declared = {
        "200": {
            "description": "the file",
            "content": {"application/octet-stream": {"schema": {"type": "string"}}},
        }
    }
    queries, (command,) = parse_spec(_get({"responses": declared}))
    assert queries == []
    assert (command.reads, command.binary) == (True, True)


def test_swagger_2_declares_a_file_by_what_the_operation_produces():
    spec = {
        "swagger": "2.0",
        "info": {"title": "Test", "version": "1.0.0"},
        "produces": ["application/json"],
        "paths": {
            "/downloads": {
                "get": {
                    "operationId": "getDownload",
                    "produces": ["application/octet-stream"],
                    "responses": {"200": {"description": "the file"}},
                }
            },
            "/log": {
                "get": {"operationId": "getLog", "responses": {"200": {"description": "text"}}}
            },
        },
    }
    _, commands = parse_spec(spec)
    assert {c.operation_id: c.binary for c in commands} == {"getDownload": True, "getLog": False}


# -- which response and which media type the rows are read from ------------------------------------

_ERROR = {
    "description": "error",
    "content": {
        "application/json": {
            "schema": {"type": "object", "properties": {"error": {"type": "string"}}}
        }
    },
}


def test_a_json_media_type_with_parameters_gives_the_columns():
    typed = {
        "200": {
            "description": "ok",
            "content": {
                "application/json;charset=UTF-8": {
                    "schema": {"type": "object", "properties": {"id": {"type": "integer"}}}
                }
            },
        }
    }
    (query,), commands = parse_spec(_get({"responses": typed}))
    assert (set(query.response_schema["properties"]), commands) == ({"id"}, [])


def test_the_error_response_is_never_read_as_the_rows():
    untyped = {"200": {"description": "the diff"}, "default": _ERROR}
    queries, (command,) = parse_spec(_get({"responses": untyped}))
    assert (queries, command.response_schema, command.reads) == ([], None, True)


def test_a_pdf_is_a_file_and_the_error_beside_it_is_not_its_columns():
    declared = {
        "200": {
            "description": "the invoice",
            "content": {"application/pdf": {"schema": {"type": "string", "format": "binary"}}},
        },
        "default": _ERROR,
    }
    queries, (command,) = parse_spec(_get({"responses": declared}))
    assert (queries, command.binary, command.response_schema) == ([], True, None)


def test_an_answer_declared_as_text_or_as_anything_is_not_a_file():
    for media in ("text/plain", "*/*", "application/xml"):
        declared = {"200": {"description": "ok", "content": {media: {}}}}
        _, (command,) = parse_spec(_get({"responses": declared}))
        assert command.binary is False, media

# Copyright (c) 2026 Kenneth Stott
# Canary: d928bc46-d67e-4795-ad85-16d617d3a39d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map an OpenAPI 3.x or Swagger 2.0 spec to query and mutation descriptors.

The spec is read through ``jsonschema_path``, which follows its references; nothing here resolves
one. What is here is the mapping: which operation is a table or a command, and its columns."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from jsonschema_path import SchemaPath
from referencing.exceptions import Unresolvable

from provisa.core.paging import PaginationConfig, PaginationType

# Requirements: REQ-314, REQ-316, REQ-317, REQ-408


@dataclass
class OpenAPIQuery:  # REQ-316
    operation_id: str
    path: str
    method: str = "GET"
    summary: str | None = None
    path_params: list[dict] = field(default_factory=list)  # [{name, type}]
    query_params: list[dict] = field(default_factory=list)  # [{name, type}]
    response_schema: dict | None = None  # JSON Schema of 200 response (item schema if is_list)
    is_list: bool = False  # True when the raw 200 response was an array type
    # REQ-318: the paging the operation's parameters and responses suggest, offered to the steward
    # who registers the table (accepted or edited there); None when nothing suggests one.
    pagination: PaginationConfig | None = None


@dataclass
class OpenAPIMutation:  # REQ-317
    operation_id: str
    path: str
    method: str
    summary: str | None = None
    input_schema: dict | None = None  # JSON Schema of requestBody
    response_schema: dict | None = None
    # REQ-1924: a GET whose response declares no row schema. It changes nothing in the remote
    # system; it is a command because what it answers is not rows a table could hold.
    reads: bool = False
    # Its answer is a file (``application/octet-stream``): one binary value, not rows or text.
    binary: bool = False


# What an undeclared section of a spec reads as.
_NOTHING = SchemaPath.from_dict({})


def _at(node: SchemaPath, *keys: str) -> SchemaPath | None:
    """The node under ``keys``, or None where the spec does not declare it."""
    for key in keys:
        if key not in node:
            return None
        node = node / key
    return node


_SCALAR_TYPES = {"string", "number", "boolean", "integer"}

# How far a schema's properties are read: a table's columns, and the fields of an object column.
_PROPERTY_DEPTH = 2


def _schema(node: SchemaPath, depth: int = _PROPERTY_DEPTH) -> dict:
    """A schema as a table reads it: the members of an ``allOf`` as one set of properties, and
    each property likewise down to ``depth``. References are followed by the spec reader."""
    schema = dict(node.read_value())
    properties: dict = {}
    for member in _at(node, "allOf") or ():
        merged = _schema(member, depth)
        properties.update(merged.pop("properties", {}))
        schema = {**merged, **schema}
    for name, prop in (_at(node, "properties") or _NOTHING).str_items():
        properties[name] = _schema(prop, depth - 1) if depth > 1 else dict(prop.read_value())
    schema.pop("allOf", None)
    if properties:
        schema["properties"] = properties
    return schema


def _row_schema(node: SchemaPath) -> tuple[dict, bool]:
    """(the schema of one row, whether the declared schema is an array of them)."""
    is_list = node.read_value().get("type") == "array" and "items" in node
    return _schema(node / "items" if is_list else node), is_list


def _success_response(operation: SchemaPath) -> SchemaPath | None:
    responses = _at(operation, "responses")
    if responses is None:
        return None
    return next((responses / code for code in ("200", "2xx", "default") if code in responses), None)


# A response of this media type is a file: one binary value, not rows or text.
_BINARY_MEDIA = "application/octet-stream"


def _answers_binary(root: SchemaPath, operation: SchemaPath) -> bool:
    """Whether every media type the operation declares for its answer is binary: the keys of
    the success response's ``content`` (OpenAPI 3.x), or the operation's ``produces``, else the
    spec's (Swagger 2.0)."""
    response = _success_response(operation)
    content = None if response is None else _at(response, "content")
    if content is not None:
        media = list(content.str_keys())
    else:
        produces = _at(operation, "produces")
        if produces is None:
            produces = _at(root, "produces")
        media = [] if produces is None else list(produces.read_value())
    return bool(media) and all(m == _BINARY_MEDIA for m in media)


def _extract_response_schema(operation: SchemaPath) -> tuple[dict | None, bool]:
    """(row schema, is_list) of the 200/2xx/default response; is_list when it is an array."""
    for code in ("200", "2xx", "default"):
        resp = _at(operation, "responses", code)
        if resp is None:
            continue
        # OpenAPI 3.x declares the schema per media type; Swagger 2.0 on the response itself.
        for where in (("content", "application/json", "schema"), ("schema",)):
            schema = _at(resp, *where)
            if schema is not None:
                return _row_schema(schema)
    return None, False


def _extract_request_schema(operation: SchemaPath) -> dict | None:
    """The request body's schema: ``requestBody`` (OpenAPI 3.x) or the body parameter (Swagger 2.0)."""
    schema = _at(operation, "requestBody", "content", "application/json", "schema")
    if schema is not None:
        return _row_schema(schema)[0]
    for param in _at(operation, "parameters") or ():
        if param.read_value().get("in") == "body" and "schema" in param:
            return _row_schema(param / "schema")[0]
    return None


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _operation_id(operation: dict, method: str, path: str) -> str:
    if "operationId" in operation:
        return operation["operationId"]
    return _slugify(f"{method}_{path}")


def _parameters(path_item: SchemaPath, operation: SchemaPath | None) -> list[dict]:
    """The operation's parameters: the path's, overridden by the operation's own (by name+in)."""
    merged = {
        (p["name"], p["in"]): p
        for node in (path_item, operation)
        if node is not None
        for p in (param.read_value() for param in _at(node, "parameters") or ())
    }
    return list(merged.values())


def _extract_params(params: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split parameters into path_params and query_params."""
    path_params: list[dict] = []
    query_params: list[dict] = []
    for p in params:
        location = p.get("in", "")
        schema = p.get("schema", {})
        param_type = schema.get("type") if isinstance(schema, dict) else p.get("type", "string")
        entry = {"name": p.get("name", ""), "type": param_type or "string"}
        if location == "path":
            path_params.append(entry)
        elif location == "query":
            query_params.append(entry)
    return path_params, query_params


def operation_parameters(spec: dict, path: str, method: str = "get") -> list[dict]:
    """The declared parameters of ``method`` at ``path``; none where the spec has no such path."""
    path_item = _at(SchemaPath.from_dict(spec), "paths", path)
    if path_item is None:
        return []
    return _parameters(path_item, _at(path_item, method))


# Query parameter names that say how an operation pages (REQ-318). First match wins.
_PAGE_NAMES = ("page", "page_number", "pageNumber", "page_no")
_OFFSET_NAMES = ("offset", "skip", "start")
_SIZE_NAMES = ("limit", "per_page", "perPage", "page_size", "pageSize", "size", "top", "count")


def _declares_link_header(operation: SchemaPath) -> bool:
    """Whether the operation's success response declares a ``Link`` header (RFC 8288 paging)."""
    response = _success_response(operation)
    headers = None if response is None else _at(response, "headers")
    return headers is not None and any(name.lower() == "link" for name in headers.str_keys())


def propose_paging(
    operation: SchemaPath, query_params: list[dict], is_list: bool
) -> PaginationConfig | None:
    """The paging a GET operation suggests, from what it declares: a page-number parameter, an
    offset with a size parameter, or a ``Link`` response header. Only a list response is paged
    this way. A cursor carried in a wrapped response is not proposed: the rows of such a response
    sit under a root no OpenAPI table declares."""
    if not is_list:
        return None
    names = [p["name"] for p in query_params]
    size = next((n for n in _SIZE_NAMES if n in names), None)
    page = next((n for n in _PAGE_NAMES if n in names), None)
    if page is not None:
        declared = {"type": PaginationType.page_number, "page_param": page}
        if size is not None:
            declared["page_size_param"] = size
        return PaginationConfig.model_validate(declared)
    offset = next((n for n in _OFFSET_NAMES if n in names), None)
    if offset is not None and size is not None:
        return PaginationConfig.model_validate(
            {"type": PaginationType.offset, "page_param": offset, "page_size_param": size}
        )
    if _declares_link_header(operation):
        return PaginationConfig.model_validate({"type": PaginationType.link_header})
    return None


def parse_spec(
    spec: dict,
    operation_overrides: dict[str, str] | None = None,
) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:  # REQ-314, REQ-316, REQ-317, REQ-408
    """Parse an OpenAPI 3.x or Swagger 2.0 spec into queries and mutations.

    operation_overrides: {operationId: "query" | "mutation"} — takes priority over x-provisa-kind.
    """
    try:
        return _map_operations(SchemaPath.from_dict(spec), operation_overrides or {})
    except Unresolvable as exc:
        # The reader's own message carries the whole spec; name the reference only.
        raise ValueError(f"unresolvable $ref: {exc.ref}") from None


def _map_operations(
    root: SchemaPath, overrides: dict[str, str]
) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:
    queries: list[OpenAPIQuery] = []
    mutations: list[OpenAPIMutation] = []

    for path, path_item in (_at(root, "paths") or _NOTHING).str_items():
        if not isinstance(path_item.read_value(), dict):
            continue

        for method in ("get", "post", "put", "patch", "delete"):
            operation = _at(path_item, method)
            if operation is None:
                continue
            raw = operation.read_value()
            binary = _answers_binary(root, operation)
            path_params, query_params = _extract_params(_parameters(path_item, operation))
            op_id = _operation_id(raw, method, path)
            summary = raw.get("summary") or raw.get("description")
            response_schema, is_list = _extract_response_schema(operation)

            # Payload override > x-provisa-kind > a GET that declares the rows it answers with
            explicit_kind = (
                overrides.get(op_id, "").lower() or (raw.get("x-provisa-kind") or "").lower()
            )
            response_is_scalar = (
                isinstance(response_schema, dict) and response_schema.get("type") in _SCALAR_TYPES
            )
            reads = method == "get" and explicit_kind != "mutation"
            is_query = not (response_is_scalar or binary) and (
                explicit_kind == "query" or (reads and response_schema is not None)
            )

            if is_query:
                queries.append(
                    OpenAPIQuery(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=summary,
                        path_params=path_params,
                        query_params=query_params,
                        response_schema=response_schema,
                        is_list=is_list,
                        pagination=propose_paging(operation, query_params, is_list),
                    )
                )
            else:
                mutations.append(
                    OpenAPIMutation(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=summary,
                        input_schema=_extract_request_schema(operation),
                        response_schema=response_schema,
                        reads=reads,
                        binary=binary,
                    )
                )

    return queries, mutations


def classify_operation(
    method: str, _path: str, operation: dict
) -> str:  # REQ-408  # pyright: ignore[reportUnusedParameter]
    """Return 'query' or 'mutation' for an OpenAPI operation."""
    explicit_kind = (operation.get("x-provisa-kind") or "").lower()
    if explicit_kind in ("query", "mutation"):
        return explicit_kind
    return "query" if method.lower() == "get" else "mutation"

# Copyright (c) 2026 Kenneth Stott
# Canary: d928bc46-d67e-4795-ad85-16d617d3a39d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parse an OpenAPI 3.x or Swagger 2.0 spec into query and mutation descriptors."""

from __future__ import annotations
from dataclasses import dataclass, field

from provisa.core.paging import PaginationConfig, PaginationType
import re

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


def _resolve_ref(spec: dict, ref: str) -> dict:
    """Resolve a $ref like #/components/schemas/Foo or #/definitions/Foo."""
    if not ref.startswith("#/"):
        raise ValueError(f"unresolvable external $ref: {ref}")
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            raise ValueError(f"unresolvable $ref: {ref}")
        node = node[part]
    if not isinstance(node, dict):
        raise ValueError(f"$ref does not resolve to an object: {ref}")
    return node


def _resolve_properties(spec: dict, schema: dict) -> dict:
    """Resolve $ref on each property of an object schema (one level deep)."""
    props = schema.get("properties")
    if not props:
        return schema
    resolved_props = {
        name: _resolve_ref(spec, prop["$ref"]) if "$ref" in prop else prop
        for name, prop in props.items()
    }
    return {**schema, "properties": resolved_props}


def _maybe_resolve(spec: dict, schema: dict | None) -> dict | None:
    if schema is None:
        return None
    if "$ref" in schema:
        resolved = _resolve_ref(spec, schema["$ref"])
        # Unwrap array wrapper after resolving
        if resolved.get("type") == "array" and "items" in resolved:
            items = resolved["items"]
            if "$ref" in items:
                items = _resolve_ref(spec, items["$ref"])
            return _resolve_properties(spec, items)
        return _resolve_properties(spec, resolved)
    if schema.get("type") == "array" and "items" in schema:
        items = schema["items"]
        if "$ref" in items:
            items = _resolve_ref(spec, items["$ref"])
        return _resolve_properties(spec, items)
    return _resolve_properties(spec, schema)


def _raw_schema_is_list(spec: dict, schema: dict) -> bool:
    """Return True if schema (before unwrapping) represents an array response."""
    if schema.get("type") == "array":
        return True
    if "$ref" in schema:
        resolved = _resolve_ref(spec, schema["$ref"])
        return resolved.get("type") == "array"
    return False


def _extract_response_schema(spec: dict, operation: dict) -> tuple[dict | None, bool]:
    """Extract JSON Schema from 200/2xx/default response.

    Returns (item_schema, is_list) where is_list is True when the raw response was array-typed.
    """
    responses = operation.get("responses", {})
    for code in ("200", "2xx", "default"):
        resp = responses.get(code)
        if resp is None:
            continue
        if "$ref" in resp:
            resp = _resolve_ref(spec, resp["$ref"])
        content = resp.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema")
        if schema is not None:
            return _maybe_resolve(spec, schema), _raw_schema_is_list(spec, schema)
        # Swagger 2.0 puts schema directly on response
        schema = resp.get("schema")
        if schema is not None:
            return _maybe_resolve(spec, schema), _raw_schema_is_list(spec, schema)
    return None, False


def _extract_request_schema(spec: dict, operation: dict) -> dict | None:
    """Extract JSON Schema from requestBody (OpenAPI 3) or parameters body (Swagger 2)."""
    # OpenAPI 3.x
    body = operation.get("requestBody")
    if body:
        if "$ref" in body:
            body = _resolve_ref(spec, body["$ref"])
        content = body.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema")
        if schema is not None:
            return _maybe_resolve(spec, schema)
    # Swagger 2.0 — body parameter
    for param in operation.get("parameters", []):
        if param.get("in") == "body":
            schema = param.get("schema")
            if schema:
                return _maybe_resolve(spec, schema)
    return None


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _operation_id(operation: dict, method: str, path: str) -> str:
    if "operationId" in operation:
        return operation["operationId"]
    return _slugify(f"{method}_{path}")


def _merge_parameters(path_level: list, op_level: list) -> list:
    """Merge path-level and operation-level parameters; op-level overrides by name+in."""
    result = {(p["name"], p["in"]): p for p in path_level if isinstance(p, dict)}
    for p in op_level:
        if isinstance(p, dict):
            result[(p["name"], p["in"])] = p
    return list(result.values())


def _extract_params(spec: dict, params: list) -> tuple[list[dict], list[dict]]:
    """Split parameters into path_params and query_params."""
    path_params: list[dict] = []
    query_params: list[dict] = []
    for p in params:
        if "$ref" in p:
            p = _resolve_ref(spec, p["$ref"])
        location = p.get("in", "")
        schema = p.get("schema", {})
        param_type = schema.get("type") if isinstance(schema, dict) else p.get("type", "string")
        entry = {"name": p.get("name", ""), "type": param_type or "string"}
        if location == "path":
            path_params.append(entry)
        elif location == "query":
            query_params.append(entry)
    return path_params, query_params


# Query parameter names that say how an operation pages (REQ-318). First match wins.
_PAGE_NAMES = ("page", "page_number", "pageNumber", "page_no")
_OFFSET_NAMES = ("offset", "skip", "start")
_SIZE_NAMES = ("limit", "per_page", "perPage", "page_size", "pageSize", "size", "top", "count")


def _declares_link_header(spec: dict, operation: dict) -> bool:
    """Whether the operation's success response declares a ``Link`` header (RFC 8288 paging)."""
    responses = operation.get("responses", {})
    for code in ("200", "2xx", "default"):
        resp = responses.get(code)
        if resp is None:
            continue
        if "$ref" in resp:
            resp = _resolve_ref(spec, resp["$ref"])
        return any(name.lower() == "link" for name in (resp.get("headers") or {}))
    return False


def propose_paging(
    spec: dict, operation: dict, query_params: list[dict], is_list: bool
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
    if _declares_link_header(spec, operation):
        return PaginationConfig.model_validate({"type": PaginationType.link_header})
    return None


def parse_spec(
    spec: dict,
    operation_overrides: dict[str, str] | None = None,
) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:  # REQ-314, REQ-316, REQ-317, REQ-408
    """Parse an OpenAPI 3.x or Swagger 2.0 spec into queries and mutations.

    operation_overrides: {operationId: "query" | "mutation"} — takes priority over x-provisa-kind.
    """
    queries: list[OpenAPIQuery] = []
    mutations: list[OpenAPIMutation] = []
    _op_overrides = operation_overrides or {}

    paths = spec.get("paths", {})
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        path_level_params = path_item.get("parameters", [])

        for method in ("get", "post", "put", "patch", "delete"):
            operation = path_item.get(method)
            if operation is None:
                continue
            op_params = operation.get("parameters", [])
            merged = _merge_parameters(path_level_params, op_params)
            path_params, query_params = _extract_params(spec, merged)
            op_id = _operation_id(operation, method, path)
            summary = operation.get("summary") or operation.get("description")
            response_schema, is_list = _extract_response_schema(spec, operation)

            # Payload override > x-provisa-kind > GET heuristic
            explicit_kind = (
                _op_overrides.get(op_id, "").lower()
                or (operation.get("x-provisa-kind") or "").lower()
            )
            _SCALAR_TYPES = {"string", "number", "boolean", "integer"}
            response_is_scalar = (
                isinstance(response_schema, dict) and response_schema.get("type") in _SCALAR_TYPES
            )
            is_query = not response_is_scalar and (
                explicit_kind == "query" or (method == "get" and explicit_kind != "mutation")
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
                        pagination=propose_paging(spec, operation, query_params, is_list),
                    )
                )
            else:
                input_schema = _extract_request_schema(spec, operation)
                mutations.append(
                    OpenAPIMutation(
                        operation_id=op_id,
                        path=path,
                        method=method.upper(),
                        summary=summary,
                        input_schema=input_schema,
                        response_schema=response_schema,
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

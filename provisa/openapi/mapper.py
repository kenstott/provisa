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
import re


@dataclass
class OpenAPIQuery:
    operation_id: str
    path: str
    method: str = "GET"
    summary: str | None = None
    path_params: list[dict] = field(default_factory=list)    # [{name, type}]
    query_params: list[dict] = field(default_factory=list)   # [{name, type}]
    response_schema: dict | None = None  # JSON Schema of 200 response


@dataclass
class OpenAPIMutation:
    operation_id: str
    path: str
    method: str
    summary: str | None = None
    input_schema: dict | None = None      # JSON Schema of requestBody
    response_schema: dict | None = None


def _resolve_ref(spec: dict, ref: str) -> dict:
    """Resolve a $ref like #/components/schemas/Foo or #/definitions/Foo."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        if not isinstance(node, dict):
            return {}
        node = node.get(part, {})
    return node if isinstance(node, dict) else {}


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
            return items
        return resolved
    if schema.get("type") == "array" and "items" in schema:
        items = schema["items"]
        if "$ref" in items:
            items = _resolve_ref(spec, items["$ref"])
        return items
    return schema


def _extract_response_schema(spec: dict, operation: dict) -> dict | None:
    """Extract JSON Schema from 200/2xx/default response."""
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
            return _maybe_resolve(spec, schema)
        # Swagger 2.0 puts schema directly on response
        schema = resp.get("schema")
        if schema is not None:
            return _maybe_resolve(spec, schema)
    return None


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


def parse_spec(spec: dict) -> tuple[list[OpenAPIQuery], list[OpenAPIMutation]]:
    """Parse an OpenAPI 3.x or Swagger 2.0 spec into queries and mutations."""
    queries: list[OpenAPIQuery] = []
    mutations: list[OpenAPIMutation] = []

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
            response_schema = _extract_response_schema(spec, operation)

            if method == "get":
                queries.append(OpenAPIQuery(
                    operation_id=op_id,
                    path=path,
                    method="GET",
                    summary=summary,
                    path_params=path_params,
                    query_params=query_params,
                    response_schema=response_schema,
                ))
            else:
                input_schema = _extract_request_schema(spec, operation)
                mutations.append(OpenAPIMutation(
                    operation_id=op_id,
                    path=path,
                    method=method.upper(),
                    summary=summary,
                    input_schema=input_schema,
                    response_schema=response_schema,
                ))

    return queries, mutations

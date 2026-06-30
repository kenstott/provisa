# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dynamic per-table OpenAPI 3.1 spec for /data/jsonapi endpoints."""

from __future__ import annotations

from typing import Any

from graphql import (
    GraphQLBoolean,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
)

_FILTER_OPS = ["eq", "neq", "gt", "gte", "lt", "lte", "like"]


def _gql_to_schema(gql_type: Any) -> dict[str, Any]:
    if isinstance(gql_type, GraphQLNonNull):
        return _gql_to_schema(gql_type.of_type)
    if isinstance(gql_type, GraphQLList):
        return {"type": "array", "items": _gql_to_schema(gql_type.of_type)}
    if isinstance(gql_type, GraphQLScalarType):
        if gql_type == GraphQLInt:
            return {"type": "integer"}
        if gql_type == GraphQLFloat:
            return {"type": "number"}
        if gql_type == GraphQLBoolean:
            return {"type": "boolean"}
        if gql_type == GraphQLString:
            return {"type": "string"}
    return {"type": "string"}


def _is_scalar(gql_type: Any) -> bool:
    if isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        return _is_scalar(gql_type.of_type)
    return isinstance(gql_type, GraphQLScalarType)


def generate_jsonapi_openapi_spec(
    state: Any, role_id: str, domains: list[str] | None = None
) -> dict[str, Any]:
    """Generate an OpenAPI 3.1 spec for /data/jsonapi endpoints."""
    if role_id not in state.schemas:
        return _empty_spec()

    schema = state.schemas[role_id]
    query_type = schema.query_type
    if query_type is None:
        return _empty_spec()

    path_map: dict[str, dict] = getattr(state, "table_path_maps", {}).get(role_id, {})
    domain_filter = set(domains) if domains else None

    # Build domain description map from schema_build_cache
    raw_domains: list[dict] = (getattr(state, "schema_build_cache", {}) or {}).get("domains", [])
    domain_desc_map: dict[str, str] = {
        d["id"]: d.get("description") or "" for d in raw_domains if d.get("id")
    }

    paths: dict[str, Any] = {}
    tags: list[dict[str, str]] = []
    seen_tags: set[str] = set()

    for field_name, field in query_type.fields.items():
        meta = path_map.get(field_name)
        if meta is None:
            continue
        domain_id = meta.get("domain_id", "")
        if domain_filter is not None and domain_id not in domain_filter:
            continue

        table_name = meta["table_name"]
        table_description = field.description or ""

        # Accumulate tags (one per domain) with domain description
        if domain_id and domain_id not in seen_tags:
            seen_tags.add(domain_id)
            tag_entry: dict[str, str] = {"name": domain_id}
            domain_description = domain_desc_map.get(domain_id, "")
            if domain_description:
                tag_entry["description"] = domain_description
            tags.append(tag_entry)

        inner = field.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        if not isinstance(inner, GraphQLObjectType):
            continue

        columns: list[tuple[str, dict[str, Any], str]] = [
            (col_name, _gql_to_schema(col_field.type), col_field.description or "")
            for col_name, col_field in inner.fields.items()
            if _is_scalar(col_field.type)
        ]

        attributes_props: dict[str, Any] = {}
        for col, col_schema, col_desc in columns:
            if col == "id":
                continue
            prop = dict(col_schema)
            if col_desc:
                prop["description"] = col_desc
            attributes_props[col] = prop

        resource_schema = {
            "type": "object",
            "properties": {
                "type": {"type": "string", "example": table_name},
                "id": {"type": "string"},
                "attributes": {
                    "type": "object",
                    "properties": attributes_props,
                },
            },
        }

        parameters: list[dict[str, Any]] = [
            {
                "name": "page[size]",
                "in": "query",
                "schema": {"type": "integer", "minimum": 1, "default": 20},
                "description": "Page size",
            },
            {
                "name": "page[number]",
                "in": "query",
                "schema": {"type": "integer", "minimum": 1, "default": 1},
                "description": "Page number",
            },
            {
                "name": f"fields[{table_name}]",
                "in": "query",
                "schema": {"type": "string"},
                "description": "Sparse fieldset — comma-separated list of attributes to return",
                "example": ",".join(col for col, _, _ in columns[:3] if col != "id"),
            },
            {
                "name": "sort",
                "in": "query",
                "schema": {"type": "string"},
                "description": "Sort fields, prefix with '-' for descending",
                "example": columns[0][0] if columns else "",
            },
            {
                "name": "include",
                "in": "query",
                "schema": {"type": "string"},
                "description": "Comma-separated relationship names to sideload",
            },
        ]
        for col, col_schema, col_desc in columns:
            parameters.append(
                {
                    "name": f"filter[{col}]",
                    "in": "query",
                    "schema": col_schema,
                    "description": f"Filter {col} (equality)"
                    + (f" — {col_desc}" if col_desc else ""),
                    "required": False,
                }
            )
            for op in _FILTER_OPS:
                if op == "eq":
                    continue
                parameters.append(
                    {
                        "name": f"filter[{col}][{op}]",
                        "in": "query",
                        "schema": col_schema,
                        "description": f"Filter {col} where {op}"
                        + (f" — {col_desc}" if col_desc else ""),
                        "required": False,
                    }
                )

        get_op: dict[str, Any] = {
            "summary": f"List {table_name}",
            "operationId": f"list_{field_name}",
            "tags": [domain_id] if domain_id else [table_name],
            "parameters": parameters,
        }
        if table_description:
            get_op["description"] = table_description

        paths[f"/{domain_id}/{table_name}"] = {
            "get": {
                **get_op,
                "responses": {
                    "200": {
                        "description": "JSON:API document",
                        "content": {
                            "application/vnd.api+json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": resource_schema,
                                        },
                                        "links": {
                                            "type": "object",
                                            "properties": {
                                                "self": {"type": "string"},
                                                "first": {"type": "string"},
                                                "last": {"type": "string"},
                                                "prev": {"type": "string"},
                                                "next": {"type": "string"},
                                            },
                                        },
                                    },
                                }
                            }
                        },
                    },
                    "400": {"description": "Bad request / invalid filter"},
                    "403": {"description": "Governance policy violation"},
                    "404": {"description": "Resource type not found"},
                },
            }
        }

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Provisa JSON:API",
            "version": "0.1.0",
            "description": (
                "Auto-generated JSON:API endpoints per resource type. "
                "Supports sparse fieldsets, filtering, sorting, pagination, and inclusion. "
                "Governance applies uniformly across all query interfaces."
            ),
        },
        "servers": [{"url": "/data/jsonapi"}],
        "tags": tags,
        "paths": paths,
    }


def _empty_spec() -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Provisa JSON:API", "version": "0.1.0"},
        "paths": {},
    }

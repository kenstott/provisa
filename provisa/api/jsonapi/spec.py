# Copyright (c) 2026 Kenneth Stott
# Canary: 97eddbcc-e082-4c18-9d25-916b653d1d3d
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

from provisa.api.command_fields import CommandField, command_fields
from provisa.api.jsonapi.naming import scalar_name_maps
from provisa.api.jsonapi.resource_id import resource_id_columns
from provisa.api.role_surface import role_surface
from provisa.compiler.type_map import JSONScalar

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
    surface = role_surface(state, role_id)  # one snapshot: the schema and its path map
    if surface is None:
        return _empty_spec()

    schema = surface.schema
    query_type = schema.query_type
    if query_type is None:
        return _empty_spec()

    path_map: dict[str, dict] = surface.table_path_map
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
                "name": "page[total]",
                "in": "query",
                "schema": {"type": "boolean", "default": False},
                "description": (
                    "true counts the rows the filter matches and returns meta.total and "
                    "links.last; without it neither is returned and no count is run"
                ),
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
                "description": (
                    "Comma-separated relationship names to sideload. On a groupBy request with "
                    "includeNodes=true, an entry may also be a 'relationship.column' dot-path, "
                    "selecting just that column inside nodes"
                ),
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
                                        "meta": {
                                            "type": "object",
                                            "properties": {
                                                "total": {
                                                    "type": "integer",
                                                    "description": "Present with page[total]=true",
                                                },
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

        # REQ-1929: GET /<type>/<id> only for a type whose id is a real identifier.
        table_id = surface.context.tables[field_name].table_id
        scalar_names = [c for c, _, _ in columns if not (c.startswith("_") and c.endswith("_"))]
        _, physical_to_gql = scalar_name_maps(surface.context, table_id, scalar_names)
        id_columns = resource_id_columns(surface.context, table_id, physical_to_gql)
        if id_columns:
            paths[f"/{domain_id}/{table_name}/{{id}}"] = _lookup_path(
                field_name, domain_id, table_name, id_columns, resource_schema, parameters
            )

    # REQ-1929: the role's GraphQL command fields, under their REST command bindings: GET reads a
    # Query field, POST runs a Mutation field.
    for cf in command_fields(schema):
        if domain_filter is not None and cf.domain_id not in domain_filter:
            continue
        cmd_name = cf.name_in("rest")
        if cmd_name is None:
            continue
        if cf.domain_id not in seen_tags:
            seen_tags.add(cf.domain_id)
            tag_entry = {"name": cf.domain_id}
            if domain_desc_map.get(cf.domain_id):
                tag_entry["description"] = domain_desc_map[cf.domain_id]
            tags.append(tag_entry)
        paths[f"/{cf.domain_id}/commands/{cmd_name}"] = _command_path(cf, cmd_name)

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


def _lookup_path(  # REQ-1929
    field_name: str,
    domain_id: str,
    table_name: str,
    id_columns: list[str],
    resource_schema: dict[str, Any],
    parameters: list[dict[str, Any]],
) -> dict[str, Any]:
    """The single-resource lookup's path item: the id, and the list's fields[] and include."""
    id_desc = (
        f"The resource's {id_columns[0]}"
        if len(id_columns) == 1
        else f"The resource's {', '.join(id_columns)}, joined by ':' (each ':' and '\\' "
        "escaped by '\\')"
    )
    return {
        "get": {
            "summary": f"Get one {table_name}",
            "operationId": f"get_{field_name}",
            "tags": [domain_id] if domain_id else [table_name],
            "parameters": [
                {
                    "name": "id",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                    "description": id_desc,
                },
                *[p for p in parameters if p["name"] in (f"fields[{table_name}]", "include")],
            ],
            "responses": {
                "200": {
                    "description": "JSON:API document",
                    "content": {
                        "application/vnd.api+json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "data": resource_schema,
                                    "links": {
                                        "type": "object",
                                        "properties": {"self": {"type": "string"}},
                                    },
                                },
                            }
                        }
                    },
                },
                "400": {"description": "Bad request / a collection parameter"},
                "403": {"description": "Governance policy violation"},
                "404": {"description": "Resource type or resource not found"},
            },
        }
    }


def _argument_schema(arg: Any) -> dict[str, Any]:
    named = arg.type
    while isinstance(named, (GraphQLNonNull, GraphQLList)):
        named = named.of_type
    # REQ-1924: a JSON argument takes any JSON value, passed to the command as written.
    return {} if named is JSONScalar else _gql_to_schema(arg.type)


def _command_path(cf: CommandField, cmd_name: str) -> dict[str, Any]:  # REQ-1929
    """A command's path item: GET with a collection's filter[], sort and page[] and the
    command's arguments for a read, POST with the arguments as the body's meta for a write. The
    rows answer under meta.rows: a command's rows are not resources."""
    row_type = cf.row_type
    row_props = (
        {n: _gql_to_schema(f.type) for n, f in row_type.fields.items() if _is_scalar(f.type)}
        if row_type is not None
        else {}
    )
    rows_doc = {
        "type": "object",
        "properties": {
            "meta": {
                "type": "object",
                "properties": {
                    "rows": {"type": "array", "items": {"type": "object", "properties": row_props}}
                },
            }
        },
    }
    operation: dict[str, Any] = {
        "summary": f"{'Read' if cf.is_read else 'Run'} command {cf.domain_id}.{cmd_name}",
        "operationId": f"call_{cmd_name}",
        "tags": [cf.domain_id],
        **({"description": cf.field.description} if cf.field.description else {}),
        "responses": {
            "200": {
                "description": "JSON:API document",
                "content": {"application/vnd.api+json": {"schema": rows_doc}},
            },
            "400": {"description": "Bad request / invalid parameter"},
            "403": {"description": "Governance policy violation"},
            "404": {"description": "Command not found"},
        },
    }
    arguments = {name: _argument_schema(arg) for name, arg in cf.arguments.items()}
    if not cf.is_read:
        operation["requestBody"] = {
            "required": bool(arguments),
            "content": {
                "application/vnd.api+json": {
                    "schema": {
                        "type": "object",
                        "properties": {"meta": {"type": "object", "properties": arguments}},
                    }
                }
            },
        }
        return {"post": operation}
    parameters: list[dict[str, Any]] = [
        {"name": "page[size]", "in": "query", "schema": {"type": "integer", "minimum": 1}},
        {"name": "page[number]", "in": "query", "schema": {"type": "integer", "minimum": 1}},
        {
            "name": "sort",
            "in": "query",
            "schema": {"type": "string"},
            "description": "Comma-separated columns; a leading '-' sorts descending",
        },
    ]
    for column in row_props:
        parameters.append(
            {"name": f"filter[{column}]", "in": "query", "schema": {"type": "string"}}
        )
    for name, schema in arguments.items():
        parameters.append(
            {
                "name": name,
                "in": "query",
                "required": isinstance(cf.arguments[name].type, GraphQLNonNull),
                "schema": schema,
            }
        )
    operation["parameters"] = parameters
    return {"get": operation}


def _empty_spec() -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Provisa JSON:API", "version": "0.1.0"},
        "paths": {},
    }

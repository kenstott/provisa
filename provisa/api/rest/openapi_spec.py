# Copyright (c) 2026 Kenneth Stott
# Canary: f76f1866-cd28-4cc5-9a98-d7c923b0f20c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Dynamic per-table OpenAPI 3.1 spec for /data/rest endpoints.

Generates a spec with one path entry per visible table, typed query parameters
per column, and typed response schemas — derived from the GraphQL schema.
"""

from __future__ import annotations

import json
from typing import Any

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
    Undefined,
)

from provisa.api._query_helpers import native_arguments
from provisa.api.rest.generator import _agg_fields_map
from provisa.api.role_surface import role_surface
from provisa.api.command_fields import CommandField, command_fields
from provisa.compiler.type_map import JSONScalar

_WHERE_OPS = ["eq", "neq", "gt", "gte", "lt", "lte", "like"]


def _agg_result_schema(
    agg_map: dict[str, list[str]], col_schemas: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Build the OpenAPI schema for an `aggregate { ... }` result object (REQ-1359).

    ``agg_map`` comes from ``_agg_fields_map`` — {func_name: [eligible column names]}.
    """
    props: dict[str, Any] = {"count": {"type": "integer"}}
    for func_name in ("sum", "avg", "stddev", "variance"):
        cols = agg_map.get(func_name)
        if cols:
            props[func_name] = {
                "type": "object",
                "properties": {c: {"type": "number", "format": "double"} for c in cols},
            }
    for func_name in ("min", "max"):
        cols = agg_map.get(func_name)
        if cols:
            props[func_name] = {
                "type": "object",
                "properties": {c: col_schemas.get(c, {"type": "string"}) for c in cols},
            }
    return {"type": "object", "properties": props}


def _gql_to_openapi_schema(gql_type: Any) -> dict[str, Any]:
    if isinstance(gql_type, GraphQLNonNull):
        return _gql_to_openapi_schema(gql_type.of_type)
    if isinstance(gql_type, GraphQLList):
        return {"type": "array", "items": _gql_to_openapi_schema(gql_type.of_type)}
    if gql_type is GraphQLString:
        return {"type": "string"}
    if gql_type is GraphQLInt:
        return {"type": "integer"}
    if gql_type is GraphQLFloat:
        return {"type": "number", "format": "double"}
    if gql_type is GraphQLBoolean:
        return {"type": "boolean"}
    # Custom scalars (DateTime, BigInt, JSON, etc.) — use string with description
    if isinstance(gql_type, GraphQLScalarType):
        return {"type": "string", "description": gql_type.name}
    if isinstance(gql_type, GraphQLEnumType):
        return {"type": "string", "enum": list(gql_type.values.keys())}
    return {"type": "string"}


def _is_scalar(gql_type: Any) -> bool:
    if isinstance(gql_type, (GraphQLNonNull, GraphQLList)):
        return _is_scalar(gql_type.of_type)
    return isinstance(gql_type, (GraphQLScalarType, GraphQLEnumType))


_FIELD_PATH_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"


def _fields_items(
    field_enum_name: str, relationships: list[tuple[str, GraphQLObjectType, bool]]
) -> dict[str, Any]:
    """The items of a row read's ``fields`` parameter: a field of the row, or, when the row has
    relationships, a dot-path through them (REQ-1929)."""
    field_ref = {"$ref": f"#/components/schemas/{field_enum_name}"}
    if not relationships:
        return field_ref
    examples = []
    for rel_name, rel_type, _is_list in relationships:
        scalars = [
            n for n, f in rel_type.fields.items() if not n.startswith("_") and _is_scalar(f.type)
        ]
        examples.append(f"{rel_name}.{scalars[0]}" if scalars else rel_name)
    path_form = {
        "type": "string",
        "pattern": _FIELD_PATH_PATTERN,
        "description": "A path through the row's relationships, one dot per relationship",
        "examples": examples,
    }
    return {"anyOf": [field_ref, path_form]}


def _argument_schema(arg: GraphQLArgument) -> dict[str, Any]:
    """A command argument's OpenAPI schema, from its GraphQL argument (REQ-1929)."""
    named = arg.type
    while isinstance(named, (GraphQLNonNull, GraphQLList)):
        named = named.of_type
    # REQ-1924: a JSON argument takes any JSON value, passed to the command as written.
    out: dict[str, Any] = {} if named is JSONScalar else _gql_to_openapi_schema(arg.type)
    if arg.default_value is not Undefined:  # REQ-2168: left out, it takes its default
        out = {**out, "default": arg.default_value}
    return out


def _command_rows_schema(cf: CommandField) -> dict[str, Any]:
    row_type = cf.row_type
    items: dict[str, Any] = {"type": "object"}
    if row_type is not None:
        items["properties"] = {
            n: _gql_to_openapi_schema(f.type)
            for n, f in row_type.fields.items()
            if not n.startswith("_") and _is_scalar(f.type)
        }
    return {"type": "object", "properties": {"data": {"type": "array", "items": items}}}


def _command_read_parameters(cf: CommandField) -> list[dict[str, Any]]:
    """A query-kind command's GET parameters: the ones a table read takes (filter, orderBy,
    limit, offset) and its declared arguments by name (REQ-1929)."""
    row_type = cf.row_type
    column: dict[str, Any] = {"type": "string"}
    if row_type is not None:
        column["enum"] = [
            n for n, f in row_type.fields.items() if not n.startswith("_") and _is_scalar(f.type)
        ]
    filter_item = {
        "type": "object",
        "required": ["field", "comparator", "value"],
        "properties": {
            "field": column,
            "comparator": {"$ref": "#/components/schemas/Comparator"},
            "value": {},
        },
    }
    order_item = {
        "type": "object",
        "required": ["field"],
        "properties": {"field": column, "direction": {"$ref": "#/components/schemas/Direction"}},
    }
    parameters: list[dict[str, Any]] = [
        {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1}},
        {"name": "offset", "in": "query", "schema": {"type": "integer", "minimum": 0}},
        {
            "name": "filter",
            "in": "query",
            "content": {"application/json": {"schema": {"type": "array", "items": filter_item}}},
        },
        {
            "name": "orderBy",
            "in": "query",
            "content": {"application/json": {"schema": {"type": "array", "items": order_item}}},
        },
    ]
    for name, arg in cf.arguments.items():
        parameters.append(
            {
                "name": name,
                "in": "query",
                "required": isinstance(arg.type, GraphQLNonNull),
                "schema": _argument_schema(arg),
            }
        )
    return parameters


def _command_path(cf: CommandField, cmd_name: str) -> dict[str, Any]:
    """The path item of one command: GET for a read, POST for a write (REQ-1929)."""
    operation: dict[str, Any] = {
        "summary": f"{'Read' if cf.is_read else 'Run'} command {cf.domain_id}.{cmd_name}",
        "operationId": f"call_{cmd_name}",
        "tags": [cf.domain_id],
        **({"description": cf.field.description} if cf.field.description else {}),
    }
    if cf.is_read:
        operation["parameters"] = _command_read_parameters(cf)
    else:
        props = {name: _argument_schema(arg) for name, arg in cf.arguments.items()}
        operation["requestBody"] = {
            "required": bool(props),
            "content": {"application/json": {"schema": {"type": "object", "properties": props}}},
        }
    error = {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
    operation["responses"] = {
        "200": {
            "description": "Success",
            "content": {"application/json": {"schema": _command_rows_schema(cf)}},
        },
        "400": {"description": "Bad request / invalid parameter", "content": error},
        "403": {"description": "Governance policy violation", "content": error},
        "404": {"description": "Command not found", "content": error},
    }
    return {"get" if cf.is_read else "post": operation}


def generate_rest_openapi_spec(
    state: Any,
    role_id: str,
    domains: list[str] | None = None,
    tables: list[str] | None = None,
) -> dict[str, Any]:
    """Generate an OpenAPI 3.1 spec with per-table paths for /data/rest.

    ``domains`` and ``tables`` narrow what the role is served; they never widen it. A description
    narrowed to tables holds those tables alone, so it holds no commands (REQ-2158).
    """
    surface = role_surface(state, role_id)  # one snapshot: the schema and its path map
    if surface is None:
        return _empty_spec()

    schema = surface.schema
    query_type = schema.query_type
    if query_type is None:
        return _empty_spec()

    path_map: dict[str, dict] = surface.table_path_map
    domain_filter = set(domains) if domains else None
    table_filter = set(tables) if tables else None

    paths: dict[str, Any] = {}
    domain_tag_descriptions: dict[str, str | None] = {}
    components: dict[str, Any] = {
        "Error": {
            "type": "object",
            "required": ["detail"],
            "properties": {
                "detail": {"type": "string", "description": "Error message"},
            },
        },
        "Comparator": {
            "type": "string",
            "enum": _WHERE_OPS,
        },
        "Direction": {
            "type": "string",
            "enum": ["asc", "desc"],
        },
    }

    for field_name, field in query_type.fields.items():
        meta = path_map.get(field_name)
        if meta is None:
            continue
        if domain_filter is not None and meta["domain_id"] not in domain_filter:
            continue
        if table_filter is not None and meta["table_name"] not in table_filter:
            continue

        domain_id = meta["domain_id"]
        table_name = meta["table_name"]
        table_description = meta.get("table_description") or field.description
        domain_description = meta.get("domain_description")
        path_key = f"/{domain_id}/{table_name}"

        # Unwrap NonNull / List to get the ObjectType
        inner = field.type
        while isinstance(inner, (GraphQLNonNull, GraphQLList)):
            inner = inner.of_type
        if not isinstance(inner, GraphQLObjectType):
            continue

        type_name = inner.name  # e.g. "Pets", "PetStore__Animals"

        # Collect scalar columns with their OpenAPI types (skip internal _meta_ fields)
        columns: list[tuple[str, dict[str, Any]]] = []
        for col_name, col_field in inner.fields.items():
            if col_name.startswith("_"):
                continue
            if _is_scalar(col_field.type):
                col_schema = _gql_to_openapi_schema(col_field.type)
                if col_field.description:
                    col_schema = {**col_schema, "description": col_field.description}
                columns.append((col_name, col_schema))

        # REQ-1929: each relationship of the row, read nested when ?fields= names a path through
        # it: a many-to-one as an object, a one-to-many as a list.
        relationships: list[tuple[str, GraphQLObjectType, bool]] = []
        for rel_name, rel_field in inner.fields.items():
            if rel_name.startswith("_") or _is_scalar(rel_field.type):
                continue
            rel_type, is_list = rel_field.type, False
            while isinstance(rel_type, (GraphQLNonNull, GraphQLList)):
                is_list = is_list or isinstance(rel_type, GraphQLList)
                rel_type = rel_type.of_type
            if isinstance(rel_type, GraphQLObjectType):
                relationships.append((rel_name, rel_type, is_list))

        field_enum_name = f"{type_name}Field"
        filter_type_name = f"{type_name}Filter"
        col_names = [col for col, _ in columns]

        # Register named schemas in components
        row_properties: dict[str, Any] = {col: schema_obj for col, schema_obj in columns}
        for rel_name, rel_type, is_list in relationships:
            nested = f"Present when fields names a path through {rel_name} ({rel_name}.<field>)"
            row_properties[rel_name] = (
                {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": f"The related {rel_type.name} rows. {nested}",
                }
                if is_list
                else {
                    "type": "object",
                    "description": f"The related {rel_type.name} row. {nested}",
                }
            )
        row_schema: dict[str, Any] = {"type": "object", "properties": row_properties}
        if table_description:
            row_schema["description"] = table_description
        components[type_name] = row_schema

        # Track domain descriptions for top-level tags
        domain_tag_descriptions[domain_id] = domain_description
        order_by_type_name = f"{type_name}OrderBy"

        components[field_enum_name] = {
            "type": "string",
            "enum": col_names,
        }
        components[filter_type_name] = {
            "type": "object",
            "required": ["field", "comparator", "value"],
            "properties": {
                "field": {"$ref": f"#/components/schemas/{field_enum_name}"},
                "comparator": {"$ref": "#/components/schemas/Comparator"},
                "value": {"type": "string"},
            },
        }
        components[order_by_type_name] = {
            "type": "object",
            "required": ["field", "direction"],
            "properties": {
                "field": {"$ref": f"#/components/schemas/{field_enum_name}"},
                "direction": {"$ref": "#/components/schemas/Direction"},
            },
        }

        # REQ-1359: aggregate/groupBy params + component schemas are only registered for
        # tables that actually expose {field}_aggregate / {field}_group_by root fields —
        # same schema-presence gate `_agg_fields_map` uses in the REST generator.
        col_schemas = dict(columns)
        agg_map = _agg_fields_map(schema, field_name, group_by=False)
        gb_agg_map = _agg_fields_map(schema, field_name, group_by=True)
        agg_result_name = f"{type_name}AggregateResult"
        group_by_row_name = f"{type_name}GroupByRow"

        if agg_map is not None:
            components[agg_result_name] = _agg_result_schema(agg_map, col_schemas)
        if gb_agg_map is not None:
            if agg_result_name not in components:
                components[agg_result_name] = _agg_result_schema(gb_agg_map, col_schemas)
            components[group_by_row_name] = {
                "type": "object",
                "properties": {
                    "groupKey": {
                        "description": (
                            "Group-by key value (or array of values for multi-column groupBy)"
                        )
                    },
                    "aggregate": {"$ref": f"#/components/schemas/{agg_result_name}"},
                },
            }

        parameters: list[dict[str, Any]] = [
            {
                "name": "limit",
                "in": "query",
                "schema": {"type": "integer", "minimum": 1},
                "description": "Maximum rows to return",
            },
            {
                "name": "offset",
                "in": "query",
                "schema": {"type": "integer", "minimum": 0},
                "description": "Row offset for pagination",
            },
            {
                "name": "fields",
                "in": "query",
                "style": "form",
                "explode": False,
                "schema": {"type": "array", "items": _fields_items(field_enum_name, relationships)},
                "description": (
                    "Fields to return. A bare name is a field of the row; a dot-path goes "
                    "through relationships (`<relationship>.<field>`, one dot per relationship) and "
                    "returns the related row nested: a many-to-one as an object, a one-to-many "
                    "as a list. A relationship named alone returns its fields."
                ),
            },
            {
                "name": "filter",
                "in": "query",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "array",
                            "items": {"$ref": f"#/components/schemas/{filter_type_name}"},
                            "default": [],
                        }
                    }
                },
            },
            {
                "name": "orderBy",
                "in": "query",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "array",
                            "items": {"$ref": f"#/components/schemas/{order_by_type_name}"},
                            "default": [],
                        }
                    }
                },
            },
        ]

        # REQ-1986: a table with parameters takes them as one JSON object, by name.
        table_parameters = native_arguments(field)
        if table_parameters:
            required = [
                n for n, (_, a) in table_parameters.items() if isinstance(a.type, GraphQLNonNull)
            ]
            parameters.append(
                {
                    "name": "params",
                    "in": "query",
                    "required": bool(required),
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    n: _gql_to_openapi_schema(a.type)
                                    for n, (_, a) in table_parameters.items()
                                },
                                "required": required,
                                "additionalProperties": False,
                            }
                        }
                    },
                    "description": "The table's parameters, by name",
                }
            )

        if agg_map is not None or gb_agg_map is not None:
            parameters.append(
                {
                    "name": "aggregate",
                    "in": "query",
                    "schema": {
                        "type": "string",
                        "description": "Comma-separated aggregate functions, e.g. count,sum,avg",
                    },
                    "description": "Request an aggregate result instead of rows",
                }
            )
        if gb_agg_map is not None:
            parameters.append(
                {
                    "name": "groupBy",
                    "in": "query",
                    "schema": {
                        "type": "string",
                        "description": "Comma-separated column names, e.g. species,breedName",
                    },
                    "description": "Group rows by these columns, returning per-group aggregates",
                }
            )
            parameters.append(
                {
                    "name": "includeNodes",
                    "in": "query",
                    "schema": {"type": "string"},
                    "description": (
                        "Attach a nodes array of the underlying rows to each group"
                        ". `true` attaches every field; a JSON array of "
                        'dot-notated paths (e.g. `["user_id","user.email"]`) projects '
                        "to just those fields — a bare path names a scalar field on the "
                        "row, each further dot segment names a scalar field one more "
                        "relationship hop away. Handled by generator.py's "
                        "raw_params lookup regardless of this declaration; without it, "
                        "Swagger UI's Try-it-out never renders an input to send it."
                    ),
                }
            )

        row_array_schema = {
            "type": "object",
            "properties": {
                "data": {
                    "type": "array",
                    "items": {"$ref": f"#/components/schemas/{type_name}"},
                }
            },
        }

        operation: dict[str, Any] = {
            "summary": f"Query {domain_id}.{table_name}",
            "operationId": f"get_{field_name}",
            "tags": [domain_id],
            "parameters": parameters,
        }
        if table_description:
            operation["description"] = table_description

        paths[path_key] = {
            "get": {
                **operation,
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {
                            "application/json": {"schema": row_array_schema},
                            "text/csv": {"schema": {"type": "string"}},
                            "application/vnd.apache.parquet": {
                                "schema": {"type": "string", "format": "binary"}
                            },
                            "application/vnd.apache.arrow.stream": {
                                "schema": {"type": "string", "format": "binary"}
                            },
                        },
                    },
                    "400": {
                        "description": "Bad request / invalid filter",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Error"}}
                        },
                    },
                    "403": {
                        "description": "Governance policy violation",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Error"}}
                        },
                    },
                    "404": {
                        "description": "Table not found",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Error"}}
                        },
                    },
                },
            }
        }

    # REQ-1929: the commands are the role's GraphQL command fields, under their REST command
    # bindings (REQ-2168): a Query field a GET read, a Mutation field a POST write. A description
    # narrowed to tables holds no commands (REQ-2158).
    if table_filter is None:
        for cf in command_fields(schema):
            if domain_filter is not None and cf.domain_id not in domain_filter:
                continue
            cmd_name = cf.name_in("rest")
            if cmd_name is None:
                continue
            domain_tag_descriptions.setdefault(
                cf.domain_id, cf.definition.get("domain_description")
            )
            paths[f"/{cf.domain_id}/commands/{cmd_name}"] = _command_path(cf, cmd_name)

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "Provisa REST API",
            "version": "0.1.0",
            "description": (
                "Auto-generated per-table REST endpoints. "
                "Governance applies uniformly across all query interfaces."
            ),
        },
        "servers": [{"url": "/data/rest"}],
        "tags": [
            {"name": domain_id, **({"description": desc} if desc else {})}
            for domain_id, desc in sorted(domain_tag_descriptions.items())
        ],
        "paths": paths,
        "components": {"schemas": components},
    }


def _empty_spec() -> dict[str, Any]:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Provisa REST API", "version": "0.1.0"},
        "paths": {},
    }


_THEME_CLASS_SLOT = "__PROVISA_THEME_CLASS__"
_DOC_SCOPE_SLOT = "__PROVISA_DOC_SCOPE__"

SWAGGER_UI_HTML = """\
<!DOCTYPE html>
<html lang="en" class="__PROVISA_THEME_CLASS__">
<head>
  <meta charset="UTF-8" />
  <title>Provisa REST API</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="stylesheet" href="/swagger-ui/swagger-ui.css" />
  <style>
    /* Provisa design tokens — light defaults, dark overrides below */
    :root {
      --bg: #ffffff;
      --surface: #f4f5f8;
      --border: #e1e4ed;
      --text: #1a1d27;
      --text-muted: #6b7080;
      --primary: #6366f1;
      --primary-hover: #4f46e5;
      --destructive: #ef4444;
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    html.dark-mode {
      --bg: #0f1117;
      --surface: #1a1d27;
      --border: #2a2d37;
      --text: #e1e4ed;
      --text-muted: #8b8fa3;
      --primary-hover: #818cf8;
    }

    html, body { margin: 0; padding: 0; background: var(--bg); color: var(--text); font-family: var(--font); }

    /* Download bar */
    #download-bar {
      padding: 6px 16px;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      display: flex; align-items: center; gap: 12px;
    }
    #download-bar a { color: var(--primary-hover); font-size: 0.8rem; text-decoration: none; }
    #download-bar a:hover { text-decoration: underline; }

    /* Root containers — dark-mode class activates swagger-ui's built-in dark CSS;
       override its palette to match Provisa tokens */
    html.dark-mode { background: var(--bg) !important; }
    .swagger-ui { background: var(--bg) !important; color: var(--text) !important; font-family: var(--font) !important; }
    .swagger-ui .topbar { display: none; }
    .swagger-ui .wrapper { background: var(--bg) !important; }
    .swagger-ui .scheme-container { background: var(--bg) !important; box-shadow: none !important; border: none !important; padding: 8px 0; }

    /* Info block */
    .swagger-ui .info { margin: 20px 0; }
    .swagger-ui .info .title,
    .swagger-ui .info h1,
    .swagger-ui .info h2,
    .swagger-ui .info h3,
    .swagger-ui .info h4,
    .swagger-ui .info h5 { color: var(--text) !important; font-size: 1.4rem; }
    .swagger-ui .info p,
    .swagger-ui .info li { color: var(--text-muted) !important; }
    .swagger-ui .info a { color: var(--primary-hover) !important; }

    /* Tag / section headers */
    .swagger-ui .opblock-tag { color: var(--text) !important; border-bottom: 1px solid var(--border) !important; font-size: 1rem; }
    .swagger-ui .opblock-tag:hover { background: var(--surface) !important; }
    .swagger-ui .opblock-tag small { color: var(--text-muted) !important; }
    .swagger-ui .opblock-tag-section h3 { color: var(--text) !important; }

    /* Operation blocks */
    .swagger-ui .opblock { background: var(--surface) !important; border-color: var(--border) !important; box-shadow: none !important; margin: 4px 0; }
    .swagger-ui .opblock .opblock-summary { border-bottom: 1px solid var(--border) !important; }
    .swagger-ui .opblock .opblock-summary-method { min-width: 70px; font-size: 0.75rem; font-weight: 700; }
    .swagger-ui .opblock .opblock-summary-path { color: var(--text) !important; font-family: monospace; }
    .swagger-ui .opblock .opblock-summary-description { color: var(--text-muted) !important; font-size: 0.8rem; }
    .swagger-ui .opblock-description-wrapper p { color: var(--text-muted) !important; }
    .swagger-ui .opblock-body { background: var(--bg) !important; }
    .swagger-ui .opblock-section-header { background: var(--surface) !important; border-bottom: 1px solid var(--border) !important; }
    .swagger-ui .opblock-section-header h4 { color: var(--text-muted) !important; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }

    /* GET method color */
    .swagger-ui .opblock.opblock-get { border-color: var(--primary) !important; }
    .swagger-ui .opblock.opblock-get .opblock-summary { background: rgba(99,102,241,0.08) !important; }
    .swagger-ui .opblock.opblock-get .opblock-summary-method { background: var(--primary) !important; color: #fff !important; }

    /* Parameters */
    .swagger-ui .parameters-col_name { color: var(--text) !important; font-family: monospace; font-size: 0.85rem; }
    .swagger-ui .parameters-col_description { color: var(--text-muted) !important; }
    .swagger-ui .parameter__name { color: var(--text) !important; }
    .swagger-ui .parameter__type { color: var(--primary-hover) !important; font-size: 0.75rem; }
    .swagger-ui .parameter__in { color: var(--text-muted) !important; font-size: 0.7rem; font-style: italic; }
    .swagger-ui table thead tr td,
    .swagger-ui table thead tr th { color: var(--text-muted) !important; border-bottom: 1px solid var(--border) !important; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
    .swagger-ui table tbody tr td { border-bottom: 1px solid var(--border) !important; color: var(--text) !important; }

    /* Inputs */
    .swagger-ui input { background: var(--bg) !important; color: var(--text) !important; border-color: var(--border) !important; }
    .swagger-ui textarea { background: var(--bg) !important; color: var(--text) !important; border-color: var(--border) !important; }
    .swagger-ui select { background: var(--bg) !important; color: var(--text) !important; border-color: var(--border) !important; }
    .swagger-ui input:focus { border-color: var(--primary) !important; }

    /* Buttons */
    .swagger-ui .btn { font-family: var(--font); border-radius: 4px; }
    .swagger-ui .btn.execute { background: var(--primary) !important; border-color: var(--primary) !important; color: #fff !important; }
    .swagger-ui .btn.execute:hover { background: var(--primary-hover) !important; border-color: var(--primary-hover) !important; }
    .swagger-ui .btn.cancel { background: transparent !important; border-color: var(--border) !important; color: var(--text-muted) !important; }
    .swagger-ui .btn.try-out__btn { background: transparent !important; border: 1px solid var(--primary) !important; color: var(--primary) !important; }
    .swagger-ui .btn.authorize { border-color: var(--primary) !important; color: var(--primary) !important; background: transparent !important; }

    /* Response section */
    .swagger-ui .responses-inner { background: var(--bg) !important; }
    .swagger-ui .response-col_status { color: var(--text) !important; }
    .swagger-ui .response-col_description { color: var(--text-muted) !important; }
    .swagger-ui .highlight-code { background: var(--surface) !important; border-radius: 4px; }
    .swagger-ui .highlight-code pre { background: var(--surface) !important; color: var(--text) !important; }
    .swagger-ui .microlight { background: var(--surface) !important; color: var(--text) !important; }
    .swagger-ui .response-control-media-type select { background: var(--bg) !important; color: var(--text) !important; border-color: var(--border) !important; }

    /* ── Models / Schemas section ── */
    .swagger-ui section.models { border: 1px solid var(--border) !important; background: var(--bg) !important; border-radius: 6px; margin-top: 1rem; }
    .swagger-ui section.models h4 { color: var(--text) !important; border-bottom: 1px solid var(--border) !important; border-color: var(--border) !important; padding: 12px 20px; margin: 0; font-size: 0.875rem; text-transform: uppercase; letter-spacing: 0.05em; }
    .swagger-ui section.models .model-container { background: var(--bg) !important; border-top: 1px solid var(--border); margin: 0; padding: 6px 20px; }
    .swagger-ui section.models .model-container:first-of-type { border-top: none; }
    .swagger-ui section.models a { color: var(--text-muted) !important; font-size: 0.72rem; }
    .swagger-ui section.models a:hover { color: var(--primary-hover) !important; }

    /* model-box-control button (schema collapse toggle) */
    .swagger-ui .model-box-control { color: var(--text) !important; background: transparent !important; }
    .swagger-ui .model-box-control:not(.prop) { color: var(--text) !important; }
    .swagger-ui .model-box { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: 4px; }

    /* ── OAS 3.1 json-schema-2020-12 renderer — this is the key fix ──
       Swagger UI renders schema names using json-schema-2020-12-accordion <button>
       elements. Without explicit overrides, <button> defaults to native
       browser light-gray background even when the parent is dark. */
    .swagger-ui .json-schema-2020-12 { background: var(--bg) !important; border-radius: 4px; }
    .swagger-ui .json-schema-2020-12 button { background: transparent !important; color: var(--text) !important; }
    .swagger-ui .json-schema-2020-12-accordion { background: transparent !important; color: var(--text) !important; border: none !important; }
    .swagger-ui .json-schema-2020-12__title { color: var(--text) !important; font-family: monospace !important; font-weight: 600 !important; }
    .swagger-ui .json-schema-2020-12-keyword--description { color: var(--text-muted) !important; }
    .swagger-ui .json-schema-2020-12-keyword--description p { color: var(--text-muted) !important; }
    .swagger-ui .json-schema-2020-12-json-viewer__name,
    .swagger-ui .json-schema-2020-12-json-viewer__value { color: var(--text) !important; }
    .swagger-ui .json-schema-2020-12-keyword--enum .json-schema-2020-12-json-viewer__name,
    .swagger-ui .json-schema-2020-12-keyword--enum .json-schema-2020-12-json-viewer__value { color: var(--text-muted) !important; }
    .swagger-ui .json-schema-2020-12-keyword--const .json-schema-2020-12-json-viewer__name,
    .swagger-ui .json-schema-2020-12-keyword--const .json-schema-2020-12-json-viewer__value,
    .swagger-ui .json-schema-2020-12-keyword--default .json-schema-2020-12-json-viewer__name,
    .swagger-ui .json-schema-2020-12-keyword--default .json-schema-2020-12-json-viewer__value { color: var(--text) !important; }
    .swagger-ui .json-schema-2020-12__constraint { background-color: var(--primary) !important; color: #fff !important; }
    .swagger-ui .json-schema-2020-12__constraint--string { background-color: var(--primary-hover) !important; color: #fff !important; }
    .swagger-ui .json-schema-2020-12-expand-deep-button { background: transparent !important; color: var(--text-muted) !important; border: 1px solid var(--border) !important; border-radius: 3px; }
    .swagger-ui .json-schema-2020-12__title .json-schema-2020-12-keyword__name { color: var(--text) !important; }
    .swagger-ui .json-schema-2020-12-keyword--\\$vocabulary-uri { color: var(--text-muted) !important; }
    .swagger-ui .json-schema-2020-12-keyword ul { border-left-color: var(--border) !important; }

    /* Legacy model renderer (used when not OAS 3.1) */
    .swagger-ui .model { color: var(--text) !important; background: transparent !important; }
    .swagger-ui .model .property { color: var(--text-muted) !important; }
    .swagger-ui .model .property.primitive { color: var(--text) !important; }
    .swagger-ui .model .prop-type { color: var(--primary-hover) !important; background: transparent !important; }
    .swagger-ui .model .prop-format { color: var(--text-muted) !important; background: transparent !important; }
    .swagger-ui .model-title { color: var(--text) !important; }
    .swagger-ui .model a { color: var(--primary-hover) !important; }
    .swagger-ui .inner-object { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: 4px; }

    /* Required star */
    .swagger-ui table.model tr.property-row .star { color: var(--destructive) !important; }

    /* Loading */
    .swagger-ui .loading-container .loading::after { color: var(--text-muted) !important; }

    /* Scrollbar */
    .swagger-ui ::-webkit-scrollbar { width: 6px; height: 6px; }
    .swagger-ui ::-webkit-scrollbar-track { background: var(--bg); }
    .swagger-ui ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    .swagger-ui ::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
  </style>
</head>
<body>
  <div id="download-bar"></div>
  <div id="swagger-ui"></div>
  <script src="/swagger-ui/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      // The role and domains this page describes, stamped by the server (see swagger_ui_html).
      const scope = __PROVISA_DOC_SCOPE__;
      const urlRole = scope.role || "";
      const urlDomains = scope.domains || "";
      const specParams = new URLSearchParams();
      if (urlRole) specParams.set("role", urlRole);
      if (urlDomains) specParams.set("domains", urlDomains);
      const baseQuery = specParams.toString();
      const specUrl = "/data/rest/openapi.json" + (baseQuery ? "?" + baseQuery : "");
      const dlParams = new URLSearchParams(specParams);
      dlParams.set("download", "1");
      const dlUrl = "/data/rest/openapi.json?" + dlParams.toString();
      // The headers every request from this page carries: the role the page was opened for and the
      // signed-in credential. A break-glass operator session is stored under its own key,
      // because the Firebase token sync deletes `provisa_token` whenever it sees no signed-in user.
      // This page runs in its own srcdoc document with no access to the app's fetch interceptor, so
      // it repeats the same preference order the app's storedToken() uses, and the same place:
      // a demo sandbox's visitor keeps the tab's identity in sessionStorage (browserStorage.ts).
      const visitor = sessionStorage.getItem("provisa_visitor") === "true";
      const identity = (key) =>
        visitor ? sessionStorage.getItem("visitor:" + key) : localStorage.getItem(key);
      const withIdentity = (headers) => {
        const role = urlRole || identity("provisa_role");
        if (role) headers["x-provisa-role"] = role;
        const token = identity("provisa_su_token") || identity("provisa_token");
        if (token && !headers["Authorization"]) headers["Authorization"] = "Bearer " + token;
        const orgId = identity("provisa_org");
        if (orgId && !headers["X-Org-Provisa"]) headers["X-Org-Provisa"] = orgId;
        return headers;
      };

      // The download is fetched as the page's own requests are — a bare link carries neither the
      // role header nor the credential, so it was answered as some other role or not at all.
      const bar = document.getElementById("download-bar");
      const link = document.createElement("a");
      link.href = "#";
      link.id = "download-spec";
      link.textContent = "⬇ Download openapi.json";
      const status = document.createElement("span");
      status.id = "download-status";
      link.addEventListener("click", async (event) => {
        event.preventDefault();
        status.textContent = "";
        const res = await fetch(dlUrl, { headers: withIdentity({}) });
        if (!res.ok) {
          const body = await res.text();
          let detail = body;
          try { detail = JSON.parse(body).detail || body; } catch (e) { /* not JSON: show the text */ }
          status.textContent = "Download failed (" + res.status + "): " + detail;
          return;
        }
        const objectUrl = URL.createObjectURL(await res.blob());
        const save = document.createElement("a");
        save.href = objectUrl;
        save.download = "openapi.json";
        save.click();
        URL.revokeObjectURL(objectUrl);
      });
      bar.append(link, status);

      SwaggerUIBundle({
        url: specUrl,
        dom_id: "#swagger-ui",
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: "BaseLayout",
        deepLinking: true,
        tryItOutEnabled: true,
        requestInterceptor: (req) => {
          withIdentity(req.headers);
          return req;
        },
      });
    };
  </script>
</body>
</html>
"""


def swagger_ui_html(theme: str | None, role: str | None = None, domains: str | None = None) -> str:
    """The Swagger UI page with the app's colour scheme, role and domain selection already applied.

    The theme is stamped into the markup rather than read from ``location.search`` in the browser:
    the UI loads this page into a ``srcDoc`` iframe so it can attach the bearer token, and a
    srcdoc document's location is ``about:srcdoc`` — it carries no query string, so the page read
    no theme and every deployment rendered dark on a light app.

    Anything other than "light" is dark, which is what a direct visit to /data/rest/docs (no theme
    param, no app around it) gets.

    The role and domains are stamped for the same reason: read from ``location.search`` they were
    always empty in the app, so the spec described every domain whatever the navbar selection was.
    """
    scope = json.dumps({"role": role or "", "domains": domains or ""})
    # Inside a <script>, "</" could close the element early; \u003c reads back as the same "<".
    scope = scope.replace("<", "\\u003c")
    return SWAGGER_UI_HTML.replace(
        _THEME_CLASS_SLOT, "" if theme == "light" else "dark-mode"
    ).replace(_DOC_SCOPE_SLOT, scope)

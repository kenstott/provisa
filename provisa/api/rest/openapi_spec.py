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

from typing import Any

from graphql import (
    GraphQLBoolean,
    GraphQLEnumType,
    GraphQLFloat,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLScalarType,
    GraphQLString,
)

_WHERE_OPS = ["eq", "neq", "gt", "gte", "lt", "lte", "like"]


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


def _arg_type_to_openapi(arg_type: str) -> dict[str, Any]:
    """Map a command argument's GraphQL scalar name to an OpenAPI schema (REQ-1155)."""
    t = (arg_type or "").lower()
    if t == "int":
        return {"type": "integer"}
    if t == "float":
        return {"type": "number", "format": "double"}
    if t == "boolean":
        return {"type": "boolean"}
    return {"type": "string"}


def generate_rest_openapi_spec(
    state: Any, role_id: str, domains: list[str] | None = None
) -> dict[str, Any]:
    """Generate an OpenAPI 3.1 spec with per-table paths for /data/rest."""
    if role_id not in state.schemas:
        return _empty_spec()

    schema = state.schemas[role_id]
    query_type = schema.query_type
    if query_type is None:
        return _empty_spec()

    path_map: dict[str, dict] = getattr(state, "table_path_maps", {}).get(role_id, {})
    domain_filter = set(domains) if domains else None

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

        field_enum_name = f"{type_name}Field"
        filter_type_name = f"{type_name}Filter"
        col_names = [col for col, _ in columns]

        # Register named schemas in components
        row_schema: dict[str, Any] = {
            "type": "object",
            "properties": {col: schema_obj for col, schema_obj in columns},
        }
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
                "schema": {
                    "type": "array",
                    "items": {"$ref": f"#/components/schemas/{field_enum_name}"},
                },
                "description": "Fields to return",
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

    # REQ-1155: registered commands are POST paths so they appear in the OpenAPI/Swagger surface
    # alongside tables. Invocation routes through the same governed executor as every other surface.
    _seen_cmd: set[str] = set()
    # Functions AND webhooks are governed commands (REQ-872 / REQ-1155): both appear as POST paths.
    for fn in [
        *(getattr(state, "tracked_functions", {}) or {}).values(),
        *(getattr(state, "tracked_webhooks", {}) or {}).values(),
    ]:
        cmd_name = fn.get("name")
        if not cmd_name or cmd_name in _seen_cmd:
            continue
        cmd_domain = fn.get("domain_id", "")
        if domain_filter is not None and cmd_domain not in domain_filter:
            continue
        visible_to = fn.get("visible_to") or []
        if visible_to and role_id not in visible_to:
            continue
        _seen_cmd.add(cmd_name)

        arg_props: dict[str, Any] = {}
        for a in fn.get("arguments") or []:
            a_name = a.get("name")
            if not a_name:
                continue
            arg_props[a_name] = _arg_type_to_openapi(a.get("type", "String"))

        cmd_path = f"/{cmd_domain}/commands/{cmd_name}"
        domain_tag_descriptions.setdefault(cmd_domain, fn.get("domain_description"))
        paths[cmd_path] = {
            "post": {
                "summary": f"Run command {cmd_domain}.{cmd_name}",
                "operationId": f"call_{cmd_name}",
                "tags": [cmd_domain],
                **({"description": fn["description"]} if fn.get("description") else {}),
                "requestBody": {
                    "required": bool(arg_props),
                    "content": {
                        "application/json": {
                            "schema": {"type": "object", "properties": arg_props}
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": {"type": "object"},
                                        }
                                    },
                                }
                            }
                        },
                    },
                    "403": {
                        "description": "Governance policy violation",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Error"}}
                        },
                    },
                    "404": {
                        "description": "Command not found",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Error"}}
                        },
                    },
                },
            }
        }

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


SWAGGER_UI_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Provisa REST API</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  <script>
    // Apply the app's theme (passed via ?theme=dark|light) before paint so the
    // page never flashes the wrong scheme. Defaults to dark when absent.
    (function () {
      var theme = new URLSearchParams(window.location.search).get("theme");
      if (theme !== "light") document.documentElement.classList.add("dark-mode");
    })();
  </script>
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
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      const params = new URLSearchParams(window.location.search);
      const urlRole = params.get("role") || "";
      const urlDomains = params.get("domains") || "";
      const specParams = new URLSearchParams();
      if (urlRole) specParams.set("role", urlRole);
      if (urlDomains) specParams.set("domains", urlDomains);
      const baseQuery = specParams.toString();
      const specUrl = "/data/rest/openapi.json" + (baseQuery ? "?" + baseQuery : "");
      const dlParams = new URLSearchParams(specParams);
      dlParams.set("download", "1");
      const dlUrl = "/data/rest/openapi.json?" + dlParams.toString();
      const bar = document.getElementById("download-bar");
      bar.innerHTML = '<a href="' + dlUrl + '" download="openapi.json">⬇ Download openapi.json</a>';

      SwaggerUIBundle({
        url: specUrl,
        dom_id: "#swagger-ui",
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: "BaseLayout",
        deepLinking: true,
        tryItOutEnabled: true,
        requestInterceptor: (req) => {
          const role = urlRole || localStorage.getItem("provisa_role");
          if (role) req.headers["x-provisa-role"] = role;
          return req;
        },
      });
    };
  </script>
</body>
</html>
"""

# Copyright (c) 2026 Kenneth Stott
# Canary: 6c9f5540-6f97-482a-b00b-6b4a45e0ac66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's reach into the admin API the UI uses (REQ-2114).

Polly acts as the chatting user, through the same API the UI calls, so every action meets the
capability checks the UI meets. Two general tools stand in for one tool per action:

- the admin GraphQL schema is read (:func:`schema_fields`, :func:`schema_type`) and its queries
  run as the user (:func:`run_query`), so she finds what exists as the UI does;
- an admin mutation is proposed, shown in the chat, and sent by the user's own browser session
  once the user confirms there (the ``admin_mutation`` client tool) -- the mutation the UI's own
  page sends, under the user's own identity. A mutation added later is hers with no Polly change.

A UI action that is a REST route and not a mutation (a file upload, a settings change, the org
folder's encryption, a code archive upload) is listed (:func:`rest_routes`) and reached the same
way, through the ``admin_request`` client tool. "Create any relationships to <table>" runs the
relationship discovery the Relationships page runs, for that table (:func:`discover_relationships`),
and a candidate the user confirms is accepted through the page's own route.
"""

# Requirements: REQ-2114

from __future__ import annotations

import inspect
from typing import Any

#: REST routes that are not the UI's actions: the admin GraphQL endpoint itself (reached through
#: the schema tools) and the chat that is asking.
_NOT_ACTIONS = ("/admin/graphql", "/admin/mcp")
#: The REST surfaces the UI's admin screens call.
_ADMIN_PREFIXES = ("/admin/", "/auth/email", "/data-products/")


def _graphql_schema() -> Any:
    from provisa.api.admin.schema import admin_schema

    return admin_schema._schema


def _type_name(gql_type: Any) -> str:
    """A GraphQL type as written in SDL: ``[String!]!``."""
    from graphql import GraphQLList, GraphQLNonNull

    if isinstance(gql_type, GraphQLNonNull):
        return f"{_type_name(gql_type.of_type)}!"
    if isinstance(gql_type, GraphQLList):
        return f"[{_type_name(gql_type.of_type)}]"
    return gql_type.name


def _field(name: str, field: Any) -> dict:
    args = ", ".join(f"{a}: {_type_name(arg.type)}" for a, arg in field.args.items())
    return {
        "name": name,
        "signature": f"{name}({args}): {_type_name(field.type)}"
        if args
        else f"{name}: {_type_name(field.type)}",
        "description": field.description or "",
    }


def schema_fields(kind: str, search: str | None = None) -> list[dict]:
    """The admin schema's root fields of ``kind`` ("query" or "mutation"), each as its SDL
    signature and description, narrowed to those whose name or description holds ``search``."""
    schema = _graphql_schema()
    root = schema.query_type if kind == "query" else schema.mutation_type
    if root is None:
        raise ValueError(f"the admin schema has no {kind} type")
    needle = (search or "").lower()
    return [
        _field(name, field)
        for name, field in sorted(root.fields.items())
        if not needle or needle in name.lower() or needle in (field.description or "").lower()
    ]


def schema_type(name: str) -> dict:
    """One named type of the admin schema: its kind and its fields, input fields or values."""
    from graphql import (
        GraphQLEnumType,
        GraphQLInputObjectType,
        GraphQLInterfaceType,
        GraphQLObjectType,
    )

    gql_type = _graphql_schema().get_type(name)
    if gql_type is None:
        raise ValueError(f"the admin schema has no type {name!r}")
    if isinstance(gql_type, GraphQLEnumType):
        return {"name": name, "kind": "enum", "values": sorted(gql_type.values)}
    if isinstance(gql_type, GraphQLInputObjectType):
        return {
            "name": name,
            "kind": "input",
            "fields": [
                {"name": f, "type": _type_name(v.type), "description": v.description or ""}
                for f, v in gql_type.fields.items()
            ],
        }
    if isinstance(gql_type, (GraphQLObjectType, GraphQLInterfaceType)):
        return {
            "name": name,
            "kind": "object",
            "fields": [_field(f, v) for f, v in gql_type.fields.items()],
        }
    return {"name": name, "kind": "scalar", "description": gql_type.description or ""}


async def run_query(query: str, variables: dict | None, *, request: Any) -> dict:
    """Run an admin GraphQL query as the user: the schema's own resolvers, under the request the
    user's chat arrived on, so it sees what the UI shows that user. A mutation is refused here --
    it is proposed and confirmed through ``admin_mutation`` instead."""
    from graphql import OperationDefinitionNode, OperationType, parse

    document = parse(query)
    operations = [d for d in document.definitions if isinstance(d, OperationDefinitionNode)]
    if not operations or any(op.operation != OperationType.QUERY for op in operations):
        raise ValueError(
            "admin_query runs queries only; propose a mutation with admin_mutation, which the "
            "user confirms in the chat"
        )
    from provisa.api.admin.schema import admin_schema

    result = await admin_schema.execute(
        query, variable_values=variables or {}, context_value={"request": request}
    )
    return {
        "data": result.data,
        "errors": [e.message for e in result.errors] if result.errors else [],
    }


def _body_schema(route: Any) -> dict | None:
    """What a route takes in its body: the file and fields of a multipart form, or the JSON
    model's fields."""
    from fastapi.params import Form

    params = route.dependant.body_params
    if not params:
        return None
    if any(isinstance(p.field_info, Form) for p in params):
        return {"multipart": [p.name for p in params]}
    annotation = getattr(route.body_field.field_info, "annotation", None)
    fields = getattr(annotation, "model_fields", None)
    if fields is None:
        return {"json": {p.name: str(getattr(p.field_info, "annotation", "")) for p in params}}
    return {
        "json": {
            name: str(getattr(f.annotation, "__name__", f.annotation)) for name, f in fields.items()
        }
    }


def rest_routes(app: Any, search: str | None = None) -> list[dict]:
    """The REST routes the UI's admin screens call, each with its method, path, what it does
    (its docstring's first line), its query parameters and its body, narrowed to those whose path
    or summary holds ``search``. An upload route reads the file's bytes as its body, so it shows
    none: its summary and its ``filename`` parameter say what it takes."""
    needle = (search or "").lower()
    found = []
    for route in app.routes:
        path = getattr(route, "path", "")
        methods = getattr(route, "methods", None)
        if not methods or not path.startswith(_ADMIN_PREFIXES) or path.startswith(_NOT_ACTIONS):
            continue
        doc = inspect.getdoc(getattr(route, "endpoint", None)) or ""
        summary = doc.split("\n\n", 1)[0].replace("\n", " ")
        if needle and needle not in path.lower() and needle not in summary.lower():
            continue
        query = [p.name for p in route.dependant.query_params]
        for method in sorted(methods - {"HEAD", "OPTIONS"}):
            found.append(
                {
                    "method": method,
                    "path": path,
                    "summary": summary,
                    "query": query,
                    "body": _body_schema(route),
                }
            )
    return sorted(found, key=lambda r: (r["path"], r["method"]))


async def discover_relationships(state: Any, schema: str, table: str, *, request: Any) -> dict:
    """Run the Relationships page's discovery for one table, as the user, and the candidates it
    found: each with its id, the columns it would join and why. Accepting one is the page's own
    route (``POST /admin/discover/candidates/{id}/accept``), sent once the user confirms."""
    from provisa.api.admin.discovery import (
        DiscoverRequest,
        list_candidates,
        trigger_discovery,
    )
    from provisa.compiler.naming import apply_sql_name

    wanted = (apply_sql_name(schema), apply_sql_name(table))
    matches = [
        t
        for t in (getattr(state, "tables", None) or [])
        if (apply_sql_name(t["domain_id"]), apply_sql_name(t["table_name"])) == wanted
        or (apply_sql_name(t["schema_name"]), apply_sql_name(t["table_name"])) == wanted
    ]
    if len(matches) != 1:
        raise ValueError(
            f"{schema}.{table} names {len(matches)} registered tables; name exactly one"
        )
    found = await trigger_discovery(
        request, DiscoverRequest(scope="table", table_id=matches[0]["id"])
    )
    stored = set(found["stored_ids"])
    candidates = [c for c in await list_candidates(request) if c.get("id") in stored]
    return {"table": f"{schema}.{table}", "candidates": candidates}

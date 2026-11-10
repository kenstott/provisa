# Copyright (c) 2026 Kenneth Stott
# Canary: 578501ce-c02c-4e68-85d6-fb876f099e96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``POST /data/explain``: what SQL a statement in any language becomes (REQ-2222).

One endpoint with a language field. Each language runs its own front door's lowering and stops
where it hands SQL to the one pipeline (REQ-1519): the pipeline builds the plan under
``explain=``, so what is described is what would run. There is no second pipeline here.
"""

# Requirements: REQ-1519, REQ-2222

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from graphql import GraphQLError
from pydantic import BaseModel

from provisa.api.errors import ApiError
from provisa.api.role_surface import role_surface
from provisa.compiler.complexity import ComplexityLimitExceeded
from provisa.compiler.parser import GraphQLValidationError
from provisa.core.parameters import ParameterRequired
from provisa.core.read_refusal import ReadRefused

router = APIRouter(prefix="/data", tags=["data"])

Language = Literal["sql", "graphql", "cypher", "sparql", "grpc", "rest", "mcp"]


class ExplainRequest(BaseModel):
    language: Language
    # The statement: its text for sql, graphql, cypher and sparql; the type name for grpc; the
    # path and query string for rest; the tool's name for mcp.
    statement: str
    # GraphQL's variables, Cypher's parameters, a gRPC request or an MCP tool's arguments.
    arguments: dict[str, Any] | None = None
    role: str | None = None
    # REQ-1519: True runs the statement and reports real timings.
    analyze: bool = False


@dataclass(frozen=True)
class _Door:
    """What one request needs from its front door."""

    request: Request
    state: Any
    role_id: str
    surface: Any
    statement: str
    arguments: dict[str, Any]
    analyze: bool


def _invalid(message: str, **params: Any) -> ApiError:
    return ApiError(400, "explain.invalid_statement", message, **params)


async def _sql_plan(door: _Door, sql: str) -> list[Any]:
    from provisa.compiler.sql_rewrite import split_sql_statements
    from provisa.pgwire._pipeline import _govern_and_route

    statements = split_sql_statements(sql)
    if len(statements) != 1:
        raise _invalid("EXPLAIN describes one statement; send a single statement")
    return [await _govern_and_route(statements[0], door.role_id, explain=door.analyze)]


async def _graphql_plans(door: _Door, text: str, variables: Any) -> list[Any]:
    """GraphQL text compiled as every GraphQL-shaped surface compiles it, each root field handed
    to the pipeline's compiled entrance."""
    from provisa.api.generated_plan import compile_generated_graphql
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    surface = door.surface
    try:
        compiled = compile_generated_graphql(
            door.state, door.role_id, surface.schema, surface.context, text, variables
        )
    except GraphQLValidationError as exc:
        raise _invalid("; ".join(e.message for e in exc.errors)) from exc
    except GraphQLError as exc:
        raise _invalid(exc.message) from exc
    return [
        await _govern_and_route_compiled(
            c.sql,
            door.role_id,
            exec_params=c.params or None,
            state=door.state,
            cache_hint=NO_CACHE_HINT,
            sdl_joins=True,
            explain=door.analyze,
        )
        for c in compiled
    ]


async def _cypher_plan(door: _Door, text: str, params: dict[str, Any]) -> list[Any] | Response:
    from provisa.api.rest.cypher_router import CypherRequest, planned_read
    from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write

    try:
        parse_cypher_write(text)
    except CypherWriteParseError:
        pass  # not a write: the read path below
    else:
        raise _invalid("EXPLAIN is only supported for read statements")
    planned = await planned_read(
        CypherRequest(query=text, params=params), door.request, explain=door.analyze
    )
    return planned if isinstance(planned, Response) else [planned.plan]


async def _sparql_plan(door: _Door) -> list[Any] | Response:
    from provisa.api.rest.sparql_router import compiled_query, semantic_statement
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    made = await compiled_query(door.statement, door.surface, door.role_id, door.state)
    if isinstance(made, Response):
        return made
    compiled, _vocab, graph = made
    if compiled.empty:
        return []  # answered without a statement: there is nothing the pipeline would run
    lowered = semantic_statement(compiled, graph, door.surface, door.role_id, door.state)
    if isinstance(lowered, Response):
        return lowered
    plan = await _govern_and_route_compiled(
        lowered[0],
        door.role_id,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=False,
        explain=door.analyze,
    )
    return [plan]


async def _grpc_plans(door: _Door) -> list[Any]:
    from provisa.api.data.endpoint_grpc_proxy import aggregate_graphql_text, row_read_graphql

    type_name, body = door.statement, door.arguments
    ctx, schema = door.surface.context, door.surface.schema
    if type_name.endswith("Aggregate") or type_name.endswith("GroupBy"):
        return await _graphql_plans(
            door, aggregate_graphql_text(ctx, type_name, body, door.role_id), None
        )
    text, variables, _mask = row_read_graphql(
        ctx, schema, type_name, int(body.get("limit", 100)), body, door.role_id
    )
    return await _graphql_plans(door, text, variables)


def _rest_page_value(params: dict[str, str], name: str, least: int) -> int | None:
    raw = params.get(name)
    if raw is None:
        return None
    if not raw.isdecimal() or int(raw) < least:
        raise _invalid(f"{name} must be an integer of at least {least}", field=name)
    return int(raw)


async def _rest_plans(door: _Door) -> list[Any]:
    from provisa.api.rest.generator import rest_read_graphql

    target = urlsplit(door.statement)
    parts = target.path.strip("/").split("/")
    if parts[:2] == ["data", "rest"]:
        parts = parts[2:]
    if len(parts) != 2:
        raise _invalid(
            "A REST read is /data/rest/{domain}/{table}, with its query string",
            path=target.path,
        )
    params = dict(parse_qsl(target.query, keep_blank_values=True))
    read = rest_read_graphql(
        door.surface,
        parts[0],
        parts[1],
        params,
        limit=_rest_page_value(params, "limit", 1),
        offset=_rest_page_value(params, "offset", 0),
        fields=params.get("fields"),
        as_of=params.get("as_of"),
        as_recorded=params.get("as_recorded"),
    )
    return await _graphql_plans(door, read.gql_query, read.variables.values)


async def _mcp_plans(door: _Door) -> list[Any] | Response:
    """The MCP tools that run a statement, each stopping where its tool hands that statement on."""
    from provisa.api.mcp.tools import _metric_sql

    tool, arguments = door.statement, door.arguments
    if tool == "run_sql":
        return await _sql_plan(door, arguments["sql"])
    if tool == "run_cypher":
        return await _cypher_plan(door, arguments["cypher"], arguments.get("params") or {})
    if tool == "query_metric":
        sql = _metric_sql(
            arguments["metric"], list(arguments.get("dimensions") or []), arguments.get("filters")
        )
        return await _sql_plan(door, sql)
    raise ApiError(
        400,
        "explain.tool_runs_no_statement",
        f"MCP tool {tool!r} runs no statement to explain; run_sql, run_cypher and query_metric do",
        tool=tool,
    )


async def _plans(language: Language, door: _Door) -> list[Any] | Response:
    if language == "sql":
        return await _sql_plan(door, door.statement)
    if language == "graphql":
        return await _graphql_plans(door, door.statement, door.arguments)
    if language == "cypher":
        return await _cypher_plan(door, door.statement, door.arguments)
    if language == "sparql":
        return await _sparql_plan(door)
    if language == "grpc":
        return await _grpc_plans(door)
    if language == "rest":
        return await _rest_plans(door)
    return await _mcp_plans(door)


def _tables_read(plan: Any, state: Any) -> list[dict[str, str]]:  # REQ-2222
    """The registered tables ``plan`` reads, once each, as the pipeline resolved them. A Class has
    no rows of its own, so it reports the tables it is bound to. ``address`` is
    ``source/schema/table``, as a configuration names the table."""
    rows = {table["id"]: table for table in state.tables}
    read: dict[str, dict[str, str]] = {}
    for table_id in plan.table_ids:
        row = rows.get(table_id)
        assert row is not None, f"the pipeline resolved table {table_id} from this build's tables"
        bound = (
            [rows[b["table_id"]] for b in row["class_bindings"]]
            if "class_bindings" in row
            else [row]
        )
        for table in bound:
            address = f"{table['source_id']}/{table['schema_name']}/{table['table_name']}"
            read[address] = {
                "address": address,
                "domain": table["domain_id"],
                "table": table["table_name"],
            }
    return [read[address] for address in sorted(read)]


@router.post("/explain")
async def explain_endpoint(  # REQ-2222
    raw_request: Request,
    request: ExplainRequest,
    x_provisa_role: str | None = Header(None),
):
    """The SQL a statement in any language becomes: the semantic statement, that statement
    governed, the optimized statement with the rules that fired, the route, the native statement
    and the engine's own plan, one entry per statement the pipeline would run, each with the
    tables it reads."""
    from provisa.api.app import state
    from provisa.api.data.endpoint_dev import _check_sql_capabilities, _resolve_role_id
    from provisa.executor.explain import ExplainUnsupported, explained_plan

    role_id = _resolve_role_id(raw_request, x_provisa_role, request)
    surface = role_surface(state, role_id)
    if surface is None:
        raise ApiError(
            400, "data.no_schema_for_role", f"No schema for role {role_id!r}", role_id=role_id
        )
    # The governed SQL shows a role's masks and row filters: the right /data/sql/explain requires.
    _check_sql_capabilities(state.roles.get(role_id))

    door = _Door(
        raw_request,
        state,
        role_id,
        surface,
        request.statement,
        request.arguments or {},
        request.analyze,
    )
    try:
        plans = await _plans(request.language, door)
        if isinstance(plans, Response):
            return plans
        explained = [
            {
                **await explained_plan(p, state, analyze=request.analyze),
                "tables": _tables_read(p, state),
            }
            for p in plans
        ]
    except (ComplexityLimitExceeded, ReadRefused, PermissionError, HTTPException):
        raise  # each answered under its own status and code, as on every surface
    except ParameterRequired as exc:
        raise ApiError(400, "data.parameter_required", str(exc), table=exc.table) from exc
    except KeyError as exc:
        raise _invalid(f"{request.language} needs the argument {exc.args[0]}") from exc
    except (ValueError, ExplainUnsupported) as exc:
        raise _invalid(str(exc)) from exc
    except Exception as exc:  # allow-ble: request boundary — an arbitrary statement can raise ANY engine/driver exception type
        return JSONResponse(
            status_code=400,
            content={"detail": str(exc), "code": "explain.failed", "params": {}},
        )
    return {"language": request.language, "statements": explained}

# Copyright (c) 2026 Kenneth Stott
# Canary: 43696015-b2cf-4d6c-bccf-e4e4f5783140
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The OData V4 service: service document, per-role $metadata and entity-set reads (REQ-2193).

A read is GraphQL text synthesized from its options, as on REST and JSON:API, compiled and handed
to the one governed pipeline (``_govern_and_route_compiled``): row rules, column grants, masks, the
role's row ceiling and the audit record are that pipeline's, and nothing here applies any of them.

A ``$filter`` is added to the compiled statement (``filter_sql``) before it goes to that pipeline.

Pages are read in one order (the client's, then ``model._ordering``), one row past the page: that
row says a next page exists, and is not returned.
"""

# Requirements: REQ-2193

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, Response
from graphql import GraphQLError

from provisa.api._query_helpers import NO_VARIABLES, build_graphql_query
from provisa.api.errors import timeout_error
from provisa.api.generated_plan import compile_generated_graphql
from provisa.api.json_response import OrjsonResponse
from provisa.api.odata.csdl import metadata_document, service_document
from provisa.api.odata.edm import edm_value
from provisa.api.odata.expression import Node
from provisa.api.odata.filter_sql import with_filter
from provisa.api.odata.headers import SERVICE_ROOT
from provisa.api.odata.model import EntitySet, entity_set, entity_sets
from provisa.api.odata.query import (
    QueryOptionRefused,
    QueryOptions,
    check_format,
    parse_query_options,
)
from provisa.api.role_surface import RoleSurface, role_surface
from provisa.cache.middleware import build_cache_headers
from provisa.compiler.complexity import ComplexityLimitExceeded
from provisa.compiler.directives import NO_CACHE_HINT, request_cache_hint
from provisa.compiler.parser import GraphQLValidationError
from provisa.compiler.sql_types import CompiledQuery
from provisa.core.read_refusal import ReadRefused
from provisa.federation.pgwire_replica import SourceStillStartingError

log = logging.getLogger(__name__)

SERVER_PAGE_SIZE = 5000
_JSON = "application/json;odata.metadata=minimal"
_XML = "application/xml"


class _Refused(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


def _error(status: int, code: str, message: str) -> Response:
    body = {"error": {"code": code, "message": message}}
    return OrjsonResponse(content=body, status_code=status, media_type=_JSON)


def _service_root(request: Request) -> str:
    """The service's absolute URL as the client reached it, which a nextLink must lead back to."""
    scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers["host"]
    return f"{scheme}://{host}{SERVICE_ROOT}"


def _caller(state: Any, request: Request) -> tuple[str, RoleSurface]:  # noqa: ANN401 — AppState
    role_id = getattr(request.state, "role", None)
    if not role_id:
        raise _Refused(401, "unauthenticated", "authenticated role required")
    surface = role_surface(state, role_id)  # one snapshot: parts of one build
    if surface is None:
        raise _Refused(403, "no_surface", f"role {role_id!r} is served no data")
    return role_id, surface


def _served_set(surface: RoleSurface, name: str) -> EntitySet:
    entity = entity_set(surface, name)
    if entity is None:
        # The same answer for a table the role cannot see and one that does not exist.
        raise _Refused(404, "not_found", f"no entity set {name!r}")
    return entity


async def _run(state: Any, role_id: str, sql: str, params: list, request: Request) -> Any:  # noqa: ANN401
    """``sql`` through the one governed pipeline. Unbuffered: a page is bounded, and a landed
    result has no form in an OData response."""
    from provisa.pgwire._pipeline import _execute_plan, _govern_and_route_compiled

    try:
        cache_hint = request_cache_hint(NO_CACHE_HINT, request.headers)
    except ValueError as exc:
        raise _Refused(400, "invalid_cache_control", str(exc)) from exc
    try:
        plan = await _govern_and_route_compiled(
            sql,
            role_id,
            exec_params=params or None,
            state=state,
            cache_hint=cache_hint,
            serve_cached=True,  # REQ-1897: _execute_plan serves the pre-route HIT
            sdl_joins=True,
        )
        return await _execute_plan(plan, state)
    except (ComplexityLimitExceeded, ReadRefused, SourceStillStartingError):
        raise  # each has the app's own answer, as on REST
    except PermissionError as exc:
        raise _Refused(403, "forbidden", str(exc)) from exc
    except TimeoutError as exc:
        raise timeout_error(exc) from exc


def _compiled(
    state: Any,  # noqa: ANN401 — AppState
    role_id: str,
    surface: RoleSurface,
    gql_query: str,
) -> CompiledQuery:
    log.debug("OData -> GraphQL: %s", gql_query)
    try:
        compiled = compile_generated_graphql(
            state, role_id, surface.schema, surface.context, gql_query
        )
    except (GraphQLValidationError, GraphQLError) as exc:
        raise _Refused(400, "invalid_request", str(exc)) from exc
    if not compiled:
        raise _Refused(400, "invalid_request", "the request compiled to no statement")
    return compiled[0]


async def _count(
    state: Any,  # noqa: ANN401 — AppState
    role_id: str,
    surface: RoleSurface,
    entity: EntitySet,
    condition: Node | None,
    request: Request,
) -> int:
    """The rows the role reads of ``entity`` that meet ``condition``: its row rules bind to the
    inner statement, so the count is of what it may see, and only the count crosses the wire
    (REQ-028)."""
    inner = with_filter(
        _compiled(
            state,
            role_id,
            surface,
            build_graphql_query(
                entity.name, [entity.properties[0].name], NO_VARIABLES, [], None, None
            ),
        ),
        entity,
        condition,
    )
    sql = f"SELECT COUNT(*) AS total FROM ({inner.sql}) AS _provisa_count"
    result = await _run(state, role_id, sql, inner.params, request)
    return int(next(iter(result.rows))[0])


def _selected(entity: EntitySet, options: QueryOptions) -> list[str]:
    if options.select is None:
        return [p.name for p in entity.properties]
    for name in options.select:
        if entity.property(name) is None:
            # A column the role cannot see is unknown, as one that does not exist is.
            raise _Refused(400, "invalid_query_option", f"no property {name!r} of {entity.name}")
    return list(options.select)


def _order_by(entity: EntitySet, options: QueryOptions) -> list[dict[str, str]]:
    """The client's order, then the read's own, so a page boundary between rows equal in the
    client's keys loses none. OData sorts a null first ascending and last descending."""
    for key in options.order_by:
        if entity.property(key.name) is None:
            # A column the role cannot see is unknown, as one that does not exist is.
            raise _Refused(
                400, "invalid_query_option", f"no property {key.name!r} of {entity.name}"
            )
    asked = [
        {"field": key.name, "dir": "desc_nulls_last" if key.descending else "asc_nulls_first"}
        for key in options.order_by
    ]
    named = {key.name for key in options.order_by}
    return asked + [{"field": n, "dir": "asc"} for n in entity.ordering if n not in named]


def _next_link(request: Request, name: str, options: QueryOptions, taken: int) -> str:
    query = [(k, v) for k, v in request.query_params.multi_items() if k not in ("$top", "$skip")]
    if options.top is not None:
        query.append(("$top", str(options.top - taken)))
    query.append(("$skip", str(options.skip + taken)))
    return f"{_service_root(request)}/{name}?{urlencode(query, safe='$,*')}"


def _entities(rows: list[tuple], compiled: CompiledQuery, entity: EntitySet) -> list[dict]:
    types = {p.name: p.type for p in entity.properties}
    typed = [(c.field_name, types[c.field_name]) for c in compiled.columns]
    return [{name: edm_value(value, edm) for (name, edm), value in zip(typed, row)} for row in rows]


async def _read(
    state: Any,  # noqa: ANN401 — AppState
    role_id: str,
    surface: RoleSurface,
    entity: EntitySet,
    options: QueryOptions,
    request: Request,
) -> Response:
    fields = _selected(entity, options)
    context = f"{_service_root(request)}/$metadata#{entity.name}"
    if options.select is not None:
        context += f"({','.join(options.select)})"
    document: dict[str, Any] = {"@odata.context": context}
    if options.count:
        document["@odata.count"] = await _count(
            state, role_id, surface, entity, options.filter, request
        )

    page = SERVER_PAGE_SIZE if options.top is None else min(options.top, SERVER_PAGE_SIZE)
    if page == 0:  # $top=0 asks for no row: nothing to read
        document["value"] = []
        return OrjsonResponse(content=document, media_type=_JSON)

    probe = 1 if options.top is None or options.top > page else 0
    gql_query = build_graphql_query(
        entity.name, fields, NO_VARIABLES, _order_by(entity, options), page + probe, options.skip
    )
    compiled = with_filter(_compiled(state, role_id, surface, gql_query), entity, options.filter)
    result = await _run(state, role_id, compiled.sql, compiled.params, request)
    if result.redirect is not None:
        raise _Refused(500, "landed_result", "the result was landed, which OData cannot answer")

    rows = list(result.rows)
    document["value"] = _entities(rows[:page], compiled, entity)
    if len(rows) > page:
        document["@odata.nextLink"] = _next_link(request, entity.name, options, page)
    return OrjsonResponse(
        content=document, media_type=_JSON, headers=build_cache_headers(result.cache_entry)
    )


def create_odata_router(state: Any) -> APIRouter:  # noqa: ANN401 — AppState  # REQ-2193
    """The OData service, mounted at ``SERVICE_ROOT``. Its responses' headers are
    ``ODataResponseHeaders``', registered beside it."""
    router = APIRouter(prefix=SERVICE_ROOT, tags=["odata"])

    async def _answered(work: Any) -> Response:  # noqa: ANN401 — the route's coroutine
        try:
            return await work
        except _Refused as refused:
            return _error(refused.status, refused.code, str(refused))
        except QueryOptionRefused as refused:
            return _error(refused.status, refused.code, str(refused))

    async def _service(request: Request) -> Response:
        check_format(request.query_params.get("$format"), "json", "application/json")
        _, surface = _caller(state, request)
        document = service_document(_service_root(request), entity_sets(surface))
        return OrjsonResponse(content=document, media_type=_JSON)

    async def _metadata(request: Request) -> Response:
        check_format(request.query_params.get("$format"), "xml", _XML)
        _, surface = _caller(state, request)
        return Response(content=metadata_document(entity_sets(surface)), media_type=_XML)

    async def _collection(request: Request, name: str) -> Response:
        options = parse_query_options(request.query_params.multi_items())
        role_id, surface = _caller(state, request)
        entity = _served_set(surface, name)
        return await _read(state, role_id, surface, entity, options, request)

    async def _collection_count(request: Request, name: str) -> Response:
        options = parse_query_options(request.query_params.multi_items())
        role_id, surface = _caller(state, request)
        entity = _served_set(surface, name)
        count = await _count(state, role_id, surface, entity, options.filter, request)
        return PlainTextResponse(str(count))

    @router.get("", include_in_schema=False)
    @router.get("/", include_in_schema=False)
    async def odata_service(request: Request):  # pyright: ignore[reportUnusedFunction]
        return await _answered(_service(request))

    @router.get("/$metadata", include_in_schema=False)
    async def odata_metadata(request: Request):  # pyright: ignore[reportUnusedFunction]
        return await _answered(_metadata(request))

    @router.get("/{entity_set_name}/$count", include_in_schema=False)
    async def odata_count(  # pyright: ignore[reportUnusedFunction]
        request: Request, entity_set_name: str
    ):
        return await _answered(_collection_count(request, entity_set_name))

    @router.get("/{entity_set_name}", include_in_schema=False)
    async def odata_entity_set(  # pyright: ignore[reportUnusedFunction]
        request: Request, entity_set_name: str
    ):
        return await _answered(_collection(request, entity_set_name))

    return router

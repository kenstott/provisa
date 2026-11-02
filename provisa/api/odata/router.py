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

A ``$filter`` is added to the compiled statement (``filter_sql``) before it goes to that pipeline,
and a ``$expand`` is a relationship field of the same text (``expand``).

What a read's answer says about itself (REQ-1350), its rows cut at the role's row limit among
them (REQ-1949), rides the ``X-Provisa-Warnings`` header as on every HTTP surface and the body's
``@Provisa.warnings`` instance annotation, ahead of ``value``.

Pages are read in one order (the client's, then ``model._ordering``), one row past the page: that
row says a next page exists, and is not returned.
"""

# Requirements: REQ-1350, REQ-1949, REQ-2193

from __future__ import annotations

import logging
import re
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse, Response
from graphql import GraphQLError

from provisa.api._query_helpers import NO_VARIABLES, build_graphql_query
from provisa.api.errors import timeout_error
from provisa.api.generated_plan import compile_generated_graphql
from provisa.api.json_response import OrjsonResponse
from provisa.api.odata.csdl import NAMESPACE, metadata_document, service_document
from provisa.api.odata.expand import (
    Addressing,
    Plan,
    context_selection,
    ordered_by,
    page_link,
    page_of,
    plans_of,
    read_fields,
    selected,
    with_expansions,
    write,
)
from provisa.api.odata.expression import Comparison, Logical, Name, Node, parse_key
from provisa.api.odata.filter_sql import with_filter
from provisa.api.odata.headers import SERVICE_ROOT
from provisa.api.odata.model import EntitySet, entity_set, entity_sets
from provisa.api.odata.query import (
    Expansion,
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
from provisa.core.statement_warnings import as_entries
from provisa.federation.pgwire_replica import SourceStillStartingError
from provisa.security.masking import MaskType

log = logging.getLogger(__name__)

SERVER_PAGE_SIZE = 5000
_JSON = "application/json;odata.metadata=minimal"
_XML = "application/xml"
_ADDRESS = re.compile(r"([^()/]+)\((.*)\)", re.DOTALL)
_WARNINGS = f"@{NAMESPACE}.warnings"


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
    entity = entity_set(surface, name, navigations=True)
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


def _addressing(state: Any, role_id: str) -> Addressing:  # noqa: ANN401 — AppState
    """The key a role addresses a set's rows by. A filter on a masked key is refused (REQ-531) and
    a row shows its mask, so such a key addresses nothing; a fake is the value a filter compares."""

    def key_of(entity: EntitySet) -> tuple[str, ...]:
        rules = state.masking_rules.get((entity.table_id, role_id), {})
        for name in entity.key:
            found = entity.property(name)
            assert found is not None
            if found.column in rules and rules[found.column][0].mask_type != MaskType.fake:
                return ()
        return entity.key

    return key_of


def _landed(result: Any) -> None:  # noqa: ANN401 — an engine's result
    if result.redirect is not None:
        raise _Refused(500, "landed_result", "the result was landed, which OData cannot answer")


def _column_names(compiled: CompiledQuery) -> list[str]:
    return [c.field_name for c in compiled.columns]


def _entities(
    rows: list[tuple],
    compiled: CompiledQuery,
    entity: EntitySet,
    fields: tuple[str, ...],
    plans: tuple[Plan, ...],
    root: str,
) -> list[dict]:
    names = _column_names(compiled)
    return [write(dict(zip(names, row)), entity, fields, plans, root) for row in rows]


def _annotate_warnings(document: dict[str, Any]) -> None:
    """The request's statement warnings as the document's instance annotation (REQ-1350), set
    once its reads have run and before ``value``; none when nothing was raised."""
    if warnings := as_entries():
        document[_WARNINGS] = warnings


def _context(request: Request, name: str, options: QueryOptions) -> str:
    selection = context_selection(options)
    suffix = f"({selection})" if selection else ""
    return f"{_service_root(request)}/$metadata#{name}{suffix}"


async def _read(
    state: Any,  # noqa: ANN401 — AppState
    role_id: str,
    surface: RoleSurface,
    entity: EntitySet,
    options: QueryOptions,
    request: Request,
) -> Response:
    fields = selected(entity, options)
    plans = plans_of(surface, entity, options.expand, _addressing(state, role_id))
    document: dict[str, Any] = {"@odata.context": _context(request, entity.name, options)}
    if options.count:
        document["@odata.count"] = await _count(
            state, role_id, surface, entity, options.filter, request
        )

    page, probe = page_of(options.top, SERVER_PAGE_SIZE)
    if page == 0:  # $top=0 asks for no row: nothing to read
        document["value"] = []
        return OrjsonResponse(content=document, media_type=_JSON)

    gql_query = build_graphql_query(
        entity.name,
        read_fields(fields, plans),
        NO_VARIABLES,
        ordered_by(entity, options),
        page + probe,
        options.skip,
    )
    compiled = with_filter(_compiled(state, role_id, surface, gql_query), entity, options.filter)
    compiled = with_expansions(compiled, plans)
    result = await _run(state, role_id, compiled.sql, compiled.params, request)
    _landed(result)

    rows = list(result.rows)
    root = _service_root(request)
    _annotate_warnings(document)
    document["value"] = _entities(rows[:page], compiled, entity, fields, plans, root)
    if len(rows) > page:
        document["@odata.nextLink"] = page_link(
            f"{root}/{entity.name}", tuple(request.query_params.multi_items()), options, page
        )
    return OrjsonResponse(
        content=document, media_type=_JSON, headers=build_cache_headers(result.cache_entry)
    )


def _key_condition(entity: EntitySet, predicate: str) -> Node:
    """The key predicate of ``set(predicate)`` as a filter of the set, typed as any filter is. A
    set with no key has no address: as for a row that does not exist."""
    pairs = parse_key(predicate)
    if not entity.key:
        raise _Refused(404, "not_found", f"{entity.name} has no key")
    named = (
        {entity.key[0]: pairs[0][1]}
        if len(pairs) == 1 and pairs[0][0] is None
        else {name: value for name, value in pairs if name is not None}
    )
    if len(pairs) != len(entity.key) or set(named) != set(entity.key):
        raise _Refused(
            400,
            "invalid_key",
            f"the key of {entity.name} is {', '.join(entity.key)}: {predicate!r}",
        )
    terms: list[Node] = [Comparison("eq", Name(k), named[k]) for k in entity.key]
    return terms[0] if len(terms) == 1 else Logical("and", tuple(terms))


def _parsed_address(surface: RoleSurface, address: str) -> tuple[EntitySet, Node]:
    match = _ADDRESS.fullmatch(address)
    if match is None:
        raise _Refused(404, "not_found", f"no entity set {address!r}")
    entity = _served_set(surface, match.group(1))
    return entity, _key_condition(entity, match.group(2))


async def _navigate(
    state: Any,  # noqa: ANN401 — AppState
    role_id: str,
    surface: RoleSurface,
    address: str,
    name: str,
    options: QueryOptions,
    request: Request,
) -> Response:
    """``set(key)/navigation``: the parent's one row expanded, its list the page. Where a nested
    list's nextLink leads, and so the same statement, row rules and masks as the list it follows."""
    entity, condition = _parsed_address(surface, address)
    navigation = entity.navigation(name)
    if navigation is None:
        raise _Refused(404, "not_found", f"no navigation property {name!r} of {entity.name}")
    if not navigation.collection:
        raise _Refused(501, "not_implemented", f"reading the single-valued {name} is not served")
    if options.count:
        raise _Refused(501, "not_implemented", "$count on a navigation property is not served")

    items = tuple(request.query_params.multi_items())
    expansion = Expansion(name, options, items)
    plans = plans_of(surface, entity, (expansion,), _addressing(state, role_id))
    gql_query = build_graphql_query(
        entity.name, read_fields(entity.key, plans), NO_VARIABLES, [], 1, None
    )
    compiled = with_filter(_compiled(state, role_id, surface, gql_query), entity, condition)
    compiled = with_expansions(compiled, plans)
    result = await _run(state, role_id, compiled.sql, compiled.params, request)
    _landed(result)
    rows = list(result.rows)
    if not rows:
        raise _Refused(404, "not_found", f"no row {address!r}")

    root = _service_root(request)
    (parent,) = _entities(rows[:1], compiled, entity, entity.key, plans, root)
    document: dict[str, Any] = {"@odata.context": _context(request, navigation.target_set, options)}
    _annotate_warnings(document)
    document["value"] = parent[name]
    link = parent.get(f"{name}@odata.nextLink")
    if link is not None:
        document["@odata.nextLink"] = link
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
        if options.expand:
            raise _Refused(400, "invalid_query_option", "$count takes no $expand")
        role_id, surface = _caller(state, request)
        entity = _served_set(surface, name)
        count = await _count(state, role_id, surface, entity, options.filter, request)
        return PlainTextResponse(str(count))

    async def _navigation(request: Request, address: str, name: str) -> Response:
        options = parse_query_options(request.query_params.multi_items())
        role_id, surface = _caller(state, request)
        return await _navigate(state, role_id, surface, address, name, options, request)

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

    # A path: a string key may hold a slash.
    @router.get("/{address:path}/{navigation}", include_in_schema=False)
    async def odata_navigation(  # pyright: ignore[reportUnusedFunction]
        request: Request, address: str, navigation: str
    ):
        return await _answered(_navigation(request, address, navigation))

    @router.get("/{entity_set_name}", include_in_schema=False)
    async def odata_entity_set(  # pyright: ignore[reportUnusedFunction]
        request: Request, entity_set_name: str
    ):
        return await _answered(_collection(request, entity_set_name))

    return router

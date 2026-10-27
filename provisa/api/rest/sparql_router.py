# Copyright (c) 2026 Kenneth Stott
# Canary: 9cb89f70-b3f7-4371-aab6-c801b5121b9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The SPARQL 1.1 Protocol endpoint, GET and POST /data/sparql (REQ-2135).

A query arrives as the Protocol sends it: ``?query=`` on a GET, a form's ``query`` field or an
``application/sparql-query`` body on a POST. It is read against the vocabulary of the caller's
role (its label map and the live terms' groundings), lowered by the SPARQL front end to the graph
IR, compiled to one semantic SQL statement by the back end Cypher shares, and run through the one
pipeline every read takes -- governed whole, routed and audited as any query is. The caller is
authenticated, and its role settled, as on /data/cypher. The answer is written in the media type
the request accepts (provisa.sparql_query.results); a statement warning (a path cut at the depth
limit) rides the X-Provisa-Warnings header as on every HTTP surface. A query opts into the
response cache as SQL and Cypher do, in its own comment syntax (``# @provisa cache=true``, REQ-544),
and X-Provisa-Stats: true answers its time split (REQ-1911).
"""

# Requirements: REQ-2135, REQ-2141

from __future__ import annotations

import json
import logging
import time
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import Response

from provisa.api.rest.cypher_exec import _build_label_map, _resolve_role_id
from provisa.api.rest.cypher_router import cypher_error
from provisa.compiler.complexity import ComplexityLimitExceeded

log = logging.getLogger(__name__)

router = APIRouter()

_SPARQL_QUERY = "application/sparql-query"


async def _query_text(request: Request) -> str | Response:
    """The query the request carries, as the SPARQL 1.1 Protocol has it sent."""
    params = request.query_params
    if "default-graph-uri" in params or "named-graph-uri" in params:
        return cypher_error(
            400,
            "sparql.dataset_not_supported",
            "default-graph-uri and named-graph-uri are not supported",
        )
    if request.method == "GET":
        query = params.get("query")
    else:
        content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if content_type == _SPARQL_QUERY:
            query = (await request.body()).decode("utf-8")
        elif content_type == "application/x-www-form-urlencoded":
            form = await request.form()
            if "default-graph-uri" in form or "named-graph-uri" in form:
                return cypher_error(
                    400,
                    "sparql.dataset_not_supported",
                    "default-graph-uri and named-graph-uri are not supported",
                )
            value = form.get("query")
            query = value if isinstance(value, str) else None
        else:
            return cypher_error(
                415,
                "sparql.unsupported_content_type",
                "A SPARQL query is POSTed as application/sparql-query or as the query field of "
                "an application/x-www-form-urlencoded form",
            )
    if not query:
        return cypher_error(400, "sparql.no_query", "The request carries no query")
    return query


async def vocabulary_for(state: Any, role_id: str, ctx: Any, label_map: Any) -> Any:
    """The vocabulary of ``role_id``: its label map's classes, predicates and edges, and the
    live terms' groundings of what that map holds."""
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.gql.iri import iri_base
    from provisa.sparql_query.vocabulary import (
        build_vocabulary,
        column_types_of,
        grounding_from_refs,
    )

    base = iri_base()
    pool = state.model_db
    assert pool is not None, "the control plane is opened before any request is served"
    async with pool.acquire() as conn:
        table_terms, column_terms = await glossary_repo.vocabulary_refs(conn)
    return build_vocabulary(
        label_map,
        base,
        column_types_of(label_map, ctx),
        grounding_from_refs(label_map, table_terms, column_terms),
    )


@router.api_route("/data/sparql", methods=["GET", "POST"])
async def sparql_query(request: Request) -> Response:  # REQ-2135
    """Answer a SPARQL SELECT, ASK or CONSTRUCT over the semantic model, for the caller's role."""
    from provisa.api.app import state
    from provisa.api.rest.cypher_router import (
        _dict_rows,
        _run_plan,
        access_violations,
        statement_stats,
    )
    from provisa.api.role_surface import role_surface
    from provisa.cache.middleware import build_cache_headers
    from provisa.compiler.directives import cache_hint_for, request_cache_hint
    from provisa.executor import stats as query_stats
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.gql.assembler import assemble_rows, take_cut
    from provisa.gql.graph_rewriter import apply_graph_rewrites
    from provisa.gql.translator import cypher_to_sql
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.pgwire._pipeline import _govern_and_route_compiled
    from provisa.sparql_query.compiler import compile_sparql
    from provisa.sparql_query.results import (
        NotAcceptable,
        negotiate,
        write_ask,
        write_construct,
        write_select,
    )

    query = await _query_text(request)
    if isinstance(query, Response):
        return query

    role_id = _resolve_role_id(request, state)
    surface = role_surface(state, role_id)
    if surface is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )
    ctx = surface.context
    label_map = _build_label_map(ctx, role_id, state)
    try:
        vocab = await vocabulary_for(state, role_id, ctx, label_map)
        compiled = compile_sparql(query, vocab, label_map)
        media, fmt = negotiate(request.headers.get("accept"), compiled.form)
    except NotAcceptable as exc:
        return cypher_error(406, "sparql.not_acceptable", str(exc), reason=str(exc))
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))

    # REQ-544: the query's `# @provisa cache` opt-in, its x-provisa-cache headers and its
    # Cache-Control header; two that disagree are refused (400), naming them.
    try:
        cache_hint = request_cache_hint(cache_hint_for("sparql", query), request.headers)
    except ValueError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
    # REQ-1911: X-Provisa-Stats: true answers the statement's time split with it.
    stats_enabled = request.headers.get("x-provisa-stats", "").lower() == "true"
    if stats_enabled:
        query_stats.begin()
    t0 = time.perf_counter()
    physical_sql = ""
    cache_entry = None
    rows: list[dict] = [compiled.empty_row] if compiled.empty_row is not None else []
    if not compiled.empty:
        assert compiled.ast is not None
        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(compiled.ast, label_map, {})
        except CypherTranslateError as exc:
            return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
        semantic_sql = make_semantic_sql(sql_ast.sql(dialect="postgres"), ctx)
        assert not ordered_params, "a SPARQL statement binds no parameters"
        refusal = access_violations(semantic_sql, surface, role_id, state)
        if refusal is not None:
            return refusal
        try:
            # The one pipeline: governed whole (REQ-264's relationship guard, the role's rights,
            # masks and row filters), routed, executed and audited as every read is.
            plan = await _govern_and_route_compiled(
                semantic_sql,
                role_id,
                buffered=True,
                cache_hint=cache_hint,
                # REQ-1897: an opted-in read that hits is answered from the cache before routing;
                # _run_plan, the terminal, serves that plan.
                serve_cached=True,
                sdl_joins=False,
            )
        except ComplexityLimitExceeded:
            raise  # REQ-1174: answered as 413 by the app's handler
        except PermissionError as exc:
            return cypher_error(403, "sparql.permission_denied", str(exc), reason=str(exc))
        if plan.span_attrs is not None:
            plan.span_attrs["provisa.query_text"] = query
        executed = await _run_plan(plan, state)
        if isinstance(executed, Response):
            return executed
        rows = assemble_rows(take_cut(_dict_rows(executed), plan.warnings), graph_vars)
        physical_sql = plan.physical_sql or plan.exec_sql or ""
        cache_entry = executed.cache_entry

    if compiled.form == "ASK":
        body = write_ask(bool(rows), fmt)
    elif compiled.form == "CONSTRUCT":
        body = write_construct(rows, vocab.base, vocab.prefixes, fmt)
    else:
        body = write_select(compiled.variables, rows, vocab.base, fmt)
    # REQ-536: HIT (with the entry's age) when the rows came from the response cache, else MISS.
    headers = build_cache_headers(cache_entry)
    if stats_enabled:
        stats = statement_stats("sparql", len(rows), physical_sql, t0)
        if stats is not None:
            body, stats_headers = with_stats(body, media, stats)
            headers = {**headers, **stats_headers}
    return Response(content=body, media_type=media, headers=headers)


def with_stats(body: bytes, media: str, stats: dict) -> tuple[bytes, dict[str, str]]:
    """``body`` with the statement's provisa_stats: beside ``head`` and ``results`` in a SPARQL JSON
    result, else (a format with nowhere for them) in the X-Provisa-Stats response header."""
    if media == "application/sparql-results+json":
        doc = json.loads(body)
        doc["provisa_stats"] = stats
        return json.dumps(doc).encode(), {}
    return body, {"X-Provisa-Stats": json.dumps(stats)}


@router.get("/data/sparql/vocabulary")
async def sparql_vocabulary(request: Request) -> Response:  # REQ-2135
    """The caller's role's vocabulary as IRIs, for a client to offer as a query is written: its
    base and prefixes, classes, predicates, edge types and the live Concept terms
    (provisa.sparql_query.vocabulary.vocabulary_document)."""
    from fastapi.responses import JSONResponse

    from provisa.api.app import state
    from provisa.api.role_surface import role_surface
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.sparql_query.vocabulary import concepts_of, vocabulary_document

    role_id = _resolve_role_id(request, state)
    surface = role_surface(state, role_id)
    if surface is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )
    label_map = _build_label_map(surface.context, role_id, state)
    try:
        vocab = await vocabulary_for(state, role_id, surface.context, label_map)
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
    # The concepts the role may read: those scoped to a domain its label map holds, and the org's
    # shared ones (glossary_repo.list_terms's readable_term rule).
    domains = frozenset(nm.domain_id for nm in label_map.nodes.values() if nm.domain_id)
    pool = state.model_db
    assert pool is not None, "the control plane is opened before any request is served"
    async with pool.acquire() as conn:
        terms = await glossary_repo.list_terms(conn, include_deprecated=False, domains=domains)
    return JSONResponse(vocabulary_document(vocab, concepts_of(vocab.base, terms)))

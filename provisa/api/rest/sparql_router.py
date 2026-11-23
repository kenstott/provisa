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

POST /data/sparql/export and /data/sparql/export/load export the role's governed graph as RDF
(REQ-2165, provisa.sparql_query.rdf_export): each of its CONSTRUCT queries runs through the same
one pipeline, at the request's as_of and as_recorded.
"""

# Requirements: REQ-2135, REQ-2141, REQ-2165

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import BaseModel

from provisa.api.rest.cypher_exec import _build_label_map, _resolve_role_id
from provisa.api.rest.cypher_router import cypher_error
from provisa.compiler.complexity import ComplexityLimitExceeded
from provisa.core.parameter_binding import ParameterBindingRefused

log = logging.getLogger(__name__)

router = APIRouter()

_SPARQL_QUERY = "application/sparql-query"
_SPARQL_UPDATE = "application/sparql-update"


@dataclass(frozen=True)
class SparqlUpdate:
    """An update the request carries (SPARQL 1.1 Protocol, 2.2): its text."""

    text: str


async def _query_text(request: Request) -> str | SparqlUpdate | Response:
    """The query or update the request carries, as the SPARQL 1.1 Protocol has it sent."""
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
        elif content_type == _SPARQL_UPDATE:
            return update_of((await request.body()).decode("utf-8"))
        elif content_type == "application/x-www-form-urlencoded":
            form = await request.form()
            update = form.get("update")
            if "using-graph-uri" in form or "using-named-graph-uri" in form:
                return cypher_error(
                    400,
                    "sparql.dataset_not_supported",
                    "using-graph-uri and using-named-graph-uri are not supported",
                )
            if isinstance(update, str):
                return update_of(update)
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
                "an application/x-www-form-urlencoded form, an update as "
                "application/sparql-update or as its update field",
            )
    if not query:
        return cypher_error(400, "sparql.no_query", "The request carries no query")
    return query


def update_of(text: str) -> SparqlUpdate | Response:
    """The update ``text`` (REQ-2135, amended 2026-11-22 and 2026-11-23), or its refusal by name:
    text that is no SPARQL Update, and LOAD, which reads from outside the session."""
    from provisa.sparql_query.update import update_refusal

    refused = update_refusal(text)
    if refused is not None:
        code, said = refused
        return cypher_error(400, code, said, reason=said)
    return SparqlUpdate(text)


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


async def read_of(surface: Any, role_id: str, state: Any) -> Any:
    """What a SPARQL read compiles over for the role (session_graph.SessionRead): its vocabulary
    and label map, with the objects of the graph session bound now (REQ-2135, amended
    2026-11-23; REQ-2176), when one is."""
    from provisa.compiler import temp_tables
    from provisa.sparql_query.session_graph import read_over

    ctx = surface.context
    label_map = _build_label_map(ctx, role_id, state)
    vocab = await vocabulary_for(state, role_id, ctx, label_map)
    # REQ-2163: SPARQL lowers its RDFS/OWL/SKOS patterns to ontology._axioms, a table SQL alone
    # serves and no Cypher label, so it reads through the map that holds the SQL-only tables.
    return await read_over(state, temp_tables.current(), label_map, vocab)


async def compiled_query(query: str, surface: Any, role_id: str, state: Any) -> Any:
    """``(compiled, read)`` for a SPARQL query under the role (:func:`read_of`), or the
    refusal."""
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.sparql_query.compiler import compile_sparql

    try:
        read = await read_of(surface, role_id, state)
        return compile_sparql(query, read.vocab, read.graph), read
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))


def semantic_statement(  # REQ-2135, REQ-2222
    compiled: Any, read: Any, surface: Any, role_id: str, state: Any
) -> tuple[str, Any] | Response:
    """The semantic SQL a compiled query hands the one pipeline and its graph variables, or the
    refusal. Its one lowering: the route runs it, and an EXPLAIN stops here (REQ-2222)."""
    from provisa.api.rest.cypher_router import access_violations
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.gql.graph_rewriter import apply_graph_rewrites
    from provisa.gql.translator import cypher_to_sql
    from provisa.gql.translator_types import CypherTranslateError

    assert compiled.ast is not None
    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(compiled.ast, read.graph, {})
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, read.graph)
    # REQ-2176: the graph session's relations in place, as a Cypher read's (seam.expand).
    sql_ast = read.expand(sql_ast)
    semantic_sql = make_semantic_sql(sql_ast.sql(dialect="postgres"), surface.context)
    assert not ordered_params, "a SPARQL statement binds no parameters"
    refusal = access_violations(semantic_sql, surface, role_id, state)
    if refusal is not None:
        return refusal
    return semantic_sql, graph_vars


async def answer_rows(  # REQ-2135, REQ-2165
    query: str,
    lowered: tuple[str, Any],
    role_id: str,
    state: Any,
    *,
    cache_hint: Any,
    serve_cached: bool,
    as_of: str | None,
) -> tuple[list[dict], Any, Any] | Response:
    """The rows of a lowered statement (:func:`semantic_statement`), its plan and its result, run
    through the one pipeline at ``as_of`` (``AsOf.key``, REQ-1163), or the refusal."""
    from provisa.api.rest.cypher_router import _dict_rows, _run_plan
    from provisa.gql.assembler import assemble_rows, take_cut
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    semantic_sql, graph_vars = lowered
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
            serve_cached=serve_cached,
            as_of=as_of,
            sdl_joins=False,
        )
    except ComplexityLimitExceeded:
        raise  # REQ-1174: answered as 413 by the app's handler
    except PermissionError as exc:
        return cypher_error(403, "sparql.permission_denied", str(exc), reason=str(exc))
    except ParameterBindingRefused as exc:
        # REQ-1986: a condition that binds a parameter no value, or a class whose table needs
        # a parameter read without one, under the refusal's own code.
        return cypher_error(400, exc.code, str(exc), **exc.params)
    if plan.span_attrs is not None:
        plan.span_attrs["provisa.query_text"] = query
    executed = await _run_plan(plan, state)
    if isinstance(executed, Response):
        return executed
    return assemble_rows(take_cut(_dict_rows(executed), plan.warnings), graph_vars), plan, executed


@router.api_route("/data/sparql", methods=["GET", "POST"])
async def sparql_query(request: Request) -> Response:  # REQ-2135
    """Answer a SPARQL SELECT, ASK, CONSTRUCT or DESCRIBE over the semantic model, for the
    caller's role."""
    from provisa.api.app import state
    from provisa.api.role_surface import role_surface

    query = await _query_text(request)
    if isinstance(query, Response):
        return query

    role_id = _resolve_role_id(request, state)
    surface = role_surface(state, role_id)
    if surface is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )
    if isinstance(query, SparqlUpdate):
        return await answer_update(query.text, request, surface, role_id, state)
    from provisa.api import graph_session

    # REQ-2135 (amended 2026-11-23), REQ-2176: a query reads the principal's graph session too.
    user_id, claims = graph_session.principal(state, request.state.identity)
    async with graph_session.session_for(state, user_id, claims, writes=False):
        return await _answered_query(query, request, surface, role_id, state)


async def _answered_query(  # REQ-2135
    query: str, request: Request, surface: Any, role_id: str, state: Any
) -> Response:
    """Answer the SPARQL query ``query`` in the graph session bound now."""
    from provisa.api.rest.cypher_router import statement_stats
    from provisa.cache.middleware import build_cache_headers
    from provisa.compiler.directives import cache_hint_for, request_cache_hint
    from provisa.executor import stats as query_stats
    from provisa.sparql_query.results import (
        NotAcceptable,
        negotiate,
        write_ask,
        write_construct,
        write_select,
    )

    made = await compiled_query(query, surface, role_id, state)
    if isinstance(made, Response):
        return made
    compiled, read = made
    vocab = read.vocab
    try:
        media, fmt = negotiate(request.headers.get("accept"), compiled.form)
    except NotAcceptable as exc:
        return cypher_error(406, "sparql.not_acceptable", str(exc), reason=str(exc))

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
        lowered = semantic_statement(compiled, read, surface, role_id, state)
        if isinstance(lowered, Response):
            return lowered
        answered = await answer_rows(
            query,
            lowered,
            role_id,
            state,
            cache_hint=cache_hint,
            serve_cached=True,
            as_of=None,
        )
        if isinstance(answered, Response):
            return answered
        rows, plan, executed = answered
        physical_sql = plan.physical_sql or plan.exec_sql or ""
        cache_entry = executed.cache_entry

    if compiled.form == "ASK":
        body = write_ask(bool(rows), fmt)
    elif compiled.form == "CONSTRUCT":
        body = write_construct(rows, vocab.base, vocab.prefixes, fmt, read.terms)
    else:
        body = write_select(compiled.variables, rows, vocab.base, fmt, read.terms)
    # REQ-536: HIT (with the entry's age) when the rows came from the response cache, else MISS.
    headers = build_cache_headers(cache_entry)
    if stats_enabled:
        stats = statement_stats("sparql", len(rows), physical_sql, t0)
        if stats is not None:
            body, stats_headers = with_stats(body, media, stats)
            headers = {**headers, **stats_headers}
    return Response(content=body, media_type=media, headers=headers)


async def solutions_of(  # REQ-2135
    op: Any, read: Any, surface: Any, role_id: str, state: Any
) -> list[dict[str, Any]] | Response:
    """The solutions of an update operation's WHERE (REQ-2135, amended 2026-11-22 and
    2026-11-23): its SELECT of the variables its templates use, read through the one governed
    read as any SELECT is, over the graph session as it stands (``read``, read_of), each solution
    a variable's name -> its RDF term; a session node of no IRI the node itself
    (session_graph.SessionNode)."""
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.sparql_query.compiler import compile_where
    from provisa.sparql_query.results import rdf_term
    from provisa.sparql_query.update import where_variables

    variables = where_variables(op)
    try:
        compiled = compile_where(op.where, variables, read.vocab, read.graph)
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
    if compiled.empty:
        return []
    lowered = semantic_statement(compiled, read, surface, role_id, state)
    if isinstance(lowered, Response):
        return lowered
    answered = await answer_rows(
        "", lowered, role_id, state, cache_hint=NO_CACHE_HINT, serve_cached=False, as_of=None
    )
    if isinstance(answered, Response):
        return answered
    rows = answered[0]
    return [
        {
            v: t
            for v in variables
            if (t := rdf_term(row.get(v), read.vocab.base, read.terms, update=True)) is not None
        }
        for row in rows
    ]


async def answer_update(  # REQ-2135, REQ-2176
    text: str, request: Request, surface: Any, role_id: str, state: Any
) -> Response:
    """Answer a SPARQL Update (REQ-2135, amended 2026-11-22 and 2026-11-23) in the principal's
    graph session (REQ-2176), as a Cypher write runs: the answer says when it wrote to it."""
    from provisa.api import graph_session
    from provisa.api.rest import cypher_session

    user_id, claims = graph_session.principal(state, request.state.identity)
    async with graph_session.session_for(state, user_id, claims, writes=True) as held:
        response = await _updated(text, request, surface, role_id, state, held)
    return cypher_session.answered(response, held)


async def _updated(  # REQ-2135, REQ-2176
    text: str, request: Request, surface: Any, role_id: str, state: Any, held: Any
) -> Response:
    """Each operation in turn: its WHERE read first, then its deletes and its inserts -- a model
    row's as the governed table writes Cypher's writes make, through the one write planner
    (provisa.api.rest.graph_write); a session object's in the graph session
    (provisa.sparql_query.session_graph) -- or a graph management operation on the session's
    graphs. The answer says the rows written and the counters, as a Cypher write's does."""
    from fastapi.responses import JSONResponse

    from provisa.api.rest.cypher_router import _counters
    from provisa.api.rest.graph_write import CypherReads, run_graph_writes, write_refusal
    from provisa.cypher.counters import counting
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.pgwire.temp_exec import TempTableLimit
    from provisa.sparql_query.session_graph import SessionWrite, names_of, read_over
    from provisa.sparql_query.update import (
        delete_writes,
        ground_graphs,
        insert_write,
        model_identity,
        parse_update,
        split_session,
    )

    ctx = surface.context
    label_map = _build_label_map(ctx, role_id, state)
    try:
        vocab = await vocabulary_for(state, role_id, ctx, label_map)
        operations = parse_update(text, vocab.base)
    except CypherTranslateError as exc:
        return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
    session = held.session
    names = await names_of(state, session, label_map)
    objects = SessionWrite(
        state, session, label_map, vocab, names, lambda t: model_identity(t, vocab, label_map)
    )
    reads = CypherReads(request, None)
    affected = 0
    # One update, one count: each operation's writes add to the same counters.
    with counting() as counted:
        try:
            for op in operations:
                if op.manage is not None:
                    await objects.manage(op.manage)
                    continue
                deletes, inserts = op.delete_graphs, op.insert_graphs
                if op.where is not None:
                    # Each WHERE reads the session as the operations before it left it.
                    read = await read_over(state, session, label_map, vocab)
                    solutions = await solutions_of(op, read, surface, role_id, state)
                    if isinstance(solutions, Response):
                        return solutions
                    deletes = ground_graphs(deletes, solutions)
                    inserts = ground_graphs(inserts, solutions)
                model_deletes, deletes[""] = split_session(deletes.get("", []), vocab.base)
                model_inserts, inserts[""] = split_session(inserts.get("", []), vocab.base)
                await objects.delete(deletes)
                asts, params = delete_writes(model_deletes, vocab, label_map)
                inserted = insert_write(model_inserts, vocab, label_map)
                if inserted is not None:
                    asts.append(inserted)
                if asts:
                    done = await run_graph_writes(asts, label_map, role_id, state, params, reads)
                    affected += done.affected
                await objects.insert(inserts)
        except TempTableLimit as exc:  # REQ-1926: past the session's bounds, by name
            return cypher_error(400, "sparql.refused", str(exc), reason=str(exc))
        except Exception as exc:  # allow-ble: request boundary — a refusal, or a source or driver error of any type, is this update's outcome, answered by write_refusal
            return write_refusal(exc, "sparql")
        finally:
            if objects.wrote:
                held.wrote = True
    return JSONResponse(
        content={"type": "sparql", "affected_rows": affected, **_counters(counted, text)}
    )


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
    from provisa.sparql_query.vocabulary import (
        concepts_of,
        superclasses_of,
        term_domains_of,
        vocabulary_document,
    )

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
        edges = await glossary_repo.term_edges(conn)
    return JSONResponse(
        vocabulary_document(
            vocab,
            concepts_of(vocab.base, terms),
            # A Class term's superclasses, among the terms the role reads (REQ-2135).
            superclasses_of(vocab.base, terms, edges),
            term_domains_of(vocab.base, terms),
        )
    )


class RdfExportRequest(BaseModel):  # REQ-2165
    """What an export holds: ``scope`` the whole graph, a domain (``value`` its id) or a class
    (``value`` its IRI), at the time travel ``as_of`` and ``as_recorded`` give (REQ-1163)."""

    scope: Literal["graph", "domain", "class"]
    value: str | None = None
    as_of: str | None = None
    as_recorded: str | None = None


class RdfFileRequest(RdfExportRequest):
    #: Turtle, one document; N-Triples, streamed a query's triples at a time for a large export.
    format: Literal["turtle", "ntriples"] = "turtle"


class RdfLoadRequest(RdfExportRequest):
    """A load into a running store: its SPARQL 1.1 Graph Store HTTP Protocol endpoint, the named
    graph to merge into (else the default graph), and the store's credentials when it asks."""

    endpoint: str
    graph: str | None = None
    username: str | None = None
    password: str | None = None


async def _export_of(request: Request, body: RdfExportRequest) -> Any:
    """``(plan, runner, vocabulary)`` of an export for the caller's role, or the refusal. Every
    query is compiled and held to the role's rights before any is run, so a refusal is answered
    before the first triple is written."""
    from provisa.api.app import state
    from provisa.api.role_surface import role_surface
    from provisa.compiler.directives import NO_CACHE_HINT
    from provisa.gql.translator_types import CypherTranslateError
    from provisa.mv.bitemporal import request_as_of
    from provisa.sparql_query.rdf_export import ScopeRefused, plan_export

    try:
        as_of = request_as_of(body.as_of, body.as_recorded)
    except ValueError as exc:
        return cypher_error(400, "sparql.bad_time_travel", str(exc), reason=str(exc))
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
    domains = {label: nm.domain_id for label, nm in label_map.nodes.items()}
    try:
        plan = plan_export(vocab, domains, body.scope, body.value)
    except ScopeRefused as exc:
        return cypher_error(404, "sparql.export_scope_refused", str(exc), reason=str(exc))

    lowered: dict[str, tuple[str, Any] | None] = {}
    for query in plan.queries:
        made = await compiled_query(query, surface, role_id, state)
        if isinstance(made, Response):
            return made
        compiled, read = made
        if compiled.empty:
            lowered[query] = None  # the role reads nothing it asks for
            continue
        statement = semantic_statement(compiled, read, surface, role_id, state)
        if isinstance(statement, Response):
            return statement
        lowered[query] = statement

    async def run(query: str) -> list[dict]:
        statement = lowered[query]
        if statement is None:
            return []
        answered = await answer_rows(
            query,
            statement,
            role_id,
            state,
            cache_hint=NO_CACHE_HINT,
            serve_cached=False,
            as_of=as_of,
        )
        if isinstance(answered, Response):
            raise RdfExportFailed(answered)
        return answered[0]

    return plan, run, vocab


class RdfExportFailed(Exception):
    """A query of an export failed as it ran; ``response`` is the refusal it was answered."""

    def __init__(self, response: Response) -> None:
        super().__init__(bytes(response.body).decode("utf-8", "replace"))
        self.response = response


@router.post("/data/sparql/export")
async def sparql_export(request: Request, body: RdfFileRequest) -> Response:  # REQ-2165
    """The role's governed graph in ``scope`` as an RDF file: Turtle, or N-Triples streamed."""
    from fastapi.responses import StreamingResponse

    from provisa.sparql_query.rdf_export import NTRIPLES, batches, ntriples, turtle

    made = await _export_of(request, body)
    if isinstance(made, Response):
        return made
    plan, run, vocab = made
    if body.format == "turtle":
        try:
            document = await turtle(plan, run, vocab.base, vocab.prefixes)
        except RdfExportFailed as exc:
            return exc.response
        return Response(
            content=document,
            media_type="text/turtle",
            headers={"Content-Disposition": 'attachment; filename="export.ttl"'},
        )

    async def stream() -> AsyncIterator[bytes]:
        # A query that fails once the stream has begun ends it short: RdfExportFailed is raised
        # through the response, so the client sees a broken transfer, never a whole-looking file.
        async for batch in batches(plan, run, vocab.base):
            if batch:
                yield ntriples(batch)

    return StreamingResponse(
        stream(),
        media_type=NTRIPLES,
        headers={"Content-Disposition": 'attachment; filename="export.nt"'},
    )


@router.post("/data/sparql/export/load")
async def sparql_export_load(request: Request, body: RdfLoadRequest) -> Response:  # REQ-2165
    """The role's governed graph in ``scope`` loaded into a running triple store over the SPARQL
    1.1 Graph Store HTTP Protocol, answered with the triples loaded and each failure, as the Neo4j
    export answers (REQ-392, REQ-2087)."""
    import base64

    import httpx
    from fastapi.responses import JSONResponse

    from provisa.api.engine_error_names import client_value
    from provisa.core.limits import request_timeout_for
    from provisa.security import sandbox_egress
    from provisa.sparql_query.rdf_export import load

    made = await _export_of(request, body)
    if isinstance(made, Response):
        return made
    plan, run, vocab = made
    headers: dict[str, str] = {}
    if body.username is not None:
        token = base64.b64encode(f"{body.username}:{body.password or ''}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    try:
        # REQ-2006: a held org loads into public destinations only, each request checked.
        async with httpx.AsyncClient(event_hooks=sandbox_egress.endpoint_event_hooks()) as client:
            report = await load(
                plan,
                run,
                vocab.base,
                client,
                body.endpoint,
                body.graph,
                headers,
                request_timeout_for("sparql_http"),  # REQ-1905
            )
    except RdfExportFailed as exc:
        return exc.response
    except httpx.ConnectError as exc:
        return cypher_error(
            502,
            "sparql.store_unreachable",
            f"Cannot connect to the triple store: {exc}",
            error=str(exc),
        )
    except httpx.TimeoutException:
        return cypher_error(504, "sparql.store_timed_out", "The triple store request timed out")
    return JSONResponse(content={"loaded": report.loaded, "errors": client_value(report.errors)})

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
limit) rides the X-Provisa-Warnings header as on every HTTP surface.
"""

# Requirements: REQ-2135, REQ-2141

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import unquote

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from provisa.api.rest.cypher_exec import _build_label_map, _resolve_role_id
from provisa.compiler.complexity import ComplexityLimitExceeded

log = logging.getLogger(__name__)

router = APIRouter()

_SPARQL_QUERY = "application/sparql-query"


def _refused(status: int, message: str) -> Response:
    return JSONResponse(status_code=status, content={"error": message})


async def _query_text(request: Request) -> str | Response:
    """The query the request carries, as the SPARQL 1.1 Protocol has it sent."""
    params = request.query_params
    if "default-graph-uri" in params or "named-graph-uri" in params:
        return _refused(400, "default-graph-uri and named-graph-uri are not supported")
    if request.method == "GET":
        query = params.get("query")
    else:
        content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
        if content_type == _SPARQL_QUERY:
            query = (await request.body()).decode("utf-8")
        elif content_type == "application/x-www-form-urlencoded":
            form = await request.form()
            if "default-graph-uri" in form or "named-graph-uri" in form:
                return _refused(400, "default-graph-uri and named-graph-uri are not supported")
            value = form.get("query")
            query = value if isinstance(value, str) else None
        else:
            return _refused(
                415,
                "A SPARQL query is POSTed as application/sparql-query or as the query field of "
                "an application/x-www-form-urlencoded form",
            )
    if not query:
        return _refused(400, "The request carries no query")
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
    from provisa.api.rest.cypher_router import _dict_rows, _run_plan, access_violations
    from provisa.api.role_surface import role_surface
    from provisa.compiler.directives import NO_CACHE_HINT
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
        return _refused(503, "Schema not loaded")
    ctx = surface.context
    label_map = _build_label_map(ctx, role_id, state)
    try:
        vocab = await vocabulary_for(state, role_id, ctx, label_map)
        compiled = compile_sparql(query, vocab, label_map)
        media, fmt = negotiate(request.headers.get("accept"), compiled.form)
    except NotAcceptable as exc:
        return _refused(406, str(exc))
    except CypherTranslateError as exc:
        return _refused(400, str(exc))

    rows: list[dict] = [compiled.empty_row] if compiled.empty_row is not None else []
    if not compiled.empty:
        assert compiled.ast is not None
        try:
            sql_ast, ordered_params, graph_vars = cypher_to_sql(compiled.ast, label_map, {})
        except CypherTranslateError as exc:
            return _refused(400, str(exc))
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
                cache_hint=NO_CACHE_HINT,
                sdl_joins=False,
            )
        except ComplexityLimitExceeded:
            raise  # REQ-1174: answered as 413 by the app's handler
        except PermissionError as exc:
            return _refused(403, str(exc))
        if plan.span_attrs is not None:
            plan.span_attrs["provisa.query_text"] = query
        executed = await _run_plan(plan, state)
        if isinstance(executed, Response):
            return executed
        rows = assemble_rows(take_cut(_dict_rows(executed), plan.warnings), graph_vars)

    if compiled.form == "ASK":
        body = write_ask(bool(rows), fmt)
    elif compiled.form == "CONSTRUCT":
        body = write_construct(rows, vocab.base, vocab.prefixes, fmt)
    else:
        body = write_select(compiled.variables, rows, vocab.base, fmt)
    return Response(content=body, media_type=media)


@router.get("/data/sparql/vocabulary")
async def sparql_vocabulary(request: Request) -> Response:  # REQ-2135, REQ-2141
    """The role's SPARQL vocabulary as IRIs: its prefixes, the classes (each table's own and the
    Class terms grounding tables), the predicates (each column's own and the Property terms), the
    edge types, and the live Concept terms."""
    from provisa.api.app import state
    from provisa.api.role_surface import role_surface
    from provisa.core.repositories import glossary as glossary_repo

    role_id = _resolve_role_id(request, state)
    surface = role_surface(state, role_id)
    if surface is None:
        return _refused(503, "Schema not loaded")
    label_map = _build_label_map(surface.context, role_id, state)
    vocab = await vocabulary_for(state, role_id, surface.context, label_map)
    pool = state.model_db
    assert pool is not None, "the control plane is opened before any request is served"
    async with pool.acquire() as conn:
        concepts = await glossary_repo.concept_terms(conn)
    return JSONResponse(content=vocabulary_body(vocab, concepts))


def vocabulary_body(vocab: Any, concepts: list[str]) -> dict:
    """``vocab`` as the JSON /data/sparql/vocabulary answers with."""
    from provisa.sparql_query.vocabulary import term_iri

    term_prefix = vocab.prefixes["term"]

    def term_of(iri: str) -> str | None:
        return unquote(iri[len(term_prefix) :]) if iri.startswith(term_prefix) else None

    return {
        "base": vocab.base,
        "prefixes": vocab.prefixes,
        "classes": [
            {
                "iri": iri,
                "kind": "class" if term_of(iri) is not None else "table",
                "labels": labels,
                "term": term_of(iri),
            }
            for iri, labels in vocab.classes.items()
        ],
        "predicates": [
            {
                "iri": iri,
                "kind": "property" if term_of(iri) is not None else "column",
                "label": cp.label,
                "property": cp.prop,
                "datatype": cp.datatype,
                "term": term_of(iri),
            }
            for iri, cps in vocab.columns.items()
            for cp in cps
        ],
        "edges": [
            {"iri": iri, "rel_type": ep.rel_type, "source": ep.source, "target": ep.target}
            for iri, eps in vocab.edges.items()
            for ep in eps
        ],
        "concepts": [{"iri": term_iri(vocab.base, name), "term": name} for name in concepts],
    }

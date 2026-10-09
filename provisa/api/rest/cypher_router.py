# Copyright (c) 2026 Kenneth Stott
# Canary: 8b87946f-5300-4b32-9dad-a261b03987ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
# complexity-gate: allow-loc=1250 reason="REQ-392 exclude parameterized (native-filter) nodes from the schema-wide count sweep so one uncountable label no longer zeroes the whole panel; cypher_router.py breakup into per-stage modules is separately-tracked debt (already flagged by the gate)"

"""POST /query/cypher — Cypher query endpoint (Phase AU, REQ-345–353).

Five-stage pipeline:
  1. Cypher parser + translator → SQLGlot AST (physical refs)
  2. Graph type rewriter → CAST(ROW(...) AS JSON) for node/edge columns
  3. make_semantic_sql → semantic refs; apply_governance → RLS/masking/visibility
  4. rewrite_semantic_to_physical → catalog-qualified refs
  5. Federation executor → flat rows → assembler → typed response
"""

from __future__ import annotations


# Requirements: REQ-345, REQ-346, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-392, REQ-398


import logging
import time as _time
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

if TYPE_CHECKING:
    from provisa.cypher.label_map import CypherLabelMap  # noqa: F401
    from provisa.api.app import AppState  # noqa: F401
    from provisa.compiler.sql_gen import CompilationContext  # noqa: F401
    from provisa.core.database import Connection  # noqa: F401

import re as _re
from sqlalchemy import select

from provisa.core import request_deadline
from provisa.core.read_refusal import ReadRefused
from provisa.executor.result import QueryResult
from provisa.core.schema_org import node_ids
from provisa.api.rest.registered_call import (
    _detect_procedure,  # noqa: F401 — re-exported for tests
    _handle_procedure,  # noqa: F401 — re-exported for tests
    intercept_precompile,
)
from provisa.compiler.naming import apply_cql_property as _cql_prop


from provisa.executor import stats as _qs_mod
from provisa.cache.middleware import build_cache_headers
from provisa.compiler.directives import NO_CACHE_HINT, cache_hint_for
from provisa.api.rest.cypher_exec import (
    _build_label_map,
    _execute_call_body,
    _resolve_role_id,
)
from provisa.observability.span_attrs import span_attrs_from_semantic_sql
from provisa.compiler.complexity import ComplexityLimitExceeded

log = logging.getLogger(__name__)

router = APIRouter()

_ID_IN_LIST_RE = _re.compile(
    r"id\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+IN\s+\[([^\]]+)\]",
    _re.IGNORECASE,
)


async def _resolve_id_references(query: str, model_db: Any, label_map: "CypherLabelMap") -> str:
    """Rewrite id(var) IN [int1, int2, ...] replacing stable node ids with the
    id-column value looked up from node_ids.properties via the label_map."""

    all_ints: set[int] = set()
    for m in _ID_IN_LIST_RE.finditer(query):
        for item in m.group(2).split(","):
            try:
                all_ints.add(int(item.strip()))
            except ValueError:
                pass
    if not all_ints:
        return query

    async with model_db.acquire() as _conn:
        _result = await _conn.execute_core(
            select(node_ids.c.id, node_ids.c.composite_id, node_ids.c.label).where(
                node_ids.c.id.in_(sorted(all_ints))
            )
        )
        rows = [dict(r._mapping) for r in _result.fetchall()]

    nm_by_label = {nm.label: nm for nm in label_map.nodes.values()}
    # Also index by table_label for nodes stored with just the table label
    for nm in label_map.nodes.values():
        nm_by_label.setdefault(nm.table_label, nm)
    id_to_val: dict[int, int] = {}
    for r in rows:
        nm = nm_by_label.get(r["label"])
        if nm is None:
            continue
        # composite_id is stored as "Label|rawPk" — extract the physical PK from it
        parts = r["composite_id"].split("|", 1)
        if len(parts) == 2:
            try:
                id_to_val[int(r["id"])] = int(parts[1])
            except ValueError:
                pass

    def _replace(m: _re.Match) -> str:
        new_items: list[str] = []
        for item in m.group(2).split(","):
            item = item.strip()
            try:
                val = id_to_val.get(int(item))
                new_items.append(str(val) if val is not None else item)
            except ValueError:
                new_items.append(item)
        return f"id({m.group(1)}) IN [{', '.join(new_items)}]"

    return _ID_IN_LIST_RE.sub(_replace, query)


def _federation_error(exc: Exception) -> str:
    """Format execution errors without leaking the engine backend name."""
    import re as _re_mod

    from provisa.api.app import state

    _engine_name = state.federation_engine.name  # scrub bound engine name, not hardcoded

    def _scrub(s: str) -> str:
        return _re_mod.sub(
            rf"\b{_re_mod.escape(_engine_name)}\b", "the query engine", s, flags=_re_mod.IGNORECASE
        )

    # Structured engine query error (duck-typed, so no engine-specific exception import): any
    # driver error exposing type/name/message formats into the neutral FederationUserError shape.
    _fields = ("error_type", "error_name", "message")
    if all(hasattr(exc, a) for a in _fields):
        v = {a: getattr(exc, a) for a in _fields}
        parts = [
            f"type={v['error_type']}",
            f"name={v['error_name']}",
            f'message="{_scrub(str(v["message"]))}"',
        ]
        query_id = getattr(exc, "query_id", None)
        if query_id:
            parts.append(f"query_id={query_id}")
        return "FederationUserError(" + ", ".join(parts) + ")"
    return _scrub(str(exc))


def _exec_error(status: int, exc: Exception, sql: str) -> JSONResponse:
    """Neutral execution-error response (scrubbed message + the physical SQL)."""
    return JSONResponse(
        status_code=status,
        content={"error": f"Execution failed: {_federation_error(exc)}", "sql": sql},
    )


class CypherRequest(BaseModel):  # REQ-345
    query: str
    params: dict[str, Any] = {}


async def _execute_multi_call(
    non_corr_calls: list,
    label_map: CypherLabelMap,
    body: CypherRequest,
    state: AppState,
    role_id: str,
    ctx: CompilationContext,
    assemble_rows: Any,
    to_serializable: Any,
) -> Response:
    """Execute independent (non-correlated) CALL subqueries and CROSS JOIN results."""
    all_rows: list[list[dict]] = []
    merged_graph_vars: dict = {}
    for call_sq in non_corr_calls:
        try:
            rows_i, gvars_i = await _execute_call_body(
                call_sq.body, label_map, body.params, state, ctx, role_id
            )
            all_rows.append(rows_i)
            merged_graph_vars.update(gvars_i)
        except Exception as exc:
            log.exception("Cypher multi-CALL execution failed")
            return JSONResponse(
                status_code=500, content={"error": f"Execution failed: {_federation_error(exc)}"}
            )

    combined: list[dict] = [{}]
    for rs in all_rows:
        combined = [{**a, **b} for a in combined for b in (rs or [{}])]

    try:
        assembled = assemble_rows(combined, merged_graph_vars)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Assembly failed: {exc}"})
    try:
        columns = list(combined[0].keys()) if combined else []
        serializable_rows = [to_serializable(r) for r in assembled]
    except Exception as exc:
        log.exception("Cypher serialization failed")
        return JSONResponse(status_code=500, content={"error": f"Serialization failed: {exc}"})
    return JSONResponse(
        content={"columns": columns, "rows": serializable_rows, "type": "cypher"},
        headers=build_cache_headers(None),  # REQ-536
    )


def _build_sql_from_ast(
    ast: Any,
    label_map: CypherLabelMap,
    body: CypherRequest,
    cypher_to_sql: Any,
    apply_graph_rewrites: Any,
) -> tuple[Any, list, dict] | Response:
    """Stages 1-2: translate Cypher AST → SQL AST with graph rewrites. Returns (sql_str, ordered_params, graph_vars) or error Response."""
    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, body.params)
    except Exception as exc:
        from provisa.cypher.translator_types import (
            CypherCrossSourceError,
            CypherTranslateError,
            UnregisteredRelationshipType,
        )

        if isinstance(exc, UnregisteredRelationshipType):
            # REQ-603: refused by the translation every Cypher surface performs.
            return JSONResponse(status_code=403, content={"error": str(exc)})
        if isinstance(exc, (CypherCrossSourceError, CypherTranslateError)):
            return JSONResponse(status_code=400, content={"error": str(exc)})
        raise

    try:
        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    except Exception as exc:
        log.exception("Cypher graph rewrite failed")
        return JSONResponse(status_code=500, content={"error": f"Graph rewrite failed: {exc}"})

    try:
        sql_str = sql_ast.sql(dialect="postgres")
        if ast.comments:
            prefix = "\n".join(f"-- {c}" for c in ast.comments)
            sql_str = f"{prefix}\n{sql_str}"
    except Exception as exc:
        log.exception("Cypher SQL render failed")
        return JSONResponse(status_code=500, content={"error": f"SQL generation failed: {exc}"})

    return sql_str, ordered_params, graph_vars


async def _run_plan(plan: Any, state: AppState) -> QueryResult | Response:
    """Execute a governed Cypher plan through the one pipeline terminal (``_execute_plan``), under
    this transport's request timeout (REQ-1905). The terminal lands what the plan reads, holds its
    live-read permits, reads and writes the response cache, runs the statement on its route and
    audits it; this answers its failure the way Cypher over HTTP always has: the timeout as 504, a
    lost connection as 503, a query error as 400, anything else as 500, each naming the SQL."""
    import asyncio as _asyncio

    from provisa.core.limits import request_timeout_for, request_timeout_setting
    from provisa.pgwire._pipeline import _execute_plan, require_governed_plan

    require_governed_plan(plan)  # REQ-1176: verify at the last moment, before the engine executes
    physical_sql = plan.physical_sql or plan.exec_sql or ""
    _timeout = request_timeout_for("cypher_http")  # REQ-1905: this transport's own
    log.info("Cypher final SQL: %s", physical_sql)
    try:
        # REQ-1882: the deadline watchdog cancels an in-flight blocking statement at _timeout;
        # wait_for alone cannot fire while inline driver work holds the request thread.
        with request_deadline.within(_timeout):
            return await _asyncio.wait_for(_execute_plan(plan, state), timeout=_timeout)
    except _asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={
                # REQ-1905: names the transport and the setting its timeout came from.
                "error": (
                    f"cypher_http request timed out after {_timeout:g}s "
                    f"({request_timeout_setting('cypher_http')})"
                ),
                "sql": physical_sql,
            },
        )
    except OSError as exc:
        log.warning("Cypher execution: network error: %s", exc)
        return _exec_error(503, exc, physical_sql)
    except (PermissionError, ReadRefused):
        raise  # a refusal is answered by its own handler, not as an execution failure
    except Exception as exc:
        # Engine driver errors are classified through the seam (no engine-specific exception
        # import): connection loss → 503, query error → 400. Then HTTP-transport failures → 503,
        # else an unexpected 500.
        kind = state.federation_engine.classify_error(exc)
        if kind == "connection":
            log.warning("Cypher execution: engine connection failed: %s", exc)
            return _exec_error(503, exc, physical_sql)
        if kind == "query":
            log.warning("Cypher execution: engine query error: %s", exc)
            return _exec_error(400, exc, physical_sql)

        import httpx as _httpx

        if isinstance(
            exc,
            (
                _httpx.ConnectError,
                _httpx.NetworkError,
                _httpx.TimeoutException,
                _httpx.InvalidURL,
                _httpx.UnsupportedProtocol,
            ),
        ):
            log.warning("Cypher execution: HTTP network error: %s", exc)
            return _exec_error(503, exc, physical_sql)
        log.exception("Cypher execution failed: %s", physical_sql)
        return _exec_error(500, exc, physical_sql)


def _dict_rows(result: QueryResult) -> list[dict]:
    return [dict(zip(result.column_names, row)) for row in result.rows]


def _serialize_rows(
    rows: list[dict],
    graph_vars: dict,
    assemble_rows: Any,
    to_serializable: Any,
) -> tuple[list[str], list] | Response:
    """Assemble and serialize rows. Returns (columns, serializable_rows) or error Response."""
    try:
        assembled = assemble_rows(rows, graph_vars)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": f"Assembly failed: {exc}"})

    try:
        columns = list(rows[0].keys()) if rows else []
        serializable_rows = [to_serializable(r) for r in assembled]
    except Exception as exc:
        log.exception("Cypher serialization failed")
        return JSONResponse(status_code=500, content={"error": f"Serialization failed: {exc}"})

    return columns, serializable_rows


def _build_stats_content(
    columns: list[str],
    serializable_rows: list,
    physical_sql: str,
    stats_enabled: bool,
    t0: float,
) -> dict:
    """Build response content dict, optionally including query stats."""
    content: dict = {"columns": columns, "rows": serializable_rows}
    if stats_enabled:
        from provisa.api.app import state  # noqa: PLC0415

        _qs_mod.record(
            field="cypher",
            source=state.federation_engine.name,
            strategy="federated",
            elapsed_ms=(_time.perf_counter() - t0) * 1000,
            rows=len(serializable_rows),
            physical_sql=physical_sql,
        )
        qs = _qs_mod.current()
        if qs is not None:
            content["provisa_stats"] = qs.to_dict()
    return content


@router.post("/data/cypher")
async def cypher_query(  # REQ-345, REQ-346, REQ-347, REQ-349, REQ-350, REQ-351, REQ-352
    body: CypherRequest,
    request: Request,
    query_id: str | None = Query(None),
    x_provisa_stats: str | None = Header(None),
) -> Response:
    """Execute a Cypher read or write query and return typed rows or affected_rows."""
    from provisa.api.app import state

    if query_id:
        return JSONResponse(
            status_code=410,
            content={
                "error": "execute-by-approved-query-id is removed; submit the Cypher query "
                "directly — access is governed by table/view and relationship rights"
            },
        )

    # --- Write path (REQ-670): CREATE / DELETE / UPDATE ---
    from provisa.cypher.write_translator import (  # noqa: PLC0415
        CypherWriteParseError as _CWPE,
        WriteTranslator as _WT,
        bind_write_params,
        parse_cypher_write as _pwc,
    )
    from provisa.cypher.params import CypherParamError  # noqa: PLC0415

    _write_ast = None
    try:
        _write_ast = _pwc(body.query)
    except _CWPE:
        pass  # not a write query; fall through to read path

    if _write_ast is not None:
        from provisa.compiler.directives import NO_CACHE_HINT as _NO_CACHE
        from provisa.pgwire._pipeline import (
            _execute_plan as _execute_write_plan,
            _govern_and_route_compiled as _govern_write,
        )

        _role_id = _resolve_role_id(request, state)
        _ctx = state.contexts.get(_role_id)
        if _ctx is None:
            return JSONResponse(status_code=503, content={"error": "Schema not loaded"})
        _label_map = _build_label_map(_ctx, _role_id, state)
        try:
            # The request's parameters are bound into the statement (``$name`` → ``$k``), so the
            # admission's new-row check and the source see the values, never their names.
            _write_sql, _write_params = bind_write_params(
                _WT(_label_map).translate(_write_ast), body.params
            )
        except (_CWPE, CypherParamError) as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

        # ONE write path: the translated statement goes through the pipeline every other surface's
        # write goes through (Bolt's Cypher writes included) — its admission (the write right, the
        # columns' writable_by, the role's row filter), its lowering to the source's own
        # addressing and dialect, and its execution. This route used to check, address and run
        # the statement itself.
        try:
            _plan = await _govern_write(
                _write_sql,
                _role_id,
                exec_params=_write_params or None,
                state=state,
                cache_hint=_NO_CACHE,
                sdl_joins=False,
            )
            _result = await _execute_write_plan(_plan, state)
        except PermissionError as exc:
            return JSONResponse(status_code=403, content={"error": str(exc)})
        except Exception as exc:  # allow-ble: request boundary — a source or driver error of any type is this write's outcome, answered as an error response
            return JSONResponse(status_code=500, content={"error": f"Write failed: {exc}"})
        affected = _result.rowcount if _result.rowcount is not None else len(_result.rows)

        # The steps after a write (cache, views, change events, sinks, hot copy) ran at the
        # pipeline's terminal (pgwire._pipeline._after_write), as on every surface.
        return JSONResponse(content={"affected_rows": affected, "type": "cypher"})

    try:
        from provisa.cypher.parser import parse_cypher, CypherParseError
        from provisa.cypher.translator import cypher_to_sql
        from provisa.cypher.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.params import collect_param_names, bind_params, CypherParamError
        from provisa.cypher.assembler import assemble_rows, to_serializable
        from provisa.compiler.sql_rewrite import make_semantic_sql
        from provisa.compiler.stage2 import build_governance_context
        from provisa.compiler.rls import RLSContext
        from provisa.compiler.sql_validator import validate_sql as _validate_sql
        from provisa.pgwire._pipeline import relationship_guard_bypassed
        from provisa.pgwire._pipeline import _govern_and_route_compiled
    except Exception as exc:
        log.exception("Cypher imports failed")
        return JSONResponse(status_code=500, content={"error": f"Import failed: {exc}"})

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = _build_label_map(ctx, role_id, state)

    _pre = await intercept_precompile(body, state, role_id, label_map)  # procs + REQ-872 CALLs
    if _pre is not None:
        return _pre

    # Resolve stable node ids in id(var) IN [...] to id-column values
    query_text = body.query
    if state.model_db is not None:
        query_text = await _resolve_id_references(query_text, state.model_db, label_map)

    # REQ-1877: a read this role already translated and was admitted for under this schema
    # generation is not parsed, translated or validated again — only its values are bound.
    from provisa.api.rest.cypher_plan import CypherTranslation, TranslationRequest
    from provisa.security.rights import effective_domain_access_role

    _role_dict = effective_domain_access_role(role_id, state.roles)
    kept = TranslationRequest(
        state,
        role_id,
        surface="http",
        domain_access=_role_dict.get("domain_access"),
        cypher=query_text,
        params=body.params,
    )
    translation = kept.cached()
    if translation is None:
        # Stage 1: Parse
        try:
            ast = parse_cypher(query_text)
        except CypherParseError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

        # Validate and bind params
        param_names = collect_param_names(query_text)
        try:
            bind_params(param_names, body.params)
        except CypherParamError as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})

        # Multi-CALL pattern: independent (non-correlated) CALL blocks with no outer MATCH.
        _non_corr_calls = [cs for cs in ast.call_subqueries if not cs.imported_vars]
        if _non_corr_calls and not ast.match_clauses:
            return await _execute_multi_call(
                _non_corr_calls,
                label_map,
                body,
                state,
                role_id,
                ctx,
                assemble_rows,
                to_serializable,
            )

        # Stages 1-2: Translate + graph rewrites
        _sql_result = _build_sql_from_ast(ast, label_map, body, cypher_to_sql, apply_graph_rewrites)
        if isinstance(_sql_result, Response):
            return _sql_result
        sql_str, ordered_params, graph_vars = _sql_result

        # Stage 3: Semantic conversion + access validation (transport responsibility)
        semantic_sql = make_semantic_sql(sql_str, ctx)
        rls = state.rls_contexts.get(role_id, RLSContext.empty())
        _gov_ctx_for_validate = build_governance_context(
            role_id,
            rls,
            state.masking_rules,
            ctx,
            getattr(state, "tables", []),
            role=_role_dict,
            relationships=getattr(state, "relationships", None),
        )
        _violations = _validate_sql(
            semantic_sql,
            ctx,
            _gov_ctx_for_validate,
            _role_dict,
            getattr(state, "tables", []),
            # REQ-264: the relationship guard is held by the pipeline for every Cypher surface
            # (provisa.pgwire._pipeline._govern_compiled), by the one bypass rule; here it is
            # decided by that same rule, never switched off for the surface.
            bypass_relationship_guard=relationship_guard_bypassed(
                _role_dict, state, statement_opts_out=False
            ),
            bypass_uncovered_relationships=True,
        )
        if _violations:
            return JSONResponse(
                status_code=403,
                content={
                    "violations": [{"code": v.code, "message": v.message} for v in _violations]
                },
            )

        translation = CypherTranslation(
            semantic_sql=semantic_sql,
            ordered_params=tuple(ordered_params),
            param_names=tuple(param_names),
            graph_vars=graph_vars,
            span_attrs=span_attrs_from_semantic_sql(semantic_sql, role_id),
        )
        kept.record(translation)

    semantic_sql, graph_vars = translation.semantic_sql, translation.graph_vars
    try:
        resolved_params = translation.bind(body.params)
    except CypherParamError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})

    # Stage 4: Pipeline (governance + routing)
    from provisa.api.redirect_headers import delivery_from_headers

    # REQ-1194: a redirect the request forces (X-Provisa-Redirect*), landed by the pipeline's
    # materialize stage; else REQ-1224's threshold decides (a buffered transport), when the
    # operator has enabled it. A header that cannot be read is refused (400) here.
    delivery = delivery_from_headers(request.headers, role_id)
    try:
        plan = await _govern_and_route_compiled(
            semantic_sql,
            role_id,
            exec_params=resolved_params or None,
            deliver=delivery,
            buffered=True,
            # REQ-544: the Cypher request's own `// @provisa cache` opt-in.
            cache_hint=cache_hint_for("cypher", body.query),
            # REQ-1897: an opted-in read is looked up in the response cache before it is routed.
            serve_cached=True,
            sdl_joins=False,
        )
    except ComplexityLimitExceeded:
        raise  # REQ-1174: answered as 413 by the app's handler
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except Exception as exc:
        log.exception("Cypher governance/routing failed")
        return JSONResponse(status_code=500, content={"error": f"Governance failed: {exc}"})

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()
    _t0 = _time.perf_counter()

    # Stage 5: Execute, through the one pipeline terminal: a response-cache HIT (answered before
    # routing, REQ-1897), the landing of what the plan reads (REQ-1661/REQ-1865), its live-read
    # permits (REQ-1909), the statement on its route with the API stage's fetches already in it,
    # the cache write and the audit row (REQ-074/REQ-1386) are all the terminal's.
    if plan.span_attrs is not None:
        # The statement span names the Cypher the caller sent, not the SQL it became (the ops
        # `queries` report reads it).
        plan.span_attrs["provisa.query_text"] = body.query
    _executed = await _run_plan(plan, state)
    if isinstance(_executed, Response):
        return _executed
    if _executed.redirect is not None:
        # Landed in the results store: no row crossed the wire, the handle names where it is.
        return JSONResponse(
            content={"type": "cypher", "columns": [], "rows": [], "redirect": _executed.redirect}
        )
    rows = _dict_rows(_executed)
    physical_sql = plan.physical_sql or plan.exec_sql or ""

    # Assemble & serialize
    _ser_result = _serialize_rows(rows, graph_vars, assemble_rows, to_serializable)
    if isinstance(_ser_result, Response):
        return _ser_result
    columns, serializable_rows = _ser_result

    # Register nodes and relationships, replacing composite string IDs with stable integers
    from provisa.cypher.assembler import register_node_ids, register_rel_ids

    await register_node_ids(serializable_rows, state.model_db)
    await register_rel_ids(serializable_rows, state.model_db)

    content = _build_stats_content(columns, serializable_rows, physical_sql, stats_enabled, _t0)
    content["type"] = "cypher"
    # REQ-536: HIT (with the entry's age) when the rows came from the response cache, else MISS.
    return JSONResponse(content=content, headers=build_cache_headers(_executed.cache_entry))


@router.get("/data/graph-schema")
async def graph_schema(request: Request) -> JSONResponse:  # REQ-392, REQ-398
    """Return node labels and relationship types for the current role."""
    from provisa.api.app import state

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = _build_label_map(ctx, role_id, state)
    all_tables: list[dict] = getattr(state, "schema_build_cache", {}).get("tables", [])
    col_types: dict = getattr(state, "schema_build_cache", {}).get("column_types", {})
    from provisa.cypher.label_map import _table_label_from_table_name

    cluster_by_name: dict[str, dict] = {
        _table_label_from_table_name(t["table_name"], t.get("domain_id")): {
            "scl1": t.get("l1_cluster"),
            "scl2": t.get("l2_cluster"),
            "scl3": t.get("l3_cluster"),
        }
        for t in all_tables
    }

    def _property_types(n: Any) -> dict[str, str]:
        col_metas = col_types.get(n.table_id, [])
        col_type_map = {cm.column_name: cm.data_type for cm in col_metas}
        return {
            cypher_prop: col_type_map[phys_col]
            for cypher_prop, phys_col in n.physical_properties.items()
            if phys_col in col_type_map
        }

    return JSONResponse(
        content={
            "node_labels": [
                {
                    "label": n.label,
                    "domain_label": n.domain_label,
                    "domain_id": n.domain_id,
                    "table_label": n.table_label,
                    "properties": list(n.properties.keys()),
                    # REQ-392: singular primary-key column name (first designated PK), or null.
                    "pk": _cql_prop(n.pk_columns[0]) if n.pk_columns else None,
                    "pk_columns": [_cql_prop(c) for c in n.pk_columns],
                    "id_column": _cql_prop(n.id_column),
                    "native_filter_columns": sorted(
                        (
                            {"name": _cql_prop(k[4:] if k.startswith("_nf_") else k), "type": v}
                            for k, v in {
                                (kk[4:] if kk.startswith("_nf_") else kk): vv
                                for kk, vv in n.native_filter_columns.items()
                            }.items()
                        ),
                        key=lambda x: x["name"],
                    ),
                    "property_types": _property_types(n),
                    "traversal_only": n.traversal_only,
                    **cluster_by_name.get(
                        n.table_label, {"scl1": None, "scl2": None, "scl3": None}
                    ),
                }
                for n in label_map.nodes.values()
            ],
            "relationship_types": [
                {
                    "type": r.rel_type,
                    "source": label_map.nodes[r.source_label].label,
                    "target": label_map.nodes[r.target_label].label,
                    # REQ-1586: the associative table a junction-backed edge traverses, and the
                    # columns of it that read as relationship properties. Both are absent on an
                    # FK/PK-backed edge, which is how a client tells the two apart.
                    "junction_table_name": r.via.table_name if r.via else None,
                    "properties": sorted(r.properties.keys()),
                }
                for r in label_map.relationships.values()
            ],
        }
    )


def _countable_labels(
    label_map, filtered_domains: set[str], reads_domain
) -> tuple[list[str], list[str]]:
    """The node labels and relationship types the schema-wide count sweep may safely count.

    ``reads_domain(domain_id)``: whether the role reads that domain's tables directly. A count is a
    direct read of the label's table, so a label the role reaches only by traversal (the meta
    domain, for a role not granted it — REQ-1132) is not counted: its count statement is refused
    (V001) on every surface, as a direct FROM of it is.

    A PARAMETERIZED node (native-filter columns) is a function f(args) -> rows with no snapshot:
    ``MATCH (n:Label) RETURN count(n)`` has no arg to satisfy, so it cannot be counted — exclude it
    and any relationship that touches it. Including one would binder-error and, because ``_run_count``
    re-raises, zero the WHOLE panel. Domain filtering (when a domain set is supplied) is also applied.
    """
    node_labels = [
        nm.label
        for nm in label_map.nodes.values()
        if (not filtered_domains or nm.domain_id in filtered_domains)
        and not nm.native_filter_columns
        and reads_domain(nm.domain_id)
    ]
    seen: set[str] = set()
    rel_types: list[str] = []
    for rel in label_map.relationships.values():
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        if filtered_domains and (
            src_nm.domain_id not in filtered_domains or tgt_nm.domain_id not in filtered_domains
        ):
            continue
        if src_nm.native_filter_columns or tgt_nm.native_filter_columns:
            continue  # a rel to/from a parameterized node is uncountable without its arg
        if not (reads_domain(src_nm.domain_id) and reads_domain(tgt_nm.domain_id)):
            continue  # its count reads both ends' tables directly
        if rel.rel_type not in seen:
            seen.add(rel.rel_type)
            rel_types.append(rel.rel_type)
    return node_labels, rel_types


@router.get("/data/graph-counts")
async def graph_counts(request: Request) -> JSONResponse:  # REQ-392
    """Count nodes and relationships via the normal Cypher pipeline, filtered by domain."""
    from provisa.api.app import state
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    domains_param = request.query_params.get("domains", "")
    filtered_domains: set[str] = (
        set(d for d in domains_param.split(",") if d) if domains_param else set()
    )

    label_map = _build_label_map(ctx, role_id, state)

    async def _run_count(cypher: str) -> int | None:
        ast = parse_cypher(cypher)
        body = CypherRequest(query=cypher, params={})
        result = _build_sql_from_ast(ast, label_map, body, cypher_to_sql, apply_graph_rewrites)
        if isinstance(result, Response):
            return 0
        sql_str, _, _ = result
        semantic_sql = make_semantic_sql(sql_str, ctx)
        plan = await _govern_and_route_compiled(
            semantic_sql, role_id, exec_params=None, cache_hint=NO_CACHE_HINT, sdl_joins=False
        )
        executed = await _run_plan(plan, state)
        if isinstance(executed, Response):
            # A label the engine cannot count (its catalog is not attached on this engine) is
            # omitted rather than reported as 0 rows; see below.
            return None
        rows = _dict_rows(executed)
        return int(rows[0]["cnt"]) if rows else 0

    from provisa.security.rights import reaches_all_domains

    _domain_access = state.roles[role_id]["domain_access"]

    def _reads_domain(domain_id: str | None) -> bool:
        return not domain_id or reaches_all_domains(_domain_access) or domain_id in _domain_access

    node_labels, rel_types = _countable_labels(label_map, filtered_domains, _reads_domain)

    # Counts run SEQUENTIALLY, not via asyncio.gather: a native engine (DuckDB) executes on ONE
    # connection whose ATTACH/cache state is not reentrant, so concurrent count queries race and
    # return sporadic zeros for attached/materialized sources. Sequential matches the single-query
    # path (/data/cypher) exactly.
    #
    # A label whose count query the engine cannot execute (its meta/ops catalog is not attached on
    # this engine, or a parameterized table has no snapshot) returns None — OMIT it rather than
    # report a misleading 0 (which reads as "zero rows"). The panel then shows counts only for the
    # labels this engine can actually count.
    label_counts: dict[str, int] = {}
    for lbl in node_labels:
        cnt = await _run_count(f"MATCH (n:{lbl}) RETURN count(n) AS cnt")
        if cnt is not None:
            label_counts[lbl] = cnt

    node_count = sum(label_counts.values())

    rel_count = 0
    for rt in rel_types:
        cnt = await _run_count(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt")
        if cnt is not None:
            rel_count += cnt

    return JSONResponse(
        content={"node_count": node_count, "rel_count": rel_count, "label_counts": label_counts}
    )


# Register the graph-tools routes (/impute-relationships, /neo4j-export) on `router`.
from provisa.api.rest import graph_tools_router as _graph_tools_router  # noqa: E402,F401

# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
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

from provisa.core.schema_org import node_ids
from provisa.api.rest.registered_call import (
    _detect_procedure,  # noqa: F401 — re-exported for tests
    _handle_procedure,  # noqa: F401 — re-exported for tests
    intercept_precompile,
)
from provisa.compiler.naming import apply_cql_property as _cql_prop


from provisa.executor import stats as _qs_mod
from provisa.api.rest.cypher_exec import (
    _build_label_map,
    _execute,
    _execute_call_body,
    _execute_with_api,
    _execute_with_gql_remote,
    _lookup_api_endpoint,
    _lookup_gql_remote_table,
    _resolve_role_id,
    _span_attrs_from_semantic_sql,
)

log = logging.getLogger(__name__)

router = APIRouter()

_ID_IN_LIST_RE = _re.compile(
    r"id\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+IN\s+\[([^\]]+)\]",
    _re.IGNORECASE,
)


async def _resolve_id_references(query: str, tenant_db: Any, label_map: "CypherLabelMap") -> str:
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

    async with tenant_db.acquire() as _conn:
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


def _resolve_table_meta(ctx, table_name: str):  # by GraphQL field name or physical table name
    return ctx.tables.get(table_name) or next(
        (m for m in ctx.tables.values() if m.table_name == table_name), None
    )


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
    return JSONResponse(content={"columns": columns, "rows": serializable_rows, "type": "cypher"})


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
        from provisa.cypher.translator import CypherCrossSourceError, CypherTranslateError

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


async def _dispatch_execution(
    exec_sql: str,
    physical_sql: str,
    resolved_params: list,
    state: AppState,
    span_attrs: dict[str, str],
) -> list[dict] | Response:
    """Stage 5: route to the correct executor based on table backing. Returns rows or error Response."""
    import asyncio as _asyncio
    from provisa.api.data.endpoint import _request_timeout
    from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names

    clean_exec_sql, clean_params, nf_args = extract_nf_args(exec_sql, resolved_params)
    _api_table_names = find_api_table_names(clean_exec_sql)
    _has_api_tables = any(_lookup_api_endpoint(state, tn) is not None for tn in _api_table_names)
    _has_gql_remote = any(
        _lookup_gql_remote_table(state, tn) is not None for tn in _api_table_names
    )

    _timeout = _request_timeout()
    log.info("Cypher final SQL: %s", physical_sql)
    try:
        if _has_gql_remote:
            rows = await _asyncio.wait_for(
                _execute_with_gql_remote(clean_exec_sql, clean_params, nf_args, state, span_attrs),
                timeout=_timeout,
            )
        elif nf_args or _has_api_tables:
            rows = await _asyncio.wait_for(
                _execute_with_api(clean_exec_sql, clean_params, nf_args, state, span_attrs),
                timeout=_timeout,
            )
        else:
            rows = await _asyncio.wait_for(
                _execute(physical_sql, resolved_params, state, span_attrs),
                timeout=_timeout,
            )
    except _asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={"error": f"Query timed out after {_timeout:.0f}s", "sql": physical_sql},
        )
    except OSError as exc:
        log.warning("Cypher execution: network error: %s", exc)
        return _exec_error(503, exc, physical_sql)
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
    return rows


async def _dispatch_execution_direct(
    exec_sql: str,
    source_id: str,
    resolved_params: list,
    state: Any,
) -> list[dict] | Response:
    """Execute SQL against a direct (non-the engine) source. Returns rows or error Response."""
    from provisa.executor.result import QueryResult

    try:
        if source_id == "provisa-admin" or not state.source_pools.has(source_id):
            tenant_db = state.tenant_db
            if tenant_db is None:
                raise RuntimeError("Admin tenant_db not available")
            async with tenant_db.acquire() as _conn:
                _rows = await _conn.fetch(exec_sql)
                if _rows:
                    col_names = list(_rows[0].keys())
                    rows = [tuple(r) for r in _rows]
                else:
                    stmt = await _conn.prepare(exec_sql)
                    col_names = [a.name for a in stmt.get_attributes()]
                    rows = []
            result = QueryResult(rows=rows, column_names=col_names)
        else:
            result = await state.federation_engine.execute_native(
                state.source_pools, source_id, exec_sql, resolved_params or None
            )
        return [dict(zip(result.column_names, row)) for row in result.rows]
    except Exception as exc:
        log.exception("Cypher direct execution failed: %s", exec_sql)
        return _exec_error(500, exc, exec_sql)


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
        parse_cypher_write as _pwc,
        write_acl_error,
    )

    _write_ast = None
    try:
        _write_ast = _pwc(body.query)
    except _CWPE:
        pass  # not a write query; fall through to read path

    if _write_ast is not None:
        import asyncio as _asyncio

        from provisa.compiler.mutation_gen import (
            MutationResult as _MutationResult,
            inject_rls_into_mutation as _inject_rls,
        )
        from provisa.compiler.rls import RLSContext as _RLSContext
        from provisa.transpiler.transpile import transpile as _transpile

        _role_id = _resolve_role_id(request, state)
        _ctx = state.contexts.get(_role_id)
        if _ctx is None:
            return JSONResponse(status_code=503, content={"error": "Schema not loaded"})
        _label_map = _build_label_map(_ctx, _role_id, state)
        try:
            _translator = _WT(_label_map)
            _mapping = _translator._resolve_mapping(_write_ast.label)
            _write_sql = _translator.translate(_write_ast)
        except _CWPE as exc:
            return JSONResponse(status_code=400, content={"error": str(exc)})
        _source_id = _mapping.source_id
        if not state.source_pools.has(_source_id):
            return JSONResponse(
                status_code=503,
                content={
                    "error": f"Source '{_source_id}' does not support writes or is not connected"
                },
            )

        # Build MutationResult so the full write pipeline applies (RLS, dialect
        # transpilation, post-mutation hooks) — same as GraphQL and SQL mutations.
        _mutation_type = {"create": "insert", "update": "update"}.get(_write_ast.kind, "delete")
        _mut = _MutationResult(
            sql=_write_sql,
            params=[],
            mutation_type=_mutation_type,
            table_name=_mapping.table_name,
            source_id=_source_id,
            returning_columns=[],
        )

        # Look up table_meta for RLS and post-mutation hooks (same pattern as GraphQL)
        _table_meta = _resolve_table_meta(_ctx, _mapping.table_name)

        # Enforce writable_by column ACL for CREATE/SET, uniformly with the
        # GraphQL/SQL mutation path (REQ-663).
        if _acl := write_acl_error(_table_meta, _write_ast, _mapping, _role_id):
            return JSONResponse(status_code=_acl[0], content={"error": _acl[1]})

        # Apply RLS into UPDATE/DELETE (same as GraphQL mutations)
        if _table_meta is not None:
            _rls = state.rls_contexts.get(_role_id, _RLSContext.empty())
            if _rls.has_rules():
                _mut = _inject_rls(_mut, _table_meta.table_id, _rls.rules)

        # Transpile to target dialect then add RETURNING for row-count on write-capable backends
        _dialect = state.source_dialects.get(_source_id, "postgres")
        _target_sql = _transpile(_mut.sql, _dialect)
        _source_type = state.source_types.get(_source_id, "")
        if _source_type == "postgresql":
            _target_sql += " RETURNING 1"

        try:
            _result = await state.federation_engine.execute_native(
                state.source_pools, _source_id, _target_sql
            )
            affected = len(_result.rows)
        except Exception as exc:
            return JSONResponse(status_code=500, content={"error": f"Write failed: {exc}"})

        # Post-mutation hooks: cache invalidation, MV staleness, Kafka events,
        # hot-table reload — same as GraphQL mutations.
        if _table_meta is not None:
            await state.response_cache_store.invalidate_by_table(_table_meta.table_id)
            state.mv_registry.mark_stale(_table_meta.table_name)
            from provisa.kafka.change_events import emit_change_event as _emit_change
            from provisa.kafka.sink_executor import trigger_sinks_for_table as _trigger_sinks

            _emit_change(_mapping.table_name, _source_id)
            _asyncio.create_task(_trigger_sinks(_mapping.table_name, state))
            if state.hot_manager is not None:
                from provisa.cache.hot_tables import HotTableManager as _HotMgr

                _hot = state.hot_manager
                assert isinstance(_hot, _HotMgr)
                if _hot.is_hot(_table_meta.table_name):
                    await _hot.invalidate(_table_meta.table_name)
                    if _hot.get_entry(_table_meta.table_name) is None:
                        await _hot.load_table(
                            state.federation_engine,
                            _table_meta.table_name,
                            _table_meta.schema_name,
                            _table_meta.catalog_name,
                            "id",
                        )

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
    if state.tenant_db is not None:
        query_text = await _resolve_id_references(query_text, state.tenant_db, label_map)

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
    _role_dict = state.roles.get(role_id) or {}
    _gov_ctx_for_validate = build_governance_context(
        role_id, rls, state.masking_rules, ctx, getattr(state, "tables", []), role=_role_dict
    )
    _violations = _validate_sql(
        semantic_sql,
        ctx,
        _gov_ctx_for_validate,
        _role_dict,
        getattr(state, "tables", []),
        bypass_relationship_guard=True,
        bypass_uncovered_relationships=True,
    )
    if _violations:
        return JSONResponse(
            status_code=403,
            content={"violations": [{"code": v.code, "message": v.message} for v in _violations]},
        )

    resolved_params = [body.params.get(name) for name in ordered_params]
    span_attrs: dict[str, str] = _span_attrs_from_semantic_sql(semantic_sql, role_id, body.query)

    # Stage 4: Pipeline (governance + routing)
    try:
        plan = await _govern_and_route_compiled(
            semantic_sql,
            role_id,
            exec_params=resolved_params or None,
        )
    except PermissionError as exc:
        return JSONResponse(status_code=403, content={"error": str(exc)})
    except Exception as exc:
        log.exception("Cypher governance/routing failed")
        return JSONResponse(status_code=500, content={"error": f"Governance failed: {exc}"})

    stats_enabled = (x_provisa_stats or "").lower() == "true"
    if stats_enabled:
        _qs_mod.begin()
    _t0 = _time.perf_counter()

    # Stage 5: Execute
    from provisa.transpiler.router import Route as _Route

    exec_sql = plan.exec_sql or ""
    physical_sql = plan.physical_sql or ""
    if plan.route != _Route.ENGINE and plan.source_id:
        # Single-source direct route — cypher SQL rewritten to physical (no catalog)
        _exec_result = await _dispatch_execution_direct(
            exec_sql, plan.source_id, resolved_params, state
        )
    else:
        _exec_result = await _dispatch_execution(
            exec_sql, physical_sql, resolved_params, state, span_attrs
        )
    if isinstance(_exec_result, Response):
        return _exec_result
    rows = _exec_result

    # Assemble & serialize
    _ser_result = _serialize_rows(rows, graph_vars, assemble_rows, to_serializable)
    if isinstance(_ser_result, Response):
        return _ser_result
    columns, serializable_rows = _ser_result

    # Register nodes and relationships, replacing composite string IDs with stable integers
    from provisa.cypher.assembler import register_node_ids, register_rel_ids

    await register_node_ids(serializable_rows, state.tenant_db)
    await register_rel_ids(serializable_rows, state.tenant_db)

    content = _build_stats_content(columns, serializable_rows, physical_sql, stats_enabled, _t0)
    content["type"] = "cypher"
    return JSONResponse(content=content)


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
                }
                for r in label_map.relationships.values()
            ],
        }
    )


def _countable_labels(label_map, filtered_domains: set[str]) -> tuple[list[str], list[str]]:
    """The node labels and relationship types the schema-wide count sweep may safely count.

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
    from provisa.transpiler.router import Route as _Route

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
        try:
            ast = parse_cypher(cypher)
            body = CypherRequest(query=cypher, params={})
            result = _build_sql_from_ast(ast, label_map, body, cypher_to_sql, apply_graph_rewrites)
            if isinstance(result, Response):
                return 0
            sql_str, _, _ = result
            semantic_sql = make_semantic_sql(sql_str, ctx)
            plan = await _govern_and_route_compiled(semantic_sql, role_id, exec_params=None)
            if plan.route != _Route.ENGINE and plan.source_id:
                rows = await _dispatch_execution_direct(
                    plan.exec_sql or "", plan.source_id, [], state
                )
            else:
                rows = await _dispatch_execution(
                    plan.exec_sql or "", plan.physical_sql or "", [], state, {}
                )
            if isinstance(rows, Response):
                return None
            return int(rows[0]["cnt"]) if rows else 0
        except Exception:
            # Swallowing here corrupts totals/pagination — propagate.
            raise

    node_labels, rel_types = _countable_labels(label_map, filtered_domains)

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


class ImputeRequest(BaseModel):
    nodes: list[dict]  # [{label: str, id: str}, ...]


@router.post("/data/impute-relationships")
async def impute_relationships(
    request: Request, body: ImputeRequest
) -> JSONResponse:  # REQ-345, REQ-351
    """Generate and execute all relationship queries for a set of visible graph nodes.

    Accepts the visible node set, uses label_map to determine pk columns and known
    schema relationships, executes one query per relationship pair, and returns
    merged nodes+edges in the standard cypher response format.
    """
    from provisa.api.app import state
    from provisa.cypher.assembler import assemble_rows, to_serializable
    from provisa.cypher.parser import parse_cypher

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return JSONResponse(status_code=503, content={"error": "Schema not loaded"})

    label_map = _build_label_map(ctx, role_id, state)
    nm_by_label = {nm.label: nm for nm in label_map.nodes.values()}

    # Collect stable node ids per label from request
    int_ids: list[int] = []
    id_to_label: dict[int, str] = {}
    for node in body.nodes:
        lbl = str(node.get("label", ""))
        nid = node.get("id")
        if lbl and nid is not None:
            try:
                i = int(nid)
                int_ids.append(i)
                id_to_label[i] = lbl
            except (ValueError, TypeError):
                pass

    def _cql_literal(v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    # Resolve stable ids to id-column values via composite_id ("label|pk_value")
    by_label: dict[str, list[Any]] = {}
    if int_ids and state.tenant_db:
        async with state.tenant_db.acquire() as _pg_conn:
            _pg_result = await _pg_conn.execute_core(
                select(node_ids.c.id, node_ids.c.label, node_ids.c.composite_id).where(
                    node_ids.c.id.in_(int_ids)
                )
            )
            _pg_rows = [dict(r._mapping) for r in _pg_result.fetchall()]
        for _r in _pg_rows:
            _nm = nm_by_label.get(_r["label"])
            if _nm is None:
                continue
            _pk_str = _r["composite_id"].rsplit("|", 1)[-1]
            _val: Any = int(_pk_str) if _pk_str.lstrip("-").isdigit() else _pk_str
            by_label.setdefault(_r["label"], []).append(_val)

    visible_labels = set(by_label.keys())

    # Build queries for every relationship pair where both endpoints are visible
    queries: list[str] = []
    for rel in label_map.relationships.values():
        src_label = label_map.nodes[rel.source_label].label
        tgt_label = label_map.nodes[rel.target_label].label
        if src_label not in visible_labels or tgt_label not in visible_labels:
            continue
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        src_prop = _cql_prop(src_nm.id_column)
        tgt_prop = _cql_prop(tgt_nm.id_column)
        src_ids = ", ".join(_cql_literal(i) for i in by_label[src_label])
        tgt_ids = ", ".join(_cql_literal(i) for i in by_label[tgt_label])
        queries.append(
            f"MATCH (a:{src_label})-[r:{rel.rel_type}]->(b:{tgt_label})"
            f" WHERE a.{src_prop} IN [{src_ids}] AND b.{tgt_prop} IN [{tgt_ids}]"
            f" RETURN a, r, b"
        )

    if not queries:
        return JSONResponse(content={"columns": [], "rows": []})

    all_nodes: dict[str, Any] = {}
    all_edges: dict[str, Any] = {}
    for cypher_query in queries:
        try:
            ast = parse_cypher(cypher_query)
            rows, graph_vars = await _execute_call_body(ast, label_map, {}, state, ctx, role_id)
            assembled = assemble_rows(rows, graph_vars)
        except Exception:
            log.exception("Impute query failed: %s", cypher_query)
            continue
        for row in assembled:
            for val in row.values():
                ser = to_serializable(val)
                if isinstance(ser, dict):
                    if "identity" in ser:
                        all_edges[ser["identity"]] = ser
                    elif "label" in ser:
                        key = f"{ser['label']}:{ser['id']}"
                        all_nodes[key] = ser

    from provisa.cypher.assembler import register_node_ids, register_rel_ids

    serializable_merged = [{"node": r} for r in list(all_nodes.values()) + list(all_edges.values())]
    await register_node_ids(serializable_merged, state.tenant_db)
    await register_rel_ids(serializable_merged, state.tenant_db)
    return JSONResponse(content={"columns": ["node"], "rows": serializable_merged})


class Neo4jExportRequest(BaseModel):
    url: str
    username: str
    password: str
    database: str = "neo4j"
    nodes: list[dict]
    edges: list[dict]


def _neo4j_cypher_literal(v: Any) -> str:
    """Render a Python value as a Cypher literal."""
    import json as _json

    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return _json.dumps(v)
    return _json.dumps(_json.dumps(v))


@router.post("/data/neo4j-export")
async def neo4j_export(body: Neo4jExportRequest) -> JSONResponse:
    """Forward graph nodes/edges to a Neo4j server via its HTTP transactional API."""
    import base64 as _base64
    import httpx as _httpx

    statements: list[str] = []

    for n in body.nodes:
        table_label = n.get("tableLabel", "")
        full_label = n.get("label", "")
        # Domain-union nodes omit tableLabel; reconstruct from compound label "Domain:Table"
        parts = full_label.split(":", 1) if ":" in full_label else [full_label]
        effective_table = table_label or (parts[1] if len(parts) == 2 else full_label) or "Node"
        effective_domain = parts[0] if len(parts) == 2 else ""
        node_id = n.get("id")
        props: dict = n.get("properties", {})
        set_parts = ", ".join(f"{k}: {_neo4j_cypher_literal(v)}" for k, v in props.items())
        set_str = f" SET n += {{{set_parts}}}" if set_parts else ""
        label_str = (
            f"`{effective_table}`:`{effective_domain}`"
            if effective_domain and effective_domain != effective_table
            else f"`{effective_table}`"
        )
        statements.append(f"MERGE (n:{label_str} {{_provisa_id: {node_id}}}){set_str}")

    for e in body.edges:
        start = e.get("start")
        end = e.get("end")
        rel_type = e.get("type", "REL")
        src_label = e.get("startNodeLabel", "Node")
        tgt_label = e.get("endNodeLabel", "Node")
        statements.append(
            f"MATCH (a:`{src_label}` {{_provisa_id: {start}}}), "
            f"(b:`{tgt_label}` {{_provisa_id: {end}}}) "
            f"MERGE (a)-[:`{rel_type}`]->(b)"
        )

    http_url = body.url.rstrip("/") + f"/db/{body.database}/tx/commit"
    token = _base64.b64encode(f"{body.username}:{body.password}".encode()).decode()

    errors: list[str] = []
    try:
        async with _httpx.AsyncClient() as client:
            resp = await client.post(
                http_url,
                json={"statements": [{"statement": s} for s in statements]},
                headers={
                    "Authorization": f"Basic {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=30.0,
            )
    except _httpx.ConnectError as exc:
        return JSONResponse(status_code=502, content={"error": f"Cannot connect to Neo4j: {exc}"})
    except _httpx.TimeoutException:
        return JSONResponse(status_code=504, content={"error": "Neo4j request timed out"})

    if resp.status_code == 401:
        return JSONResponse(status_code=401, content={"error": "Neo4j authentication failed"})
    if resp.status_code // 100 != 2:
        return JSONResponse(
            status_code=resp.status_code,
            content={"error": f"Neo4j HTTP {resp.status_code}: {resp.text[:200]}"},
        )

    data = resp.json()
    for err in data.get("errors", []):
        errors.append(err.get("message", str(err)))

    imported = len(statements) - len(errors)
    return JSONResponse(content={"imported": imported, "errors": errors})

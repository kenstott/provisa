# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7a4c1f-9b5d-4f8a-8c3e-6d2b4f7a9c1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""POST /query/cypher — Cypher query endpoint (Phase AU, REQ-345–353).

Five-stage pipeline:
  1. Cypher parser + translator → SQLGlot AST (physical refs)
  2. Graph type rewriter → CAST(ROW(...) AS JSON) for node/edge columns
  3. make_semantic_sql → semantic refs; apply_governance → RLS/masking/visibility
  4. rewrite_semantic_to_trino_physical → catalog-qualified refs
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

import re as _re
from provisa.compiler.naming import apply_cql_property as _cql_prop

import trino.exceptions as _trino_exc

from provisa.executor import stats as _qs_mod

log = logging.getLogger(__name__)

router = APIRouter()

_PROC_RE = _re.compile(
    r"^\s*CALL\s+(db\.labels|db\.relationshipTypes|db\.propertyKeys)\s*\(\s*\)\s*$", _re.IGNORECASE
)

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
        rows = await _conn.fetch(
            "SELECT id, composite_id, label FROM node_ids WHERE id = ANY($1::int[])",
            sorted(all_ints),
        )

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


def _span_attrs_from_semantic_sql(
    semantic_sql: str,
    role_id: str,
    query_text: str | None = None,
) -> dict[str, str]:
    """Extract normalized table names from semantic SQL for OTel span attributes.

    Semantic SQL uses domain_to_sql_name(domain_id) as schema, e.g. pet_store.pets.
    sqlglot Table nodes expose .db (schema/domain) and .name (table).
    """
    import sqlglot
    import sqlglot.expressions as _sge

    tables: set[str] = set()
    domains: set[str] = set()
    try:
        parsed = sqlglot.parse_one(semantic_sql, dialect="postgres")
        # Walk only the primary FROM clause — LATERAL joins (ops/meta traversal) live in
        # parsed.args["joins"] and must be excluded so provisa.table is a single root
        # table name rather than a comma list that never matches the exact-match join condition.
        from_node = parsed.args.get("from")
        if from_node:
            for tbl in from_node.find_all(_sge.Table):
                db = tbl.db
                name = tbl.name
                if db:
                    tables.add(f"{db}.{name}")
                    domains.add(db)
                elif name:
                    tables.add(name)
    except Exception:
        pass
    attrs: dict[str, str] = {
        "provisa.table": ", ".join(sorted(tables)) or "cypher",
        "provisa.domain": ", ".join(sorted(domains)) or "cypher",
        "provisa.role": role_id,
    }
    if query_text is not None:
        attrs["provisa.query_text"] = query_text
    return attrs


def _federation_error(exc: Exception) -> str:
    """Format execution errors without leaking the Trino backend name."""
    import re as _re_mod

    def _scrub(s: str) -> str:
        return _re_mod.sub(r"\bTrino\b", "the query engine", s, flags=_re_mod.IGNORECASE)

    if isinstance(exc, _trino_exc.TrinoQueryError):
        parts = [
            f"type={exc.error_type}",
            f"name={exc.error_name}",
            f'message="{_scrub(exc.message)}"',
        ]
        if exc.query_id:
            parts.append(f"query_id={exc.query_id}")
        return "FederationUserError(" + ", ".join(parts) + ")"
    return _scrub(str(exc))


def _detect_procedure(query: str) -> str | None:
    m = _PROC_RE.match(query.strip())
    return m.group(1).lower() if m else None


class CypherRequest(BaseModel):  # REQ-345
    query: str
    params: dict[str, Any] = {}


def _handle_procedure(proc: str, label_map: CypherLabelMap) -> JSONResponse:
    """Return schema-inspection results for Neo4j-compatible CALL procedures."""
    if proc == "db.labels":
        all_labels: set[str] = set()
        for nm in label_map.nodes.values():
            if nm.domain_label:
                all_labels.add(nm.domain_label)
            all_labels.add(nm.table_label)
        rows = [{"label": lbl} for lbl in sorted(all_labels)]
        return JSONResponse(content={"columns": ["label"], "rows": rows})
    if proc == "db.relationshiptypes":
        rows = [
            {"relationshipType": r.rel_type}
            for r in sorted(label_map.relationships.values(), key=lambda x: x.rel_type)
        ]
        return JSONResponse(content={"columns": ["relationshipType"], "rows": rows})
    # proc == "db.propertykeys"
    keys: set[str] = set()
    for nm in label_map.nodes.values():
        keys.update(nm.properties.keys())
    rows = [{"propertyKey": k} for k in sorted(keys)]
    return JSONResponse(content={"columns": ["propertyKey"], "rows": rows})


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
    trino_sql: str,
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
    log.info("Cypher final SQL: %s", trino_sql)
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
                _execute(trino_sql, resolved_params, state, span_attrs),
                timeout=_timeout,
            )
    except _asyncio.TimeoutError:
        return JSONResponse(
            status_code=504,
            content={"error": f"Query timed out after {_timeout:.0f}s", "sql": trino_sql},
        )
    except _trino_exc.TrinoConnectionError as exc:
        log.warning("Cypher execution: Trino connection failed: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
        )
    except _trino_exc.TrinoQueryError as exc:
        log.warning("Cypher execution: Trino query error: %s", exc)
        return JSONResponse(
            status_code=400,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
        )
    except OSError as exc:
        log.warning("Cypher execution: network error: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
        )
    except Exception as exc:
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
            return JSONResponse(
                status_code=503,
                content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
            )
        log.exception("Cypher execution failed: %s", trino_sql)
        return JSONResponse(
            status_code=500,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": trino_sql},
        )
    return rows


async def _dispatch_execution_direct(
    exec_sql: str,
    source_id: str,
    resolved_params: list,
    state: Any,
) -> list[dict] | Response:
    """Execute SQL against a direct (non-Trino) source. Returns rows or error Response."""
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import QueryResult

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
            result = await execute_direct(
                state.source_pools, source_id, exec_sql, resolved_params or None
            )
        return [dict(zip(result.column_names, row)) for row in result.rows]
    except Exception as exc:
        log.exception("Cypher direct execution failed: %s", exec_sql)
        return JSONResponse(
            status_code=500,
            content={"error": f"Execution failed: {_federation_error(exc)}", "sql": exec_sql},
        )


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
    trino_sql: str,
    stats_enabled: bool,
    t0: float,
) -> dict:
    """Build response content dict, optionally including query stats."""
    content: dict = {"columns": columns, "rows": serializable_rows}
    if stats_enabled:
        _qs_mod.record(
            field="cypher",
            source="trino",
            strategy="federated",
            elapsed_ms=(_time.perf_counter() - t0) * 1000,
            rows=len(serializable_rows),
            physical_sql=trino_sql,
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
        from provisa.executor.direct import execute_direct as _exec_direct
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
        _mutation_type = {"create": "insert", "delete": "delete", "update": "update"}[
            _write_ast.kind
        ]
        _mut = _MutationResult(
            sql=_write_sql,
            params=[],
            mutation_type=_mutation_type,
            table_name=_mapping.table_name,
            source_id=_source_id,
            returning_columns=[],
        )

        # Look up table_meta for RLS and post-mutation hooks (same pattern as GraphQL)
        _table_meta = _ctx.tables.get(_mapping.table_name)
        if _table_meta is None:
            for _m in _ctx.tables.values():
                if _m.table_name == _mapping.table_name:
                    _table_meta = _m
                    break

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
            _result = await _exec_direct(state.source_pools, _source_id, _target_sql)
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
                            state.trino_conn,
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
        from provisa.compiler.sql_gen import make_semantic_sql
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

    # Intercept Neo4j-compatible schema procedures before parse
    _proc = _detect_procedure(body.query)
    if _proc is not None:
        return _handle_procedure(_proc, label_map)

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
    trino_sql = plan.trino_sql or ""
    if plan.route != _Route.TRINO and plan.source_id:
        # Single-source direct route — cypher SQL rewritten to physical (no catalog)
        _exec_result = await _dispatch_execution_direct(
            exec_sql, plan.source_id, resolved_params, state
        )
    else:
        _exec_result = await _dispatch_execution(
            exec_sql, trino_sql, resolved_params, state, span_attrs
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

    content = _build_stats_content(columns, serializable_rows, trino_sql, stats_enabled, _t0)
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


@router.get("/data/graph-counts")
async def graph_counts(request: Request) -> JSONResponse:  # REQ-392
    """Count nodes and relationships via the normal Cypher pipeline, filtered by domain."""
    import asyncio

    from provisa.api.app import state
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_gen import make_semantic_sql
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

    async def _run_count(cypher: str) -> int:
        try:
            ast = parse_cypher(cypher)
            body = CypherRequest(query=cypher, params={})
            result = _build_sql_from_ast(ast, label_map, body, cypher_to_sql, apply_graph_rewrites)
            if isinstance(result, Response):
                return 0
            sql_str, _, _ = result
            semantic_sql = make_semantic_sql(sql_str, ctx)
            plan = await _govern_and_route_compiled(semantic_sql, role_id, exec_params=None)
            if plan.route != _Route.TRINO and plan.source_id:
                rows = await _dispatch_execution_direct(
                    plan.exec_sql or "", plan.source_id, [], state
                )
            else:
                rows = await _dispatch_execution(
                    plan.exec_sql or "", plan.trino_sql or "", [], state, {}
                )
            if isinstance(rows, Response):
                return 0
            return int(rows[0]["cnt"]) if rows else 0
        except Exception:
            return 0

    node_labels = [
        nm.label
        for nm in label_map.nodes.values()
        if not filtered_domains or nm.domain_id in filtered_domains
    ]

    seen_rel_types: set[str] = set()
    rel_types: list[str] = []
    for rel in label_map.relationships.values():
        src_nm = label_map.nodes[rel.source_label]
        tgt_nm = label_map.nodes[rel.target_label]
        if filtered_domains and (
            src_nm.domain_id not in filtered_domains or tgt_nm.domain_id not in filtered_domains
        ):
            continue
        if rel.rel_type not in seen_rel_types:
            seen_rel_types.add(rel.rel_type)
            rel_types.append(rel.rel_type)

    BATCH = 5

    label_counts: dict[str, int] = {}
    for i in range(0, len(node_labels), BATCH):
        batch = node_labels[i : i + BATCH]
        results = await asyncio.gather(
            *[_run_count(f"MATCH (n:{lbl}) RETURN count(n) AS cnt") for lbl in batch]
        )
        for lbl, cnt in zip(batch, results):
            label_counts[lbl] = cnt

    node_count = sum(label_counts.values())

    rel_count = 0
    for i in range(0, len(rel_types), BATCH):
        batch = rel_types[i : i + BATCH]
        results = await asyncio.gather(
            *[_run_count(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt") for rt in batch]
        )
        rel_count += sum(results)

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
            _pg_rows = await _pg_conn.fetch(
                "SELECT id, label, composite_id FROM node_ids WHERE id = ANY($1::int[])",
                int_ids,
            )
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


def _resolve_role_id(request: Request, state: AppState) -> str:
    """Resolve the role_id from X-Provisa-Role header, falling back to the first registered role."""
    roles: dict = getattr(state, "roles", {})
    header_role = request.headers.get("x-provisa-role") or request.headers.get("X-Provisa-Role")
    if header_role and header_role in roles:
        return header_role
    if roles:
        return next(iter(roles))
    return "default"


def _build_label_map(ctx: CompilationContext, role_id: str, state: AppState) -> CypherLabelMap:
    """Build CypherLabelMap with cross-domain traversal nodes for the given role."""
    from provisa.cypher.label_map import CypherLabelMap

    role = getattr(state, "roles", {}).get(role_id, {})
    cache = getattr(state, "schema_build_cache", {})
    return CypherLabelMap.from_schema(
        ctx,
        domain_access=role.get("domain_access"),
        all_tables=cache.get("tables"),
        all_relationships=cache.get("relationships"),
        all_column_types=cache.get("column_types"),
        source_catalogs=getattr(state, "source_catalogs", None),
    )


def _lookup_api_endpoint(state: AppState, table_name: str):
    """Look up an API endpoint by table name."""
    ep_map: dict = getattr(state, "api_endpoints", {})
    return ep_map.get(table_name)


def _lookup_gql_remote_table(state: AppState, table_name: str) -> dict | None:
    """Look up graphql_remote source info by SQL table name (snake_case)."""
    for reg in getattr(state, "graphql_remote_sources", {}).values():
        for t in reg.get("tables", []):
            if t["sql_name"] == table_name:
                return {
                    "source_id": reg["source_id"],
                    "url": reg["url"],
                    "auth": reg.get("auth"),
                    "field_name": t.get("field_name", t["name"]),
                    "columns": t.get("columns", []),
                    "required_args": t.get("required_args", []),
                    "cache_ttl": reg.get("cache_ttl", 300),
                    "cache_catalog": reg.get("cache_catalog", "provisa_admin"),
                }
    return None


async def _execute_with_api(
    exec_sql: str,
    params: list,
    nf_args: dict,
    state: Any,
    span_attrs: dict[str, str] | None = None,
) -> list[dict]:
    """Phase 1 (REST) + Phase 2 (Trino) execution for ALL API-backed tables in the query.

    For each API-backed table referenced in FROM/JOIN clauses:
      1. Derive URL params from nf_args columns that match the endpoint's native params.
      2. Materialize into Trino cache (cache miss) or reuse (cache hit).
      3. Rewrite all API table references in the SQL to their respective cache tables.
    This ensures json-typed JSONB columns are always exposed as VARCHAR in the cache.
    """
    import asyncio
    from provisa.api_source.router_integration import handle_api_query
    from provisa.api_source.trino_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino
    from provisa.compiler.nf_extractor import find_api_table_names

    table_names = find_api_table_names(exec_sql)
    api_endpoints_in_sql: list[tuple[str, Any]] = []
    for tn in table_names:
        ep = _lookup_api_endpoint(state, tn)
        if ep is not None:
            api_endpoints_in_sql.append((tn, ep))

    if not api_endpoints_in_sql:
        raise RuntimeError(f"No API endpoint found for tables: {table_names}")

    hot_mgr = getattr(state, "hot_manager", None)

    # Hot table bypass: only applies when there is exactly one API table and it is hot.
    if len(api_endpoints_in_sql) == 1:
        table_name, endpoint = api_endpoints_in_sql[0]
        if hot_mgr is not None and hot_mgr.is_hot(table_name):
            from provisa.cache.hot_tables import build_values_cte_sql

            entry = hot_mgr.get_entry(table_name)
            hot_sql = build_values_cte_sql(exec_sql, table_name, entry)
            trino_sql = transpile_to_trino(hot_sql)
            log.info("[HOT TABLE] hit — %s (%d rows inline)", table_name, len(entry.rows))
            trino_result = execute_trino(state.trino_conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(trino_result.column_names, row)) for row in trino_result.rows]

    from provisa.executor.redirect import RedirectConfig

    redirect_config = RedirectConfig.from_env()

    # Materialize every API-backed table into its Trino cache slot.
    cache_rewrites: dict[str, tuple] = {}  # physical table name → (CacheLocation, cache_tbl)
    for table_name, endpoint in api_endpoints_in_sql:
        _response_cols = [c for c in endpoint.columns if c.param_type is None]
        if not _response_cols:
            from provisa.compiler.nf_extractor import drop_union_branches_for_table

            exec_sql = drop_union_branches_for_table(exec_sql, table_name)
            log.warning(
                "[API CACHE] %s has no response columns — dropping union branch", table_name
            )
            continue
        source_id: Any = getattr(endpoint, "source_id", None)
        api_source = getattr(state, "api_sources", {}).get(source_id)

        # Filter nf_args to columns belonging to this endpoint.
        param_name_map: dict = {}
        valid_nf_keys: set = set()
        for c in endpoint.columns:
            if c.param_name:
                param_name_map[c.name] = c.param_name
                param_name_map[f"_{c.name}"] = c.param_name
                valid_nf_keys.add(c.name)
                valid_nf_keys.add(f"_{c.name}")
        url_params = {param_name_map.get(k, k): v for k, v in nf_args.items() if k in valid_nf_keys}

        _unfilled_path_params = [
            c.param_name or c.name
            for c in endpoint.columns
            if c.param_type is not None
            and c.param_type.value == "path"
            and (c.param_name or c.name) not in url_params
        ]
        if _unfilled_path_params:
            from provisa.compiler.nf_extractor import drop_union_branches_for_table

            exec_sql = drop_union_branches_for_table(exec_sql, table_name)
            log.warning(
                "[API CACHE] %s missing path params %s — dropping union branch",
                table_name,
                _unfilled_path_params,
            )
            continue

        _cc = getattr(api_source, "cache_catalog", None) if api_source else None
        _default_cs = f"org_{getattr(state, 'org_id', 'default')}_api_cache"
        _cs = getattr(api_source, "cache_schema", _default_cs) if api_source else _default_cs
        _cache_loc = cache_location(source_id, _cc, _cs)
        cache_tbl = cache_table_name(source_id, table_name, url_params)
        cache_rewrites[table_name] = (_cache_loc, cache_tbl)

        loop = asyncio.get_event_loop()

        def _check_schema_and_exists() -> bool:
            import trino as _trino

            conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
            try:
                ensure_cache_schema(conn, _cache_loc)
                return table_exists(conn, _cache_loc, cache_tbl)
            finally:
                conn.close()

        hit = await loop.run_in_executor(None, _check_schema_and_exists)
        if not hit:
            result = await handle_api_query(
                endpoint=endpoint,
                params=url_params,
                conn=state.trino_conn,
                source=api_source,
                source_ttl=getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl"),
                global_ttl=getattr(state, "response_cache_default_ttl", None),
                loc=_cache_loc,
            )

            ttl = (
                getattr(state, "source_cache", {}).get(source_id, {}).get("cache_ttl")
                or getattr(state, "response_cache_default_ttl", None)
                or endpoint.ttl
            )
            asyncio.create_task(
                schedule_drop(state.trino_conn, _cache_loc, cache_tbl, ttl, redirect_config)
            )

            if hot_mgr is not None and result.rows:
                asyncio.create_task(hot_mgr.maybe_promote_dicts(table_name, result.rows))
        else:
            log.info("[API CACHE] hit — %s", cache_tbl)

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites)
    trino_sql = transpile_to_trino(rewritten_sql)

    def _run_query() -> list[dict]:
        import trino as _trino

        conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
        try:
            result = execute_trino(conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(result.column_names, row)) for row in result.rows]
        finally:
            conn.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run_query)


async def _execute_with_gql_remote(
    exec_sql: str,
    params: list,
    nf_args: dict,
    state: Any,
    span_attrs: dict[str, str] | None = None,
) -> list[dict]:
    """Materialize graphql_remote tables into Trino cache and execute the query."""
    import asyncio
    from dataclasses import dataclass
    from provisa.graphql_remote.executor import execute_remote
    from provisa.api_source.trino_cache import (
        cache_table_name,
        cache_location,
        ensure_cache_schema,
        table_exists,
        create_and_insert,
        rewrite_all_from_cache,
        schedule_drop,
    )
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.transpile import transpile_to_trino
    from provisa.compiler.nf_extractor import (
        find_api_table_names,
        drop_joined_table,
        drop_union_branches_for_table,
        where_referenced_tables,
    )
    from provisa.compiler.naming import apply_sql_name as _apply_sql_name

    @dataclass
    class _Col:
        name: str
        type: str

    _GQL_TO_CACHE_TYPE = {
        "text": "string",
        "integer": "integer",
        "numeric": "number",
        "boolean": "boolean",
        "jsonb": "jsonb",
    }

    table_names = find_api_table_names(exec_sql)
    where_tables = where_referenced_tables(exec_sql)
    cache_rewrites: dict[str, tuple] = {}
    gql_remote_skipped: set[str] = set()

    for tn in table_names:
        info = _lookup_gql_remote_table(state, tn)
        if info is None:
            continue

        required_args: list[dict] = info.get("required_args", [])
        # Build variables for this table from nf_args keyed by arg name
        gql_vars = {a["name"]: nf_args[a["name"]] for a in required_args if a["name"] in nf_args}
        missing = [a["name"] for a in required_args if a["name"] not in nf_args]
        if missing:
            if tn not in where_tables:
                # Not explicitly filtered — drop JOIN or UNION branch.
                exec_sql = drop_joined_table(exec_sql, tn)
                exec_sql = drop_union_branches_for_table(exec_sql, tn)
                gql_remote_skipped.add(tn)
                continue
            raise ValueError(
                f"Table '{tn}' requires argument(s) {missing} — "
                f"add a WHERE clause, e.g. WHERE n.{missing[0]} = <value>"
            )

        cache_loc = cache_location(
            info["source_id"],
            info["cache_catalog"],
            f"org_{getattr(state, 'org_id', 'default')}_gql_cache",
        )
        cache_tbl = cache_table_name(info["source_id"], tn, gql_vars)
        cache_rewrites[tn] = (cache_loc, cache_tbl)
        _info_columns: list = info["columns"]

        loop = asyncio.get_event_loop()

        def _check_or_create_cache(fetch_rows: list | None) -> bool:
            import trino as _trino

            conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
            try:
                ensure_cache_schema(conn, cache_loc)
                if table_exists(conn, cache_loc, cache_tbl):
                    return True
                if fetch_rows is not None:
                    col_objs = [
                        _Col(
                            name=_apply_sql_name(c["name"]),
                            type=_GQL_TO_CACHE_TYPE.get(c.get("type", "text"), "string"),
                        )
                        for c in _info_columns
                    ]
                    _gql_to_sql = {c["name"]: _apply_sql_name(c["name"]) for c in _info_columns}
                    fetch_rows = [
                        {_gql_to_sql.get(k, k): v for k, v in row.items()} for row in fetch_rows
                    ]
                    create_and_insert(conn, cache_loc, cache_tbl, fetch_rows, col_objs)
                return False
            finally:
                conn.close()

        hit = await loop.run_in_executor(None, _check_or_create_cache, None)
        if not hit:
            col_selections = [c.get("gql_selection", c["name"]) for c in info["columns"]]
            fetched_rows = await execute_remote(
                url=info["url"],
                auth=info["auth"],
                field_name=info["field_name"],
                columns=col_selections,
                variables=gql_vars or None,
                required_args=required_args or None,
            )
            await loop.run_in_executor(None, _check_or_create_cache, fetched_rows)
            asyncio.create_task(
                schedule_drop(state.trino_conn, cache_loc, cache_tbl, info["cache_ttl"])
            )
        else:
            log.info("[GQL CACHE] hit — %s", cache_tbl)

    # If skipped gql_remote tables (missing required args) weren't dropped from the main FROM,
    # they still reference an uncacheable catalog — return empty instead of failing in Trino.
    if gql_remote_skipped and not cache_rewrites:
        still_present = gql_remote_skipped & set(find_api_table_names(exec_sql))
        if still_present:
            return []

    rewritten_sql = rewrite_all_from_cache(exec_sql, cache_rewrites) if cache_rewrites else exec_sql
    trino_sql = transpile_to_trino(rewritten_sql)

    def _run_query() -> list[dict]:
        import trino as _trino

        conn = _trino.dbapi.connect(**state.trino_conn_kwargs)
        try:
            result = execute_trino(conn, trino_sql, params, span_attrs=span_attrs)
            return [dict(zip(result.column_names, row)) for row in result.rows]
        finally:
            conn.close()

    return await asyncio.get_event_loop().run_in_executor(None, _run_query)


async def _execute(
    sql: str, params: list, state: Any, span_attrs: dict[str, str] | None = None
) -> list[dict]:
    """Execute SQL against the federation engine and return rows as dicts."""
    from provisa.executor.trino import execute_trino

    trino_conn = getattr(state, "trino_conn", None)
    if trino_conn is None:
        raise RuntimeError("Federation engine not connected")

    import asyncio

    def _run() -> list[dict]:
        result = execute_trino(trino_conn, sql, params or [], span_attrs=span_attrs)
        return [dict(zip(result.column_names, row)) for row in result.rows]

    return await asyncio.get_event_loop().run_in_executor(None, _run)


async def _execute_call_body(
    call_body: Any,
    label_map: Any,
    params: dict,
    state: Any,
    ctx: Any,
    role_id: str = "default",
) -> tuple[list[dict], dict]:
    """Full pipeline execution for a single CALL subquery body."""
    from provisa.cypher.translator import cypher_to_sql
    from provisa.cypher.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_gen import make_semantic_sql
    from provisa.compiler.nf_extractor import extract_nf_args, find_api_table_names
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    sql_ast, ordered_params, graph_vars = cypher_to_sql(call_body, label_map, params)
    sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    sql_str = sql_ast.sql(dialect="postgres")
    semantic_sql = make_semantic_sql(sql_str, ctx)
    _cb_span_attrs: dict[str, str] = _span_attrs_from_semantic_sql(semantic_sql, role_id)
    resolved_params = [params.get(name) for name in ordered_params]

    plan = await _govern_and_route_compiled(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
    )
    exec_sql = plan.exec_sql or ""
    trino_sql = plan.trino_sql or ""

    clean_exec_sql, clean_params, nf_args = extract_nf_args(exec_sql, resolved_params)
    api_table_names = find_api_table_names(exec_sql)
    has_api = any(_lookup_api_endpoint(state, tn) is not None for tn in api_table_names)
    has_gql_remote = any(_lookup_gql_remote_table(state, tn) is not None for tn in api_table_names)

    if nf_args or has_api:
        rows = await _execute_with_api(clean_exec_sql, clean_params, nf_args, state, _cb_span_attrs)
    elif has_gql_remote:
        rows = await _execute_with_gql_remote(
            exec_sql, resolved_params, nf_args, state, _cb_span_attrs
        )
    elif trino_sql:
        rows = await _execute(trino_sql, resolved_params, state, _cb_span_attrs)
    else:
        from provisa.pgwire._pipeline import _execute_plan as _exec_plan

        qr = await _exec_plan(plan, state)
        rows = [dict(zip(qr.column_names, row)) for row in qr.rows]

    return rows, graph_vars

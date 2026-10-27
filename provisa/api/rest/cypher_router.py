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

from provisa.api.role_surface import role_surface


# Requirements: REQ-345, REQ-346, REQ-347, REQ-348, REQ-349, REQ-350, REQ-351, REQ-352, REQ-353, REQ-392, REQ-398


import logging
import time as _time
from typing import TYPE_CHECKING, Any, Literal

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

if TYPE_CHECKING:
    from provisa.gql.label_map import CypherLabelMap  # noqa: F401
    from provisa.api.app import AppState  # noqa: F401
    from provisa.compiler.sql_gen import CompilationContext  # noqa: F401
    from provisa.core.database import Connection  # noqa: F401
    from provisa.gql.translator_types import CypherMistake  # noqa: F401

import re as _re
from sqlalchemy import select

from provisa.core import request_deadline
from provisa.core.parameters import ParameterRequired
from provisa.api.errors import ApiError
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
from provisa.compiler.directives import (
    NO_CACHE_HINT,
    cache_hint_for,
    extract_directives_from_cypher_comments,
    request_cache_hint,
)
from provisa.api.rest.cypher_exec import (
    _build_label_map,
    _execute_call_body,
    _resolve_role_id,
)
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


def cypher_error(
    status: int,
    code: str,
    message: str,
    *,
    sql: str | None = None,
    violations: list[dict[str, str]] | None = None,
    **params: Any,
) -> JSONResponse:
    """An error of a graph surface over HTTP, in the app's ApiError body (REQ-1350): the English
    ``detail``, a stable ``code`` and its ``params``, so every client reads the reason the same
    way. ``sql`` is the statement that failed, where there is one; ``violations`` lists each
    access violation (``code``, ``message``) that ``detail`` joins."""
    content: dict[str, Any] = {"detail": message, "code": code, "params": params}
    if sql is not None:
        content["sql"] = sql
    if violations is not None:
        content["violations"] = violations
    return JSONResponse(status_code=status, content=content)


def mistake_error(exc: "CypherMistake") -> JSONResponse:
    """REQ-778: a mistake in the user's statement, refused before any engine ran it -- 400, its
    own code and params, its message in the user's terms."""
    return cypher_error(400, exc.code, str(exc), **exc.params)


def _exec_error(status: int, exc: Exception, sql: str) -> JSONResponse:
    """Neutral execution-error response (scrubbed message + the physical SQL)."""
    error = _federation_error(exc)
    return cypher_error(
        status, "cypher.execution_failed", f"Execution failed: {error}", sql=sql, error=error
    )


class CypherRequest(BaseModel):  # REQ-345
    query: str
    params: dict[str, Any] = {}
    # "last" (the default): the query is one statement, as Neo4j takes it. "each": the query holds
    # statements separated by ";" (as Neo4j Browser's editor holds them), each run on its own in
    # order and answered by its own result or error; the parameters are shared.
    results: Literal["last", "each"] = "last"


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
        except (ApiError, ReadRefused):
            raise  # REQ-1350: a refusal by name is answered by the app's handler for it
        except Exception as exc:
            log.exception("Cypher multi-CALL execution failed")
            error = _federation_error(exc)
            return cypher_error(
                500, "cypher.execution_failed", f"Execution failed: {error}", error=error
            )

    combined: list[dict] = [{}]
    for rs in all_rows:
        combined = [{**a, **b} for a in combined for b in (rs or [{}])]
    from provisa.gql.assembler import take_cut

    combined = take_cut(combined, None)  # rows of several plans: the request's collector

    try:
        assembled = assemble_rows(combined, merged_graph_vars)
    except Exception as exc:
        return cypher_error(
            500, "cypher.assembly_failed", f"Assembly failed: {exc}", error=str(exc)
        )
    try:
        columns = list(combined[0].keys()) if combined else []
        serializable_rows = [to_serializable(r) for r in assembled]
    except Exception as exc:
        log.exception("Cypher serialization failed")
        return cypher_error(
            500, "cypher.serialization_failed", f"Serialization failed: {exc}", error=str(exc)
        )
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
    role_id: str,
    state: Any,
) -> tuple[Any, list, dict] | Response:
    """Stages 1-2: translate Cypher AST → SQL AST with graph rewrites. Returns (sql_str, ordered_params, graph_vars) or error Response."""
    try:
        sql_ast, ordered_params, graph_vars = cypher_to_sql(ast, label_map, body.params)
    except Exception as exc:
        from provisa.gql.translator_types import CypherMistake
        from provisa.gql.translator_types import CypherCrossSourceError, CypherTranslateError

        if isinstance(exc, CypherMistake):
            return mistake_error(exc)
        if isinstance(exc, (CypherCrossSourceError, CypherTranslateError)):
            return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))
        raise

    # REQ-2168: functions named by their Cypher command bindings, as the role may use them.
    from provisa.api.data.cypher_commands import expand_functions

    sql_ast = expand_functions(sql_ast, role_id, state)

    try:
        sql_ast = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    except Exception as exc:
        log.exception("Cypher graph rewrite failed")
        return cypher_error(
            500, "cypher.rewrite_failed", f"Graph rewrite failed: {exc}", error=str(exc)
        )

    try:
        sql_str = sql_ast.sql(dialect="postgres")
        if ast.comments:
            prefix = "\n".join(f"-- {c}" for c in ast.comments)
            sql_str = f"{prefix}\n{sql_str}"
    except Exception as exc:
        log.exception("Cypher SQL render failed")
        return cypher_error(
            500, "cypher.sql_generation_failed", f"SQL generation failed: {exc}", error=str(exc)
        )

    return sql_str, ordered_params, graph_vars


async def _governed_plan(semantic_sql: str, role_id: str, **route: Any) -> Any:
    """The Cypher read's plan, governed and routed by the one pipeline, or the failure as this
    transport answers it. A refusal by name -- the app's coded errors (ApiError: a row-level
    table read without a filter its source can apply, REQ-1915) and a read the deployment cannot
    answer now (ReadRefused) -- is answered by the app's own handler for it, with its own status
    and code, as on every other surface (REQ-1350); only what nothing names is a 500 here."""
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    try:
        return await _govern_and_route_compiled(semantic_sql, role_id, **route)
    except (ComplexityLimitExceeded, ApiError, ReadRefused):
        raise  # REQ-1174 (413), REQ-1350, REQ-1915: answered by the app's handlers
    except PermissionError as exc:
        return cypher_error(403, "cypher.permission_denied", str(exc), reason=str(exc))
    except ParameterRequired as exc:
        # REQ-1986: a read leaving a required parameter without a value -- the caller's to give.
        return cypher_error(
            400, "cypher.parameter_required", str(exc), table=exc.table, reason=str(exc)
        )
    except Exception as exc:
        log.exception("Cypher governance/routing failed")
        return cypher_error(
            500, "cypher.governance_failed", f"Governance failed: {exc}", error=str(exc)
        )


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
        # REQ-1905: names the transport and the setting its timeout came from.
        setting = request_timeout_setting("cypher_http")
        return cypher_error(
            504,
            "data.query_timeout",
            f"cypher_http request timed out after {_timeout:g}s ({setting})",
            sql=physical_sql,
            timeout_s=f"{_timeout:g}",
            transport="cypher_http",
            setting=setting,
        )
    except OSError as exc:
        log.warning("Cypher execution: network error: %s", exc)
        return _exec_error(503, exc, physical_sql)
    except (PermissionError, ReadRefused, ApiError):
        raise  # a refusal is answered by its own handler, not as an execution failure
    except Exception as exc:
        # Engine driver errors are classified through the seam (no engine-specific exception
        # import): connection loss → 503, query error → 400. Then HTTP-transport failures → 503,
        # else an unexpected 500.
        refused = state.federation_engine.refusal_of(exc)
        if refused is not None:
            raise refused from exc  # REQ-778: said by name, by the ReadRefused handler
        kind = state.federation_engine.classify_error(exc)
        if kind == "connection":
            log.warning("Cypher execution: engine connection failed: %s", exc)
            return _exec_error(503, exc, physical_sql)
        if kind == "query":
            log.warning("Cypher execution: engine query error: %s", exc)
            # REQ-778: a statement the engine refused, after the translation took it: the
            # engine's own text, said as the engine's.
            error = _federation_error(exc)
            return cypher_error(
                400,
                "cypher.engine_refused",
                f"The engine refused the query: {error}",
                sql=physical_sql,
                error=error,
            )

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


def access_violations(
    semantic_sql: str, surface: Any, role_id: str, state: "AppState"
) -> Response | None:
    """The 403 naming each access violation of a graph statement the role sent, or None: the
    validation every graph surface over HTTP runs before the pipeline (Cypher's, SPARQL's)."""
    from provisa.compiler.sql_validator import validate_sql
    from provisa.compiler.stage2 import build_governance_context
    from provisa.pgwire._pipeline import relationship_guard_bypassed
    from provisa.security.rights import effective_domain_access_role

    role = effective_domain_access_role(role_id, state.roles)
    ctx = surface.context
    governance = build_governance_context(
        role_id,
        surface.rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role,
        relationships=getattr(state, "relationships", None),
    )
    violations = validate_sql(
        semantic_sql,
        ctx,
        governance,
        role,
        getattr(state, "tables", []),
        # REQ-264: the relationship guard is held by the pipeline for every graph surface
        # (provisa.pgwire._pipeline._govern_compiled), by the one bypass rule; here it is
        # decided by that same rule, never switched off for the surface.
        bypass_relationship_guard=relationship_guard_bypassed(
            role, state, statement_opts_out=False
        ),
        bypass_uncovered_relationships=True,
    )
    if not violations:
        return None
    listed = [{"code": v.code, "message": v.message} for v in violations]
    return cypher_error(
        403,
        "cypher.access_violations",
        "; ".join(v["message"] for v in listed),
        violations=listed,
        count=len(listed),
    )


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
        return cypher_error(
            500, "cypher.assembly_failed", f"Assembly failed: {exc}", error=str(exc)
        )

    try:
        columns = list(rows[0].keys()) if rows else []
        serializable_rows = [to_serializable(r) for r in assembled]
    except Exception as exc:
        log.exception("Cypher serialization failed")
        return cypher_error(
            500, "cypher.serialization_failed", f"Serialization failed: {exc}", error=str(exc)
        )

    found = _non_finite(serializable_rows)
    if found is not None:
        # REQ-778: JSON has no Infinity or NaN (t.id / 0, log(0)): the answer is refused, naming
        # the column, rather than failing as it is written.
        column, value = found
        return cypher_error(
            400,
            "cypher.non_finite_number",
            f"The answer holds a number JSON cannot carry: {column} is {value}",
            column=column,
            value=value,
        )
    return columns, serializable_rows


def _non_finite(rows: list) -> tuple[str, str] | None:
    """The first column holding Infinity or NaN anywhere in its value, and which it is."""
    import math

    def _in(value: Any) -> str | None:
        if isinstance(value, float) and not math.isfinite(value):
            return "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")
        if isinstance(value, dict):
            return next((v for v in map(_in, value.values()) if v is not None), None)
        if isinstance(value, (list, tuple)):
            return next((v for v in map(_in, value) if v is not None), None)
        return None

    for row in rows:
        for column, value in row.items() if isinstance(row, dict) else enumerate(row):
            held = _in(value)
            if held is not None:
                return str(column), held
    return None


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
        stats = statement_stats("cypher", len(serializable_rows), physical_sql, t0)
        if stats is not None:
            content["provisa_stats"] = stats
    return content


def statement_stats(field: str, rows: int, physical_sql: str, t0: float) -> dict | None:
    """A graph statement's provisa_stats (X-Provisa-Stats: true): its execution since ``t0``
    recorded as one entry on the federation engine, beside what the pipeline recorded. Shared by
    the Cypher and SPARQL endpoints."""
    from provisa.api.app import state  # noqa: PLC0415

    _qs_mod.record(
        field=field,
        source=state.federation_engine.name,
        strategy="federated",
        elapsed_ms=(_time.perf_counter() - t0) * 1000,
        rows=rows,
        physical_sql=physical_sql,
    )
    qs = _qs_mod.current()
    return qs.to_dict() if qs is not None else None


async def _each_statement(
    body: CypherRequest, request: Request, x_provisa_stats: str | None
) -> Response:
    """The statements of ``body.query``, split as Neo4j Browser splits them
    (``split_cypher_statements``), each run on its own by this route -- its own governed run, in
    order -- and answered as it alone would be: ``{"results": [{statement, status, body}]}``. A
    statement that fails is its entry's error, with the status and body the app answers it with,
    and the statements after it still run."""
    import json as _json

    from provisa.api.error_answer import error_answer
    from provisa.cypher.parser import split_cypher_statements

    results: list[dict[str, Any]] = []
    for statement in split_cypher_statements(body.query):
        one = CypherRequest(query=statement, params=body.params)
        try:
            answered = await cypher_query(one, request, None, x_provisa_stats)
        except (
            Exception
        ) as exc:  # allow-ble: answered as this statement's error, by the app's own handler for it
            results.append({"statement": statement, **await error_answer(request, exc)})
            continue
        results.append(
            {
                "statement": statement,
                "status": answered.status_code,
                "body": _json.loads(bytes(answered.body)),
            }
        )
    return JSONResponse(content={"results": results})


@router.post("/data/cypher")
async def cypher_query(  # REQ-345, REQ-346, REQ-347, REQ-349, REQ-350, REQ-351, REQ-352
    body: CypherRequest,
    request: Request,
    query_id: str | None = Query(None),
    x_provisa_stats: str | None = Header(None),
) -> Response:
    """Execute a Cypher read or write query and return typed rows or affected_rows."""
    from provisa.api.app import state

    if body.results == "each":
        return await _each_statement(body, request, x_provisa_stats)

    if query_id:
        return cypher_error(
            410,
            "cypher.query_id_removed",
            "execute-by-approved-query-id is removed; submit the Cypher query "
            "directly — access is governed by table/view and relationship rights",
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
            return cypher_error(
                503, "data.no_schema_for_role", f"No schema for role '{_role_id}'", role_id=_role_id
            )
        _label_map = _build_label_map(_ctx, _role_id, state)
        try:
            # The request's parameters are bound into the statement (``$name`` → ``$k``), so the
            # admission's new-row check and the source see the values, never their names.
            _write_sql, _write_params = bind_write_params(
                _WT(_label_map).translate(_write_ast), body.params
            )
        except (_CWPE, CypherParamError) as exc:
            return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))

        # ONE write path: the translated statement goes through the pipeline every other surface's
        # write goes through (Bolt's Cypher writes included) — its admission (the write right and
        # the role's write grant, REQ-2074), its lowering to the source's own
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
            return cypher_error(403, "cypher.permission_denied", str(exc), reason=str(exc))
        except (ApiError, ReadRefused):
            raise  # REQ-1350: a refusal by name is answered by the app's handler for it
        except Exception as exc:  # allow-ble: request boundary — a source or driver error of any type is this write's outcome, answered as an error response
            return cypher_error(500, "cypher.write_failed", f"Write failed: {exc}", error=str(exc))
        affected = _result.rowcount if _result.rowcount is not None else len(_result.rows)

        # The steps after a write (cache, views, change events, sinks, hot copy) ran at the
        # pipeline's terminal (pgwire._pipeline._after_write), as on every surface.
        return JSONResponse(content={"affected_rows": affected, "type": "cypher"})

    try:
        from provisa.cypher.parser import parse_cypher, CypherParseError
        from provisa.gql.translator import cypher_to_sql
        from provisa.gql.graph_rewriter import apply_graph_rewrites
        from provisa.cypher.params import collect_param_names, bind_params, CypherParamError
        from provisa.gql.assembler import assemble_rows, to_serializable
        from provisa.compiler.sql_rewrite import make_semantic_sql
    except Exception as exc:
        log.exception("Cypher imports failed")
        return cypher_error(500, "cypher.import_failed", f"Import failed: {exc}", error=str(exc))

    role_id = _resolve_role_id(request, state)
    surface = role_surface(state, role_id)  # one snapshot: context and row filters of one build
    if surface is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )
    ctx = surface.context

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
        from provisa.gql.translator_types import CypherMistake

        try:
            ast = parse_cypher(query_text, label_map)
        except CypherMistake as exc:
            return mistake_error(exc)
        except CypherParseError as exc:
            return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))

        # Validate and bind params
        param_names = collect_param_names(query_text)
        try:
            bind_params(param_names, body.params)
        except CypherParamError as exc:
            return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))

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
        _sql_result = _build_sql_from_ast(
            ast, label_map, body, cypher_to_sql, apply_graph_rewrites, role_id, state
        )
        if isinstance(_sql_result, Response):
            return _sql_result
        sql_str, ordered_params, graph_vars = _sql_result

        # Stage 3: Semantic conversion + access validation (transport responsibility)
        semantic_sql = make_semantic_sql(sql_str, ctx)
        # REQ-2168: a command composed in the statement runs now, as the role, and its rows take
        # its place. Its argument values are in the statement, so it is never kept.
        from provisa.api.data.cypher_commands import calls_commands, put_command_rows

        composed = calls_commands(semantic_sql)
        if composed:
            semantic_sql = await put_command_rows(semantic_sql, role_id, state)
        _refusal = access_violations(semantic_sql, surface, role_id, state)
        if _refusal is not None:
            return _refusal

        translation = CypherTranslation(
            semantic_sql=semantic_sql,
            ordered_params=tuple(ordered_params),
            param_names=tuple(param_names),
            graph_vars=graph_vars,
        )
        if not composed:
            kept.record(translation)

    semantic_sql, graph_vars = translation.semantic_sql, translation.graph_vars
    from provisa.api.rest.cypher_drawing import graph_columns

    # A read that returns nodes, relationships or paths is drawn: answered inline, never delivered
    # to a file -- the redirect threshold is a rule for rows. The row limits apply as to any read.
    drawn = graph_columns(graph_vars)
    try:
        resolved_params = translation.bind(body.params)
    except CypherParamError as exc:
        return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))

    # Stage 4: Pipeline (governance + routing)
    from provisa.api.redirect_headers import delivery_from_headers

    # REQ-1194: a redirect the request forces (X-Provisa-Redirect*), landed by the pipeline's
    # materialize stage; else REQ-1224's threshold decides (a buffered transport), when the
    # operator has enabled it. A header that cannot be read is refused (400) here.
    delivery = delivery_from_headers(request.headers, role_id)
    # REQ-544: the request's `// @provisa cache` opt-in, its x-provisa-cache headers and its
    # Cache-Control header; two that disagree are refused (400), naming them.
    try:
        cache_hint = request_cache_hint(cache_hint_for("cypher", body.query), request.headers)
    except ValueError as exc:
        return cypher_error(400, "cypher.refused", str(exc), reason=str(exc))
    plan = await _governed_plan(
        semantic_sql,
        role_id,
        exec_params=resolved_params or None,
        deliver=delivery,
        # A drawing is never delivered to a file: the redirect threshold is a rule for rows.
        buffered=not drawn,
        cache_hint=cache_hint,
        # REQ-030: the statement's own `// @provisa route=...` comment.
        steward_hint=extract_directives_from_cypher_comments(body.query).steward_hint,
        # REQ-1897: an opted-in read is looked up in the response cache before it is routed.
        serve_cached=True,
        sdl_joins=False,
    )
    if isinstance(plan, Response):
        return plan

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
    from provisa.gql.assembler import take_cut

    rows = take_cut(_dict_rows(_executed), plan.warnings)
    physical_sql = plan.physical_sql or plan.exec_sql or ""

    # Assemble & serialize
    _ser_result = _serialize_rows(rows, graph_vars, assemble_rows, to_serializable)
    if isinstance(_ser_result, Response):
        return _ser_result
    columns, serializable_rows = _ser_result

    # Register nodes and relationships, replacing composite string IDs with stable integers
    from provisa.gql.assembler import register_node_ids, register_rel_ids

    await register_node_ids(serializable_rows, state.model_db)
    await register_rel_ids(serializable_rows, state.model_db)

    content = _build_stats_content(columns, serializable_rows, physical_sql, stats_enabled, _t0)
    content["type"] = "cypher"
    content["graph_columns"] = drawn
    # REQ-536: HIT (with the entry's age) when the rows came from the response cache, else MISS.
    return JSONResponse(content=content, headers=build_cache_headers(_executed.cache_entry))


@router.get("/data/graph-schema")
async def graph_schema(request: Request) -> JSONResponse:  # REQ-392, REQ-398
    """Return node labels and relationship types for the current role."""
    from provisa.api.app import state

    from provisa.api.role_surface import role_surface

    role_id = _resolve_role_id(request, state)
    # One surface, taken once: its context and its root fields come from the same build.
    surface = role_surface(state, role_id)
    if surface is None or surface.context is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )
    ctx = surface.context

    label_map = _build_label_map(ctx, role_id, state)
    all_tables: list[dict] = getattr(state, "schema_build_cache", {}).get("tables", [])
    col_types: dict = getattr(state, "schema_build_cache", {}).get("column_types", {})
    from provisa.gql.label_map import _table_label_from_table_name

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
                    # The registered table the label reads, so a client can link a node to it.
                    "table_id": n.table_id,
                    "properties": list(n.properties.keys()),
                    # REQ-392: singular primary-key column name (first designated PK), or null.
                    "pk": n.property_of(n.pk_columns[0]) if n.pk_columns else None,
                    "pk_columns": [n.property_of(c) for c in n.pk_columns],
                    "id_column": n.property_of(n.id_column),
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
            # REQ-2128: every table's root query field in this role's GraphQL schema and the name
            # it is registered under, so a client queries a table by the field the compiler named
            # it rather than re-deriving the name. Every table the role has a root field for,
            # including the junctions a relationship traverses, which are no node label.
            "root_fields": [
                {
                    "domain_id": info["domain_id"],
                    "table_name": info["table_name"],
                    "graphql_field": field,
                }
                for field, info in surface.table_path_map.items()
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
    from provisa.gql.translator import cypher_to_sql
    from provisa.gql.graph_rewriter import apply_graph_rewrites
    from provisa.compiler.sql_rewrite import make_semantic_sql
    from provisa.pgwire._pipeline import _govern_and_route_compiled

    role_id = _resolve_role_id(request, state)
    ctx = state.contexts.get(role_id)
    if ctx is None:
        return cypher_error(
            503, "data.no_schema_for_role", f"No schema for role '{role_id}'", role_id=role_id
        )

    domains_param = request.query_params.get("domains", "")
    filtered_domains: set[str] = (
        set(d for d in domains_param.split(",") if d) if domains_param else set()
    )

    label_map = _build_label_map(ctx, role_id, state)

    async def _run_count(cypher: str) -> dict[str, Any]:
        """``{"count": n}``, or ``{"count": None, "error": why}`` when the statement could not be
        built or run: an uncounted label is said with its reason, never counted as 0 rows."""
        ast = parse_cypher(cypher)
        body = CypherRequest(query=cypher, params={})
        result = _build_sql_from_ast(
            ast, label_map, body, cypher_to_sql, apply_graph_rewrites, role_id, state
        )
        if isinstance(result, Response):
            return _uncounted(cypher, result)
        sql_str, _, _ = result
        semantic_sql = make_semantic_sql(sql_str, ctx)
        try:
            plan = await _govern_and_route_compiled(
                semantic_sql, role_id, exec_params=None, cache_hint=NO_CACHE_HINT, sdl_joins=False
            )
        except ParameterRequired as exc:
            # A parameterized table: a count gives no parameter, so the read is refused -- said,
            # with that refusal's reason.
            log.warning("graph-counts: %s not counted: %s", cypher, exc)
            return {"count": None, "error": str(exc)}
        executed = await _run_plan(plan, state)
        if isinstance(executed, Response):
            # A label the engine cannot count (its catalog is not attached on this engine): said,
            # with the engine's reason.
            return _uncounted(cypher, executed)
        rows = _dict_rows(executed)
        return {"count": int(rows[0]["cnt"]) if rows else 0}

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
    # Each label's entry is its count, or null with the reason the engine could not count it. A
    # total is a total only when every label in it was counted: with any uncounted, it is null, and
    # the entries say which and why.
    label_counts: dict[str, dict[str, Any]] = {}
    for lbl in node_labels:
        label_counts[lbl] = await _run_count(f"MATCH (n:{lbl}) RETURN count(n) AS cnt")
    rel_counts: dict[str, dict[str, Any]] = {}
    for rt in rel_types:
        rel_counts[rt] = await _run_count(f"MATCH ()-[r:{rt}]->() RETURN count(r) AS cnt")

    return JSONResponse(
        content={
            "node_count": _total(label_counts),
            "rel_count": _total(rel_counts),
            "label_counts": label_counts,
            "rel_counts": rel_counts,
        }
    )


def _uncounted(cypher: str, refused: Response) -> dict[str, Any]:
    """A count statement's refusal, as its entry: no count, and the reason it gave."""
    import json as _json

    why = _json.loads(bytes(refused.body))["detail"]
    log.warning("graph-counts: %s not counted (%s): %s", cypher, refused.status_code, why)
    return {"count": None, "error": why}


def _total(counts: dict[str, dict[str, Any]]) -> int | None:
    """The sum of the counts, or None when any entry has none."""
    if any(c["count"] is None for c in counts.values()):
        return None
    return sum(c["count"] for c in counts.values())


# Register the graph-tools routes (/impute-relationships, /neo4j-export) on `router`.
from provisa.api.rest import graph_tools_router as _graph_tools_router  # noqa: E402,F401

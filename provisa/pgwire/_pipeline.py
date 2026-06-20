# Copyright (c) 2026 Kenneth Stott
# Canary: c3d4e5f6-a7b8-9012-cdef-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Execute SQL through the full Provisa governance pipeline.

Mirrors the steps in endpoint_dev.sql_endpoint but without HTTP/FastAPI.
Called from pgwire handler threads via asyncio.run_coroutine_threadsafe.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from provisa.executor.trino import QueryResult

log = logging.getLogger(__name__)


@dataclass
class _Plan:
    route: object  # transpiler.router.Route
    sql: str
    source_id: str
    dialect: str
    exec_params: list | None = field(default=None)
    # Trino-specific: catalog-qualified postgres SQL (pre-transpile, for NF args extraction)
    exec_sql: str | None = field(default=None)
    # Trino-specific: fully qualified SQL ready to run
    trino_sql: str | None = field(default=None)


async def _govern_and_route(sql: str, role_id: str) -> _Plan:
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment, extract_relationship_guard_comment
    from provisa.compiler.sql_gen import qualify_with_catalogs
    from provisa.compiler.stage2 import apply_governance, build_governance_context, extract_sources
    from provisa.compiler.sql_validator import validate_sql
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    if role:
        from provisa.security.rights import Capability, has_capability

        if not has_capability(role, Capability.AD_HOC_QUERY):
            raise PermissionError("Role lacks ad_hoc_query capability")

    raw_sql, embedded_params = extract_params_comment(sql)
    raw_sql, sql_opts_out = extract_relationship_guard_comment(raw_sql)

    normalized_sql = raw_sql
    try:
        sqlglot.parse_one(normalized_sql, read="postgres")
    except Exception as exc:
        raise ValueError(f"SQL parse error: {exc}") from exc

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role,
    )

    _role_guard = (role or {}).get("relationship_guard", True)
    _bypass_guard = (not _role_guard) and sql_opts_out
    violations = validate_sql(
        normalized_sql,
        ctx,
        gov_ctx,
        role or {},
        getattr(state, "tables", []),
        bypass_relationship_guard=_bypass_guard,
        bypass_uncovered_relationships=True,
    )

    _role_domain_access = (role or {}).get("domain_access") or []
    if "*" not in _role_domain_access:
        try:
            parsed_tree = sqlglot.parse_one(normalized_sql, read="postgres")
            for tbl in parsed_tree.find_all(exp.Table):
                tbl_name = tbl.name
                tbl_db = tbl.db
                full_key = f"{tbl_db}.{tbl_name}" if tbl_db else tbl_name
                if full_key not in gov_ctx.table_map and tbl_name not in gov_ctx.table_map:
                    from provisa.compiler.sql_validator import ValidationViolation

                    violations.append(
                        ValidationViolation(
                            "V000", f"Table {full_key!r} not accessible for role {role_id!r}"
                        )
                    )
        except Exception:
            pass

    if violations:
        msgs = "; ".join(f"[{v.code}] {v.message}" for v in violations)
        raise PermissionError(msgs)

    # REQ-272: apply_governance enforces full Stage-2 governance on this SQL path — RLS,
    # masking, visibility, and the role row-cap ceiling (gov_ctx carries the role, so
    # resolve_row_cap applies). Statistical sampling is the GraphQL `sample` arg → TABLESAMPLE,
    # a query-construction feature with no equivalent for already-formed raw SQL, so it is N/A
    # here; there is no ungoverned access path.
    governed_semantic = apply_governance(normalized_sql, gov_ctx)

    sources = extract_sources(governed_semantic, gov_ctx, ctx)
    _default_source = next(
        (sid for sid, t in state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
        next(iter(state.source_pools.source_ids), "pg"),
    )
    decision = decide_route(
        sources=sources or {_default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract="->>" in governed_semantic,
        source_dsns=getattr(state, "source_dsns", None),
    )

    exec_params = embedded_params or None

    if decision.route == Route.TRINO:
        from provisa.api.data.endpoint import _materialize_api_to_trino_cache
        from provisa.cache.hot_tables import build_values_cte_sql
        from provisa.api_source.trino_cache import rewrite_all_from_cache

        _qualified = qualify_with_catalogs(governed_semantic, ctx)
        _rewrites, _values_ctes = await _materialize_api_to_trino_cache(_qualified, state)
        for _tn, _entry in _values_ctes.items():
            _qualified = build_values_cte_sql(_qualified, _tn, _entry)
        if _rewrites:
            _qualified = rewrite_all_from_cache(_qualified, _rewrites)
        _known_cats_pgwire = set(getattr(state, "source_catalogs", {}).values()) | {
            "iceberg",
            "otel",
            "results",
        }
        from provisa.api.data.endpoint import _lookup_gql_remote_table as _lookup_gql
        import sqlglot as _sg
        import sqlglot.expressions as _exp

        try:
            _tree = _sg.parse_one(_qualified, dialect="postgres")
            for _tbl in _tree.find_all(_exp.Table):
                if _tbl.catalog and _tbl.catalog not in _known_cats_pgwire:
                    _, _gql_tbl = _lookup_gql(state, _tbl.name)
                    if _gql_tbl is not None and _gql_tbl.get("required_args"):
                        _req = [a["name"] for a in _gql_tbl["required_args"]]
                        raise ValueError(
                            f"Table {_tbl.name!r} requires filter(s) {_req} — "
                            "add a WHERE clause with the required parameter(s)"
                        )
                    raise ValueError(
                        f"Table {_tbl.name!r} references unknown catalog {_tbl.catalog!r} — "
                        "GQL remote fetch failed or source not loaded"
                    )
        except ValueError:
            raise
        except Exception:
            pass
        trino_sql = transpile_to_trino(_qualified)
        return _Plan(
            route=Route.TRINO,
            sql=governed_semantic,
            source_id=_default_source,
            dialect="trino",
            exec_params=exec_params,
            trino_sql=trino_sql,
        )
    else:
        dialect = decision.dialect or "postgres"
        sql_to_run = transpile(governed_semantic, dialect)
        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            source_id=decision.source_id or _default_source,
            dialect=dialect,
            exec_params=exec_params,
        )


async def _execute_plan(plan: _Plan, state: Any | None = None) -> QueryResult:
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.router import Route

    if plan.route == Route.TRINO:
        if state.trino_conn is None:
            raise RuntimeError("Trino connection not available")
        assert plan.trino_sql is not None
        _trino_conn = state.trino_conn
        _trino_sql = plan.trino_sql
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: execute_trino(_trino_conn, _trino_sql, params=plan.exec_params),
        )
    elif plan.source_id == "provisa-admin" or not state.source_pools.has(plan.source_id):
        # Admin-owned tables (meta.*) live in the provisa pg_pool, not source_pools.
        pg_pool = state.pg_pool
        if pg_pool is None:
            raise RuntimeError("Admin pg_pool not available")
        async with pg_pool.acquire() as _conn:
            _conn = _conn  # type: ignore[assignment]
            _rows = await _conn.fetch(plan.sql)
            if _rows:
                col_names = list(_rows[0].keys())
                rows = [tuple(r) for r in _rows]
            else:
                # Execute again for column names via a describe-style query
                stmt = await _conn.prepare(plan.sql)
                col_names = [a.name for a in stmt.get_attributes()]
                rows = []
        result = QueryResult(rows=rows, column_names=col_names)
    else:
        result = await execute_direct(
            state.source_pools,
            plan.source_id,
            plan.sql,
            plan.exec_params,
        )
    return result


async def _govern_and_route_compiled(
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
) -> _Plan:
    """Governance + routing for already-physical SQL.

    Used by GQL and Cypher transport paths after language-specific compilation.
    No AD_HOC_QUERY capability check, no SQL validation.
    """
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.api.data.endpoint import _materialize_api_to_trino_cache
    from provisa.api_source.trino_cache import rewrite_all_from_cache
    from provisa.cache.hot_tables import build_values_cte_sql
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import (
        rewrite_semantic_to_physical,
        rewrite_semantic_to_trino_physical,
    )
    from provisa.compiler.stage2 import apply_governance, build_governance_context, extract_sources
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=state.roles.get(role_id),
    )

    governed_sql = apply_governance(sql, gov_ctx)

    sources = extract_sources(governed_sql, gov_ctx, ctx)
    _default_source = next(
        (sid for sid, t in state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
        next(iter(state.source_pools.source_ids), "pg"),
    )
    decision = decide_route(
        sources=sources or {_default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        source_dsns=getattr(state, "source_dsns", None),
    )

    if decision.route == Route.TRINO:
        _exec_sql = rewrite_semantic_to_trino_physical(governed_sql, ctx)
        _view_map = getattr(state, "view_sql_map", None)
        if _view_map:
            from provisa.compiler.view_expand import expand_view_refs

            _exec_sql = expand_view_refs(_exec_sql, _view_map)
        _exec_sql_base = _exec_sql
        _rewrites, _values_ctes = await _materialize_api_to_trino_cache(_exec_sql, state)
        for _tn, _entry in _values_ctes.items():
            _exec_sql = build_values_cte_sql(_exec_sql, _tn, _entry)
        if _rewrites:
            _exec_sql = rewrite_all_from_cache(_exec_sql, _rewrites)
        _known_cats = set(getattr(state, "source_catalogs", {}).values()) | {
            "iceberg",
            "otel",
            "results",
        }
        import sqlglot as _sg2
        import sqlglot.expressions as _exp2
        from provisa.api.data.endpoint import _lookup_gql_remote_table as _lookup_gql2

        try:
            _tree2 = _sg2.parse_one(_exec_sql, dialect="postgres")
            for _tbl2 in _tree2.find_all(_exp2.Table):
                if _tbl2.catalog and _tbl2.catalog not in _known_cats:
                    _, _gql_tbl2 = _lookup_gql2(state, _tbl2.name)
                    if _gql_tbl2 is not None and _gql_tbl2.get("required_args"):
                        _req2 = [a["name"] for a in _gql_tbl2["required_args"]]
                        raise ValueError(
                            f"Table {_tbl2.name!r} requires filter(s) {_req2} — "
                            "add a WHERE clause with the required parameter(s)"
                        )
        except ValueError:
            raise
        except Exception:
            pass
        trino_sql = transpile_to_trino(_exec_sql)
        return _Plan(
            route=Route.TRINO,
            sql=governed_sql,
            source_id=_default_source,
            dialect="trino",
            exec_params=exec_params,
            exec_sql=_exec_sql_base,
            trino_sql=trino_sql,
        )
    else:
        dialect = decision.dialect or "postgres"
        # Rewrite semantic names to physical (no catalog) before transpile.
        # Input sql is semantic; direct drivers don't understand domain-qualified refs.
        physical_sql = rewrite_semantic_to_physical(governed_sql, ctx)
        sql_to_run = transpile(physical_sql, dialect)
        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            exec_sql=physical_sql,
            source_id=decision.source_id or _default_source,
            dialect=dialect,
            exec_params=exec_params,
        )


async def plan_pgwire_sql(sql: str, role_id: str) -> _Plan:
    return await _govern_and_route(sql, role_id)


async def execute_pgwire_sql(sql: str, role_id: str) -> QueryResult:
    """Run *sql* through governance and return rows + column names.

    Raises:
        PermissionError  – role not found or access violation
        ValueError       – SQL parse / validation error
        RuntimeError     – routing / execution error
    """
    plan = await _govern_and_route(sql, role_id)
    return await _execute_plan(plan)

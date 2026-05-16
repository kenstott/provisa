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

from provisa.executor.trino import QueryResult

log = logging.getLogger(__name__)


@dataclass
class _Plan:
    route: object  # transpiler.router.Route
    sql: str
    source_id: str
    dialect: str
    exec_params: list | None = field(default=None)
    # Trino-specific: fully qualified SQL ready to run
    trino_sql: str | None = field(default=None)


async def _govern_and_route(sql: str, role_id: str) -> _Plan:
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment, extract_relationship_guard_comment
    from provisa.compiler.sql_gen import qualify_with_catalogs, rewrite_semantic_to_physical
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

    normalized_sql = rewrite_semantic_to_physical(raw_sql, ctx)
    try:
        sqlglot.parse_one(normalized_sql, read="postgres")
    except Exception:
        normalized_sql = raw_sql

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
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
        _rewrites, _values_ctes = await _materialize_api_to_trino_cache(_qualified, None, state)
        for _tn, _entry in _values_ctes.items():
            _qualified = build_values_cte_sql(_qualified, _tn, _entry)
        if _rewrites:
            _qualified = rewrite_all_from_cache(_qualified, _rewrites)
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


async def _execute_plan(plan: _Plan) -> QueryResult:
    from provisa.api.app import state
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.router import Route

    if plan.route == Route.TRINO:
        if state.trino_conn is None:
            raise RuntimeError("Trino connection not available")
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: execute_trino(state.trino_conn, plan.trino_sql, params=plan.exec_params),
        )
    else:
        result = await execute_direct(
            state.source_pools,
            plan.source_id,
            plan.sql,
            plan.exec_params,
        )
    return result


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

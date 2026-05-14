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

from provisa.executor.trino import QueryResult

log = logging.getLogger(__name__)


async def execute_pgwire_sql(sql: str, role_id: str) -> QueryResult:
    """Run *sql* through governance and return rows + column names.

    Raises:
        PermissionError  – role not found or access violation
        ValueError       – SQL parse / validation error
        RuntimeError     – routing / execution error
    """
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment
    from provisa.compiler.sql_gen import qualify_with_catalogs, rewrite_semantic_to_physical
    from provisa.compiler.stage2 import apply_governance, build_governance_context, extract_sources
    from provisa.compiler.sql_validator import validate_sql
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino
    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    raw_sql, embedded_params = extract_params_comment(sql)

    # Step 1: normalize table refs
    normalized_sql = rewrite_semantic_to_physical(raw_sql, ctx)
    try:
        sqlglot.parse_one(normalized_sql, read="postgres")
    except Exception:
        normalized_sql = raw_sql

    # Step 2: governance context
    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
    )

    # Step 3: validate
    violations = validate_sql(
        normalized_sql, ctx, gov_ctx, role or {}, getattr(state, "tables", [])
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

    # Step 4: apply governance
    governed_semantic = apply_governance(normalized_sql, gov_ctx)

    # Step 5: route
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

    governed_physical = governed_semantic
    exec_params = embedded_params or None

    # Step 6: execute
    if decision.route == Route.TRINO:
        from provisa.api.data.endpoint import _materialize_api_to_trino_cache
        from provisa.cache.hot_tables import build_values_cte_sql
        from provisa.api_source.trino_cache import rewrite_all_from_cache

        _qualified = qualify_with_catalogs(governed_physical, ctx)
        _rewrites, _values_ctes = await _materialize_api_to_trino_cache(_qualified, None, state)
        for _tn, _entry in _values_ctes.items():
            _qualified = build_values_cte_sql(_qualified, _tn, _entry)
        if _rewrites:
            _qualified = rewrite_all_from_cache(_qualified, _rewrites)
        sql_to_run = transpile_to_trino(_qualified)
        if state.trino_conn is None:
            raise RuntimeError("Trino connection not available")
        _trino_conn = state.trino_conn
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: execute_trino(_trino_conn, sql_to_run, params=exec_params)
        )
    else:
        dialect = decision.dialect or "postgres"
        sql_to_run = transpile(governed_physical, dialect)
        result = await execute_direct(
            state.source_pools,
            decision.source_id or _default_source,
            sql_to_run,
            exec_params,
        )

    return result

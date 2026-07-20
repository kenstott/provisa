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

# Requirements: REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267, REQ-272

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from provisa.executor.result import QueryResult

log = logging.getLogger(__name__)

# RLS session-variable predicate: current_setting('provisa.<var>' [, true]).
_CURRENT_SETTING_RE = re.compile(
    r"current_setting\(\s*'provisa\.([A-Za-z0-9_]+)'\s*(?:,\s*true\s*)?\)",
    re.IGNORECASE,
)


def _resolve_session_settings(sql: str, session_vars: dict[str, str]) -> str:
    """Resolve ``current_setting('provisa.<var>')`` to a SQL literal for engines
    that lack the function (the federation engine). A missing var becomes NULL —
    the RLS predicate then matches no rows, a safe deny-by-default. PostgreSQL
    keeps native ``current_setting`` (fed by ``SET LOCAL``) and is untouched.
    """

    def _sub(m: re.Match) -> str:
        value = session_vars.get(m.group(1))
        return "NULL" if value is None else "'" + value.replace("'", "''") + "'"

    return _CURRENT_SETTING_RE.sub(_sub, sql)


@dataclass
class _Plan:
    route: object  # transpiler.router.Route
    sql: str
    source_id: str
    dialect: str
    exec_params: list | None = field(default=None)
    # the engine-specific: catalog-qualified postgres SQL (pre-transpile, for NF args extraction)
    exec_sql: str | None = field(default=None)
    # the engine-specific: fully qualified SQL ready to run
    physical_sql: str | None = field(default=None)
    # Per-query the engine session overrides (e.g. retry_policy=NONE to bypass FTE).
    session_hints: dict[str, str] | None = field(default=None)


# Connector types that don't support the engine fault-tolerant execution (FTE): their
# splits aren't replayable, so a query routed under retry-policy=TASK blocks
# forever on the exchange. Queries touching these run with retry_policy=NONE.
_NON_FTE_SOURCE_TYPES = frozenset({"kafka"})


async def _optimize_and_route(
    exec_sql: str, governed_sql: str, gov_ctx, ctx, state, *, nf_args=None, has_json_extract=False
):
    """REQ-863 post-governance optimization stage (may REMOVE sources) + routing on the reduced
    set — shared by both governed-SQL entrypoints so routing observes the optimized source set,
    not the pre-optimization one. ``exec_sql`` is the caller's already-lowered SQL (catalog-
    qualified semantic, or compiled catalog-physical); ``governed_sql`` is the pre-optimization
    governed semantic used for source extraction. Returns the optimized exec SQL, the route
    decision, the resolved default source, and whether optimization changed the SQL."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import rewrite_all_from_cache
    from provisa.cache.hot_tables import build_values_cte_sql
    from provisa.compiler.stage2 import extract_sources, reduce_sources_for_routing
    from provisa.transpiler.router import decide_route

    _rewrites, _values_ctes, _dropped = await _materialize_api_to_engine_cache(
        exec_sql, state, nf_args=nf_args
    )
    if _dropped:
        from provisa.compiler.nf_extractor import drop_union_branches_for_table

        for _dtn in _dropped:
            exec_sql = drop_union_branches_for_table(exec_sql, _dtn)
    for _tn, _entry in _values_ctes.items():
        exec_sql = build_values_cte_sql(exec_sql, _tn, _entry)
    if _rewrites:
        exec_sql = rewrite_all_from_cache(exec_sql, _rewrites)

    _inlined = set(_values_ctes) | set(_dropped)
    optimized = bool(_inlined or _rewrites)
    if optimized:
        sources = reduce_sources_for_routing(governed_sql, gov_ctx, ctx, _inlined)
    else:
        sources = extract_sources(governed_sql, gov_ctx, ctx)
    default_source = next(
        (sid for sid, t in state.source_types.items() if t in ("postgresql", "mysql", "sqlite")),
        next(iter(state.source_pools.source_ids), "pg"),
    )
    decision = decide_route(
        sources=sources or {default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract=has_json_extract,
        source_dsns=getattr(state, "source_dsns", None),
    )
    return exec_sql, decision, default_source, optimized, sources


def _reject_physical_source_refs(parsed: Any, state: Any) -> None:
    """Reject any physical source-catalog table reference — enforce the one accepted model.

    The catalog advertises exactly one reference form: the semantic ``domain.table``. A physical
    source catalog (e.g. ``"inquiries_sqlite"."default"."inquiries"``) is an internal lowering
    artifact exposed to no client; accepting it would run ungoverned against the raw source
    because RLS/masking bind to the semantic table, not the physical ref. A 3-part ref whose
    leading part is NOT a known source catalog (a client fully-qualifying with a virtual database
    name) is left alone.
    """
    import sqlglot.expressions as _exp

    source_catalogs = set(getattr(state, "source_catalogs", {}).values()) | {
        "iceberg",
        "otel",
        "results",
    }
    for tbl in parsed.find_all(_exp.Table):
        if tbl.catalog and tbl.catalog in source_catalogs:
            raise PermissionError(
                f"Invalid table reference {tbl.sql(dialect='postgres')!r}: physical source names "
                "are internal. Reference the semantic schema.table shown in the catalog."
            )


async def _localize_inline_commands(tree, role_id: str, state) -> bool:
    """REQ-1159: rewrite every inline command call in ``tree`` to a typed local relation, in place.

    Each command executes via the ONE shared governed executor (invoke_tracked_function) — its input
    governance (DEFINER/INVOKER) and I/O dataset contract are enforced there, identically to a direct
    call — so the outer statement only ever sees ordinary local relations. Returns True on any hit
    (the caller then forces engine execution). No-op when no command is composed in the statement."""
    commands = getattr(state, "tracked_functions", None)
    if not commands:
        return False
    from provisa.api.data.action_exec import invoke_tracked_function
    from provisa.executor.command_localize import localize_commands

    async def _run(name: str, args: dict) -> list[dict]:
        return await invoke_tracked_function(name, args, state, role_id)

    # normalized_sql is postgres downstream (then transpiled per route), so build the inline
    # relations in the postgres dialect for a faithful round-trip.
    return await localize_commands(tree, commands, _run, dialect="postgres")


async def _govern_and_route(
    sql: str, role_id: str, *, session_vars: dict[str, str] | None = None
) -> _Plan:  # REQ-262, REQ-263, REQ-264, REQ-266, REQ-267, REQ-272, REQ-1120, REQ-1159
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment, extract_relationship_guard_comment
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.compiler.sql_validator import validate_sql
    from provisa.transpiler.router import Route
    from provisa.transpiler.transpile import transpile

    if role_id not in state.contexts:
        raise PermissionError(f"No schema for role {role_id!r}")

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    role = state.roles.get(role_id)

    raw_sql, embedded_params = extract_params_comment(sql)
    raw_sql, sql_opts_out = extract_relationship_guard_comment(raw_sql)

    normalized_sql = raw_sql
    try:
        _parsed_input = sqlglot.parse_one(normalized_sql, read="postgres")
    except Exception as exc:
        raise ValueError(f"SQL parse error: {exc}") from exc

    # REQ-1159: localize any INLINE command call (a registered command composed within this statement
    # — joined/sub-queried) BEFORE governance/validation/routing. Each command runs via the shared
    # governed executor (its own input governance + I/O contract enforced there) and its call site is
    # replaced by a typed local relation, so the rest of the pipeline sees ordinary relations. A hit
    # forces local (engine) execution — an inline local relation cannot be pushed to a remote source.
    _localized = await _localize_inline_commands(_parsed_input, role_id, state)
    if _localized:
        normalized_sql = _parsed_input.sql(dialect="postgres")

    _reject_physical_source_refs(_parsed_input, state)

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role,
        relationships=getattr(state, "relationships", None),
    )

    from provisa.security.rights import Capability, has_capability

    _role_guard = (role or {}).get("relationship_guard", True)
    _bypass_guard = has_capability(role or {}, Capability.IGNORE_RELATIONSHIPS) or (
        (not _role_guard) and sql_opts_out
    )
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
        except Exception as exc:
            # SECURITY: never skip the domain-access check on a parse/lookup error — fail closed.
            raise PermissionError(
                f"Domain-access check could not be evaluated for role {role_id!r}: {exc}"
            ) from exc

    if violations:
        msgs = "; ".join(f"[{v.code}] {v.message}" for v in violations)
        raise PermissionError(msgs)

    # REQ-272: apply_governance enforces full Stage-2 governance on this SQL path — RLS,
    # masking, visibility, and the role row-cap ceiling (gov_ctx carries the role, so
    # resolve_row_cap applies). Statistical sampling is the GraphQL `sample` arg → TABLESAMPLE,
    # a query-construction feature with no equivalent for already-formed raw SQL, so it is N/A
    # here; there is no ungoverned access path.
    # REQ-863 pipeline order: governance → post-governance optimization → routing.
    governed_semantic = apply_governance(normalized_sql, gov_ctx)

    # REQ-1120: resolve RLS session predicates (current_setting('provisa.<var>')) to SQL
    # literals for transports whose caller supplies session vars out-of-band (e.g. the
    # airport Flight service, which has no SET LOCAL channel). A missing var becomes NULL,
    # the documented deny-by-default (_resolve_session_settings). Only applied when the
    # caller opts in by passing session_vars; None leaves native current_setting untouched.
    if session_vars is not None:
        governed_semantic = _resolve_session_settings(governed_semantic, session_vars)

    # REQ-863 pipeline order: governance → post-governance optimization → routing.
    # Lower the ONE accepted reference model — the semantic domain.table the catalog
    # advertises — to catalog-physical for the engine. rewrite_semantic_to_catalog_physical
    # is the same lowering the GQL/Cypher path uses (_govern_and_route_compiled); the raw-SQL
    # path previously used qualify_with_catalogs, which only re-qualified already-physical refs
    # and left a semantic ref like "pet_store"."inquiries" unresolved → "schema doesn't exist".
    from provisa.compiler.sql_rewrite import (
        normalize_table_refs,
        rewrite_semantic_to_catalog_physical,
    )

    # normalize_table_refs first (sqlglot parse-based): an UNQUOTED semantic ref like
    # `pet_store.inquiries` is invisible to the literal-match rewrite, so it must be
    # parsed, qualified and quoted before rewrite_semantic_to_catalog_physical can lower it.
    _qualified, decision, _default_source, _optimized, _sources = await _optimize_and_route(
        rewrite_semantic_to_catalog_physical(normalize_table_refs(governed_semantic, ctx), ctx),
        governed_semantic,
        gov_ctx,
        ctx,
        state,
        has_json_extract="->>" in governed_semantic,
    )

    exec_params = embedded_params or None

    # REQ-1159: a localized statement carries an inline local relation as a VALUES list, which rides
    # along on whichever route the router picks — DIRECT inlines the VALUES into the single source's
    # SQL (the source executes it), and a genuinely cross-source statement is detected and routed to
    # the engine by decide_route as usual. So the localizer does NOT force a route; it lets routing
    # decide, which keeps a single-source composed query on the source instead of the org store.
    if decision.route == Route.ENGINE:
        _known_cats_pgwire = set(getattr(state, "source_catalogs", {}).values()) | {
            "iceberg",
            "otel",
            "results",
        }
        from provisa.api.data.materialization import _lookup_gql_remote_table as _lookup_gql
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
        physical_sql = state.federation_engine.transpile_physical(_qualified)
        return _Plan(
            route=Route.ENGINE,
            sql=governed_semantic,
            source_id=_default_source,
            dialect=state.federation_engine.dialect,
            exec_params=exec_params,
            physical_sql=physical_sql,
        )
    else:
        dialect = decision.dialect or "postgres"
        # Direct route lowers the OPTIMIZED SQL when the optimization stage changed it (REQ-863),
        # carrying any inlined VALUES CTE onto the direct path; else the unchanged fast path.
        if _optimized:
            from provisa.compiler.sql_rewrite import strip_catalog

            sql_to_run = transpile(strip_catalog(_qualified), dialect)
        else:
            # Lower the semantic model to physical schema.table for the native driver — same as
            # _govern_and_route_compiled's DIRECT branch. Passing governed_semantic verbatim sent
            # an unresolved semantic ref (e.g. "pet_store"."inquiries") to the source.
            from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

            sql_to_run = transpile(rewrite_semantic_to_physical(governed_semantic, ctx), dialect)
        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            source_id=decision.source_id or _default_source,
            dialect=dialect,
            exec_params=exec_params,
        )


async def _execute_plan(plan: _Plan, state: Any | None = None) -> QueryResult:  # REQ-027, REQ-028
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.transpiler.router import Route

    engine = state.federation_engine

    if plan.route == Route.ENGINE:
        assert plan.physical_sql is not None
        # ENGINE terminal (REQ-825): hand the federated SQL to the bound engine.
        result = await engine.execute_engine(
            plan.physical_sql, params=plan.exec_params, session_hints=plan.session_hints
        )
    elif plan.source_id == "provisa-admin" or not state.source_pools.has(plan.source_id):
        # Admin-owned tables (meta.*) live in the provisa tenant_db, not source_pools.
        tenant_db = state.tenant_db
        if tenant_db is None:
            raise RuntimeError("Admin tenant_db not available")
        async with tenant_db.acquire() as _conn:
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
        # DIRECT terminal (REQ-825): single reachable source on its native driver.
        result = await engine.execute_native(
            state.source_pools,
            plan.source_id,
            plan.sql,
            plan.exec_params,
        )
    return result


async def _govern_and_route_compiled(  # REQ-262, REQ-263, REQ-265, REQ-266  # pyright: ignore[reportUnusedFunction]
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
    api_args: dict | None = None,
) -> _Plan:
    """Governance + routing for already-physical SQL.

    Used by GQL and Cypher transport paths after language-specific compilation.
    No AD_HOC_QUERY capability check, no SQL validation.
    """
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_rewrite import (
        rewrite_semantic_to_catalog_physical,
        rewrite_semantic_to_physical,
    )
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.transpiler.router import Route
    from provisa.transpiler.transpile import transpile

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
        relationships=getattr(state, "relationships", None),
    )

    # REQ-863 pipeline order: governance → post-governance optimization → routing.
    governed_sql = apply_governance(sql, gov_ctx)

    # Post-governance optimization stage (may REMOVE sources): lower to catalog-physical, then
    # inline hot/API tables as VALUES CTEs, prune unreachable union branches, and rewrite cached
    # tables. This MUST complete before extract_sources/decide_route so routing observes the
    # reduced source set (a query whose second source is fully inlined collapses to DIRECT).
    _exec_sql = rewrite_semantic_to_catalog_physical(governed_sql, ctx)
    _view_map = getattr(state, "view_sql_map", None)
    if _view_map:
        from provisa.compiler.view_expand import expand_view_refs

        _exec_sql = expand_view_refs(_exec_sql, _view_map)
    from provisa.compiler.nf_extractor import extract_nf_args

    _exec_sql, _nf_clean_params, _extracted_nf = extract_nf_args(_exec_sql, exec_params or [])
    exec_params = _nf_clean_params if _nf_clean_params != (exec_params or []) else exec_params
    _nf_args = {**(api_args or {}), **(_extracted_nf or {})} or None
    # Route on the OUTPUT of the optimization stage (REQ-863): sources whose every referenced
    # table was inlined/pruned drop out of the routing set.
    _exec_sql, decision, _default_source, _optimized, sources = await _optimize_and_route(
        _exec_sql, governed_sql, gov_ctx, ctx, state, nf_args=_nf_args
    )

    if decision.route == Route.ENGINE:
        _known_cats = set(getattr(state, "source_catalogs", {}).values()) | {
            "iceberg",
            "otel",
            "results",
        }
        import sqlglot as _sg2
        import sqlglot.expressions as _exp2
        from provisa.api.data.materialization import _lookup_gql_remote_table as _lookup_gql2

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
        physical_sql = state.federation_engine.transpile_physical(_exec_sql)
        # REQ-041/402: RLS is added to the governed semantic SQL as a
        # current_setting('provisa.<var>') predicate; PostgreSQL resolves it
        # natively (SET LOCAL) but the federation engine has no such function.
        # Resolve it to the session's literal value here at planning so it works
        # regardless of the requesting query language.
        _session_vars = (state.roles.get(role_id) or {}).get("session_vars", {})
        physical_sql = _resolve_session_settings(physical_sql, _session_vars)
        # Bypass FTE for queries touching non-replayable connectors (kafka), whose
        # splits stall the fault-tolerant exchange (blocks forever, 0 drivers).
        _hints = (
            {"retry_policy": "NONE"}
            if any(state.source_types.get(s) in _NON_FTE_SOURCE_TYPES for s in (sources or ()))
            else None
        )
        return _Plan(
            route=Route.ENGINE,
            sql=governed_sql,
            source_id=_default_source,
            dialect=state.federation_engine.dialect,
            exec_params=exec_params,
            exec_sql=_exec_sql,
            physical_sql=physical_sql,
            session_hints=_hints,
        )
    else:
        dialect = decision.dialect or "postgres"
        # Direct route lowers the OPTIMIZED SQL (REQ-863): when the optimization stage inlined a
        # VALUES CTE, strip the catalog so a native driver addresses schema.table with the CTE
        # carried onto the direct path. With no optimization, take the unchanged fast path.
        if _optimized:
            from provisa.compiler.sql_rewrite import strip_catalog

            physical_sql = strip_catalog(_exec_sql)
        else:
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


async def plan_pgwire_sql(sql: str, role_id: str) -> _Plan:  # REQ-267
    return await _govern_and_route(sql, role_id)


async def execute_pgwire_sql(sql: str, role_id: str) -> QueryResult:  # REQ-266, REQ-267, REQ-272
    """Run *sql* through governance and return rows + column names.

    Raises:
        PermissionError  – role not found or access violation
        ValueError       – SQL parse / validation error
        RuntimeError     – routing / execution error
    """
    # REQ-892: rewrite enabled extension-surface operators/functions (pgvector distance,
    # JSON ->/->>/#>/#>>, compat fns) to federation-engine equivalents, rejecting any
    # unimplemented capability (e.g. ivfflat/hnsw index) loudly. Passthrough when no
    # surface is opted in for this deployment.
    from provisa.pgwire.ext_surfaces import rewrite_surface_operators

    sql = rewrite_surface_operators(sql)

    # REQ-872: a bare SELECT of a registered tracked function routes to the shared executor
    # (writable_by enforced there) instead of federation, unifying invocation across surfaces.
    from provisa.api.app import state as _state
    from provisa.pgwire.function_call import maybe_invoke_registered_function

    fn_result = await maybe_invoke_registered_function(sql, role_id, _state)
    if fn_result is not None:
        return fn_result

    plan = await _govern_and_route(sql, role_id)
    return await _execute_plan(plan)

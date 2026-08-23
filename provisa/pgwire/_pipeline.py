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

import collections
import logging
import re
import secrets as _secrets
import time as _time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from provisa.audit.pipeline import PendingAudit
from provisa.executor.result import QueryResult
from provisa.otel_compat import get_tracer as _get_tracer

if TYPE_CHECKING:
    from provisa.executor.redirect import Delivery

log = logging.getLogger(__name__)
_tracer = _get_tracer(__name__)

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
    # REQ-1194/REQ-1195: an IR-level directive to materialize this result to a sink (CTAS-to-object-
    # store, presigned URL) instead of returning rows. Set by the planner from the caller's delivery
    # preference; _execute_plan runs the ONE materialize terminal when present. Every transport
    # inherits it — the redirect decision is no longer transport-local.
    materialize: Delivery | None = field(default=None)
    # REQ-1224 (streaming-uniformity Defect 4): the AUTOMATIC materialize policy for a buffered
    # transport (JSON:API, GraphQL, Bolt). Unlike `materialize` (caller-driven, unconditional CTAS),
    # this rides the plan for every buffered result and the terminal DECIDES per-result — inline the
    # body below the config row threshold, land an engine-native CTAS above it — with no caller
    # side-channel. None for streaming transports and when redirect is disabled in system config.
    auto_deliver: Delivery | None = field(default=None)
    # REQ-074/REQ-1386: the audit record opened for this statement (acting principal, surface,
    # resolved registered_tables ids, start time). Minted alongside the stamp at the top of the
    # pipeline and finalized with the real status/duration at the terminal — so every surface is
    # audited by the one pipeline instead of each transport calling log_query itself. None when
    # nothing user-initiated is running (seeding, rebuilds); see provisa.audit.context.
    audit: PendingAudit | None = field(default=None)
    # Guards against a second finalize for one statement: the streaming surfaces finalize at their
    # own terminal, and a plan that also passes through _execute_plan must still write one row.
    audit_written: bool = field(default=False)
    # REQ-074/REQ-1386 (ops `queries` report): the OTel span attributes for this statement —
    # provisa.table / provisa.domain / provisa.role / provisa.query_text, the attributes
    # TRACE_ATTR_COLS lifts into the trace table the report reads. Minted at the top of the
    # pipeline alongside the audit record and handed to the engine at the ENGINE terminal, so
    # every governed surface emits an attributed query span instead of an anonymous one. None
    # when nothing user-initiated is running (seeding, rebuilds) — same condition as `audit`.
    span_attrs: dict[str, str] | None = field(default=None)
    # Governed-provenance stamp (see below). Minted ONLY at the top of the pipeline
    # (_govern_and_route / _govern_and_route_compiled); _execute_plan refuses any plan lacking a
    # valid one, so an un-governed / side-door plan can never be executed.
    stamp: str | None = field(default=None)
    # REQ-1044: the org's tier ceilings and the plan name they came from, resolved once when this
    # plan was minted. Attached at the top of the pipeline — the one place every surface passes
    # through — so the scan-side hints are already in `session_hints` no matter which terminal
    # runs the statement, and the terminal has what it needs to bound egress and to restate an
    # engine-side kill as a tier error. None when the deployment has no billing subject.
    tier_caps: Any | None = field(default=None)
    tier_plan: str | None = field(default=None)
    # REQ-1517: the plan's own account of how it was built — the semantic sources the statement
    # resolved to, the router's reason for the route it picked, and the labels of the
    # post-governance optimizations that fired (hot-table inlining, API-cache rewrite, branch
    # drop). Populated at the top of the pipeline where those decisions are made; the stats
    # terminal renders them, so the execution DAG reports the real plan instead of the surface's
    # guess at it. Empty when nothing optimized the statement.
    sources: frozenset[str] = field(default_factory=frozenset)
    route_reason: str | None = field(default=None)
    optimizations: tuple[str, ...] = field(default=())


# --------------------------------------------------------------------------- #
# Governed-provenance stamp (the single-chokepoint contract).
#
# The one pipeline is the only code that may execute governed SQL. To make that a
# MECHANICAL invariant rather than a convention, the TOP of the pipeline mints an
# unforgeable capability token for every plan it produces, and the bottom
# (_execute_plan) refuses to run any plan whose token it did not itself issue.
#
#   * The key/nonce space is process-private (256-bit random) — no surface, test, or
#     side-door can read it or guess an issued token.
#   * Only the pipeline can VERIFY a stamp (membership in _ISSUED). "You can only ask
#     the pipeline whether an output came from it" is literally the API: stamp_is_valid.
#   * A resurrected second pipeline (a new _compile_govern_execute) cannot mint a valid
#     stamp, so _execute_plan rejects its plans — the drift class of bug becomes a
#     hard runtime failure, complementary to the static import-boundary guard test.
# --------------------------------------------------------------------------- #

# Bounded ring of issued stamps — recent-enough to verify in-flight/just-returned plans
# without unbounded growth. A stamp is a 256-bit random hex token, so collisions/guesses
# are infeasible.
_ISSUED_STAMPS: collections.deque[str] = collections.deque(maxlen=8192)
_ISSUED_SET: set[str] = set()


def _mint_stamp() -> str:
    """Issue a fresh governed-provenance stamp. Called ONLY from the top of the pipeline."""
    token = _secrets.token_hex(32)
    if len(_ISSUED_STAMPS) == _ISSUED_STAMPS.maxlen:
        _ISSUED_SET.discard(_ISSUED_STAMPS[0])  # evict the oldest as the ring wraps
    _ISSUED_STAMPS.append(token)
    _ISSUED_SET.add(token)
    return token


def stamp_is_valid(stamp: str | None) -> bool:
    """True iff ``stamp`` was minted by the top of THIS process's pipeline. The only way to
    ask the pipeline whether an output/plan actually came from it — no other module can."""
    return bool(stamp) and stamp in _ISSUED_SET


def require_governed_plan(plan: "_Plan") -> None:
    """Refuse to execute any plan the top of the pipeline did not mint (REQ-1176).

    _execute_plan is NOT the only execution terminal — the Arrow/streaming sinks (Flight, airport,
    COPY) and the Cypher/CTAS paths run ``plan.physical_sql`` / ``plan.sql`` on the engine directly.
    EVERY such sink MUST call this first, so the single-chokepoint guarantee (no ungoverned egress)
    holds universally, not only for the materialized _execute_plan path. A side-door or hand-built
    plan has no valid stamp and is rejected here before a single row leaves the engine."""
    if not stamp_is_valid(plan.stamp):
        raise PermissionError(
            "ungoverned plan rejected: missing/invalid pipeline stamp — every executed plan MUST be "
            "produced by the one governed pipeline (_govern_and_route / _govern_and_route_compiled)"
        )


# Connector types that don't support the engine fault-tolerant execution (FTE): their
# splits aren't replayable, so a query routed under retry-policy=TASK blocks
# forever on the exchange. Queries touching these run with retry_policy=NONE.
_NON_FTE_SOURCE_TYPES = frozenset({"kafka"})


async def _optimize_and_route(
    exec_sql: str,
    governed_sql: str,
    gov_ctx,
    ctx,
    state,
    *,
    nf_args=None,
    has_json_extract=False,
    is_mutation=False,
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
    from provisa.transpiler.router import Route, decide_route

    _rewrites, _values_ctes, _dropped = await _materialize_api_to_engine_cache(
        exec_sql, state, nf_args=nf_args
    )
    _actually_dropped: set[str] = set()
    if _dropped:
        from provisa.compiler.nf_extractor import (
            drop_union_branches_for_table,
            find_api_table_names,
        )

        for _dtn in _dropped:
            exec_sql = drop_union_branches_for_table(exec_sql, _dtn)
            if _dtn in find_api_table_names(exec_sql):
                # drop_union_branches_for_table only removes UNION branches — a no-op here
                # means _dtn is referenced outside a union (e.g. a plain FROM, such as a
                # required-path-param endpoint that can't be pre-materialized). It stays in
                # exec_sql untouched and routes as an ordinary live API source below.
                continue
            _actually_dropped.add(_dtn)
    for _tn, _entry in _values_ctes.items():
        exec_sql = build_values_cte_sql(exec_sql, _tn, _entry)
    if _rewrites:
        exec_sql = rewrite_all_from_cache(exec_sql, _rewrites)

    _inlined = set(_values_ctes) | _actually_dropped
    optimized = bool(_inlined or _rewrites)
    # REQ-1517: name each optimization that fired, per relation, so the execution DAG can say WHY a
    # scan is cheap (or absent) instead of rendering an unexplained node. Built here because this is
    # the only place that knows which rewrite applied to which table.
    opt_labels: list[str] = []
    for _tn in sorted(_values_ctes):
        opt_labels.append(f"hot-table inline: {_tn}")
    for _tn in sorted(_actually_dropped):
        opt_labels.append(f"branch dropped: {_tn}")
    for _tn in sorted(_rewrites):
        opt_labels.append(f"api cache: {_tn}")
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
        is_mutation=is_mutation,
    )
    if _rewrites and decision.route != Route.ENGINE:
        # A cache rewrite points the SQL at a materialized table living in the engine's
        # attached mat_store catalog — no native pool or API caller can see it, so the
        # query MUST route through the engine regardless of what decide_route picked
        # (e.g. Route.API for a query whose only remaining source is the API source
        # that got rewritten away).
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE,
            source_id=None,
            dialect=None,
            reason="query rewritten to a materialized cache table",
        )
    return exec_sql, decision, default_source, optimized, sources, tuple(opt_labels)


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


def _reject_view_writes(parsed: Any, state: Any) -> None:
    """REQ-1157: a ``view_sql`` / MV-backed relation is DERIVED, not a base table, and is query-only.

    Reject any INSERT / UPSERT (INSERT ... ON CONFLICT) / UPDATE / DELETE / MERGE whose TARGET is such
    a relation, on every raw-SQL surface funnelled through this pipeline (pgwire, REST /data/sql, Flight
    SQL, MCP, Bolt/Cypher, gRPC). A write to a view either fails at the source (non-updatable view) or
    lands in the mv_cache snapshot the next REQ-879 refresh silently overwrites — data loss with no
    error, violating the no-silent-failure rule. Only the write TARGET is checked; a view read in the
    FROM/USING of a write is fine, and a base table (not in view_sql_map) is never affected.
    """
    import sqlglot.expressions as _exp

    view_map = getattr(state, "view_sql_map", None)
    if not view_map:
        return
    if not isinstance(parsed, (_exp.Insert, _exp.Update, _exp.Delete, _exp.Merge)):
        return
    target = parsed.this
    tbl = (
        target if isinstance(target, _exp.Table) else (target.find(_exp.Table) if target else None)
    )
    if tbl is not None and tbl.name in view_map:
        op = type(parsed).__name__.upper()
        raise PermissionError(
            f"{op} into {tbl.name!r} is not allowed: it is a view/MV-backed relation and is query-only "
            "(REQ-1157). A write would fail at the source or be lost on the next materialized-view refresh."
        )


async def _reject_unbound_writes(parsed: Any, state: Any) -> None:
    """REQ-1491/REQ-1539: a write needs a binding — the environment does not decide who may write.

    WHAT THIS IS NOT. It was once also a permission check: the environment a binding was inherited
    from carried a ``branch_writable`` flag, and a write through an inherited binding was refused
    unless that flag was set. REQ-1539 removed it. A member's data rights are the rights their ROLES
    give them, in every environment alike — an environment is a namespace for the model, not a
    second permission system layered over the one that already answers "may this person write".
    Conferring model-editing authority on the creator of an environment (REQ-1528) is what needed
    bounding, and it is bounded where it arose: that authority no longer carries ``write`` at all.

    WHAT REMAINS is the question a write cannot proceed without an answer to: which binding it would
    travel. A table nobody registered has no binding, and a source unbound here and in everything it
    inherited from has none either. Both are refused — not as a permission, but because there is no
    established target to write to, which is REQ-1491's guarantee that a new environment reaches
    nothing until somebody says what it reaches.

    Checked on the ONE pipeline every raw-SQL surface funnels through, for the same reason
    REQ-1157's view guard is: a check on one surface is a check the next surface does not have.
    prod returns immediately — it inherits from nothing, so every binding it has is its own.
    """
    import sqlglot.expressions as _exp

    from provisa.api.org_runtime import active_env
    from provisa.core.environments import PROD

    env = active_env()
    if env == PROD:
        return
    if not isinstance(parsed, (_exp.Insert, _exp.Update, _exp.Delete, _exp.Merge)):
        return
    target = parsed.this
    tbl = (
        target if isinstance(target, _exp.Table) else (target.find(_exp.Table) if target else None)
    )
    if tbl is None:
        return
    source_id = next(
        (t["source_id"] for t in getattr(state, "tables", []) if t["table_name"] == tbl.name), None
    )
    if source_id is None:
        raise PermissionError(
            f"{type(parsed).__name__.upper()} into {tbl.name!r} is not allowed in environment "
            f"{env!r}: it is not a registered table, so which binding the write would travel "
            f"cannot be established, and a write with no established target is what REQ-1491 refuses."
        )
    if getattr(state, "source_binding_env", {}).get(source_id) is None:
        raise PermissionError(
            f"{type(parsed).__name__.upper()} into {tbl.name!r} is not allowed in environment "
            f"{env!r}: source {source_id!r} is unbound in {env!r} and in every environment it "
            f"inherited from (REQ-1491). Bind it to write to it."
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


def _plan_span_attrs(
    semantic_sql: str, role_id: str, query_text: str, audit: PendingAudit | None
) -> dict[str, str] | None:
    """The OTel attributes for a governed ENGINE plan, or None when no principal is acting.

    Gated on the audit record for the same reason it exists: `audit is None` means nothing
    user-initiated is running (seeding, rebuilds), and those executions are not queries the ops
    report describes.
    """
    if audit is None:
        return None
    from provisa.observability.span_attrs import span_attrs_from_semantic_sql

    return span_attrs_from_semantic_sql(semantic_sql, role_id, query_text, no_table_label="sql")


async def _attach_tier_caps(plan: _Plan, state: Any) -> _Plan:
    """Bind the org's REQ-1044 ceilings to ``plan`` and hand the scan-side ones to the engine.

    Applied to every plan the pipeline mints, so the caps travel WITH the plan rather than with
    the terminal that happens to run it: the govern-then-stream surfaces (pgwire's socketserver,
    Flight SQL, airport, gRPC) never reach ``_execute_plan``, and a cap enforced only there would
    be a cap every streaming protocol skips.

    A deployment with no billing subject — self-hosted, or any build without the commercial plugin
    — resolves no caps and the plan runs as authored; the tier gate is a SaaS monetization boundary,
    not a safety limit.
    """
    from provisa.api.org_runtime import current_org
    from provisa.core.commerce import caps_for_org, tier_session_hints

    resolved = await caps_for_org(state, current_org.get() or getattr(state, "org_id", None))
    if resolved is None:
        return plan
    caps, tier = resolved
    plan.tier_caps, plan.tier_plan = caps, tier
    hints = tier_session_hints(caps)
    if hints:
        # The plan's own hints win: they are correctness settings the planner chose for this
        # statement (e.g. retry_policy=NONE), not cost policy, and a tier must not silently
        # rewrite them.
        plan.session_hints = {**hints, **(plan.session_hints or {})}
    return plan


async def _wake_before_governing(state: Any) -> None:
    """REQ-1448: the shard the active org queries is serving before this statement is planned.

    ``_execute_plan`` wakes too, but it is not reached by every surface: the govern-then-stream
    terminals (pgwire's socketserver worker, Flight SQL, airport, Bolt, gRPC, CTAS) drain the
    engine's SYNC terminal themselves, so on a shard that had idled to zero they dialed a released
    pod address and answered a connection error — with no wake and nothing naming the cold start.
    Both halves of the ONE pipeline mint plans through ``_govern_and_route`` /
    ``_govern_and_route_compiled``, so waking HERE covers the streaming surfaces without giving any
    of them a wake of its own. Warm shards short-circuit inside ``ensure_shard_awake``.
    """
    from provisa.federation.engine_wake import ensure_engine_awake

    await ensure_engine_awake(state)


async def _govern_and_route(
    sql: str,
    role_id: str,
    *,
    session_vars: dict[str, str] | None = None,
    as_of: str | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    explain: bool | None = None,
) -> _Plan:
    """The top of the ONE pipeline: govern, route, then bind the org's tier ceilings (REQ-1044)."""
    from provisa.api.app import state

    await _wake_before_governing(state)
    plan = await _govern_and_route_planned(
        sql,
        role_id,
        session_vars=session_vars,
        as_of=as_of,
        deliver=deliver,
        buffered=buffered,
        explain=explain,
    )
    return await _attach_tier_caps(plan, state)


async def _govern_and_route_planned(
    sql: str,
    role_id: str,
    *,
    session_vars: dict[str, str] | None = None,
    as_of: str | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    # REQ-1519: describe this statement instead of running it. False wraps the FINAL routed SQL in
    # the dialect's EXPLAIN, True in its EXPLAIN ANALYZE (which does run it). Wrapping here — at
    # the bottom of the ONE pipeline, after governance, optimization and routing — is what makes
    # the explained statement the statement that would have executed.
    explain: bool | None = None,
) -> _Plan:  # REQ-262, REQ-263, REQ-264, REQ-266, REQ-267, REQ-272, REQ-1120, REQ-1159, REQ-1163
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment, extract_relationship_guard_comment
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.compiler.sql_validator import validate_sql
    from provisa.transpiler.router import Route
    from provisa.transpiler.transpile import transpile

    from provisa.audit.pipeline import begin_audit, write_denial

    if role_id not in state.contexts:
        # REQ-1386: a refusal is auditable evidence — policy_denials reports on it.
        await write_denial(sql, role_id, None, None, state)
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

    # REQ-1317: expand queries against the reserved `metrics` schema (metrics.<name>) into the
    # real grouped aggregate over the underlying semantic tables BEFORE governance, so RLS and
    # masking apply to the real columns the metric reads. Mirrors the inline-command localization
    # stage above: rewrite the tree, then re-serialize normalized_sql from it.
    from provisa.compiler.metric_expand import expand_metric_query

    _metric_registry = getattr(state, "metrics", {})
    if _metric_registry:
        _metric_tables = {
            t["table_name"]: {
                "id": t["id"],
                "columns": [c["column_name"] for c in t.get("columns", [])],
            }
            for t in getattr(state, "tables", [])
        }
        _expanded = expand_metric_query(
            _parsed_input,
            _metric_registry,
            _metric_tables,
            getattr(state, "relationships", []),
        )
        if _expanded is not None:
            _parsed_input = _expanded
            normalized_sql = _parsed_input.sql(dialect="postgres")
            # REQ-1319: metric evaluations are traced — the expanded SQL is recorded as a
            # pipeline stage event, same idiom as govern.in/govern.out.
            from provisa.observability.stage_trace import trace_stage

            trace_stage("metric.expand", normalized_sql)

    _reject_physical_source_refs(_parsed_input, state)
    _reject_view_writes(_parsed_input, state)  # REQ-1157: view/MV-backed relations are query-only
    # REQ-1529: and a branch writes only through a binding whose supplier admits it.
    await _reject_unbound_writes(_parsed_input, state)

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=role,
        relationships=getattr(state, "relationships", None),
        source_types=state.source_types,
        engine=getattr(state, "federation_engine", None),
    )

    from provisa.security.rights import Capability, has_capability

    _role_guard = (role or {}).get("relationship_guard", True)
    _bypass_guard = has_capability(role or {}, Capability.IGNORE_RELATIONSHIPS) or (
        (not _role_guard) and sql_opts_out
    )
    # REQ-693: high-security mode is belts and suspenders — the relationship guard is not
    # bypassable there at all. A deployment that improperly granted ignore_relationships (or
    # cleared relationship_guard) to a production role does not get a break-out; the grant is
    # ignored and every join must exist in the approved relationship catalog.
    if getattr(state, "security_high", False):
        _bypass_guard = False
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
            await write_denial(sql, role_id, _parsed_input, gov_ctx, state)
            raise PermissionError(
                f"Domain-access check could not be evaluated for role {role_id!r}: {exc}"
            ) from exc

    if violations:
        msgs = "; ".join(f"[{v.code}] {v.message}" for v in violations)
        await write_denial(sql, role_id, _parsed_input, gov_ctx, state)
        raise PermissionError(msgs)

    # REQ-074/REQ-1386: open the audit record once governance has accepted the statement and the
    # table references have resolved. The terminal finalizes it with the real status and duration.
    _audit = begin_audit(sql, role_id, _parsed_input, gov_ctx)

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
    # REQ-031: an UPDATE/DELETE/INSERT/MERGE always routes DIRECT — the engine terminal takes no
    # writes. decide_route only applies that rule when told; the raw-SQL surfaces (pgwire, /data/sql)
    # parse the statement themselves, so the type must be passed through explicitly.
    _is_mutation = isinstance(_parsed_input, (exp.Insert, exp.Update, exp.Delete, exp.Merge))

    if explain is not None:
        # REQ-1519: describing a statement and delivering its rows to a sink are different
        # terminals; an EXPLAIN has no result set to land, so the combination is refused rather
        # than silently resolved. A write is refused outright — EXPLAIN ANALYZE executes it.
        if deliver is not None or buffered:
            raise ValueError("EXPLAIN cannot be combined with result delivery")
        if _is_mutation:
            raise ValueError("EXPLAIN is only supported for read statements")

    # REQ-301: strip _nf_* WHERE conditions (native API params, e.g. _nf_petId) before routing,
    # same as the compiled path (_govern_and_route_compiled) — without this, an API table with a
    # required path param never resolves it, materialization skips, and the unmaterialized table
    # reaches the engine unchanged ("no such table").
    from provisa.compiler.nf_extractor import extract_nf_args

    _physical_sql = rewrite_semantic_to_catalog_physical(
        normalize_table_refs(governed_semantic, ctx), ctx
    )
    _physical_sql, _nf_clean_params, _extracted_nf = extract_nf_args(
        _physical_sql, embedded_params or []
    )
    exec_params = (
        _nf_clean_params if _nf_clean_params != (embedded_params or []) else embedded_params
    )
    _nf_args = _extracted_nf or None

    _qualified, decision, _default_source, _optimized, _sources, _opts = await _optimize_and_route(
        _physical_sql,
        governed_semantic,
        gov_ctx,
        ctx,
        state,
        has_json_extract="->>" in governed_semantic,
        is_mutation=_is_mutation,
        nf_args=_nf_args,
    )

    exec_params = exec_params or None

    # REQ-135/REQ-1163: a query referencing a __derived__ view MUST route through the engine, where the
    # view is inline-expanded. A view's virtual source has no native driver/catalog, so extract_sources
    # cannot bind it and routing would otherwise pick DIRECT against a real source, handing the
    # un-expanded view ref to a native pool. Force ENGINE so the ENGINE branch expands it.
    _view_map = getattr(state, "view_sql_map", None)
    if _view_map and decision.route != Route.ENGINE:
        _refs_view = any(
            t.name in _view_map
            for t in sqlglot.parse_one(governed_semantic, read="postgres").find_all(exp.Table)
        )
        if _refs_view:
            from provisa.transpiler.router import RouteDecision

            decision = RouteDecision(
                route=Route.ENGINE, source_id=None, dialect=None, reason="query references a view"
            )

    # REQ-1194/REQ-1195: a delivery request materializes the result via the federation engine's
    # CTAS-to-object-store terminal, so the plan MUST carry engine-physical SQL regardless of the
    # route the rows would otherwise take. Force ENGINE so the physical_sql branch below runs.
    if deliver is not None and decision.route != Route.ENGINE:
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE, source_id=None, dialect=None, reason="result delivery requested"
        )

    # REQ-1224 (Defect 4): a buffered transport (JSON:API, GraphQL, Bolt) rides the AUTOMATIC
    # threshold — the terminal inlines below the config row limit, lands an engine-native CTAS above
    # it. That CTAS needs engine-physical SQL, so force ENGINE (same as an explicit deliver). None
    # when redirect is disabled in system config, leaving inline behaviour unchanged (opt-in).
    from provisa.executor.redirect import auto_delivery_for_buffered

    auto_deliver = auto_delivery_for_buffered(role_id) if buffered and deliver is None else None
    if auto_deliver is not None and decision.route != Route.ENGINE:
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE,
            source_id=None,
            dialect=None,
            reason="buffered-transport auto-delivery",
        )

    # REQ-1159: a localized statement carries an inline local relation as a VALUES list, which rides
    # along on whichever route the router picks — DIRECT inlines the VALUES into the single source's
    # SQL (the source executes it), and a genuinely cross-source statement is detected and routed to
    # the engine by decide_route as usual. So the localizer does NOT force a route; it lets routing
    # decide, which keeps a single-source composed query on the source instead of the org store.
    if decision.route == Route.ENGINE:
        # REQ-135/REQ-1163: inline-expand any __derived__ view ref BEFORE the unknown-catalog check and
        # transpile — a request-level as-of overlays each bitemporal view's entry with an as-of
        # reconstruction over its append log (else views read current state). Same lowering the GQL/
        # Cypher path uses (_govern_and_route_compiled). _qualified is catalog-physical; a view ref is
        # source-less so it survives the rewrites unchanged and still matches a view_sql_map leaf key.
        if _view_map:
            from provisa.compiler.view_expand import expand_view_refs

            _vmap = _view_map
            if as_of and getattr(state, "bitemporal_view_reads", None):
                from provisa.mv.bitemporal import as_of_view_map

                _vmap = as_of_view_map(_view_map, state.bitemporal_view_reads, as_of)
            _qualified = expand_view_refs(_qualified, _vmap)
            # View bodies are stored in semantic form; after expansion, lower any
            # newly-introduced semantic refs to catalog-physical (same pass the outer SQL
            # went through at line 456 before routing).
            _qualified = rewrite_semantic_to_catalog_physical(
                normalize_table_refs(_qualified, ctx), ctx
            )

        _known_cats_pgwire = (
            set(getattr(state, "source_catalogs", {}).values())
            | {
                "iceberg",
                "otel",
                "results",
                "mat_store",  # REQ-1163: the materialization store an expanded bitemporal view reconstructs over
            }
        )
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
        if explain is not None:
            # REQ-1519: the ONE pipeline's own EXPLAIN — the engine describes the federated
            # statement it would have run, wrapped after transpile so nothing else changes.
            from provisa.executor.explain import wrap_explain

            physical_sql = wrap_explain(
                physical_sql, state.federation_engine.dialect, analyze=explain
            )
        return _Plan(
            route=Route.ENGINE,
            sql=governed_semantic,
            source_id=_default_source,
            dialect=state.federation_engine.dialect,
            exec_params=exec_params,
            physical_sql=physical_sql,
            materialize=deliver,  # REQ-1194/REQ-1195: sink delivery inherited by every transport
            auto_deliver=auto_deliver,  # REQ-1224: buffered-transport auto threshold (terminal decides)
            span_attrs=_plan_span_attrs(governed_semantic, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(_sources),
            route_reason=decision.reason,
            optimizations=_opts,
        )
    else:
        dialect = decision.dialect or "postgres"
        # Direct route lowers the OPTIMIZED SQL when the optimization stage changed it (REQ-863),
        # carrying any inlined VALUES CTE onto the direct path; else the unchanged fast path.
        if _optimized:
            from provisa.compiler.sql_rewrite import strip_catalog

            _physical = strip_catalog(_qualified)
        else:
            # Lower the semantic model to physical schema.table for the native driver — same as
            # _govern_and_route_compiled's DIRECT branch. Passing governed_semantic verbatim sent
            # an unresolved semantic ref (e.g. "pet_store"."inquiries") to the source.
            from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

            _physical = rewrite_semantic_to_physical(governed_semantic, ctx)
        from provisa.compiler.sql_rewrite import FLAT_NAMESPACE_SOURCES, strip_schema

        _direct_sid = decision.source_id or _default_source
        if state.source_types.get(_direct_sid) in FLAT_NAMESPACE_SOURCES:
            _physical = strip_schema(_physical)
        sql_to_run = transpile(_physical, dialect)
        if explain is not None:
            # REQ-1519: the source describes the pushed-down statement in its own dialect.
            from provisa.executor.explain import wrap_explain

            sql_to_run = wrap_explain(sql_to_run, dialect, analyze=explain)
        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            source_id=_direct_sid,
            dialect=dialect,
            exec_params=exec_params,
            # REQ-1425: every route carries the plan's span attributes, so the ops queries report
            # covers pushed-down single-source statements identically to federated ones.
            span_attrs=_plan_span_attrs(governed_semantic, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(_sources),
            route_reason=decision.reason,
            optimizations=_opts,
        )


async def finalize_audit(plan: _Plan, status_code: int, state: Any | None = None) -> None:
    """Write ``plan``'s audit row (REQ-074/REQ-1386). Idempotent per plan.

    ``_execute_plan`` calls this at its terminals. The govern-then-stream surfaces (pgwire's
    socketserver worker, Flight SQL, airport) never reach ``_execute_plan`` — they drain the
    engine's SYNC terminal themselves — so they call this at their own terminal instead. The
    idempotence guard means a plan that takes either path is audited exactly once.
    """
    if plan.audit_written:
        return
    plan.audit_written = True
    from provisa.audit.pipeline import write_audit

    await write_audit(plan.audit, status_code, state)


async def _execute_plan(plan: _Plan, state: Any | None = None) -> QueryResult:  # REQ-027, REQ-028
    require_governed_plan(plan)  # SECURITY: refuse any plan the top of the pipeline did not mint
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    # REQ-1448: the shard this org queries may have had its node released while idle. Waking it HERE
    # — before the terminal, not inside the executor's retry loop — is what makes a cold start
    # survivable: a node is ~2-4min to provision and the retry budget is 30s, so a query that
    # discovers the absence at dispatch time could never wait it out. This is also the one seam every
    # surface reaches, so no protocol server needs a wake of its own.
    from provisa.federation.engine_wake import ensure_engine_awake, readdress_lost_coordinator

    await ensure_engine_awake(state)
    _t0 = _time.perf_counter()
    # REQ-074/REQ-1386: one audit row per executed statement, with the terminal's real outcome —
    # written here rather than in each transport, so no surface can omit it.
    try:
        try:
            result = await _run_plan_terminal(plan, state)
        except Exception as exc:
            # REQ-1448: a dial that reached nothing can mean the coordinator moved while this
            # process held its address. Only re-resolving says which, and only a shard that
            # actually moved earns the second dispatch — the executor's own retries cannot help
            # here, because they rebuild the connection at the same dead address.
            if not await readdress_lost_coordinator(exc, state):
                raise
            result = await _run_plan_terminal(plan, state)
    except Exception as exc:
        # REQ-1044: the engine kills a query that breached a scan-side ceiling with its own
        # EXCEEDED_* error, which says nothing about the customer's plan. Restate it as the tier
        # boundary it is — 402, not 500 — and audit it as such.
        tier_error = _translate_tier_error(plan, exc)
        if tier_error is not None:
            await finalize_audit(plan, 402, state)
            raise tier_error from exc
        await finalize_audit(plan, 500, state)
        raise
    try:
        result = _apply_output_cap(plan, result)
    except Exception:
        # REQ-1044/REQ-1454: an egress rejection is an OUTCOME of this statement, not an absence of
        # one. It is audited at 402 and metered like any other submitted statement — the shard ran
        # the query to produce the rows it then refused to ship, and a rejection that recorded
        # nothing would leave the customer's own audit log unable to explain the error they saw.
        await finalize_audit(plan, 402, state)
        raise
    await finalize_audit(plan, 200, state)
    # REQ-1517: record this statement against the request's stats accumulator (opt-in via
    # X-Provisa-Stats) from the PLAN, at the one terminal every raw-SQL surface reaches — so the
    # route, the source and the execution DAG a surface reports are the ones that actually ran.
    # A no-op when the caller did not ask for stats.
    from provisa.executor.plan_stats import record_plan_execution

    record_plan_execution(
        plan, state, rows=len(result.rows), elapsed_ms=(_time.perf_counter() - _t0) * 1000
    )
    return result


def _translate_tier_error(plan: _Plan, exc: BaseException) -> Exception | None:
    """The tier restatement of an engine-side ceiling kill, or None when ``exc`` is unrelated."""
    if plan.tier_caps is None or plan.tier_plan is None:
        return None
    from provisa.core.commerce import translate_engine_error

    return translate_engine_error(exc, plan.tier_caps, plan.tier_plan)


def _apply_output_cap(plan: _Plan, result: QueryResult) -> QueryResult:
    """Bound the result at the tier's egress ceiling (REQ-1044) — a rejection, never a truncation."""
    if plan.tier_caps is None or plan.tier_plan is None:
        return result
    from provisa.core.commerce import enforce_output_cap

    return enforce_output_cap(result, plan.tier_caps, plan.tier_plan)


async def _run_plan_terminal(plan: _Plan, state: Any) -> QueryResult:  # REQ-027, REQ-028
    from provisa.transpiler.router import Route

    engine = state.federation_engine

    if plan.materialize is not None:
        # ONE materialize terminal (REQ-1194/REQ-1195): the governed plan asked for sink delivery.
        # The planner forced the ENGINE lowering so physical_sql is the federated CTAS source. Return
        # the delivery handle on the result; the row list is empty (zero rows transit memory).
        from typing import cast

        from provisa.executor.redirect import Delivery, run_materialize

        assert plan.physical_sql is not None
        handle = await run_materialize(state, plan.physical_sql, cast(Delivery, plan.materialize))
        return QueryResult(rows=[], column_names=[], redirect=handle)

    if plan.auto_deliver is not None:
        # AUTOMATIC threshold terminal (REQ-1224, Defect 4): a buffered transport (JSON:API, GraphQL,
        # Bolt) whose plan carries no explicit sink. The terminal DECIDES per-result — drain the ENGINE
        # stream up to the config row threshold; if the whole result fits, inline it (bounded by the
        # threshold budget); if it exceeds, abandon the partial buffer and land an engine-native CTAS
        # off Provisa's heap, surfacing the handle instead of rows. The planner forced ENGINE lowering
        # so physical_sql is the federated CTAS source — no transport-local branch, no caller side-channel.
        import asyncio
        from typing import cast

        from provisa.executor.redirect import Delivery, run_materialize

        assert plan.physical_sql is not None
        deliv = cast(Delivery, plan.auto_deliver)
        threshold = deliv.config.threshold
        physical_sql = plan.physical_sql

        def _drain() -> tuple[list[str], list[str] | None, list[tuple], bool]:
            stream = engine.execute_engine_sync(
                physical_sql, params=plan.exec_params, session_hints=plan.session_hints
            )
            it = stream.iter_rows()
            buffered_rows: list[tuple] = []
            over = False
            try:
                for row in it:
                    buffered_rows.append(row)
                    if len(buffered_rows) > threshold:
                        over = True
                        break
            finally:
                if over:
                    it.close()  # GeneratorExit → the engine cursor closes without a full drain
            return stream.column_names, stream.column_types, buffered_rows, over

        col_names, col_types, buffered_rows, over = await asyncio.to_thread(_drain)
        if not over:
            return QueryResult(rows=buffered_rows, column_names=col_names, column_types=col_types)
        handle = await run_materialize(state, physical_sql, deliv)
        return QueryResult(rows=[], column_names=[], redirect=handle)

    if plan.route == Route.ENGINE:
        assert plan.physical_sql is not None
        # ENGINE terminal (REQ-825): hand the federated SQL to the bound engine.
        result = await engine.execute_engine(
            plan.physical_sql,
            params=plan.exec_params,
            session_hints=plan.session_hints,
            span_attrs=plan.span_attrs,
        )
    elif getattr(state, "source_types", {}).get(plan.source_id) == "govdata":
        # GovData sources execute via the GovData/Calcite bridge, not a native pool or the engine.
        from provisa.api.data.endpoint_dev import _execute_govdata

        result = await _execute_govdata(plan.source_id, plan.sql, state)
    elif plan.source_id == "provisa-admin" or not state.source_pools.has(plan.source_id):
        # Admin-owned tables (meta.*) live in the provisa tenant_db, not source_pools.
        tenant_db = state.tenant_db
        if tenant_db is None:
            raise RuntimeError("Admin tenant_db not available")
        # REQ-1425: the admin terminal is a query terminal like any other — it emits the same
        # provisa.query.* span so meta/ops statements reach the ops queries report.
        _span_name = "provisa.query.postgres" if plan.span_attrs else "admin.execute"
        with _tracer.start_as_current_span(_span_name) as _span:
            if plan.span_attrs:
                for _k, _v in plan.span_attrs.items():
                    _span.set_attribute(_k, _v)
            _span.set_attribute("db.system", "postgres")
            _span.set_attribute("db.statement", plan.sql[:1000])
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
            _span.set_attribute("db.row_count", len(rows))
        result = QueryResult(rows=rows, column_names=col_names)
    else:
        # DIRECT terminal (REQ-825): single reachable source on its native driver.
        result = await engine.execute_native(
            state.source_pools,
            plan.source_id,
            plan.sql,
            plan.exec_params,
            plan.span_attrs,
        )
    return result


async def execute_sql_batch(
    sql: str,
    role_id: str,
    state: Any | None = None,
    *,
    session_vars: dict[str, str] | None = None,
    as_of: str | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
) -> QueryResult:
    """Govern + execute a (possibly multi-statement) SQL batch through the ONE pipeline, returning the
    LAST statement's result (psql/JDBC batch semantics).

    Every entry point can send multiple statements. Splitting is statement-aware (no parser
    differential) and EACH statement is governed+routed+stamped and executed IN ORDER — so a batch is
    never silently reduced to its first statement (the ``parse_one`` trap that dropped the tail on
    every non-pgwire surface). A single statement behaves exactly like _govern_and_route + _execute_plan.
    Per statement, a standalone registered-command call is invoked through the shared function hook,
    matching the single-statement surface behaviour."""
    from provisa.compiler.sql_rewrite import split_sql_statements
    from provisa.pgwire.function_call import maybe_invoke_registered_function

    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    statements = split_sql_statements(sql)
    if not statements:
        return QueryResult(rows=[], column_names=[])
    result: QueryResult | None = None
    for _i, stmt in enumerate(statements):
        cmd = await maybe_invoke_registered_function(stmt, role_id, state)
        if cmd is not None:
            result = cmd
            continue
        # Delivery applies only to the final (result) statement of the batch; leading statements run
        # inline so their side effects land without spilling intermediate results to a sink.
        _deliver = deliver if _i == len(statements) - 1 else None
        plan = await _govern_and_route(
            stmt,
            role_id,
            session_vars=session_vars,
            as_of=as_of,
            deliver=_deliver,
            buffered=buffered and _i == len(statements) - 1,
        )
        result = await _execute_plan(plan, state)
    assert result is not None
    return result


async def govern_batch_final_plan(
    sql: str,
    role_id: str,
    state: Any | None = None,
    *,
    session_vars: dict[str, str] | None = None,
) -> _Plan:
    """Govern+execute all but the LAST statement of a batch, and return the governed+stamped plan for
    the last statement — for Arrow/streaming surfaces (Flight SQL, airport) that render the final
    statement's rows themselves. Guarantees a multi-statement batch's leading statements still run
    (governed), rather than being silently dropped by ``parse_one``. A single statement runs nothing
    extra and just returns its plan."""
    from provisa.compiler.sql_rewrite import split_sql_statements

    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    statements = split_sql_statements(sql)
    if not statements:
        raise ValueError("empty SQL batch")
    for stmt in statements[:-1]:
        plan = await _govern_and_route(stmt, role_id, session_vars=session_vars)
        await _execute_plan(plan, state)
    return await _govern_and_route(statements[-1], role_id, session_vars=session_vars)


async def _govern_and_route_compiled(  # REQ-262, REQ-263, REQ-265, REQ-266, REQ-1044
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
    api_args: dict | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
) -> _Plan:
    """Governance + routing for already-physical SQL, with the org's tier ceilings bound."""
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    await _wake_before_governing(state)
    plan = await _govern_and_route_compiled_planned(
        sql,
        role_id,
        exec_params=exec_params,
        state=state,
        api_args=api_args,
        deliver=deliver,
        buffered=buffered,
    )
    return await _attach_tier_caps(plan, state)


async def _govern_and_route_compiled_planned(  # REQ-262, REQ-263, REQ-265, REQ-266
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
    api_args: dict | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
) -> _Plan:
    """Governance + routing for already-physical SQL.

    Used by GQL and Cypher transport paths after language-specific compilation.
    No SQL validation: the compiler produced this SQL from a governed AST, so there is no
    caller-authored text to validate.
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

    from provisa.audit.pipeline import begin_audit, write_denial

    if role_id not in state.contexts:
        await write_denial(sql, role_id, None, None, state)  # REQ-1386: policy_denials
        raise PermissionError(f"No schema for role {role_id!r}")

    import sqlglot as _sg

    _compiled_tree = _sg.parse_one(sql, read="postgres")

    # REQ-1319: the compiled path serves Flight and the gRPC proxy — a metric ask arriving
    # as semantic SQL (metrics.<name>) must expand through the SAME single expansion the
    # raw-SQL path uses. Guarded on a metrics-schema reference, so ordinary compiler
    # output (which never addresses the reserved schema) is untouched.
    _metric_registry = getattr(state, "metrics", {})
    if _metric_registry:
        from provisa.compiler.metric_expand import expand_metric_query

        _metric_tables = {
            t["table_name"]: {
                "id": t["id"],
                "columns": [c["column_name"] for c in t.get("columns", [])],
            }
            for t in getattr(state, "tables", [])
        }
        _expanded = expand_metric_query(
            _compiled_tree,
            _metric_registry,
            _metric_tables,
            getattr(state, "relationships", []),
        )
        if _expanded is not None:
            _compiled_tree = _expanded
            sql = _compiled_tree.sql(dialect="postgres")
            # REQ-1319: metric evaluations are traced on the compiled path too.
            from provisa.observability.stage_trace import trace_stage

            trace_stage("metric.expand", sql)

    _reject_view_writes(_compiled_tree, state)  # REQ-1157: views are query-only
    await _reject_unbound_writes(_compiled_tree, state)  # REQ-1491

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
        source_types=state.source_types,
        engine=getattr(state, "federation_engine", None),
    )

    # REQ-074/REQ-1386: the compiled surfaces (GQL, Cypher, Flight, gRPC) are audited by the same
    # record the raw-SQL path opens — the terminal finalizes it.
    _audit = begin_audit(sql, role_id, _compiled_tree, gov_ctx)

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
    _exec_sql, decision, _default_source, _optimized, sources, _opts = await _optimize_and_route(
        _exec_sql, governed_sql, gov_ctx, ctx, state, nf_args=_nf_args
    )

    # REQ-135/REQ-1163: a query referencing a __derived__ view MUST route through the engine, where
    # the view was already inline-expanded above. A view's virtual source has no native driver/
    # catalog — if routing picks DIRECT (legitimate once expansion collapses the query onto a single
    # real source), the DIRECT branch's non-optimized fallback rebuilds physical SQL from the
    # UN-expanded ``governed_sql`` and hands the raw view ref to a native pool. Force ENGINE so the
    # ENGINE branch's already-expanded ``_exec_sql`` is what actually executes. Same guard
    # ``_govern_and_route`` (the raw-SQL/pgwire path) already applies.
    if _view_map and decision.route != Route.ENGINE:
        import sqlglot as _sg3
        import sqlglot.expressions as _exp3

        _refs_view = any(
            t.name in _view_map
            for t in _sg3.parse_one(governed_sql, read="postgres").find_all(_exp3.Table)
        )
        if _refs_view:
            from provisa.transpiler.router import RouteDecision

            decision = RouteDecision(
                route=Route.ENGINE, source_id=None, dialect=None, reason="query references a view"
            )

    # REQ-1194/REQ-1195: sink delivery materializes via the federation engine's CTAS terminal, so the
    # plan MUST carry engine-physical SQL. Force ENGINE regardless of the route the rows would take.
    if deliver is not None and decision.route != Route.ENGINE:
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE, source_id=None, dialect=None, reason="result delivery requested"
        )

    # REQ-1224 (Defect 4): buffered-transport auto threshold — the terminal decides inline-vs-CTAS.
    # The CTAS needs engine-physical SQL, so force ENGINE. None when redirect is disabled (opt-in).
    from provisa.executor.redirect import auto_delivery_for_buffered

    auto_deliver = auto_delivery_for_buffered(role_id) if buffered and deliver is None else None
    if auto_deliver is not None and decision.route != Route.ENGINE:
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE,
            source_id=None,
            dialect=None,
            reason="buffered-transport auto-delivery",
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
            materialize=deliver,  # REQ-1194/REQ-1195: sink delivery inherited by every transport
            auto_deliver=auto_deliver,  # REQ-1224: buffered-transport auto threshold (terminal decides)
            span_attrs=_plan_span_attrs(governed_sql, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(sources),
            route_reason=decision.reason,
            optimizations=_opts,
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
        from provisa.compiler.sql_rewrite import FLAT_NAMESPACE_SOURCES, strip_schema

        _direct_sid = decision.source_id or _default_source
        if state.source_types.get(_direct_sid) in FLAT_NAMESPACE_SOURCES:
            physical_sql = strip_schema(physical_sql)
        sql_to_run = transpile(physical_sql, dialect)
        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            exec_sql=physical_sql,
            source_id=_direct_sid,
            dialect=dialect,
            exec_params=exec_params,
            # REQ-1425: every route carries the plan's span attributes (see _govern_and_route).
            span_attrs=_plan_span_attrs(governed_sql, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(sources),
            route_reason=decision.reason,
            optimizations=_opts,
        )


async def plan_pgwire_sql(sql: str, role_id: str) -> _Plan:  # REQ-267
    return await _govern_and_route(sql, role_id)


async def govern_pgwire_plan(sql: str, role_id: str) -> _Plan | QueryResult:  # REQ-028, REQ-266
    """Govern a pgwire statement to its last-mile plan WITHOUT executing the ENGINE terminal.

    The pgwire socketserver worker thread drains the engine's SYNC streaming terminal itself —
    the same govern-then-stream split Flight SQL uses (:func:`govern_batch_final_plan`), so a
    large user result set never materializes on the event loop. Returns a fully materialized
    :class:`QueryResult` only when the statement is a registered-function call (bounded command
    output executed through the shared function hook), otherwise the governed ENGINE/DIRECT plan.

    Raises:
        PermissionError  – role not found or access violation
        ValueError       – SQL parse / validation error
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

    return await _govern_and_route(sql, role_id)


async def execute_pgwire_sql(sql: str, role_id: str) -> QueryResult:  # REQ-266, REQ-267, REQ-272
    """Run *sql* through governance and return a fully materialized result.

    The govern-then-materialize path used by non-streaming pgwire callers (and the DIRECT/admin
    routes, which are async-native and buffer). The streaming ENGINE path splits this via
    :func:`govern_pgwire_plan` + the sync engine terminal instead.

    Raises:
        PermissionError  – role not found or access violation
        ValueError       – SQL parse / validation error
        RuntimeError     – routing / execution error
    """
    res = await govern_pgwire_plan(sql, role_id)
    if isinstance(res, _Plan):
        return await _execute_plan(res)
    return res

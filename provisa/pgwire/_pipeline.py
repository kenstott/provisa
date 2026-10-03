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
pgwire, Bolt and Arrow Flight run it on the connection thread's own event loop, on that thread
(REQ-1882, provisa.core.connection_loop); HTTP/GraphQL/gRPC run it on the process loop.
"""

# Requirements: REQ-262, REQ-263, REQ-264, REQ-265, REQ-266, REQ-267, REQ-272

from __future__ import annotations

import asyncio
import collections
from datetime import datetime
import dataclasses
import functools
import logging
import re
import secrets as _secrets
import threading
import time as _time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from provisa.audit.pipeline import PendingAudit
from provisa.executor.result import QueryResult
from provisa.otel_compat import get_tracer as _get_tracer
from provisa.otel_compat import stage as _stage

if TYPE_CHECKING:
    from provisa.compiler.directives import CacheHint
    from provisa.compiler.pk_bounds import PkBound
    from provisa.executor.redirect import Delivery
    from provisa.transpiler.router import Route

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
    # REQ-1322: the metric expansion in SEMANTIC terms — the grouped aggregate over the registered
    # schema.table names, as the expansion stage produced it and before any physical lowering. This
    # is the only expansion form a user can paste back into an editor: physical_sql names source
    # catalogs, which _reject_physical_source_refs refuses on the way in. None when the statement
    # referenced no metric.
    semantic_sql: str | None = field(default=None)
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
    # REQ-1224 (amended 2026-10-01): set with ``auto_deliver`` when the router sent the buffered
    # read DIRECT. ``sql`` is then bounded at threshold+1 rows — the probe — and this derives the
    # statement's engine-physical form, which only a result that does not fit needs (the engine
    # lands it), so it is not derived for one that does. None on the ENGINE route, where the
    # terminal drains the engine's stream instead.
    engine_landing: Callable[[], Awaitable[str]] | None = field(default=None, repr=False)
    # REQ-1897 (amended 2026-10-01): the response-cache identity of this statement — its governed
    # text and bound values as they stood BEFORE routing. The entry's key is built from these and
    # never from ``sql``/``exec_params``, which are what the chosen route executes (physical text
    # on DIRECT, a probe bound on a buffered read); a key built from them could not be computed
    # before the route is known. None on a plan the planner did not build.
    cache_sql: str | None = field(default=None)
    cache_params: list | None = field(default=None)
    # REQ-1163: the request-level as-of this plan was built for — part of the entry's identity
    # (a statement over a bitemporal view reads different rows at each as-of).
    cache_as_of: str | None = field(default=None)
    # The entry the planner read before routing, on a Route.CACHE plan: the client's pgwire
    # result format codes for a ``pg_datarows`` entry (None for a decoded one) and the store's
    # record. The terminal serves it; nothing was lowered, optimized or routed.
    cache_hit: tuple[list[int] | None, Any] | None = field(default=None, repr=False)
    # The entry kinds the planner already looked for before routing and did not find (None for a
    # decoded entry, the format-code tuple for ``pg_datarows``): the terminal does not read the
    # store for them a second time.
    cache_missed: tuple[tuple[int, ...] | None, ...] = field(default=())
    # REQ-074/REQ-1386: the audit record opened for this statement (acting principal, surface,
    # resolved registered_tables ids, start time). Minted alongside the stamp at the top of the
    # pipeline and finalized with the real status/duration at the terminal — so every surface is
    # audited by the one pipeline instead of each transport calling log_query itself. None when
    # nothing user-initiated is running (seeding, rebuilds); see provisa.audit.context.
    audit: PendingAudit | None = field(default=None)
    # REQ-1915: every replica this statement read, with the completion time (UTC) of the build
    # its read was answered from (``query_residency.Residency.replicas_read``) — the audit
    # record's data age. Empty: no replica was read (a live read, or residency never ran).
    replicas_read: dict[tuple[str, str, str], datetime] = field(default_factory=dict)
    # REQ-1350: what this statement's answer must say about itself (an API answer cut at its
    # max_pages), collected while it was governed (``core.statement_warnings``); every surface
    # reports them in its own warning channel. A warned result is never stored in the response
    # cache.
    warnings: list[Any] = field(default_factory=list)
    # Guards against a second finalize for one statement: the streaming surfaces finalize at their
    # own terminal, and a plan that also passes through _execute_plan must still write one row.
    audit_written: bool = field(default=False)
    # Rows the terminal delivered for this statement, set by the terminal before it finalizes
    # (query_audit_log.row_count). None: not reported — a refused or failed statement.
    row_count: int | None = field(default=None)
    # A streamed result's audit record, built when the stream was opened and written when its
    # drain ends (see finalize_audit's ``defer_to_drain`` and audit_on_drain).
    audit_deferred: Any = field(default=None, repr=False)
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
    # REQ-1865: row-materialize predicate resolution — the concrete PK bound(s) this statement's
    # compiled plan resolved against every row_materialize table it references (empty when the
    # statement touches no such table, or none of its predicates resolve to a bounded PK set).
    # Populated at the same construction points `sources` itself is populated.
    pk_bounds: tuple["PkBound", ...] = field(default_factory=tuple)
    # REQ-1897: whether this plan's result may be served from / written to the raw-SQL response
    # cache — a read with no sink delivery and no EXPLAIN. False (the dataclass default) is the
    # fail-closed answer for any plan a constructor does not positively mark as a cacheable read.
    response_cacheable: bool = field(default=False)
    # REQ-1897: a write statement — on success the steps after a write run (:func:`_after_write`).
    writes_tables: bool = field(default=False)
    # The registered table a write targets (None for a read): what :func:`_after_write` acts on.
    written_table_id: int | None = field(default=None)
    # REQ-1897: the governed role and the registered tables the statement reads/writes — the
    # response-cache key and policy inputs. Carried on the plan itself, not read off ``audit``: a
    # statement with no acting principal (an unsecured Flight ticket) has no audit record.
    role_id: str | None = field(default=None)
    table_ids: tuple[int, ...] = field(default=())
    # REQ-544 (amended 2026-09-30): the request's response-cache OPT-IN (`-- @provisa cache=true`
    # / `cache_ttl=N`; GraphQL @cached on its own endpoint). False — the default — means the plan
    # neither reads nor writes the response cache. cache_ttl is the request's chosen entry
    # lifetime (None = the operator-resolved TTL).
    cache_opt_in: bool = field(default=False)
    cache_ttl: int | None = field(default=None)
    # REQ-1909: ((source_id, max_live_concurrency), ...) for every capped source this plan reads
    # LIVE, and the org whose permit sets they draw from — bound when the plan is minted
    # (_attach_live_caps), so every terminal acquires them without another loop dispatch.
    live_caps: tuple[tuple[str, int], ...] = field(default=())
    live_caps_org: str | None = field(default=None)


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
#
# _STAMP_LOCK: mint/evict is a read-modify-write across two containers (deque + set) that was
# race-free only because governance ran serialized on one shared event loop. Any execution path
# that runs governance on genuine parallel OS threads (per-connection-thread dispatch, see
# pgwire/bolt/flight servers) can call _mint_stamp concurrently, which without this lock can drop
# a just-minted stamp from _ISSUED_SET while another thread's eviction races it, or corrupt the
# deque under concurrent append/popleft-by-index. threading.Lock, not asyncio.Lock: callers include
# plain OS threads, not just event-loop coroutines.
_STAMP_LOCK = threading.Lock()
_ISSUED_STAMPS: collections.deque[str] = collections.deque(maxlen=8192)
_ISSUED_SET: set[str] = set()


def _mint_stamp() -> str:
    """Issue a fresh governed-provenance stamp. Called ONLY from the top of the pipeline."""
    token = _secrets.token_hex(32)
    with _STAMP_LOCK:
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
    table_ids: tuple[int, ...],
    nf_args=None,
    has_json_extract=False,
    is_mutation=False,
):
    """REQ-863 post-governance optimization stage (may REMOVE sources) + routing on the reduced
    set — shared by both governed-SQL entrypoints so routing observes the optimized source set,
    not the pre-optimization one. ``exec_sql`` is the caller's already-lowered SQL (catalog-
    qualified semantic, or compiled catalog-physical); ``governed_sql`` is the pre-optimization
    governed semantic used for source extraction. Returns the optimized exec SQL, the route
    decision, the resolved default source, and whether optimization changed the SQL.

    ``table_ids`` are the registered tables the statement reads, as the pipeline resolved them
    (``audit.pipeline.resolve_table_ids``): the operator's floor is judged by those tables
    (REQ-826), so a statement that reads no replica-served table keeps its direct route."""
    from provisa.api.data.materialization import _materialize_api_to_engine_cache
    from provisa.api_source.engine_cache import rewrite_all_from_cache
    from provisa.cache.values_cte import build_values_cte_sql
    from provisa.compiler.stage2 import extract_sources, reduce_sources_for_routing
    from provisa.transpiler.router import Route, decide_route

    _rewrites, _values_ctes, _dropped = await _materialize_api_to_engine_cache(
        exec_sql, state, nf_args=nf_args, table_ids=table_ids
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
    from provisa.federation.registry_view import operator_floor

    decision = decide_route(
        sources=sources or {default_source},
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        has_json_extract=has_json_extract,
        source_dsns=getattr(state, "source_dsns", None),
        is_mutation=is_mutation,
        operator_floor=operator_floor(state, table_ids),
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


@functools.lru_cache(maxsize=4096)
def _routing_key(
    exec_sql: str, role_id: str, schema_boot_id: str, schema_version: int, replica_generation: int
) -> str:
    """``routing_cache_key`` for these inputs, derived once: the key is a pure function of them,
    and deriving it parses and re-generates the statement — on every execution, to look up a
    cache whose point is to skip per-execution work."""
    from provisa.compiler.compiled_query_cache import routing_cache_key

    return routing_cache_key(exec_sql, role_id, schema_boot_id, schema_version, replica_generation)


async def _kept_lowering(memo: dict[str, Any], lower: Callable[[], str]) -> str:
    """A governed statement lowered to catalog-physical SQL. The lowering is a function of the
    governed text and the role's compilation context — both fixed for a governed statement (a
    kept one answers only while that context is the same object) — so it is derived once and kept
    in the statement's ``memo``; every later execution, whatever its bound values, reuses it.

    REQ-1882: the lowering is sqlglot-parse-based rewrite work; off-loaded (see ``_off_loop``)."""
    lowered = memo.get("catalog_physical")
    if lowered is None:
        lowered = await _off_loop(lower)
        memo["catalog_physical"] = lowered
    return lowered


async def _kept_engine_form(
    memo: dict[str, Any], exec_sql: str, state: Any, derive: Callable[[], Awaitable[Any]]
) -> Any:
    """What the ENGINE branch derives from the routed statement text — the unknown-catalog check,
    the literal-predicate carry (REQ-1880), the catalog fold (REQ-1730) and the engine transpile —
    is a function of that text, the registry and the bound engine. The registry is fixed for a
    kept statement (it answers only within one schema generation), so the product is kept in the
    statement's ``memo`` and answers only for the exact text it was derived from and the engine it
    was derived for: an optimization that rewrites the text (a hot-table inline follows the data)
    or a swapped engine derives it again. A refusal raises and is never kept."""
    engine = state.federation_engine
    kept = memo.get("engine_form")
    if kept is not None and kept[0] == exec_sql and kept[1] is engine:
        return kept[2]
    form = await derive()
    memo["engine_form"] = (exec_sql, engine, form)
    return form


def _kept_span_attrs(
    memo: dict[str, Any],
    semantic_sql: str,
    role_id: str,
    query_text: str,
    audit: PendingAudit | None,
) -> dict[str, str] | None:
    """``_plan_span_attrs`` for a governed statement: its inputs are the statement's own governed
    text, role and request text, so the table/domain attributes are parsed out once and kept in
    its ``memo``. Each plan gets its own dict."""
    if audit is None:
        return None
    attrs = memo.get("span_attrs")
    if attrs is None:
        attrs = _plan_span_attrs(semantic_sql, role_id, query_text, audit)
        memo["span_attrs"] = attrs
    return dict(attrs) if attrs is not None else None


def _kept_refs_view(memo: dict[str, Any], governed_sql: str, view_map: dict) -> bool:
    """Whether the governed statement names a ``__derived__`` view of this view map."""
    kept = memo.get("refs_view")
    if kept is not None and kept[0] is view_map:
        return kept[1]
    import sqlglot
    import sqlglot.expressions as exp

    refs = any(
        t.name in view_map
        for t in sqlglot.parse_one(governed_sql, read="postgres").find_all(exp.Table)
    )
    memo["refs_view"] = (view_map, refs)
    return refs


def _kept_probe_bounds(memo: dict[str, Any], governed_sql: str) -> bool:
    """Whether a buffered DIRECT read of this statement can be bounded at threshold+1 rows
    (REQ-1224): a query. Anything else keeps the engine route the threshold has always used."""
    bounds = memo.get("probe_bounds")
    if bounds is None:
        import sqlglot
        import sqlglot.expressions as exp

        bounds = isinstance(
            sqlglot.parse_one(governed_sql, read="postgres"), (exp.Select, exp.Union)
        )
        memo["probe_bounds"] = bounds
    return bounds


def _direct_terminal_serves(decision: Any, default_source: str, state: Any) -> bool:
    """Whether the router's decision lands on the DIRECT terminal proper — one source on its own
    pooled driver (``_run_plan_terminal``'s last branch), which is the read the threshold probe
    bounds. The admin store and the GovData bridge are not that terminal."""
    from provisa.transpiler.router import Route

    if decision.route != Route.DIRECT:
        return False
    source_id = decision.source_id or default_source
    return (
        source_id != "provisa-admin"
        and state.source_types.get(source_id) != "govdata"
        and state.source_pools.has(source_id)
    )


async def _optimize_and_route_cached(
    exec_sql: str,
    governed_sql: str,
    gov_ctx,
    ctx,
    state,
    role_id: str,
    *,
    table_ids: tuple[int, ...],
    nf_args=None,
    has_json_extract=False,
    is_mutation=False,
):
    """REQ-1877 routing addendum: cache `_optimize_and_route`'s output for the ONE case proven
    safe — see `provisa/compiler/compiled_query_cache.py`'s "ROUTING-DECISION CACHING" section
    before touching this.

    `would_materialize_optimize(exec_sql, state)` (`provisa/api/data/materialization.py`) is
    re-run on EVERY call, hit or miss alike (cheap, no I/O — it mirrors
    `_materialize_api_to_engine_cache`'s own control flow table-by-table using the same in-memory
    lookups). When it returns True, caching is skipped entirely and the full, unmodified
    `_optimize_and_route` runs — identical to pre-addendum behavior, including the live hot-table
    check. Only when it returns False (a structural guarantee that no live/time-varying branch
    inside `_materialize_api_to_engine_cache` can fire for this call) is the routing cache
    consulted; on that path `_optimize_and_route` is itself a pure function of (exec-SQL shape,
    role, schema generation), proven by the module docstring's earlier `validate_sql` write-up
    applying identically to `extract_sources`/`decide_route` (structural, no literal or live
    dependency) once the live branch is ruled out.
    """
    from provisa.api.data.materialization import would_materialize_optimize

    if would_materialize_optimize(exec_sql, state, table_ids=table_ids):
        return await _optimize_and_route(
            exec_sql,
            governed_sql,
            gov_ctx,
            ctx,
            state,
            table_ids=table_ids,
            nf_args=nf_args,
            has_json_extract=has_json_extract,
            is_mutation=is_mutation,
        )

    from provisa.compiler.compiled_query_cache import RoutingOutcome

    _rt_key = _routing_key(
        exec_sql,
        role_id,
        state.schema_boot_id,
        state.schema_version,
        state.replica_routes.generation,
    )
    _cached = state.routing_cache.get(_rt_key)
    if _cached is not None:
        from provisa.transpiler.router import Route, RouteDecision

        decision = RouteDecision(
            route=Route(_cached.route),
            source_id=_cached.source_id,
            dialect=_cached.dialect,
            reason=_cached.reason,
        )
        return exec_sql, decision, _cached.default_source, False, set(_cached.sources), ()

    result = await _optimize_and_route(
        exec_sql,
        governed_sql,
        gov_ctx,
        ctx,
        state,
        table_ids=table_ids,
        nf_args=nf_args,
        has_json_extract=has_json_extract,
        is_mutation=is_mutation,
    )
    _exec_sql_out, decision, default_source, optimized, sources, opt_labels = result
    # Guaranteed by would_materialize_optimize(exec_sql, state) being False (see module docstring):
    # no rewrite/inline/drop branch had anything to act on, so exec_sql passed through unchanged
    # and no optimization fired. Only cache when that guarantee actually held for THIS call — a
    # defensive check, never expected to be False, but a correctness invariant is never trusted
    # unverified.
    if not optimized and _exec_sql_out == exec_sql and not opt_labels:
        state.routing_cache.put(
            _rt_key,
            RoutingOutcome(
                route=str(
                    decision.route.value if hasattr(decision.route, "value") else decision.route
                ),
                source_id=decision.source_id,
                dialect=decision.dialect,
                reason=decision.reason,
                default_source=default_source,
                sources=frozenset(sources),
            ),
        )
    return result


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

    from provisa.core.request_context import active_env
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


def _refuse_composed_mutators(tree, commands: dict) -> None:
    """REQ-1924: a source's write operation is an action, called on its own. Composed in a larger
    statement -- a join, a subquery, a view or a materialized view whose definition holds it --
    it would perform the write each time the statement is read or refreshed, so it is refused."""
    from provisa.executor.source_operation import writes_called_in

    called = writes_called_in(tree, commands)
    if called:
        raise PermissionError(
            f"command {called[0]!r} writes to its source and is called on its own: it cannot be "
            "composed in a query, a view or a materialized view (REQ-1924)"
        )


async def _localize_inline_commands(tree, role_id: str, state) -> bool:
    """REQ-1159: rewrite every inline command call in ``tree`` to a typed local relation, in place.

    Each command executes via the ONE shared governed executor (invoke_tracked_function) — its input
    governance (DEFINER/INVOKER) and I/O dataset contract are enforced there, identically to a direct
    call — so the outer statement only ever sees ordinary local relations. Returns True on any hit
    (the caller then forces engine execution). No-op when no command is composed in the statement."""
    from provisa.api.data.action_exec import invoke_tracked_function, usable_commands
    from provisa.executor.command_localize import localize_commands

    # Only the commands this role may call: one it may not reads as an unregistered relation.
    commands = usable_commands(state, role_id, webhooks=False)
    if not commands:
        return False

    _refuse_composed_mutators(tree, commands)

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
    from provisa.core.request_context import current_org
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


async def resolve_trace_scope(state: Any, role_id: str, *, hint: bool) -> None:  # REQ-1910
    """Request entry: decide this request's trace detail from the operator's debug-trace settings
    (a window on its org or role) and its own hint.

    Every request resolves here and either binds debug or unbinds, so a connection that served a
    debug statement does not carry the level into its next one. A hint from a role the operator
    has not permitted raises ``DebugTraceHintNotPermitted`` (REQ-030)."""
    from provisa.core.trace_scope import request_is_debug
    from provisa.otel_compat import clear_trace_detail, set_trace_detail

    if await request_is_debug(state, role_id, hint=hint):
        set_trace_detail("debug")
    else:
        # Not "normal": a request no window covers gets the deployment's own default detail.
        clear_trace_detail()


async def extend_trace_scope_to_sources(  # REQ-1910
    state: Any, role_id: str, source_ids: frozenset[str]
) -> None:
    """Once the request's sources are known: a window opened on one of them makes this a debug
    request from here on. Only ever raises the level — the entry resolution already set it."""
    from provisa.core.trace_scope import request_is_debug
    from provisa.otel_compat import set_trace_detail

    if await request_is_debug(state, role_id, hint=False, source_ids=source_ids):
        set_trace_detail("debug")


async def _attach_live_caps(plan: _Plan, state: Any) -> _Plan:
    """Bind the capped sources this plan reads live (REQ-1909) — at the top of the pipeline, the
    one place every surface passes through, so no terminal can skip the cap."""
    if plan.cache_hit is not None:
        # REQ-1897: answered from the response cache before routing — no source is read, live or
        # otherwise, so there is no cap to bind and nothing is looked up.
        return plan
    from provisa.federation.live_concurrency import live_caps_for_plan

    org_id, capped = await live_caps_for_plan(state, plan)
    plan.live_caps, plan.live_caps_org = tuple(capped), org_id
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


_BINDS_PARAMS_BY_NUMBER = frozenset({"duckdb", "postgres"})


async def propagate_literal_predicates(sql: str, state: Any) -> str:  # REQ-1880
    """Carry each literal WHERE predicate onto a table the query reaches through an INNER equi-join
    or a correlated select-list subquery (a GraphQL nested relationship), when that table's
    connector pushes a literal predicate down but has no join/parameterized-path pushdown
    (``predicate_pushdown and not join_pushdown`` -- e.g. DuckDB's Mongo scan, REQ-1871's
    finding). Without it such a connector sees no filter at all and scans its whole collection.

    The ONE place every engine-bound surface applies this: the raw-SQL path, the compiled path
    (GraphQL-via-plan/Cypher/Flight/gRPC/JSON:API) and the GraphQL engine terminal each call it on
    the already-governed catalog-physical SQL, before the catalog fold/transpile. A pure rewrite:
    each added predicate is implied by the query's own predicates, so results are unchanged."""
    import sqlglot
    import sqlglot.expressions as exp

    tree = sqlglot.parse_one(sql, read="postgres")
    if tree.find(exp.Join) is None and tree.find(exp.Subquery) is None:
        return sql
    referenced = {(t.db.lower(), t.name.lower()) for t in tree.find_all(exp.Table) if t.db}
    if not referenced:
        return sql
    from provisa.federation.registry_view import registered_sources, registered_tables

    sources_by_id = {s.id: s for s in await registered_sources(state)}
    eligible: set[tuple[str, str]] = set()
    column_types: dict[tuple[str, str, str], str] = {}
    for t in await registered_tables(state):
        phys = (t.schema_name.lower(), t.table_name.lower())
        if phys not in referenced:
            continue
        for col in t.columns:
            if col.data_type:
                column_types[(*phys, col.name.lower())] = col.data_type
        src = sources_by_id.get(t.source_id)
        if src is None:
            continue
        cap = state.federation_engine.connector_pushdown(src.type.value)
        if cap.predicate_pushdown and not cap.join_pushdown:
            eligible.add(phys)
    if not eligible:
        return sql
    from provisa.compiler.sql_rewrite import propagate_literal_join_predicates

    # A bind parameter ($N) may be copied only where the engine's runtime binds BY NUMBER —
    # DuckDBFederationRuntime natively, PgFederationRuntime via psycopg2 named %(pN)s slots
    # (pg_runtime._psycopg2_exec_args). Keyed on the engine NAME, not its dialect: the SQLAlchemy
    # engine can report a postgres-family dialect yet binds positionally (exec_driver_sql tuple),
    # as do Trino/ClickHouse/warehouse drivers (compiler.params.substitute_positional_placeholders),
    # where a reused $1 would shift every later parameter.
    by_number = state.federation_engine.engine.name in _BINDS_PARAMS_BY_NUMBER
    return propagate_literal_join_predicates(
        sql, "postgres", eligible, column_types, allow_params=by_number
    )


async def _off_loop(fn, *args, **kwargs):
    """REQ-1882: run a synchronous, CPU-bound call (sqlglot parsing, the regex-based SQL-rewrite
    passes) on the default thread pool executor instead of in-line on the caller's event loop.

    Every call site this wraps was named by a live py-spy dump of the running server under
    concurrent load (docs/arch/requirements.yaml, REQ-1882): one expensive governed query's
    tokenize/rewrite work, run in-line on the ONE shared event loop every governed query's
    ``_run_on_loop`` dispatch (``provisa/api/flight/server.py``) passes through, blocked every
    other concurrent request's governance step for its duration. It moves the blocking call off
    whichever loop is running, via that loop's own default executor.

    (Amended 2026-09-29) pgwire/Bolt/Flight now run each request on its connection thread's own
    loop, whose default executor runs work INLINE on that thread (provisa.core.connection_loop):
    that loop serves one connection, so the call blocks no one else and the request never leaves
    its thread. On the shared process loop (HTTP/GraphQL/gRPC) this still offloads to the pool.
    """
    loop = asyncio.get_running_loop()
    if kwargs:
        from functools import partial

        fn = partial(fn, **kwargs)
        return await loop.run_in_executor(None, fn, *args)
    return await loop.run_in_executor(None, fn, *args)


async def _govern_and_route(
    sql: str,
    role_id: str,
    *,
    session_vars: dict[str, str] | None = None,
    as_of: str | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    explain: bool | None = None,
    params: list | None = None,
    serve_cached: bool = False,
    wire_formats: list[int] | None = None,
) -> _Plan:
    """The top of the ONE pipeline: govern, route, then bind the org's tier ceilings (REQ-1044).

    ``serve_cached`` / ``wire_formats``: see :func:`route_governed`."""
    from provisa.api.app import state

    from provisa.core.statement_warnings import collecting

    await _wake_before_governing(state)
    with collecting() as found:
        plan = await _govern_and_route_planned(
            sql,
            role_id,
            session_vars=session_vars,
            as_of=as_of,
            deliver=deliver,
            buffered=buffered,
            explain=explain,
            params=params,
            serve_cached=serve_cached,
            wire_formats=wire_formats,
        )
    plan.warnings = list(found)
    return await _attach_live_caps(await _attach_tier_caps(plan, state), state)


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
    # REQ-589: a client's bound parameter values for the statement's $N placeholders (pgwire's
    # extended protocol). They stay BOUND through governance and execution, exactly like the
    # provisa-params comment's values (they become the same embedded_params) — never spliced into
    # the SQL, so a value can neither change the governed shape nor defeat SQL-text-keyed caches.
    params: list | None = None,
    serve_cached: bool = False,
    wire_formats: list[int] | None = None,
) -> _Plan:  # REQ-262, REQ-263, REQ-264, REQ-266, REQ-267, REQ-272, REQ-1120, REQ-1159, REQ-1163
    """Govern, then route: the two stages of the one pipeline, run back to back."""
    if explain is not None and opening_write_verb(sql) is not None:
        # A write is never explained — EXPLAIN ANALYZE would perform it — whatever the role may
        # write: said before the statement is admitted, so the answer does not depend on rights.
        raise ValueError("EXPLAIN is only supported for read statements")
    governed = await govern_statement(sql, role_id, session_vars=session_vars)
    return await route_governed(
        governed,
        params=params,
        as_of=as_of,
        deliver=deliver,
        buffered=buffered,
        explain=explain,
        serve_cached=serve_cached,
        wire_formats=wire_formats,
    )


@dataclass
class _Governed:
    """A statement the pipeline has GOVERNED but not yet routed (REQ-589, amended 2026-10-01).

    Everything here is independent of the statement's bound parameter values and of live routing
    state: the validated parse, the role's governance context, and the governed semantic SQL (RLS,
    masking, visibility, row cap applied). ``route_governed`` turns it into an executable plan once
    the values are known. pgwire holds one between a prepared statement's Describe and its Execute,
    so the statement is governed once and its Describe is answered from this alone."""

    sql: str
    role_id: str
    role: dict | None
    ctx: Any
    gov_ctx: Any
    comment_params: list | None
    parsed: Any
    metric_semantic_sql: str | None
    table_ids: tuple[int, ...]
    governed_semantic: str
    # The schema generation it was governed under; a rebuild since then makes it stale.
    schema_generation: tuple[Any, Any]
    # Governed-provenance: minted by govern_statement, verified by route_governed.
    stamp: str
    # Facts derived from this governed statement, kept with it (pgwire's result shape).
    memo: dict[str, Any] = field(default_factory=dict)


def _calls_a_registered_command(tree: Any, state: Any) -> bool:
    """Whether the statement invokes a tracked function or webhook: inline-command localization
    RUNS such a command while preparing the statement, so its governed form is per call."""
    import sqlglot.expressions as exp

    commands = {
        **(getattr(state, "tracked_functions", None) or {}),
        **(getattr(state, "tracked_webhooks", None) or {}),
    }
    return bool(commands) and any(n.name in commands for n in tree.find_all(exp.Anonymous))


def governed_statement_is_current(governed: _Governed, state: Any) -> bool:
    """Whether ``governed`` may still be routed: minted by this process's ``govern_statement`` and
    governed under the schema generation that is still live. A caller holding a stale one governs
    the statement again."""
    return stamp_is_valid(governed.stamp) and governed.schema_generation == (
        state.schema_boot_id,
        state.schema_version,
    )


async def _guard_complexity(
    sql: str, role_id: str, tree: Any, gov_ctx: Any, ctx: Any, state: Any
) -> None:  # REQ-1174
    """The complexity guard, at the semantic layer of the pipeline: the statement is parsed and
    its governance context built, and nothing has been governed or routed. Both governing stages
    call it, so every surface that lowers to a statement is held to the same limit by the same
    measure (provisa.compiler.complexity). A statement over the limit is a refusal like any
    other: it is recorded as a denial and raised."""
    from provisa.audit.pipeline import write_denial
    from provisa.compiler.complexity import ComplexityLimitExceeded, guard_complexity

    try:
        guard_complexity(tree, gov_ctx, ctx, getattr(state, "roles", {}).get(role_id))
    except ComplexityLimitExceeded:
        await write_denial(sql, role_id, tree, gov_ctx, state)  # REQ-1386
        raise


async def govern_statement(
    sql: str,
    role_id: str,
    *,
    session_vars: dict[str, str] | None = None,
) -> _Governed:
    """Stage one of the one pipeline: parse, validate and apply governance. Value-independent."""
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.params import extract_params_comment, extract_relationship_guard_comment
    from provisa.compiler.stage2 import apply_governance, build_governance_context
    from provisa.compiler.sql_validator import validate_sql

    from provisa.audit.pipeline import write_denial

    if role_id not in state.contexts:
        # REQ-1386: a refusal is auditable evidence — policy_denials reports on it.
        await write_denial(sql, role_id, None, None, state)
        raise PermissionError(f"No schema for role {role_id!r}")

    # REQ-1910: request entry on the raw-SQL path — the governance stage is traced at the level
    # the operator's windows and the statement's own `-- @provisa trace=debug` hint resolve to.
    from provisa.compiler.directives import cache_hint_for

    await resolve_trace_scope(state, role_id, hint=cache_hint_for("sql", sql).debug_trace)

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())
    # REQ-1620: domain_access follows the UNION of every role the caller is acting as ("Role:
    # All"); role_id/RLS/masking/capabilities stay single-role. See
    # security.rights.effective_domain_access_role — the one place this is computed, so every
    # surface that reaches this pipeline resolves "All" identically.
    from provisa.security.rights import effective_domain_access_role

    role = effective_domain_access_role(role_id, state.roles)

    # REQ-1877: governing is a pure function of the statement text, the role, the person, the
    # session variables RLS resolves against and the role's governance objects — none of them the
    # statement's bound values — so a governed statement is kept in the org's compiled-query cache
    # (generation-keyed, TTL-evicted, bounded) and a repeat is not parsed, validated or governed
    # again. Every raw-SQL surface reaches the pipeline here, so they all share it.
    from provisa.pgwire.governed_plan import PlanSlot, acting_role_set
    from provisa.core.request_context import session_vars_for

    _session_vars = session_vars if session_vars is not None else session_vars_for(role)
    _slot = PlanSlot(state, "sql", role_id, sql, sorted(_session_vars.items()))
    _kept = _slot.cached()
    if _kept is not None:
        # A fresh provenance stamp per use: the stamp ring is bounded and a kept statement outlives it.
        return dataclasses.replace(_kept, stamp=_mint_stamp())

    raw_sql, embedded_params = extract_params_comment(sql)
    raw_sql, sql_opts_out = extract_relationship_guard_comment(raw_sql)

    # REQ-1866: parse + REQ-1159's inline-command-localization check + REQ-1317's metric
    # expansion — the pure pre-governance stage, cached by (role, exact SQL text, schema
    # generation) when the earlier call found no inline command to localize (a hit that DID
    # localize a command is never cached — see prepare_front_end's own docstring for why).
    # Governance/routing below this point are untouched and always run per call, on every hit
    # or miss alike.
    from provisa.compiler.prepared import prepare_front_end

    from provisa.compiler.definitions import DefinitionNotAvailable

    try:
        _front = await prepare_front_end(raw_sql, role_id, state, _localize_inline_commands)
    except DefinitionNotAvailable:
        # Not a parse error: a definition statement, refused as itself on every surface.
        raise
    except Exception as exc:
        raise ValueError(f"SQL parse error: {exc}") from exc
    normalized_sql = _front.normalized_sql
    _parsed_input = _front.parsed
    _metric_semantic_sql = _front.metric_semantic_sql

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
    await _guard_complexity(sql, role_id, _parsed_input, gov_ctx, ctx, state)

    from provisa.security.rights import Capability, has_capability

    _role_guard = role.get("relationship_guard", True)
    _bypass_guard = has_capability(role, Capability.IGNORE_RELATIONSHIPS) or (
        (not _role_guard) and sql_opts_out
    )
    # REQ-693: high-security mode is belts and suspenders — the relationship guard is not
    # bypassable there at all. A deployment that improperly granted ignore_relationships (or
    # cleared relationship_guard) to a production role does not get a break-out; the grant is
    # ignored and every join must exist in the approved relationship catalog.
    if getattr(state, "security_high", False):
        _bypass_guard = False

    # REQ-1877: in-memory, TTL-evicted cache of the validate_sql + domain-access outcome — see
    # provisa/compiler/compiled_query_cache.py for the read-verified scope decision (routing/
    # physical-SQL caching is NOT done here: it is entangled with live hot-table state that must
    # be rechecked every call, per this task's own constraints). Both checks are pure functions
    # of (SQL shape, role, schema generation, relationship-guard bypass) — no query-literal or
    # live-state dependency — so a HIT means only "an identical shape already validated clean for
    # this role under this schema generation," never a stale allow.
    from provisa.audit.context import current_audit_identity
    from provisa.compiler.compiled_query_cache import CompiledOutcome, compiled_query_cache_key

    _cq_identity = current_audit_identity()
    _cq_person_id = _cq_identity.user_id if _cq_identity is not None else None
    _cq_key = compiled_query_cache_key(
        normalized_sql,
        role_id,
        _cq_person_id,
        state.schema_boot_id,
        state.schema_version,
        _bypass_guard,
        acting_roles=acting_role_set(),
    )
    if state.compiled_query_cache.get(_cq_key) is None:
        violations = validate_sql(
            normalized_sql,
            ctx,
            gov_ctx,
            role,
            getattr(state, "tables", []),
            bypass_relationship_guard=_bypass_guard,
            bypass_uncovered_relationships=True,
        )

        _role_domain_access = role["domain_access"]
        if "*" not in _role_domain_access:
            try:
                # REQ-1882: off-loaded, same rationale as prepare_front_end's parse.
                parsed_tree = await _off_loop(
                    lambda: sqlglot.parse_one(normalized_sql, read="postgres")
                )
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

        state.compiled_query_cache.put(_cq_key, CompiledOutcome())

    from provisa.audit.pipeline import resolve_table_ids

    _table_ids = tuple(resolve_table_ids(_parsed_input, gov_ctx))  # REQ-1897

    # REQ-272: apply_governance enforces full Stage-2 governance on this SQL path — RLS,
    # masking, visibility, and the role row-cap ceiling (gov_ctx carries the role, so
    # resolve_row_cap applies). Statistical sampling is the GraphQL `sample` arg → TABLESAMPLE,
    # a query-construction feature with no equivalent for already-formed raw SQL, so it is N/A
    # here; there is no ungoverned access path.
    # REQ-863 pipeline order: governance → post-governance optimization → routing.
    # REQ-1882: apply_governance re-parses+transforms the statement's AST (sqlglot) synchronously;
    # off-load it (see _off_loop's own docstring).
    governed_semantic = await _off_loop(
        apply_governance, normalized_sql, gov_ctx, _session_vars, embedded_params or None
    )

    # REQ-1120/REQ-1682: resolve RLS session predicates (current_setting('provisa.<var>')) to
    # SQL literals on EVERY route. A caller that supplies session vars out-of-band (the airport
    # Flight service) wins; otherwise the request's bindings over the role's constants. Nothing
    # SETs the variable on a direct Postgres connection, so a native current_setting there raises
    # "unrecognized configuration parameter"; the literal is the one mechanism every route shares.
    # A missing var becomes NULL, the documented deny-by-default (_resolve_session_settings).
    governed_semantic = _resolve_session_settings(governed_semantic, _session_vars)

    governed = _Governed(
        sql=sql,
        role_id=role_id,
        role=role,
        ctx=ctx,
        gov_ctx=gov_ctx,
        comment_params=embedded_params,
        parsed=_parsed_input,
        metric_semantic_sql=_metric_semantic_sql,
        table_ids=_table_ids,
        governed_semantic=governed_semantic,
        schema_generation=(state.schema_boot_id, state.schema_version),
        stamp=_mint_stamp(),
    )
    # Not kept: a write (its admission checks run per call) and a statement that runs a registered
    # command while it is prepared. The slot itself refuses one governed while a schema rebuild
    # was moving the state it read.
    if not isinstance(
        _parsed_input, (exp.Insert, exp.Update, exp.Delete, exp.Merge)
    ) and not _calls_a_registered_command(_parsed_input, state):
        _slot.keep(governed)
    return governed


async def route_governed(
    governed: _Governed,
    *,
    params: list | None = None,
    as_of: str | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    explain: bool | None = None,
    serve_cached: bool = False,
    wire_formats: list[int] | None = None,
) -> _Plan:
    """Stage two of the one pipeline: bind the statement's parameter values, optimize, route and
    build the executable plan. Runs against live state, so it runs once per execution; it accepts
    only a statement ``govern_statement`` produced.

    ``serve_cached`` (REQ-1897, amended 2026-10-01): the caller's terminal serves a Route.CACHE
    plan (``cached_result`` / ``_execute_plan``). The response cache is then read HERE, before any
    lowering or routing, and an opted-in read that hits comes back as that plan. A caller that
    does not say so always gets a routed plan and its terminal reads the cache as before.
    ``wire_formats`` — pgwire's result format codes for this execution — also looks for the
    passthrough entry written for those codes. EXPLAIN, a sink delivery and a write are never
    answered from the cache."""
    import sqlglot.expressions as exp

    from provisa.api.app import state
    from provisa.audit.pipeline import begin_audit
    from provisa.transpiler.router import Route
    from provisa.transpiler.transpile import transpile

    if not stamp_is_valid(governed.stamp):
        raise PermissionError(
            "ungoverned statement rejected: route_governed accepts only what govern_statement "
            "produced"
        )
    sql = governed.sql
    role_id = governed.role_id
    ctx = governed.ctx
    gov_ctx = governed.gov_ctx
    _parsed_input = governed.parsed
    _metric_semantic_sql = governed.metric_semantic_sql
    _table_ids = governed.table_ids
    governed_semantic = governed.governed_semantic

    embedded_params = governed.comment_params
    if params is not None:
        if embedded_params:
            raise ValueError(
                "statement carries both a provisa-params comment and bound parameters — "
                "supply the parameter values one way"
            )
        embedded_params = list(params)

    # REQ-074/REQ-1386: open the audit record for this execution of the governed statement. The
    # terminal finalizes it with the real status and duration.
    _audit = begin_audit(sql, role_id, _parsed_input, gov_ctx, state.model_stamp)

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
    from provisa.compiler.write_admission import written_table_id

    _written_table_id = written_table_id(_parsed_input, governed.gov_ctx) if _is_mutation else None
    # REQ-1897: a read whose result is rows — not a write, an EXPLAIN, or a sink delivery.
    _raw_cacheable = not _is_mutation and explain is None and deliver is None
    # REQ-544 (amended 2026-09-30): the response cache is per-request opt-in — a `-- @provisa
    # cache=true` / `cache_ttl=N` comment on the statement; without one no read and no write.
    from provisa.compiler.directives import cache_hint_for

    _cache_hint = cache_hint_for("sql", sql)
    # REQ-1910: resolved per EXECUTION, not only when the statement was governed — a prepared
    # statement is governed once and executed many times, across windows opening and closing.
    await resolve_trace_scope(state, role_id, hint=_cache_hint.debug_trace)

    # REQ-1897 (amended 2026-10-01): the cache before the route. Its key is the governed
    # statement, its bound values and the role, all known now.
    _cache_params = list(embedded_params or [])
    # REQ-1915: the key bounds — and the refusal of a read of a row-level table that binds no
    # key — before the cache and the route, so no surface answers such a read from anywhere.
    # ``embedded_params`` are the values the governed statement's own placeholders number.
    _pk_bounds_now = _kept_pk_bounds(
        governed.memo, governed_semantic, state, embedded_params or None
    )
    _cache_missed: tuple[tuple[int, ...] | None, ...] = ()
    if serve_cached and _raw_cacheable:
        _hit, _cache_missed = await _cached_before_routing(
            state,
            sql=governed_semantic,
            params=_cache_params,
            role_id=role_id,
            cache_hint=_cache_hint,
            wire_formats=wire_formats,
            as_of=as_of,
        )
        if _hit is not None:
            return _cached_plan(
                governed_sql=governed_semantic,
                params=embedded_params or None,
                role_id=role_id,
                table_ids=_table_ids,
                cache_hint=_cache_hint,
                hit=_hit,
                audit=_audit,
                span_attrs=_kept_span_attrs(governed.memo, governed_semantic, role_id, sql, _audit),
                sources=governed.memo.get("sources", frozenset()),
                semantic_sql=_metric_semantic_sql,
                as_of=as_of,
            )

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

    # REQ-1882: both stages are sqlglot-parse-based regex/AST rewrite work; off-load the combined
    # call (see _off_loop's own docstring).
    _physical_sql = await _kept_lowering(
        governed.memo,
        lambda: rewrite_semantic_to_catalog_physical(
            normalize_table_refs(governed_semantic, ctx), ctx
        ),
    )
    _physical_sql, _nf_clean_params, _extracted_nf = extract_nf_args(
        _physical_sql, embedded_params or []
    )
    exec_params = (
        _nf_clean_params if _nf_clean_params != (embedded_params or []) else embedded_params
    )
    _nf_args = _extracted_nf or None

    (
        _qualified,
        decision,
        _default_source,
        _optimized,
        _sources,
        _opts,
    ) = await _optimize_and_route_cached(
        _physical_sql,
        governed_semantic,
        gov_ctx,
        ctx,
        state,
        role_id,
        table_ids=_table_ids,
        has_json_extract="->>" in governed_semantic,
        is_mutation=_is_mutation,
        nf_args=_nf_args,
    )
    # REQ-1910: the sources are known now — a window opened on one of them covers this request.
    await extend_trace_scope_to_sources(state, role_id, frozenset(_sources))
    governed.memo["sources"] = frozenset(_sources)  # what a later cache hit reports it read

    exec_params = exec_params or None

    # REQ-135/REQ-1163: a query referencing a __derived__ view MUST route through the engine, where the
    # view is inline-expanded. A view's virtual source has no native driver/catalog, so extract_sources
    # cannot bind it and routing would otherwise pick DIRECT against a real source, handing the
    # un-expanded view ref to a native pool. Force ENGINE so the ENGINE branch expands it.
    _view_map = getattr(state, "view_sql_map", None)
    if _view_map and decision.route != Route.ENGINE:
        if _kept_refs_view(governed.memo, governed_semantic, _view_map):
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
    # REQ-1224 (amended 2026-10-01): the threshold does not choose the route. A read the router
    # sends to one source's own driver stays there and is bounded by a threshold+1 probe at the
    # terminal; the engine-physical form is derived only if the result does not fit.
    _probe_direct = (
        auto_deliver is not None
        and not _is_mutation
        and _direct_terminal_serves(decision, _default_source, state)
        and _kept_probe_bounds(governed.memo, governed_semantic)
    )
    if auto_deliver is not None and decision.route != Route.ENGINE and not _probe_direct:
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
    #
    # (Removed 2026-09-26, REQ-1864 reversal:) a single-source neo4j pattern (e.g.
    # Customer-[:PLACED]->Order-[:CONTAINS]->Product, all-neo4j) previously reverse-compiled the
    # governed SQL back to Cypher (best_effort_cypher_for_sql) and forced Route.DIRECT so Neo4j
    # itself could resolve the join. Reverted: direct Cypher execution requires a general method to
    # resolve arbitrary SQL join patterns (cardinality, relationship direction, multi-hop shape)
    # back into a correct Cypher MATCH — the reverse compiler got this wrong for a reshaped junction
    # table (bench_contains_edge's nested items array), producing a Cypher query with a
    # non-existent property reference. Row-level materialization (REQ-1865) is the sanctioned,
    # narrower mechanism for neo4j read performance: it caches individual rows by a trusted,
    # declared PK, never reconstructs a join. A single-source neo4j query now always falls through
    # to Route.ENGINE (materialize-then-join), unconditionally.
    async def _engine_physical(_qualified: str) -> str:
        """The routed catalog-physical statement in the engine's own SQL (see
        :func:`_kept_engine_form`, which keeps it with the governed statement)."""
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
        # REQ-1880: carry a literal WHERE predicate onto a table whose connector pushes literal
        # predicates but not join/parameterized paths -- see propagate_literal_predicates.
        _qualified = await propagate_literal_predicates(_qualified, state)
        # REQ-1881: wrap ClickHouse LowCardinality(String)-family column references in
        # from_utf8(...) when this route lands on Trino AND the referenced table's own registered
        # source is clickhouse-typed. Trino's ClickHouse JDBC connector/driver reports these
        # columns as raw dictionary-encoded VARBINARY bytes with no type-mapping for the wrapper
        # at all (verified live: connector bytecode has zero "LowCardinality" references);
        # CAST(col AS varchar) does not work either ("Cannot cast varbinary to varchar" --
        # varbinary/varchar aren't cast-compatible in Trino), from_utf8() is the only verified
        # decode. Gated strictly on engine dialect + source type + column data_type (see
        # is_clickhouse_lowcardinality_string) -- never applied to any other engine or source
        # type, this is a Trino/ClickHouse-JDBC-driver-specific bug, not general behavior. Runs
        # unconditionally (no join required, unlike REQ-1880 above -- a bare SELECT on an affected
        # column needs the same wrap), on the same already-governed physical-ish SQL text, BEFORE
        # the catalog-fold/transpile below.
        if state.federation_engine.dialect == "trino":
            _referenced_phys_ch = {
                (_tbl.db.lower(), _tbl.name.lower())
                for _tbl in _tree.find_all(_exp.Table)
                if _tbl.db
            }
            if _referenced_phys_ch:
                from provisa.compiler.sql_rewrite import is_clickhouse_lowcardinality_string
                from provisa.federation.registry_view import (
                    registered_sources,
                    registered_tables,
                )

                _sources_by_id_ch = {s.id: s for s in await registered_sources(state)}
                _affected_lowcard_cols: dict[tuple[str, str], set[str]] = {}
                for _t in await registered_tables(state):
                    _phys = (_t.schema_name.lower(), _t.table_name.lower())
                    if _phys not in _referenced_phys_ch:
                        continue
                    _src = _sources_by_id_ch.get(_t.source_id)
                    if _src is None or _src.type.value != "clickhouse":
                        continue
                    for _col in _t.columns:
                        if is_clickhouse_lowcardinality_string(_col.data_type):
                            _affected_lowcard_cols.setdefault(_phys, set()).add(_col.name.lower())
                if _affected_lowcard_cols:
                    from provisa.compiler.sql_rewrite import wrap_lowcardinality_columns

                    _qualified = wrap_lowcardinality_columns(
                        _qualified, "postgres", _affected_lowcard_cols
                    )
        # REQ-1730: this ENGINE route's own catalog-qualification (unlike Route.DIRECT's own
        # `strip_catalog`, applied unconditionally a few lines below in the other branch) was never
        # engine-aware — every engine got a catalog.schema.table physical reference regardless of
        # whether its own SQL dialect can express one at all. Verified live: PostgreSQL genuinely
        # cannot (no cross-database queries, full stop), so `pg`'s own declared
        # `catalog_qualified=False` (FederationEngine's own doc has the reproduction) folds the
        # catalog into the schema name here instead of a bare `strip_catalog` — the catalog still
        # carries REAL per-source disambiguating information on this route (two sources sharing a
        # native schema_name would otherwise collide), unlike Route.DIRECT's single live-attached
        # source, where the catalog is genuinely redundant and a plain drop is safe.
        if not state.federation_engine.engine.catalog_qualified:
            from provisa.compiler.sql_rewrite import fold_catalog_into_schema

            _qualified = fold_catalog_into_schema(_qualified)
        return state.federation_engine.transpile_physical(_qualified)

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
            # What each view reference becomes for THIS reader (mv/view_read.py): the view's SQL
            # with the reader's rules on every table it reads, or — for a materialized view and a
            # reader with no narrower rule on any of its inputs — its stored rows.
            from provisa.mv.view_read import view_bodies

            _qualified = expand_view_refs(
                _qualified,
                view_bodies(_qualified, _vmap, state, gov_ctx),
            )
            # View bodies are stored in semantic form; after expansion, lower any
            # newly-introduced semantic refs to catalog-physical (same pass the outer SQL
            # went through at line 456 before routing).
            _qualified = rewrite_semantic_to_catalog_physical(
                normalize_table_refs(_qualified, ctx), ctx
            )

        _routed_sql = _qualified
        physical_sql = await _kept_engine_form(
            governed.memo, _routed_sql, state, lambda: _engine_physical(_routed_sql)
        )
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
            semantic_sql=_metric_semantic_sql,  # REQ-1322
            materialize=deliver,  # REQ-1194/REQ-1195: sink delivery inherited by every transport
            auto_deliver=auto_deliver,  # REQ-1224: buffered-transport auto threshold (terminal decides)
            span_attrs=_kept_span_attrs(governed.memo, governed_semantic, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(_sources),
            route_reason=decision.reason,
            optimizations=_opts,
            pk_bounds=_pk_bounds_now,  # REQ-1865
            response_cacheable=_raw_cacheable,  # REQ-1897
            cache_sql=governed_semantic,
            cache_params=_cache_params,
            cache_as_of=as_of,
            cache_missed=_cache_missed,
            writes_tables=_is_mutation,  # REQ-1897
            written_table_id=_written_table_id,
            role_id=role_id,  # REQ-1897
            table_ids=_table_ids,  # REQ-1897
            cache_opt_in=_cache_hint.opt_in,  # REQ-544 (amended)
            cache_ttl=_cache_hint.ttl,
        )
    else:
        dialect = decision.dialect or "postgres"
        # Direct route lowers the OPTIMIZED SQL when the optimization stage changed it (REQ-863),
        # carrying any inlined VALUES CTE onto the direct path; else the unchanged fast path.
        from provisa.compiler.sql_rewrite import FLAT_NAMESPACE_SOURCES, strip_schema

        _direct_sid = decision.source_id or _default_source
        _flat = state.source_types.get(_direct_sid) in FLAT_NAMESPACE_SOURCES
        # REQ-1224: a buffered read is bounded at threshold+1 rows for the probe — the same
        # bound governance applies for a role's row ceiling (stage2.apply_row_cap).
        _cap = auto_deliver.config.threshold + 1 if _probe_direct and auto_deliver else None
        # Lower the semantic model to physical schema.table for the native driver — same as
        # _govern_and_route_compiled's DIRECT branch. Passing governed_semantic verbatim sent an
        # unresolved semantic ref (e.g. "pet_store"."inquiries") to the source.
        # The unoptimized lowering is a function of the governed text, the role's compilation
        # context and the destination — not of the bound values — so it is kept with the governed
        # statement like its catalog-physical form (see _kept_lowering).
        _direct_key = f"direct_sql\x00{_direct_sid}\x00{dialect}\x00{_flat}\x00{_cap}"
        _direct = None if _optimized else governed.memo.get(_direct_key)
        if _direct is None:
            if _optimized:
                from provisa.compiler.sql_rewrite import strip_catalog

                _physical = strip_catalog(_qualified)
            else:
                from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical

                _physical = rewrite_semantic_to_physical(governed_semantic, ctx)
            if _flat:
                _physical = strip_schema(_physical)
            if _cap is not None:
                from provisa.compiler.stage2 import apply_row_cap

                _physical = apply_row_cap(_physical, _cap)
            _direct = transpile(_physical, dialect)
            if not _optimized:
                governed.memo[_direct_key] = _direct
        sql_to_run = _direct
        _routed_sql = _qualified

        async def _engine_landing() -> str:
            return await _kept_engine_form(
                governed.memo, _routed_sql, state, lambda: _engine_physical(_routed_sql)
            )

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
            semantic_sql=_metric_semantic_sql,  # REQ-1322
            auto_deliver=auto_deliver,  # REQ-1224: probed at the terminal
            engine_landing=_engine_landing if _probe_direct else None,
            # REQ-1425: every route carries the plan's span attributes, so the ops queries report
            # covers pushed-down single-source statements identically to federated ones.
            span_attrs=_kept_span_attrs(governed.memo, governed_semantic, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(_sources),
            route_reason=decision.reason,
            optimizations=_opts,
            pk_bounds=_pk_bounds_now,  # REQ-1865
            response_cacheable=_raw_cacheable,  # REQ-1897
            cache_sql=governed_semantic,
            cache_params=_cache_params,
            cache_as_of=as_of,
            cache_missed=_cache_missed,
            writes_tables=_is_mutation,  # REQ-1897
            written_table_id=_written_table_id,
            role_id=role_id,  # REQ-1897
            table_ids=_table_ids,  # REQ-1897
            cache_opt_in=_cache_hint.opt_in,  # REQ-544 (amended)
            cache_ttl=_cache_hint.ttl,
        )


async def _resolve_pk_bounds(
    semantic_sql: str, state: Any, params: list[Any] | None = None
) -> tuple[Any, ...]:
    """REQ-1865 / REQ-1915: the concrete PK bound(s) this statement resolves against the
    row-level tables it reads — and the refusal of a statement that reads one without binding its
    key (``_pk_bounds``). Empty when the statement reads no row-level table, or reaches each one
    through a join that pushes its key down.

    ``params`` is the statement's own bind-parameter values, in bind order: a Bolt/Cypher
    statement's predicate values are never inlined as literals, so without them ``WHERE pk = $1``
    resolves no bound."""
    return _pk_bounds(_pk_bounds_inputs(semantic_sql, state), params)


def _pk_bounds_inputs(semantic_sql: str, state: Any) -> tuple[tuple[Any, ...], ...]:
    """What ``_pk_bounds`` derives from the statement and the registry, before any bound value is
    looked at: one entry per reference the statement makes to a row-level table
    (``_row_materialize_tables_in_memory``) that a key must bind — ``(table, name as written, the
    reference alone, the predicates of the SELECT that names it)``. Omitted, because no key need
    bind them: the target of an INSERT/UPDATE/DELETE/MERGE (written at the source, not read from
    the replica) and a reference bound by key pushdown.

    Key pushdown (REQ-1865, ``query_residency.pushdown_row_materialize``) binds a reference when
    all of these hold, which are exactly the conditions under which that mechanism fetches its
    rows: the statement is a SELECT; the reference is the target of a JOIN; that JOIN is the only
    one naming the table; its ON is a single column-to-column equality between one column of the
    table and a column of another relation (``_join_key_column``); and the table's primary key is
    a single column."""
    row_tables = _row_materialize_tables_in_memory(state)
    if not row_tables:
        return ()
    import sqlglot
    import sqlglot.expressions as exp

    from provisa.compiler.pk_bounds import scope_predicates
    from provisa.federation.query_residency import _join_key_column

    ast = sqlglot.parse_one(semantic_sql, read="postgres")
    written = None
    if isinstance(ast, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        written = ast.this.this if isinstance(ast.this, exp.Schema) else ast.this
    joined: dict[str, int] = {}
    for join in ast.find_all(exp.Join):
        if isinstance(join.this, exp.Table) and join.this.name in row_tables:
            name = row_tables[join.this.name].table_name
            joined[name] = joined.get(name, 0) + 1
    reads: list[tuple[Any, ...]] = []
    for ref in ast.find_all(exp.Table):
        if ref.name not in row_tables or ref is written:
            continue
        table = row_tables[ref.name]
        join = ref.parent if isinstance(ref.parent, exp.Join) and ref.parent.this is ref else None
        if (
            join is not None
            and isinstance(ast, exp.Select)
            and joined[table.table_name] == 1
            and sum(1 for c in table.columns if c.is_primary_key) == 1
            and _join_key_column(join, ref.alias_or_name) is not None
        ):
            continue  # bound by key pushdown
        reads.append((table, ref.name, exp.select("1").from_(ref.copy()), scope_predicates(ref)))
    return tuple(reads)


def _pk_bounds(inputs: tuple[tuple[Any, ...], ...], params: list[Any] | None) -> tuple[Any, ...]:
    """The statement's key bounds — and THE decision point of REQ-1915. Every surface resolves
    its bounds through here (the raw stage, the compiled stage, and the GraphQL executors via
    ``_resolve_pk_bounds``), so every surface refuses the same statements.

    A reference to a row-level table is BOUND when the SELECT that names it carries, as a
    top-level AND term of its own WHERE or of one of its JOIN ... ON conditions, a predicate that
    resolves concrete values for the table's primary key: ``key = value``, ``key IN (values)``, or
    an OR made only of such equalities on the one key column — each value a literal or a bound
    parameter; a composite key needs exactly one value per column. Or when a join pushes its key
    down (``_pk_bounds_inputs``). Anything else — no predicate, a predicate on another column, a
    range, a key predicate in an enclosing or nested query — reads rows no key names, and is
    refused: ``RowLevelKeyRequired`` names the table and its key."""
    if not inputs:
        return ()
    from dataclasses import replace

    from provisa.compiler.pk_bounds import extract_pk_bounds

    bounds: dict[str, Any] = {}
    for table, name, reference, predicates in inputs:
        found = extract_pk_bounds(reference, {name: table}, params, predicates)
        if not found or not found[0].values:
            from provisa.api.errors import RowLevelKeyRequired

            raise RowLevelKeyRequired(
                table.table_name, tuple(c.name for c in table.columns if c.is_primary_key)
            )
        seen = bounds.get(table.table_name)
        bounds[table.table_name] = (
            found[0]
            if seen is None
            else replace(seen, values=tuple(dict.fromkeys(seen.values + found[0].values)))
        )
    return tuple(bounds.values())


def _kept_pk_bounds(
    memo: dict[str, Any], semantic_sql: str, state: Any, params: list[Any] | None
) -> tuple[Any, ...]:
    """``_resolve_pk_bounds`` for a governed statement. The row_materialize tables are a function
    of the registry and the bound engine and the parsed statement of its text, so both are kept in
    the statement's ``memo``; only the bound values differ between executions. extract_pk_bounds
    reads the tree and never rewrites it."""
    kept = memo.get("pk_bounds_inputs")
    if kept is None or kept[0] is not state.tables or kept[1] is not state.federation_engine:
        kept = (state.tables, state.federation_engine, _pk_bounds_inputs(semantic_sql, state))
        memo["pk_bounds_inputs"] = kept
    return _pk_bounds(kept[2], params)


def _row_materialize_tables_in_memory(state: Any) -> dict[str, Any]:
    """table reference name -> registered table, for the tables row_materialize APPLIES to
    (REQ-1865), read from the in-memory registry ``_rebuild_schemas`` publishes (``state.tables``).

    This runs for every governed statement, so it reads no control plane: the registry rows are
    already in memory, and a rebuild republishes them. Same selection and keying as
    ``query_residency.row_materialized_tables_by_name`` — the flag is set AND the bound engine
    cannot direct-attach the table's source type; keyed by both the bare table name and the alias."""
    flagged = [t for t in state.tables if t.get("row_materialize")]
    if not flagged:
        return {}
    from types import SimpleNamespace

    from provisa.compiler.naming import apply_sql_name
    from provisa.federation.strategy import engine_attaches

    engine = state.federation_engine
    out: dict[str, Any] = {}
    for t in flagged:
        # tables.source_id is a NOT NULL foreign key to sources.id, so the type is always known.
        if engine_attaches(engine, state.source_types[t["source_id"]]):
            continue
        table = SimpleNamespace(
            id=t["id"],
            source_id=t["source_id"],
            schema_name=t["schema_name"],
            table_name=t["table_name"],
            alias=t.get("alias"),
            row_materialize=True,
            columns=[
                SimpleNamespace(
                    name=c["column_name"],
                    data_type=c["data_type"],
                    is_primary_key=c["is_primary_key"],
                    native_filter_type=c["native_filter_type"],
                )
                for c in t["columns"]
            ],
        )
        out[apply_sql_name(table.table_name)] = table
        if table.alias:
            out[apply_sql_name(table.alias)] = table
    return out


class _AuditedDrain:
    """A result's batches, with the statement's deferred audit record written when the drain
    ends — however it ends: exhausted (200), failed part-way (500), closed by a client that
    stopped reading, or dropped without ever being read (200, with the rows delivered so far)."""

    def __init__(self, plan: _Plan, batches: Any, rows_in: Callable[[Any], int]) -> None:
        self._batches = iter(batches)
        self._rows_in = rows_in
        self._record, plan.audit_deferred = plan.audit_deferred, None
        self._started = plan.audit.started if plan.audit is not None else 0.0
        self._rows = 0

    def __iter__(self) -> "_AuditedDrain":
        return self

    def __next__(self) -> Any:
        try:
            batch = next(self._batches)
        except StopIteration:
            self._complete(200)
            raise
        except BaseException:
            self._complete(500)
            raise
        self._rows += self._rows_in(batch)
        return batch

    def _complete(self, status_code: int) -> None:
        record, self._record = self._record, None
        if record is not None:
            from provisa.audit.pipeline import complete_audit_record

            complete_audit_record(record, self._started, status_code, self._rows)

    def finish(self) -> None:
        """The result is being released without being drained further: record what it delivered."""
        self._complete(200)

    def close(self) -> None:
        close = getattr(self._batches, "close", None)
        if close is not None:
            close()
        self._complete(200)

    def __del__(self) -> None:
        if self._record is not None:
            self._complete(200)


def audit_on_drain(plan: _Plan, batches: Any, rows_in: Callable[[Any], int] = len) -> Any:
    """Wrap a streamed result's batches so the audit record ``finalize_audit(...,
    defer_to_drain=True)`` held back is written when the drain ends, with the rows delivered.
    ``rows_in`` counts one batch (``len`` for row lists; ``lambda b: b.num_rows`` for Arrow).
    A plan with no deferred record (already recorded, or no acting principal) is passed through."""
    if plan.audit_deferred is None:
        return batches
    return _AuditedDrain(plan, batches, rows_in)


async def finalize_audit(
    plan: _Plan,
    status_code: int,
    state: Any | None = None,
    *,
    cache_hit: bool = False,
    cache_entry: Any = None,
    defer_to_drain: bool = False,
) -> None:
    """Write ``plan``'s audit row (REQ-074/REQ-1386). Idempotent per plan.

    ``cache_entry``: the response-cache entry a hit was served from — its age is the age of the
    rows the statement was answered with.

    The row records the route the statement was answered by (``cache`` for a response-cache hit,
    else the plan's) and ``plan.row_count``, which a terminal sets before it finalizes.
    ``defer_to_drain``: the terminal is handing back a STREAM it has not drained — the record is
    built now (in the request's context) and written by :func:`audit_on_drain` when the drain
    ends, where the row count and a mid-stream failure are known.

    ``_execute_plan`` calls this at its terminals. The govern-then-stream surfaces (pgwire's
    socketserver worker, Flight SQL, airport) never reach ``_execute_plan`` — they drain the
    engine's SYNC terminal themselves — so they call this at their own terminal instead. The
    idempotence guard means a plan that takes either path is audited exactly once.
    """
    if plan.audit_written:
        return
    plan.audit_written = True
    from provisa.audit.pipeline import write_audit
    from provisa.observability.request_facts import observe_plan

    # REQ-1910: request facts + metrics, every surface. ``cache_hit``: served from the response
    # cache, so the route it is reported under is the cache, not the route the plan would have run.
    observe_plan(plan, status_code, cache_hit=cache_hit)

    _route = "cache" if cache_hit else cast("Route", plan.route).name.lower()
    if cache_hit and cache_entry is None:
        raise RuntimeError(
            "a statement answered from the response cache was finalized without the entry it was "
            "served from: its audit row could not say how old the rows were"
        )
    from provisa.audit.provenance import data_age

    # Provenance (provisa/audit/provenance.py): why the plan took its route, what it read, and
    # how old the rows it was answered with are — decided by now, recorded here.
    _outcome = {
        "route_reason": plan.route_reason,
        "sources": plan.sources,
        "data_age": (
            data_age(plan, cache_entry if cache_hit else None)
            if status_code == 200  # noqa: PLR2004 - HTTP OK
            else None
        ),
    }
    if defer_to_drain and status_code == 200:  # noqa: PLR2004 - HTTP OK
        from provisa.audit.pipeline import build_audit_record

        plan.audit_deferred = build_audit_record(
            plan.audit, status_code, state, route=_route, **_outcome
        )
    else:
        await write_audit(
            plan.audit, status_code, state, route=_route, row_count=plan.row_count, **_outcome
        )
    # REQ-1897: every terminal finalizes here, so the steps after a successful write run once,
    # whichever surface ran it.
    if plan.writes_tables and status_code == 200:
        if state is None:
            from provisa.api.app import state  # type: ignore[assignment]
        await _after_write(plan, state)


#: REQ-1695: the reference that can only be answered by an ORG's vault. ``${env:...}`` is the
#: deployment's process environment and needs nothing bound; this one names a secret that belongs
#: to an organization, so a resolution outside that org's binding raises rather than guessing.
_ORG_SECRET_REF = "${secret:"


def _reads_an_org_secret(plan: _Plan, state: Any) -> bool:
    """Whether any source this plan reads addresses its endpoint through an org secret (REQ-1695).

    Decided from state already in memory -- the control-plane source map ``_rebuild_schemas``
    publishes, and the config's own Sources -- so a plan that reads nothing of the kind pays a
    dict lookup and no query. That matters because the binding it gates is a read of the org's
    vault, and putting one on EVERY statement would buy a round trip for the overwhelming majority
    of queries that have no secret to resolve.
    """
    wanted = set(getattr(plan, "sources", None) or ())
    if not wanted:
        return False
    runtime = getattr(state, "runtime_sources", None) or {}
    for source_id in wanted:
        row = runtime.get(source_id)
        if row is not None and any(
            isinstance(v, str) and _ORG_SECRET_REF in v for v in row.values()
        ):
            return True
    for src in getattr(getattr(state, "config", None), "sources", None) or []:
        if src.id in wanted and _ORG_SECRET_REF in src.model_dump_json():
            return True
    return False


async def _execute_plan(plan: _Plan, state: Any | None = None) -> QueryResult:  # REQ-027, REQ-028
    """The terminal every raw-SQL surface reaches, with the acting org's secrets resolvable.

    REQ-1695: a source's password reaches the engine as ``${secret:NAME}`` and is resolved at the
    moment the source is dialled -- inside the attach the engine performs on the way to answering
    this statement. That resolution needs the org's vault bound, and HERE is the one place every
    surface passes through, so no transport has to establish it for itself.
    """
    require_governed_plan(plan)  # SECURITY: refuse any plan the top of the pipeline did not mint
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    if plan.cache_hit is not None:
        # REQ-1897: answered before routing — nothing to wake, land or execute.
        return await cached_result(plan, state)
    from provisa.core import request_deadline

    # REQ-1905: every user-initiated statement runs inside a request deadline. A transport binds
    # its request's deadline at its own boundary (provisa.core.request_deadline.request /
    # open_request), around the whole request; a statement that arrives here with none (a route
    # that is not a transport of its own) is given its transport's budget here, the one place
    # every surface passes through. A plan with no audit record is background work (seeding,
    # scheduled jobs, rebuilds) and runs unbounded by a request budget, as it always has.
    if plan.audit is None:
        return await _execute_plan_bound(plan, state)
    from provisa.compiler.limits import role_max_query_time_ms

    # REQ-1174: the role's own limit, on every transport, when it is the tighter one.
    _role_ms = role_max_query_time_ms(getattr(state, "roles", {}).get(plan.role_id))
    outer = request_deadline.current()
    if outer is not None:
        if _role_ms is None or _role_ms / 1000.0 >= outer.remaining():
            return await _execute_plan_bound(plan, state)
        transport = outer.transport or plan.audit.surface
        budget, setting = _role_ms / 1000.0, f"role {plan.role_id!r} max_query_time_ms"
    else:
        # The timeout of the transport the statement arrived on — its HTTP route's, or its
        # protocol's — and the setting that value comes from, for the error below.
        from provisa.core.limits import statement_timeout

        budget = statement_budget(plan.audit.surface)
        _, transport, setting = statement_timeout(plan.audit.surface)
        if _role_ms is not None and _role_ms / 1000.0 < budget:
            budget, setting = _role_ms / 1000.0, f"role {plan.role_id!r} max_query_time_ms"
    with request_deadline.within(budget) as deadline:
        try:
            return await _execute_plan_bound(plan, state)
        except TimeoutError as exc:
            if not deadline.fired or deadline.ended_early:
                raise  # another timeout, or the process is stopping (its own message)
            raise request_deadline.RequestTimedOut(transport, budget, setting) from exc


def statement_budget(surface: str) -> float:
    """Seconds a user-initiated statement arriving under ``surface`` without a deadline may run:
    the request timeout of the transport it is on (REQ-1905, ``limits.request_timeouts``) — its
    HTTP route's when one is bound, else its protocol's, else the default."""
    from provisa.core.limits import statement_timeout

    return statement_timeout(surface)[0]


async def _execute_plan_bound(plan: _Plan, state: Any) -> QueryResult:
    """``_execute_plan`` once the request deadline is settled: the org's secrets, then the terminal."""
    if _reads_an_org_secret(plan, state):
        from provisa.core.secrets_store import bound_to_request_org

        async with bound_to_request_org():
            return await _execute_plan_in_org(plan, state)
    return await _execute_plan_in_org(plan, state)


async def _execute_plan_in_org(plan: _Plan, state: Any) -> QueryResult:  # REQ-027, REQ-028
    # REQ-1448: the shard this org queries may have had its node released while idle. Waking it HERE
    # — before the terminal, not inside the executor's retry loop — is what makes a cold start
    # survivable: a node is ~2-4min to provision and the retry budget is 30s, so a query that
    # discovers the absence at dispatch time could never wait it out. This is also the one seam every
    # surface reaches, so no protocol server needs a wake of its own.
    from provisa.federation.engine_wake import ensure_engine_awake, readdress_lost_coordinator

    await ensure_engine_awake(state)
    # REQ-1661: a MATERIALIZED source this plan reads that has never landed, or has gone stale, is
    # landed before the read -- here, the one seam every surface reaches, so no transport can
    # serve an empty replica the event loop has not filled yet.
    from provisa.federation.query_residency import (
        ensure_resident,
        ensure_rows_resident,
        pushdown_row_materialize,
    )
    from provisa.transpiler.router import Route

    # REQ-1865: any row-materialize table this plan's predicate resolved a concrete PK bound
    # against is served from the row cache, fetching from source only the missing/stale keys.
    # Runs BEFORE the key-pushdown probe below: a query can have MULTIPLE row_materialize tables
    # in one join, some directly bound (e.g. bench_customer_node's own customer_id predicate) and
    # some reached only via FK (bench_contains_edge) -- the pushdown probe's LEFT-preserved
    # "known" side must already be real data, or a query where EVERY table in the chain is
    # row_materialize would probe an empty replica end to end and always resolve zero keys.
    await ensure_rows_resident(state, plan.pk_bounds, reader_role=plan.role_id)
    # REQ-1865 key pushdown: a row-materialize table reached only through a JOIN (no literal
    # predicate naming it directly, e.g. cypher_cross_engine's bench_contains_edge) has no PK bound
    # for ensure_rows_resident to key off -- narrow its fetch to the keys this query's OTHER,
    # already-resolvable tables actually need. A statement that binds a row-level table neither
    # way was refused at planning (REQ-1915, _pk_bounds), so nothing here copies a whole table.
    # ENGINE-route only: this is a multi-table join concern, and only the ENGINE route has a
    # physical_sql to probe.
    if plan.route == Route.ENGINE and plan.physical_sql is not None:
        await pushdown_row_materialize(
            state,
            plan.physical_sql,
            state.federation_engine.dialect,
            plan.exec_params,
            reader_role=plan.role_id,
        )
    residency = await ensure_resident(
        state, plan.sources, reader_role=plan.role_id, table_ids=plan.table_ids
    )
    plan.replicas_read = residency.replicas_read
    # REQ-1897: the result cache is GraphQL's Route.CACHE candidate route, extended here so every
    # other raw-SQL surface that reaches this one chokepoint (Bolt, pgwire's non-COPY path) gets
    # the same served-without-touching-the-engine hit -- with the same audit row and tier/egress
    # accounting a live execution would have written, not a silent skip.
    cached_result = await check_response_cache(plan, state)
    if cached_result is not None:
        return cached_result
    _t0 = _time.perf_counter()
    # REQ-074/REQ-1386: one audit row per executed statement, with the terminal's real outcome —
    # written here rather than in each transport, so no surface can omit it.
    from provisa.federation.live_concurrency import acquire_plan_permits

    try:
        # REQ-1909: a capped source this plan reads live is held to its concurrency cap for the
        # whole execution; acquiring fails the statement (audited below) once the deadline passes.
        with acquire_plan_permits(state, plan):
            try:
                result = await _run_plan_terminal(plan, state)
            except Exception as exc:
                # REQ-1448: a dial that reached nothing can mean the coordinator moved while this
                # process held its address. Only re-resolving says which, and only a shard that
                # actually moved earns the second dispatch — the executor's own retries cannot
                # help here, because they rebuild the connection at the same dead address.
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
    plan.row_count = len(result.rows)
    await finalize_audit(plan, 200, state)
    # REQ-1897: the buffered chokepoint writes its row result to the raw-SQL namespace.
    await store_executed_result(plan, state, result)
    # REQ-1517: record this statement against the request's stats accumulator (opt-in via
    # X-Provisa-Stats) from the PLAN, at the one terminal every raw-SQL surface reaches — so the
    # route, the source and the execution DAG a surface reports are the ones that actually ran.
    # A no-op when the caller did not ask for stats.
    from provisa.executor.plan_stats import record_plan_execution
    from provisa.otel_compat import annotate_request

    annotate_request(db__row_count=len(result.rows))  # REQ-1910
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


def _response_cache_org_id(state: Any) -> str | None:
    """The acting org for cache key/entry prefixing (REQ-595) -- same resolution `_attach_tier_caps`
    uses, so a cache entry a plan can write is one that same org's later plans can read back."""
    from provisa.cache.tenancy import cache_tenant

    return cache_tenant(state)


def _response_cache_key(plan: _Plan, *, wire_formats: list[int] | None) -> str | None:
    """This plan's raw-SQL cache key (REQ-1897), or ``None`` when it is not cacheable at all.
    ``wire_formats`` selects a passthrough ``pg_datarows`` entry (None: a decoded entry).

    The raw-SQL namespace (``raw_sql_cache_key``) is disjoint from GraphQL's, so a raw-SQL reader
    never meets a GraphQL response entry. Raw-SQL surfaces carry no separate RLS-rules dict — the
    resolved identity is already baked into the governed ``plan.sql``/``plan.exec_params`` — so the
    one residual fail-closed gate is a governed SQL string that itself depends on unresolved
    session state (REQ-866's ``current_setting(`` check). A plan with no governed role is not
    cacheable.
    """
    if not (plan.cache_opt_in and plan.response_cacheable) or plan.role_id is None or not plan.sql:
        return None
    # The planner's plans carry their route-independent identity (see ``_Plan.cache_sql``); a plan
    # built elsewhere is identified by the statement it executes.
    if plan.cache_sql is not None:
        sql, params = plan.cache_sql, plan.cache_params or []
    else:
        sql, params = plan.sql, plan.exec_params or []
    return _raw_cache_key(sql, params, plan.role_id, wire_formats, plan.cache_as_of)


def _raw_cache_key(
    sql: str,
    params: list,
    role_id: str,
    wire_formats: list[int] | None,
    as_of: str | None = None,
) -> str | None:
    """The raw-SQL namespace key for governed ``sql``, its bound values and the request's as-of,
    or None when the text depends on unresolved session state (REQ-866)."""
    from provisa.cache.key import is_cacheable, raw_sql_cache_key

    cacheable, _ = is_cacheable(sql, {})
    if not cacheable:
        return None
    return raw_sql_cache_key(sql, params, role_id, wire_formats=wire_formats, as_of=as_of)


def _entry_kind(wire_formats: list[int] | None) -> tuple[int, ...] | None:
    return None if wire_formats is None else tuple(wire_formats)


async def _cached_before_routing(
    state: Any,
    *,
    sql: str,
    params: list | None,
    role_id: str,
    cache_hint: CacheHint,
    wire_formats: list[int] | None,
    as_of: str | None,
) -> tuple[tuple[list[int] | None, Any] | None, tuple[tuple[int, ...] | None, ...]]:
    """The response-cache read made BEFORE routing (REQ-1897, amended 2026-10-01): the entry for
    governed ``sql`` + ``params`` + role, and the kinds looked for and not found.

    The key needs nothing routing produces, so an opted-in request is answered here and never
    lowered, optimized, routed or prepared for residency. ``wire_formats`` — pgwire's result
    format codes — adds the passthrough ``pg_datarows`` entry for those codes, tried first as
    the terminal tries it first; the decoded entry any surface can serve is tried next. A request
    that did not opt in, or a store that keeps nothing, reads nothing."""
    store = state.response_cache_store  # always set (NoopCacheStore when caching is off)
    if not cache_hint.opt_in or not store.stores_results:
        return None, ()
    from provisa.cache.middleware import check_cache

    org_id = _response_cache_org_id(state)
    missed: list[tuple[int, ...] | None] = []
    for formats in [wire_formats, None] if wire_formats is not None else [None]:
        key = _raw_cache_key(sql, params or [], role_id, formats, as_of)
        if key is None:
            return None, ()
        cached = await check_cache(store, key, org_id)
        if cached is not None:
            return (formats, cached), tuple(missed)
        missed.append(_entry_kind(formats))
    return None, tuple(missed)


def _cached_plan(
    *,
    governed_sql: str,
    params: list | None,
    role_id: str,
    table_ids: tuple[int, ...],
    cache_hint: CacheHint,
    hit: tuple[list[int] | None, Any],
    audit: PendingAudit | None,
    span_attrs: dict[str, str] | None,
    sources: frozenset[str],
    semantic_sql: str | None,
    as_of: str | None,
) -> _Plan:
    """The plan of a request answered from the response cache before routing: it names no source
    and carries no executable statement — only what its terminal needs to serve, account and
    audit the entry (REQ-865: the cache is a route)."""
    from provisa.transpiler.router import Route

    return _Plan(
        route=Route.CACHE,
        sql=governed_sql,
        source_id="",
        dialect="",
        exec_params=params,
        semantic_sql=semantic_sql,
        span_attrs=span_attrs,
        audit=audit,
        stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
        sources=sources,
        route_reason="served from the response cache",
        response_cacheable=True,
        role_id=role_id,
        table_ids=table_ids,
        cache_opt_in=True,
        cache_ttl=cache_hint.ttl,
        cache_sql=governed_sql,
        cache_params=list(params or []),
        cache_as_of=as_of,
        cache_hit=hit,
    )


def _response_cache_policy(plan: _Plan, state: Any) -> tuple[int, set[int]] | None:
    """(ttl, table_ids) for storing this OPTED-IN plan's result, or None when the operator's
    settings do not permit caching it.

    The operator's resolution (``resolve_policy`` with no query TTL, REQ-544) for EVERY table the
    statement reads is the permission: any table whose source disables caching, or whose TTL
    resolves to 0, keeps the result out (``opt_in_ttl``). Permitted, the entry lives the
    request's own ``cache_ttl`` if it chose one, else the shortest operator TTL, and is indexed
    under every table so any table's invalidation drops it. Absent per-source/per-table settings
    mean "inherit" (source enabled, TTL from the next level) — REQ-544's resolution order."""
    from provisa.cache.policy import opt_in_ttl, resolve_policy

    table_ids = set(plan.table_ids)
    # The role's compilation context is what governance resolved these table ids against, so it
    # names each one's source.
    assert plan.role_id is not None  # _response_cache_key gates on it
    source_of = {m.table_id: m.source_id for m in state.contexts[plan.role_id].tables.values()}
    ttls: list[int] = []
    for table_id in table_ids:
        source_settings = state.source_cache.get(source_of[table_id], {})
        _, ttl = resolve_policy(
            stable_id=None,
            cache_ttl=None,
            default_ttl=state.response_cache_default_ttl,
            source_cache_enabled=source_settings.get("cache_enabled", True),
            source_cache_ttl=source_settings.get("cache_ttl"),
            table_cache_ttl=state.table_cache.get(table_id),
        )
        ttls.append(ttl)
    if not ttls:
        _, ttl = resolve_policy(None, None, default_ttl=state.response_cache_default_ttl)
        ttls.append(ttl)
    ttl = opt_in_ttl(plan.cache_ttl, ttls)
    return None if ttl <= 0 else (ttl, table_ids)


def _response_cache_bound() -> int:
    """The most rows a raw-SQL entry may hold: the org's large-result threshold (REQ-1224's
    inline-vs-redirect line, ``RedirectConfig.threshold``) — a result a buffered transport would
    not inline is not one the cache holds in memory either."""
    from provisa.executor.redirect import RedirectConfig

    return RedirectConfig.from_env().threshold


async def _read_response_cache(
    plan: _Plan, state: Any, *, wire_formats: list[int] | None
) -> tuple[dict, list[str] | None, Any] | None:
    """The raw-SQL entry for this plan — ``(entry, column_types, stored)`` — or None on a MISS.
    ``stored`` is the store's own record of the entry (``cache.store.CachedResult``): its age is
    what a surface reports beside a HIT (REQ-536)."""
    from provisa.cache.middleware import check_cache, decode_cached_result

    if plan.cache_hit is not None:
        # Read before routing (see _cached_before_routing): served as the kind it was read as.
        held_formats, cached = plan.cache_hit
        if held_formats != wire_formats:
            return None
        return (*decode_cached_result(cached), cached)
    if _entry_kind(wire_formats) in plan.cache_missed:
        return None  # the planner looked for this kind before routing: a MISS, not read again
    ck = _response_cache_key(plan, wire_formats=wire_formats)
    if ck is None or not state.response_cache_store.stores_results:
        return None

    # AppState always holds a store (NoopCacheStore when caching is off, app.py).
    cached = await check_cache(state.response_cache_store, ck, _response_cache_org_id(state))
    if cached is None:
        return None
    return (*decode_cached_result(cached), cached)


async def _account_cache_hit(
    plan: _Plan, state: Any, result: QueryResult, stored: Any
) -> QueryResult:
    """A HIT is served without touching the engine, but is NOT a skipped statement: the same
    egress-cap (REQ-1044) and audit (REQ-074/REQ-1386) accounting a live execution has, in the same
    order. The result names the entry it was served from (REQ-536)."""
    try:
        result = _apply_output_cap(plan, result)
    except Exception:
        await finalize_audit(plan, 402, state, cache_hit=True, cache_entry=stored)
        raise
    result.cache_entry = stored
    plan.row_count = len(result.rows)
    await finalize_audit(plan, 200, state, cache_hit=True, cache_entry=stored)
    return result


async def check_response_cache(plan: _Plan, state: Any) -> QueryResult | None:  # REQ-1897
    """Cache-HIT short circuit for every row terminal a plan reaches (REQ-1897): the chokepoint,
    and the streaming terminals in ``provisa/api/flight/server.py`` (Cypher), ``provisa/grpc/
    server.py`` and ``provisa/pgwire/server.py`` that bypass it. Returns ``None`` on a MISS or when
    the plan is not cacheable (REQ-866 fail-closed) -- the caller then runs the plan as usual.
    Either decoded kind is served as rows (``raw_sql.entry_as_result``); another kind raises."""
    hit = await _read_response_cache(plan, state, wire_formats=None)
    if hit is None:
        return None
    from provisa.cache.raw_sql import entry_as_result

    entry, column_types, stored = hit
    return await _account_cache_hit(plan, state, entry_as_result(entry, column_types), stored)


async def cached_result(plan: _Plan, state: Any) -> QueryResult:  # REQ-1897
    """The result of a Route.CACHE plan — the entry its planner read before routing, accounted
    and audited like any hit: decoded rows, or the raw DataRow replay when the entry is the
    passthrough one read for the client's format codes."""
    if plan.cache_hit is None:
        raise RuntimeError("cached_result: the plan was not answered from the response cache")
    formats = plan.cache_hit[0]
    result = await (
        check_response_cache(plan, state)
        if formats is None
        else check_response_cache_datarows(plan, state, formats)
    )
    assert result is not None  # the held entry is served as the kind it was read as
    return result


async def check_response_cache_arrow(plan: _Plan, state: Any) -> Any | None:  # REQ-1897
    """Flight SQL's Arrow-native HIT: the entry as the ``pyarrow.Table`` Flight serves (an
    ``arrow_ipc`` entry verbatim; a ``rows`` entry converted losslessly), accounted exactly like
    :func:`check_response_cache`. None on a MISS."""
    hit = await _read_response_cache(plan, state, wire_formats=None)
    if hit is None:
        return None
    from provisa.cache.raw_sql import entry_as_arrow, entry_as_result

    entry, column_types, stored = hit
    await _account_cache_hit(plan, state, entry_as_result(entry, column_types), stored)
    return entry_as_arrow(entry, column_types)


async def check_response_cache_datarows(  # REQ-1897
    plan: _Plan, state: Any, wire_formats: list[int]
) -> QueryResult | None:
    """pgwire passthrough's HIT: the ``pg_datarows`` entry written for these client format codes,
    replayed as the raw DataRow messages the miss forwarded — pgwire skips decode for them exactly
    as it did on the miss. Accounted like :func:`check_response_cache`. None on a MISS."""
    hit = await _read_response_cache(plan, state, wire_formats=wire_formats)
    if hit is None:
        return None
    from provisa.cache.raw_sql import entry_as_datarows

    entry, _, stored = hit
    return await _account_cache_hit(plan, state, entry_as_datarows(entry, wire_formats), stored)


def _cache_tee(plan: _Plan, state: Any, run: Any | None, wire_formats: list[int] | None) -> Any:
    if plan.warnings:
        return None  # a warned answer (one cut short) is never stored as the statement's answer
    ck = _response_cache_key(plan, wire_formats=wire_formats)
    store = state.response_cache_store  # always set (NoopCacheStore when caching is off)
    if ck is None or not store.stores_results:
        return None  # decided before any stream is wrapped: nothing is buffered
    policy = _response_cache_policy(plan, state)
    if policy is None:
        return None
    from provisa.cache.raw_sql import new_tee

    ttl, table_ids = policy
    return new_tee(
        store, ck, _response_cache_org_id(state), ttl, table_ids, _response_cache_bound(), run
    )


def response_cache_tee(plan: _Plan, state: Any, run: Any | None) -> Any | None:  # REQ-1897
    """The write-through tee a streaming terminal wraps its DECODED result in
    (``raw_sql.ResponseCacheTee``), or None when this plan's result is not cacheable (no key, no
    store, or policy TTL 0) — the terminal then streams unwrapped. ``run`` runs the store coroutine
    on the terminal's own loop (None for a terminal already on it, which awaits ``tee.commit()``
    after the drain)."""
    return _cache_tee(plan, state, run, None)


def serve_stream_through_cache(  # REQ-1897
    plan: _Plan,
    state: Any,
    *,
    run: Any,
    open_rows: Any,
    check_rows: bool,
    passthrough: tuple[list[int], Any] | None,
) -> Any:
    """The one read/write-through for a synchronous streaming terminal (pgwire's ENGINE sink and
    DIRECT streams, Flight's DIRECT stream), keeping each hit on the path shape its miss took.

    ``passthrough`` — ``(client format codes, open)`` when the terminal may forward a Postgres
    source's raw DataRow bytes (REQ-1863): a ``pg_datarows`` HIT is replayed undecoded; on a MISS
    the passthrough stream is teed into a ``pg_datarows`` entry. A ``PassthroughError`` (the fast
    path declined) falls through to the decoded path, exactly as before caching. The decoded path
    serves a ``rows``/``arrow_ipc`` HIT when ``check_rows`` (False where the terminal already
    checked, e.g. pgwire's ENGINE route inside its residency dispatch), else runs ``open_rows()``
    teed into a ``rows`` entry. ``run`` runs a coroutine on the terminal's loop. With caching
    disabled (a store that keeps nothing) no read is dispatched and nothing is wrapped."""
    from provisa.federation.live_concurrency import acquire_plan_permits

    caching = state.response_cache_store.stores_results
    permits = None  # REQ-1909: taken on the first live open, held until the stream ends
    if passthrough is not None:
        wire_formats, open_passthrough = passthrough
        if caching:
            replay = run(check_response_cache_datarows(plan, state, wire_formats))
            if replay is not None:
                return replay
        from provisa.pgwire.pg_passthrough import PassthroughError

        permits = acquire_plan_permits(state, plan)
        try:
            stream = open_passthrough()
        except PassthroughError:
            log.debug("[PGWIRE] passthrough declined; decoded path", exc_info=True)
        except BaseException:
            permits.release()
            raise
        else:
            tee = _cache_tee(plan, state, run, wire_formats)
            stream = stream if tee is None else tee.datarows(stream, wire_formats)
            return permits.wrap_stream(stream)
    if check_rows and caching:
        hit = run(check_response_cache(plan, state))
        if hit is not None:
            if permits is not None:
                permits.release()
            return hit
    if permits is None:
        permits = acquire_plan_permits(state, plan)
    try:
        stream = open_rows()
    except BaseException:
        permits.release()
        raise
    tee = response_cache_tee(plan, state, run=run)
    return permits.wrap_stream(stream if tee is None else tee.rows(stream))


async def serve_buffered_through_cache(  # REQ-1897
    plan: _Plan, state: Any, execute: Any
) -> QueryResult:
    """A buffered terminal's read/write-through in ONE coroutine (one loop dispatch, REQ-1887):
    the decoded HIT, else ``await execute()`` stored exactly as the chokepoint stores its result."""
    hit = await check_response_cache(plan, state)
    if hit is not None:
        return hit
    from provisa.federation.live_concurrency import acquire_plan_permits

    with acquire_plan_permits(state, plan):  # REQ-1909: held for the whole buffered execution
        result = await execute()
    await store_executed_result(plan, state, result)
    return result


async def store_executed_result(plan: _Plan, state: Any, result: QueryResult) -> None:
    """The chokepoint's write: a buffered row result goes through the same tee (bound, policy,
    namespace) as every streaming terminal's."""
    if result.redirect is not None:
        return  # a sink handle, not rows
    tee = response_cache_tee(plan, state, run=None)
    if tee is None:
        return
    for _ in tee.rows(result).batches():
        pass
    await tee.commit()


async def _after_write(plan: _Plan, state: Any) -> None:
    """What follows a successful write, on every surface, once (REQ-1897): every cached entry
    indexed under the OTHER tables the statement named is dropped (a write that reads them may
    have changed what a cached join shows), and the written table gets the one after-write step
    (:func:`provisa.api.data.table_written.after_table_written` — its cached responses, the
    views over it, its change event and sinks, its replica build, its hot copy)."""
    from provisa.api.data.table_written import after_table_written
    from provisa.cache.tenancy import invalidate_tables

    if plan.written_table_id is None or plan.role_id is None:
        raise RuntimeError(
            "a write plan reached its terminal without the table it wrote or the role it ran as"
        )
    written = next(
        (
            meta
            for meta in state.contexts[plan.role_id].tables.values()
            if meta.table_id == plan.written_table_id
        ),
        None,
    )
    if written is None:
        raise RuntimeError(
            f"the written table {plan.written_table_id} is not in role {plan.role_id!r}'s schema"
        )
    others = [tid for tid in plan.table_ids if tid != written.table_id]
    if others:
        await invalidate_tables(state, others)
    await after_table_written(
        state,
        table_id=written.table_id,
        table_name=written.table_name,
        source_id=written.source_id,
    )


async def prepare_residency_and_check_cache(plan: _Plan, state: Any) -> QueryResult | None:
    # REQ-1887, REQ-1897: folds check_response_cache into the SAME dispatch as
    # prepare_engine_residency for the terminals that call both back-to-back (Flight SQL's
    # Cypher terminal, pgwire's ENGINE route) -- one connection-loop run, not two, preserving
    # REQ-1887's fold. A HIT means the plan never touches the
    # engine, so residency prep (potentially a real materialization) is skipped entirely, not
    # merely deferred, on a cache HIT; a MISS prepares residency exactly as before REQ-1897 existed
    # and returns None so the caller executes the plan against a now-resident source set.
    cached = await check_response_cache(plan, state)
    if cached is not None:
        return cached
    from provisa.federation.query_residency import prepare_engine_residency

    await prepare_engine_residency(state, plan)
    return None


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
        handle = await run_materialize(
            state, plan.physical_sql, cast(Delivery, plan.materialize), plan.exec_params
        )
        return QueryResult(rows=[], column_names=[], redirect=handle)

    if plan.auto_deliver is not None and plan.engine_landing is not None:
        # AUTOMATIC threshold terminal, DIRECT route (REQ-1224 amended 2026-10-01): the router sent
        # this buffered read to one source's own driver, and the plan's statement is bounded at
        # threshold+1 rows. Probe the source with it and inline a result that fits. Only one that
        # does not is landed by the engine, off Provisa's heap, from the engine-physical form
        # derived now.
        from typing import cast

        from provisa.executor.redirect import Delivery, run_materialize

        deliv = cast(Delivery, plan.auto_deliver)
        result = await engine.execute_native(
            state.source_pools, plan.source_id, plan.sql, plan.exec_params, plan.span_attrs
        )
        if len(result.rows) <= deliv.config.threshold:
            return result
        handle = await run_materialize(state, await plan.engine_landing(), deliv, plan.exec_params)
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
        handle = await run_materialize(state, physical_sql, deliv, plan.exec_params)
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
        with _stage(_tracer, _span_name, name="execute") as _span:
            if plan.span_attrs:
                for _k, _v in plan.span_attrs.items():
                    _span.set_attribute(_k, _v)
            _span.set_attribute("db.system", "postgres")
            _span.set_attribute("db.statement", plan.sql[:1000])
            async with tenant_db.acquire() as _conn:
                # Column names come from the result itself, so an empty result still has them.
                col_names, _rows = await _conn.fetch_with_columns(plan.sql)
                rows = [tuple(r) for r in _rows]
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
            serve_cached=True,  # REQ-1897: this function executes at the chokepoint, which serves it
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
    serve_cached: bool = False,
) -> _Plan:
    """Govern+execute all but the LAST statement of a batch, and return the governed+stamped plan for
    the last statement — for Arrow/streaming surfaces (Flight SQL, airport) that render the final
    statement's rows themselves. Guarantees a multi-statement batch's leading statements still run
    (governed), rather than being silently dropped by ``parse_one``. A single statement runs nothing
    extra and just returns its plan. ``serve_cached``: the caller serves a Route.CACHE final plan
    (see :func:`route_governed`)."""
    from provisa.compiler.sql_rewrite import split_sql_statements

    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    statements = split_sql_statements(sql)
    if not statements:
        raise ValueError("empty SQL batch")
    for stmt in statements[:-1]:
        plan = await _govern_and_route(stmt, role_id, session_vars=session_vars, serve_cached=True)
        await _execute_plan(plan, state)
    return await _govern_and_route(
        statements[-1], role_id, session_vars=session_vars, serve_cached=serve_cached
    )


async def govern_batch_final_plan_with_fn(
    sql: str,
    role_id: str,
    state: Any | None = None,
    *,
    session_vars: dict[str, str] | None = None,
    serve_cached: bool = False,
) -> _Plan | QueryResult:
    """``govern_batch_final_plan`` with the registered-function check folded into the SAME
    coroutine (REQ-1887).

    Matches pgwire's ``govern_pgwire_plan`` pattern (function-invocation check, then
    governance, in one dispatch) for the Flight SQL DIRECT route: a `SELECT fn(...)` ticket
    still short-circuits on the full ticket SQL before any statement splitting/governance,
    exactly as the two separate cross-thread calls this replaces did — only the hop count
    changes, not the check order or its inputs."""
    from provisa.pgwire.function_call import maybe_invoke_registered_function

    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    fn_result = await maybe_invoke_registered_function(sql, role_id, state)
    if fn_result is not None:
        return fn_result
    return await govern_batch_final_plan(
        sql, role_id, state, session_vars=session_vars, serve_cached=serve_cached
    )


async def _govern_and_route_compiled(  # REQ-262, REQ-263, REQ-265, REQ-266, REQ-1044
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
    api_args: dict | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    cache_hint: CacheHint,
    serve_cached: bool = False,
) -> _Plan:
    """Governance + routing for already-physical SQL, with the org's tier ceilings bound.

    ``cache_hint`` is the request's response-cache opt-in (REQ-544), required so every caller
    states it: ``compiler.directives.cache_hint_for(language, request_text)``, gRPC's
    ``cache_hint_from_grpc_metadata``, or ``NO_CACHE_HINT`` for a surface with no hint syntax.
    ``serve_cached``: the caller's terminal serves a Route.CACHE plan — see
    :func:`route_governed`."""
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.core.statement_warnings import collecting

    await _wake_before_governing(state)
    with collecting() as found:
        plan = await _govern_and_route_compiled_planned(
            sql,
            role_id,
            exec_params=exec_params,
            state=state,
            api_args=api_args,
            deliver=deliver,
            buffered=buffered,
            cache_hint=cache_hint,
            serve_cached=serve_cached,
        )
    plan.warnings = list(found)
    return await _attach_live_caps(await _attach_tier_caps(plan, state), state)


async def _govern_and_route_compiled_planned(  # REQ-262, REQ-263, REQ-265, REQ-266
    sql: str,
    role_id: str,
    *,
    exec_params: list | None = None,
    state: Any | None = None,
    api_args: dict | None = None,
    deliver: Delivery | None = None,
    buffered: bool = False,
    cache_hint: CacheHint,
    serve_cached: bool = False,
) -> _Plan:
    """Governance + routing for already-physical SQL.

    Used by GQL and Cypher transport paths after language-specific compilation.
    No SQL validation: the compiler produced this SQL from a governed AST, so there is no
    caller-authored text to validate.
    """
    if state is None:
        from provisa.api.app import state  # type: ignore[assignment]
    from provisa.audit.pipeline import begin_audit, write_denial

    if role_id not in state.contexts:
        await write_denial(sql, role_id, None, None, state)  # REQ-1386: policy_denials
        raise PermissionError(f"No schema for role {role_id!r}")

    import sqlglot.expressions as _sg_exp

    from provisa.pgwire.governed_plan import PlanSlot
    from provisa.core.request_context import session_vars_for

    # REQ-1877: the governed form of a compiled statement is a pure function of its text, the role,
    # the person, the session variables and the role's governance objects — its bound values
    # travel separately in ``exec_params`` — so it is kept like the raw-SQL stage's and the GraphQL
    # endpoint's (pgwire.governed_plan) and a repeat is not parsed or governed again.
    _session_vars = session_vars_for(state.roles.get(role_id))
    _slot = PlanSlot(state, "compiled", role_id, sql, sorted(_session_vars.items()))
    _governed = _slot.cached()
    if _governed is None:
        _governed = await _govern_compiled(sql, role_id, state, _session_vars, exec_params)
        # A write is not kept: its admission checks (view writes, unbound branch writes) run per call.
        if not isinstance(
            _governed.parsed, (_sg_exp.Insert, _sg_exp.Update, _sg_exp.Delete, _sg_exp.Merge)
        ):
            _slot.keep(_governed)
    sql, _compiled_tree, gov_ctx = _governed.sql, _governed.parsed, _governed.gov_ctx
    _table_ids, governed_sql = _governed.table_ids, _governed.governed_sql
    # REQ-1910: request entry on the compiled path (GraphQL over Flight, Cypher, gRPC, MCP, REST).
    await resolve_trace_scope(state, role_id, hint=cache_hint.debug_trace)
    _audit = begin_audit(sql, role_id, _compiled_tree, gov_ctx, state.model_stamp)

    # REQ-1897 (amended 2026-10-01): the cache before the route (see route_governed). A sink
    # delivery returns a handle, not rows, and is never answered from it.
    _cache_params = list(exec_params or [])
    # REQ-1915: key bounds and the unbound-read refusal before the cache and the route (see
    # route_governed).
    _pk_bounds_now = _kept_pk_bounds(_governed.memo, governed_sql, state, exec_params)
    _cache_missed: tuple[tuple[int, ...] | None, ...] = ()
    if serve_cached and deliver is None:
        _hit, _cache_missed = await _cached_before_routing(
            state,
            sql=governed_sql,
            params=_cache_params,
            role_id=role_id,
            cache_hint=cache_hint,
            wire_formats=None,
            as_of=None,  # the compiled stage takes no request-level as-of
        )
        if _hit is not None:
            return _cached_plan(
                governed_sql=governed_sql,
                params=exec_params,
                role_id=role_id,
                table_ids=_table_ids,
                cache_hint=cache_hint,
                hit=_hit,
                audit=_audit,
                span_attrs=_kept_span_attrs(_governed.memo, governed_sql, role_id, sql, _audit),
                sources=_governed.memo.get("sources", frozenset()),
                semantic_sql=None,
                as_of=None,
            )

    return await _route_compiled(
        sql,
        role_id,
        state,
        ctx=state.contexts[role_id],
        gov_ctx=gov_ctx,
        governed_sql=governed_sql,
        compiled_tree=_compiled_tree,
        table_ids=_table_ids,
        audit=_audit,
        cache_hint=cache_hint,
        exec_params=exec_params,
        api_args=api_args,
        deliver=deliver,
        buffered=buffered,
        memo=_governed.memo,
        cache_params=_cache_params,
        cache_missed=_cache_missed,
        pk_bounds=_pk_bounds_now,
    )


@dataclass
class _GovernedCompiled:
    """A compiled statement the pipeline has governed but not yet routed — the compiled stage's
    counterpart of :class:`_Governed`, kept the same way (REQ-1877). Value-independent: the
    statement's bound values travel separately."""

    sql: str  # after metric expansion
    parsed: Any
    gov_ctx: Any
    table_ids: tuple[int, ...]
    governed_sql: str
    # Facts derived from this governed statement, kept with it (as _Governed.memo).
    memo: dict[str, Any] = field(default_factory=dict)


async def _govern_compiled(
    sql: str,
    role_id: str,
    state: Any,
    session_vars: dict[str, str],
    exec_params: list | None = None,
) -> _GovernedCompiled:
    """The value-independent half of the compiled stage: parse, metric expansion, write admission,
    governance."""
    import sqlglot as _sg

    from provisa.audit.pipeline import resolve_table_ids
    from provisa.compiler.rls import RLSContext
    from provisa.compiler.stage2 import apply_governance, build_governance_context

    # REQ-1882: sqlglot tokenization off-loaded — see _off_loop's own docstring. This is the
    # Flight SQL / gRPC compiled path's own parse, the same class of blocking call the raw-SQL
    # path's prepare_front_end/_govern_and_route_planned already off-load above.
    _compiled_tree = await _off_loop(lambda: _sg.parse_one(sql, read="postgres"))
    # Nothing is defined through a query protocol: the compiled path's statements (Flight, gRPC,
    # Cypher, GraphQL, REST) meet the same refusal the raw-SQL path's do, at the same point —
    # parsed, not yet governed (provisa/compiler/definitions.py).
    from provisa.compiler.definitions import refuse_definition

    refuse_definition(_compiled_tree)

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

    from provisa.security.rights import require_role

    ctx = state.contexts[role_id]
    rls = state.rls_contexts.get(role_id, RLSContext.empty())

    gov_ctx = build_governance_context(
        role_id,
        rls,
        state.masking_rules,
        ctx,
        getattr(state, "tables", []),
        role=require_role(state.roles, role_id),
        relationships=getattr(state, "relationships", None),
        source_types=state.source_types,
        engine=getattr(state, "federation_engine", None),
    )
    await _guard_complexity(sql, role_id, _compiled_tree, gov_ctx, ctx, state)

    _table_ids = tuple(resolve_table_ids(_compiled_tree, gov_ctx))  # REQ-1897

    # REQ-863 pipeline order: governance → post-governance optimization → routing.
    # REQ-1882: off-loaded, same rationale as the raw-SQL path's own apply_governance call above.
    governed_sql = await _off_loop(apply_governance, sql, gov_ctx, session_vars, exec_params)
    # REQ-1682: session-variable predicates resolve to the request's literals on every route (see
    # the raw path above for why the direct Postgres route cannot keep native current_setting).
    governed_sql = _resolve_session_settings(governed_sql, session_vars)
    return _GovernedCompiled(sql, _compiled_tree, gov_ctx, _table_ids, governed_sql)


async def _route_compiled(
    sql: str,
    role_id: str,
    state: Any,
    *,
    ctx: Any,
    gov_ctx: Any,
    governed_sql: str,
    compiled_tree: Any,
    table_ids: tuple[int, ...],
    audit: Any,
    cache_hint: CacheHint,
    exec_params: list | None,
    api_args: dict | None,
    deliver: Delivery | None,
    buffered: bool,
    memo: dict[str, Any],
    cache_params: list,
    cache_missed: tuple[tuple[int, ...] | None, ...],
    pk_bounds: tuple[Any, ...],
) -> _Plan:
    """The per-call half of the compiled stage: optimization, routing and the plan. ``memo`` is
    the governed statement's own (see :func:`_kept_lowering`).

    REQ-074/REQ-1386: ``audit`` is the record the compiled surfaces (GQL, Cypher, Flight, gRPC)
    open like the raw-SQL path — the terminal finalizes it. REQ-544 (amended 2026-09-30):
    ``cache_hint`` is the response-cache opt-in the REQUEST carried, handed through unchanged."""
    from provisa.compiler.sql_rewrite import (
        rewrite_semantic_to_catalog_physical,
        rewrite_semantic_to_physical,
    )
    from provisa.transpiler.router import Route
    from provisa.transpiler.transpile import transpile

    _compiled_tree = compiled_tree
    _table_ids = table_ids
    _audit = audit
    _cache_hint = cache_hint
    # A write on a compiled surface (a GraphQL mutation, a Cypher write over HTTP or Bolt) ends
    # like one on the raw-SQL surfaces: nothing cached, the steps after a write at the terminal.
    import sqlglot.expressions as exp

    from provisa.compiler.write_admission import written_table_id

    _is_write = isinstance(_compiled_tree, (exp.Insert, exp.Update, exp.Delete, exp.Merge))
    _written_table_id = written_table_id(_compiled_tree, gov_ctx) if _is_write else None

    # Post-governance optimization stage (may REMOVE sources): lower to catalog-physical, then
    # inline hot/API tables as VALUES CTEs, prune unreachable union branches, and rewrite cached
    # tables. This MUST complete before extract_sources/decide_route so routing observes the
    # reduced source set (a query whose second source is fully inlined collapses to DIRECT).
    # REQ-1882: off-loaded, same rationale as the raw-SQL path's rewrite call above.
    _exec_sql = await _kept_lowering(
        memo, lambda: rewrite_semantic_to_catalog_physical(governed_sql, ctx)
    )
    _view_map = getattr(state, "view_sql_map", None)
    if _view_map:
        from provisa.compiler.view_expand import expand_view_refs
        from provisa.mv.view_read import view_bodies

        # As on the raw-SQL stage: each view reference becomes what THIS reader may read of it.
        _exec_sql = expand_view_refs(_exec_sql, view_bodies(_exec_sql, _view_map, state, gov_ctx))
    from provisa.compiler.nf_extractor import extract_nf_args

    _exec_sql, _nf_clean_params, _extracted_nf = extract_nf_args(_exec_sql, exec_params or [])
    exec_params = _nf_clean_params if _nf_clean_params != (exec_params or []) else exec_params
    _nf_args = {**(api_args or {}), **(_extracted_nf or {})} or None
    # Route on the OUTPUT of the optimization stage (REQ-863): sources whose every referenced
    # table was inlined/pruned drop out of the routing set.
    (
        _exec_sql,
        decision,
        _default_source,
        _optimized,
        sources,
        _opts,
    ) = await _optimize_and_route_cached(
        _exec_sql,
        governed_sql,
        gov_ctx,
        ctx,
        state,
        role_id,
        table_ids=_table_ids,
        nf_args=_nf_args,
    )
    # REQ-1910: the sources are known now — a window opened on one of them covers this request.
    await extend_trace_scope_to_sources(state, role_id, frozenset(sources))
    memo["sources"] = frozenset(sources)  # what a later cache hit reports it read

    # REQ-135/REQ-1163: a query referencing a __derived__ view MUST route through the engine, where
    # the view was already inline-expanded above. A view's virtual source has no native driver/
    # catalog — if routing picks DIRECT (legitimate once expansion collapses the query onto a single
    # real source), the DIRECT branch's non-optimized fallback rebuilds physical SQL from the
    # UN-expanded ``governed_sql`` and hands the raw view ref to a native pool. Force ENGINE so the
    # ENGINE branch's already-expanded ``_exec_sql`` is what actually executes. Same guard
    # ``_govern_and_route`` (the raw-SQL/pgwire path) already applies.
    if _view_map and decision.route != Route.ENGINE:
        if _kept_refs_view(memo, governed_sql, _view_map):
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
    # None when redirect is disabled (opt-in).
    from provisa.executor.redirect import auto_delivery_for_buffered

    auto_deliver = auto_delivery_for_buffered(role_id) if buffered and deliver is None else None
    # REQ-1224 (amended 2026-10-01): the threshold does not choose the route. A read the router
    # sends to one source's own driver stays there and is bounded by a threshold+1 probe at the
    # terminal; the engine-physical form is derived only if the result does not fit.
    _probe_direct = (
        auto_deliver is not None
        and _direct_terminal_serves(decision, _default_source, state)
        and _kept_probe_bounds(memo, governed_sql)
    )
    if auto_deliver is not None and decision.route != Route.ENGINE and not _probe_direct:
        from provisa.transpiler.router import RouteDecision

        decision = RouteDecision(
            route=Route.ENGINE,
            source_id=None,
            dialect=None,
            reason="buffered-transport auto-delivery",
        )

    async def _engine_form(_exec_sql: str) -> tuple[str, str]:
        """The routed catalog-physical statement as the engine runs it: the literal-predicate
        carry and catalog fold applied, and that text in the engine's own SQL (see
        :func:`_kept_engine_form`, which keeps both with the governed statement)."""
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
        # REQ-1880: same literal-predicate carry the raw-SQL path applies (one helper, both paths).
        _exec_sql = await propagate_literal_predicates(_exec_sql, state)
        # REQ-1730: mirrors _govern_and_route_planned's identical fold — PostgreSQL cannot express
        # a catalog.schema.table reference (no cross-database queries), so an engine that declares
        # catalog_qualified=False needs the catalog folded into the schema name here too, or a
        # multi-source query on the compiled path (GQL/Cypher/Flight/gRPC) fails with "cross-
        # database references are not implemented" — confirmed live: federated_join over GraphQL
        # against the pg engine, which the raw-SQL/pgwire path handles fine because only that path
        # applied this fold before now.
        if not state.federation_engine.engine.catalog_qualified:
            from provisa.compiler.sql_rewrite import fold_catalog_into_schema

            _exec_sql = fold_catalog_into_schema(_exec_sql)
        return _exec_sql, state.federation_engine.transpile_physical(_exec_sql)

    async def _engine_forms(_routed_sql: str) -> tuple[str, str]:
        _engine_sql, _physical = await _kept_engine_form(
            memo, _routed_sql, state, lambda: _engine_form(_routed_sql)
        )
        # REQ-041/402: RLS is added to the governed semantic SQL as a
        # current_setting('provisa.<var>') predicate; PostgreSQL resolves it
        # natively (SET LOCAL) but the federation engine has no such function.
        # Resolve it to the session's literal value here at planning so it works
        # regardless of the requesting query language.
        from provisa.core.request_context import session_vars_for

        _session_vars = session_vars_for(state.roles.get(role_id))  # REQ-1682
        return _engine_sql, _resolve_session_settings(_physical, _session_vars)

    # (Removed 2026-09-26, REQ-1864 reversal:) a single-source neo4j pattern previously
    # reverse-compiled governed_sql back to Cypher (best_effort_cypher_for_sql) and forced
    # Route.DIRECT so Neo4j itself could resolve the join. Reverted: direct Cypher execution
    # requires a general method to resolve arbitrary SQL join patterns back into correct Cypher,
    # which the reverse compiler does not have (it mis-translated a reshaped junction table).
    # Row-level materialization (REQ-1865) is the sanctioned mechanism for neo4j read performance
    # instead. A single-source neo4j query now always falls through to Route.ENGINE.
    if decision.route == Route.ENGINE:
        _exec_sql, physical_sql = await _engine_forms(_exec_sql)
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
            span_attrs=_kept_span_attrs(memo, governed_sql, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(sources),
            route_reason=decision.reason,
            optimizations=_opts,
            pk_bounds=pk_bounds,  # REQ-1865
            # REQ-1897: the compiled surfaces (GraphQL-via-plan, Cypher, REST, JSON:API, gRPC)
            # hand this function a read the compiler built from a query AST; writes take the
            # mutation executor, never this path. A sink delivery returns a handle, not rows.
            response_cacheable=deliver is None and not _is_write,
            writes_tables=_is_write,
            written_table_id=_written_table_id,
            cache_sql=governed_sql,
            cache_params=cache_params,
            cache_missed=cache_missed,
            role_id=role_id,  # REQ-1897
            table_ids=_table_ids,  # REQ-1897
            cache_opt_in=_cache_hint.opt_in,  # REQ-544 (amended)
            cache_ttl=_cache_hint.ttl,
        )
    else:
        dialect = decision.dialect or "postgres"
        from provisa.compiler.sql_rewrite import FLAT_NAMESPACE_SOURCES, strip_schema

        _direct_sid = decision.source_id or _default_source
        _flat = state.source_types.get(_direct_sid) in FLAT_NAMESPACE_SOURCES
        # REQ-1224: a buffered read is bounded at threshold+1 rows for the probe — the same
        # bound governance applies for a role's row ceiling (stage2.apply_row_cap).
        _cap = auto_deliver.config.threshold + 1 if _probe_direct and auto_deliver else None
        # Direct route lowers the OPTIMIZED SQL (REQ-863): when the optimization stage inlined a
        # VALUES CTE, strip the catalog so a native driver addresses schema.table with the CTE
        # carried onto the direct path. With no optimization the lowering is a function of the
        # governed text, the role's compilation context and the destination — not of the bound
        # values — so it is kept with the governed statement (as route_governed keeps its own).
        _direct_key = f"direct_sql\x00{_direct_sid}\x00{dialect}\x00{_flat}\x00{_cap}"
        _direct = None if _optimized else memo.get(_direct_key)
        if _direct is None:
            if _optimized:
                from provisa.compiler.sql_rewrite import strip_catalog

                physical_sql = strip_catalog(_exec_sql)
            else:
                physical_sql = rewrite_semantic_to_physical(governed_sql, ctx)
            if _flat:
                physical_sql = strip_schema(physical_sql)
            if _cap is not None:
                from provisa.compiler.stage2 import apply_row_cap

                physical_sql = apply_row_cap(physical_sql, _cap)
            _direct = (transpile(physical_sql, dialect), physical_sql)
            if not _optimized:
                memo[_direct_key] = _direct
        sql_to_run, physical_sql = _direct
        _routed_sql = _exec_sql

        async def _engine_landing() -> str:
            return (await _engine_forms(_routed_sql))[1]

        return _Plan(
            route=decision.route,
            sql=sql_to_run,
            exec_sql=physical_sql,
            source_id=_direct_sid,
            dialect=dialect,
            exec_params=exec_params,
            auto_deliver=auto_deliver,  # REQ-1224: probed at the terminal
            engine_landing=_engine_landing if _probe_direct else None,
            # REQ-1425: every route carries the plan's span attributes (see _govern_and_route).
            span_attrs=_kept_span_attrs(memo, governed_sql, role_id, sql, _audit),
            audit=_audit,  # REQ-074/REQ-1386: finalized at the terminal
            stamp=_mint_stamp(),  # governed-provenance: minted at the top of the pipeline
            # REQ-1517: the plan reports how it was built (sources, route reason, optimizations).
            sources=frozenset(sources),
            route_reason=decision.reason,
            optimizations=_opts,
            pk_bounds=pk_bounds,  # REQ-1865
            # REQ-1897: the compiled surfaces (GraphQL-via-plan, Cypher, REST, JSON:API, gRPC)
            # hand this function a read the compiler built from a query AST; writes take the
            # mutation executor, never this path. A sink delivery returns a handle, not rows.
            response_cacheable=deliver is None and not _is_write,
            writes_tables=_is_write,
            written_table_id=_written_table_id,
            cache_sql=governed_sql,
            cache_params=cache_params,
            cache_missed=cache_missed,
            role_id=role_id,  # REQ-1897
            table_ids=_table_ids,  # REQ-1897
            cache_opt_in=_cache_hint.opt_in,  # REQ-544 (amended)
            cache_ttl=_cache_hint.ttl,
        )


_OPENING_WRITE_RE = re.compile(
    r"(?:\s+|--[^\n]*\n?|/\*.*?\*/)*(?P<verb>INSERT|UPDATE|DELETE|MERGE)\b",
    re.IGNORECASE | re.DOTALL,
)


def opening_write_verb(sql: str) -> str | None:
    """The verb ``sql`` opens with when it opens as a data write, else None."""
    m = _OPENING_WRITE_RE.match(sql)
    return m.group("verb").upper() if m else None


async def plan_pgwire_sql(sql: str, role_id: str) -> _Plan:  # REQ-267
    return await _govern_and_route(sql, role_id)


async def govern_pgwire_plan(  # REQ-028, REQ-266
    sql: str, role_id: str, params: list | None = None, wire_formats: list[int] | None = None
) -> _Plan | QueryResult:
    """Govern a pgwire statement to its last-mile plan WITHOUT executing the ENGINE terminal.

    ``wire_formats`` — the Bind's result format codes, when the client stated them. pgwire serves
    a Route.CACHE plan (its last terminal branch is ``_execute_plan``), so an opted-in read is
    looked up in the response cache before it is routed (REQ-1897) — the passthrough entry for
    those codes first, then the decoded one.

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
    # (its command admission there) instead of federation, unifying invocation across surfaces.
    from provisa.api.app import state as _state
    from provisa.pgwire.function_call import maybe_invoke_registered_function

    # A registered-function call's arguments are VALUES handed to the function executor, not SQL
    # sent to a source, so detection sees the value-substituted text (REQ-872). Everything else is
    # governed with its $N placeholders and the values bound (REQ-589).
    fn_sql = sql
    if params:
        from provisa.compiler.params import _sql_literal, substitute_positional_placeholders

        fn_sql = substitute_positional_placeholders(
            sql, params, lambda i: _sql_literal(params[i - 1])
        )
    fn_result = await maybe_invoke_registered_function(fn_sql, role_id, _state)
    if fn_result is not None:
        return fn_result

    plan = await _govern_and_route(
        sql, role_id, params=params, serve_cached=True, wire_formats=wire_formats
    )
    return plan


@dataclass
class _Described:
    """A pgwire statement described without running it (REQ-589, amended 2026-10-01): its result
    columns, and the governed statement its Execute continues from."""

    shape: list[tuple[str, str]]
    # None for a registered-function call: a command has no governed statement to hold — its
    # Execute invokes it through ``govern_pgwire_plan``.
    governed: _Governed | None


def _function_call_shape(name: str, state: Any) -> list[tuple[str, str]]:
    """A registered command's result columns, from its declared output contract (REQ-1159)."""
    from provisa.pgwire.result_shape import UnderivableColumn

    action = (getattr(state, "tracked_functions", None) or {}).get(name) or (
        getattr(state, "tracked_webhooks", None) or {}
    ).get(name)
    declared = (action or {}).get("output_columns")
    if isinstance(declared, str):
        import json

        declared = json.loads(declared)
    if not declared:
        raise UnderivableColumn(
            f"cannot describe a call to {name!r}: it declares no output_columns, so its result "
            "columns are unknown until it runs"
        )
    return [(c["name"], c["type"]) for c in declared]


async def describe_pgwire_statement(sql: str, role_id: str) -> _Described:  # REQ-589
    """Govern *sql* and derive its result columns from registered metadata — nothing is routed,
    executed or sent to a source. The Execute continues from the returned governed statement
    (:func:`plan_pgwire_statement`), so the statement is governed exactly once."""
    from provisa.api.app import state
    from provisa.pgwire.ext_surfaces import rewrite_surface_operators
    from provisa.pgwire.function_call import detect_sql_function_call
    from provisa.pgwire.result_shape import derive_result_shape

    sql = rewrite_surface_operators(sql)
    call = detect_sql_function_call(sql, state, role_id)
    if call is not None:
        return _Described(_function_call_shape(call[0], state), None)

    await _wake_before_governing(state)
    governed = await govern_statement(sql, role_id)
    # REQ-1882: the derivation parses and may annotate the statement (sqlglot); off-loaded like the
    # pipeline's other parse work (see _off_loop).
    shape = governed.memo.get("result_shape")
    if shape is None:
        shape = await _off_loop(
            derive_result_shape,
            governed.governed_semantic,
            governed.gov_ctx.table_map,
            governed.ctx,
            state.schema_build_cache["column_types"],
        )
        governed.memo["result_shape"] = shape
    return _Described(shape, governed)


async def plan_pgwire_statement(  # REQ-589
    governed: _Governed, params: list | None, wire_formats: list[int] | None = None
) -> _Plan:
    """The executable plan for a statement :func:`describe_pgwire_statement` already governed, with
    the Bind's parameter values. Routing runs here, against live state; governance does not run
    again. ``wire_formats``: see :func:`govern_pgwire_plan`."""
    from provisa.api.app import state

    plan = await route_governed(
        governed, params=params, serve_cached=True, wire_formats=wire_formats
    )
    return await _attach_live_caps(await _attach_tier_caps(plan, state), state)


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

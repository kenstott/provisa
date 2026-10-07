# Copyright (c) 2026 Kenneth Stott
# Canary: e4a938df-f58a-4719-bb09-b5be09706be8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-request multi-org data-plane routing (REQ-1266).

The data endpoints resolve their per-org maps (source pools, roles, compiled
schemas/contexts, catalog names, masking rules, …) through the process-global
``AppState``. Historically those maps were fixed at startup for one org, so a
newly created org was not queryable without a restart.

This module carries the per-org slice of that state in an :class:`OrgRuntime`,
holds one runtime per org in an :class:`OrgRegistry` (lazily built, no TTL —
an org's compiled state changes only on explicit reload), and selects the
active runtime for the current request via the ``current_org`` ContextVar.

``AppState`` exposes the routed maps as property shims that resolve the
ContextVar-selected runtime (defaulting to the default-org runtime when the
ContextVar is unset — startup, background boot, single-org tests), so the
hundreds of ``state.X`` call sites need no change.

No-fallback rule: :func:`require_current_org` RAISES when the ContextVar is
unset or the org has no built runtime. The single-org default binding is done
once at the entrypoint (never silently here).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.core.connection_loop import CrossLoopLock
from provisa.core.environments import PROD
from provisa.executor.pool import SourcePool
from provisa.federation.replica_address import ReplicaRoutes

if TYPE_CHECKING:
    import graphql

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext
    from provisa.core.database import Database


def runtime_key(org_id: str, env: str | None = None) -> str:
    """The registry key for one org's ONE environment (REQ-1488, REQ-1529).

    An environment is a separate schema holding a separate copy of the model, so it is a separate
    data plane: its own compiled schemas, its own source pools, and — the point of REQ-1529 — its
    own resolved bindings, which may be inherited from its base and may be read-only. One runtime
    per org would hand a branch its base's connections.

    ``prod`` and ``None`` both key on the BARE org id, which is what keeps an org that never created
    an environment byte-identical: the key it was registered under before environments existed is
    still the key it is registered under, and ``org_registry.get(org_id)`` still finds it. The
    separator matches :func:`provisa.core.environments.org_schema`'s reason for its own — REQ-1309
    forbids an underscore in an org id, so the first ``_env_`` splits the key into exactly one org
    and one environment whatever either is called.
    """
    if env is None or env == PROD:
        return org_id
    return f"{org_id}_env_{env}"


@dataclass
class OrgRuntime:
    """The per-org slice of the data plane.

    One instance per org, built lazily by the registry. Everything here is keyed
    within a single org's namespace (source_id / role_id / table_name); the
    genuinely-global handles (admin_db, federation_engine, response cache, mv
    registry, servers) stay on ``AppState``.
    """

    org_id: str

    # REQ-1487/REQ-1529: WHICH ENVIRONMENT of that org this runtime is. ``prod`` for every runtime
    # built before an org created an environment, and for every request that names none. Carried on
    # the runtime and not only in the ContextVar because the binding resolution of REQ-1529 needs to
    # know which environment asked in order to know whose lineage to walk, and the code that asks
    # holds the runtime rather than the request.
    env: str = PROD

    # REQ-1621: this environment has an expiry, so everything it holds is a copy that is thrown
    # away. Read once when the runtime is built, from ``env_registry.expires_at``. What it changes
    # is the mutation gate: a remote-schema mutation reaches a system the environment does NOT own
    # and cannot copy, so the ADMIN bypass that would otherwise let a sandbox visitor's ``org_admin``
    # write to it is withheld and the operation's own ``writable_by`` is the whole answer. It says
    # nothing about which sources the environment may point at — that is the environment's own
    # business (REQ-1620 covers the file-backed ones by forking them, not by refusing them).
    ephemeral: bool = False

    # REQ-1942: this environment's data mode (inherit | unbound | test_fake | test_synthetic) and
    # what a mutation in it does (refused | reversible | direct). None for prod, which is always
    # real. Read once when the runtime is built; a change of either rebuilds it.
    data_mode: str | None = None
    mutation_handling: str | None = None
    # REQ-1942: why every request into this environment is refused, while it stands -- a Test
    # (fake) environment whose model holds a sensitive column with no fake. Set by each schema
    # build; None when the environment serves.
    data_refusal: str | None = None
    # REQ-1942: per table with kept mutations (Reversible), its change log's address and primary
    # key. Set by each schema build; empty for prod.
    kept: dict[int, tuple[str, list[str]]] = field(default_factory=dict)

    # REQ-1043/REQ-1067/REQ-1244: per-org federation engine. ``None`` means this org runs on the
    # SHARED engine (the pooled lane, REQ-1243 lane a — every org starts here); an isolated-engine
    # org (orgs.isolated_engine) carries its OWN EngineRuntime plus its own terminal-connection
    # state, so its queries never touch the shared coordinator. The AppState shims route
    # ``state.federation_engine`` / ``state.engine_conn`` / ``state.engine_conn_kwargs`` here
    # whenever ``federation_engine`` is bound, and to the default-org (shared) runtime otherwise.
    isolated_engine: bool = False
    # REQ-1412: ``(host, port)`` of a coordinator the ORG operates (external engine). Set only in
    # external mode; the terminal binds here instead of resolving the shared or SaaS-dedicated
    # endpoint. ``isolated_engine`` is true alongside it because the org still carries its own
    # EngineRuntime — what differs is who runs the coordinator.
    engine_endpoint: tuple[str, int] | None = None
    # REQ-1418: the engine KIND this org's own coordinator is (an _ENGINE_BUILDERS key), and the
    # DSN it is addressed by when that kind is URL-addressed. ``None`` means the deployment's kind
    # / the deployment's URL — the state of every shared and isolated org. Held here rather than
    # read per query because the AppState shims make it reachable from the engine layer
    # (``state.active_engine_url``) exactly as ``engine_endpoint`` already is.
    engine_kind: str | None = None
    engine_url: str | None = None
    # REQ-1448/REQ-1450: which shared-lane shard answers this org (``orgs.shard``), and the
    # generation of that shard the org's ``CREATE CATALOG`` statements were issued to. A shard that was
    # released and rebuilt comes back with an empty dynamic catalog store, so a stamp
    # older than the shard's current generation means this runtime's catalogs are on a coordinator
    # that no longer exists and the runtime has to be rebuilt before the next query dispatches.
    shard: str = ""
    engine_generation: int = 0
    # REQ-1048: the org's OWN materialization store (bring-your-own storage), decrypted once at
    # build time from ``orgs.storage_url_enc``. Set means every MV output and landed table for this
    # org is written to a store the ORG pays for, so its footprint is neither metered nor capped by
    # the REQ-1046 tier quota. ``None`` means the platform's store, where the quota applies — the
    # state of every org that has not registered one.
    storage_url: str | None = None
    federation_engine: Any = None
    engine_conn: Any = None
    engine_conn_kwargs: dict = field(default_factory=dict)

    # REQ-1919/1922: the org's two control-plane handles. ``model_db`` holds its MODEL (shared by
    # every region the org selects); ``tenant_db`` holds this region's STATE (replica builds,
    # events, the request record). Two handles even where one database holds both, so a
    # statement on the wrong one fails everywhere (provisa/core/store_sides.py).
    model_db: "Database | None" = None
    tenant_db: "Database | None" = None
    # REQ-1922: this region's request record (query_audit_log, query_sla_log).
    record_db: "Database | None" = None

    # Physical connection + source metadata (source_id → …).
    source_pools: SourcePool = field(default_factory=SourcePool)
    source_types: dict[str, str] = field(default_factory=dict)
    source_dialects: dict[str, str] = field(default_factory=dict)
    source_dsns: dict[str, str] = field(default_factory=dict)
    # source_id → engine catalog name. For org-scoped sources this is the
    # org-prefixed name (org_{id}__{catalog}); system/fixed-warehouse catalogs
    # are stored un-prefixed (one physical catalog shared across orgs).
    source_catalogs: dict[str, str] = field(default_factory=dict)
    source_cache: dict[str, dict] = field(default_factory=dict)
    source_allowed_domains: dict[str, list[str]] = field(default_factory=dict)
    source_federation_hints: dict[str, dict[str, str]] = field(default_factory=dict)

    # Compiled, role-keyed state (role_id → …).
    roles: dict[str, dict] = field(default_factory=dict)
    schemas: dict[str, "graphql.GraphQLSchema"] = field(default_factory=dict)
    contexts: dict[str, "CompilationContext"] = field(default_factory=dict)
    # The whole model — every registered table and column, no role's grants applied — used ONLY
    # to lower view SQL to physical (api/app_rebuild.py). It belongs to no role and never answers
    # a request: who may read a view's rows is decided when the view is read.
    view_context: "CompilationContext | None" = None
    rls_contexts: dict[str, "RLSContext"] = field(default_factory=dict)
    # REQ-1677: role id → [role, parent, grandparent, …], the chain folded into the build.
    role_chains: dict[str, list[str]] = field(default_factory=dict)

    # REQ-1877: the org's kept governed plans (pgwire/governed_plan.py) and validate outcomes —
    # see provisa/compiler/compiled_query_cache.py. Scoped per org, same as
    # `contexts`/`rls_contexts`, so one org's plans never leak into another's. Not reset by a
    # schema rebuild: its key already carries schema_boot_id/schema_version, so a stale
    # generation's entries simply stop matching. No time expiry (amended 2026-10-01): a plan is
    # invalidated by its generation and anchors, and the store is bounded by size, dropping the
    # least recently used.
    compiled_query_cache: CompiledQueryCache = field(
        default_factory=lambda: CompiledQueryCache(expires=False)
    )

    # REQ-1877 (routing addendum, 2026-09-29): sibling cache of `RoutingOutcome` — the structural
    # routing decision (`_optimize_and_route`'s route/source/dialect/source-set) for a query shape
    # whose exec SQL referenced no candidate API-backed table (see
    # provisa/compiler/compiled_query_cache.py's "ROUTING-DECISION CACHING" section for the exact
    # safety boundary). Kept as a second instance rather than reusing `compiled_query_cache`
    # because `routing_cache_key` builds an unrelated key shape (no person_id/bypass flag) — see
    # that module's `CompiledQueryCache` docstring.
    routing_cache: CompiledQueryCache = field(default_factory=CompiledQueryCache)

    # REQ-1877: the org's Cypher label maps for the CURRENT schema generation, one per role and
    # domain-access scope (provisa/api/rest/cypher_plan.py). Not the bounded plan store: a map is
    # a pure function of the registry and as large as it, so it stays exactly until the generation
    # changes, and a new generation's first map drops the previous generation's.
    cypher_label_maps: dict[Any, Any] = field(default_factory=dict)

    # Governance / masking. (table_id, role_id) → {col: (rule, dtype)}.
    masking_rules: dict[Any, Any] = field(default_factory=dict)

    # Full source-row map from the DB, published by _rebuild_schemas so NativeEngineBackend can
    # attach dynamically registered sources that are not in state.config (YAML-only). Keyed by
    # source_id; values are raw dicts from the DB sources table.
    runtime_sources: dict[str, dict] = field(default_factory=dict)

    # REQ-1529: source_id → the environment whose binding supplied that source's connection values.
    # Its own name when this environment bound the source itself, an ancestor's when the binding was
    # inherited. Empty for prod: prod branches from nothing, so every binding it has is its own.
    # Recorded at rebuild time because that is when the lineage walk happens, and read by the write
    # path, which must not walk it again per statement.
    source_binding_env: dict[str, str] = field(default_factory=dict)

    # The commands of this environment's model, by the names every surface calls them by
    # (app_loaders._load_tracked_functions_and_webhooks). Each environment holds its own model,
    # so its own commands: a Test (synthetic) environment defines fewer than its parent.
    tracked_functions: dict[str, dict] = field(default_factory=dict)
    tracked_webhooks: dict[str, dict] = field(default_factory=dict)
    # REQ-1942: command name -> why it is not defined here: a command of a generated API source
    # in a Test (synthetic) environment. A call to one is refused saying so.
    undefined_commands: dict[str, str] = field(default_factory=dict)

    # Raw-SQL governance inputs (published once per org at schema-load time).
    tables: list[dict] = field(default_factory=list)
    # REQ-1912: the tables served from a replica on the bound engine, by the name a lowered
    # statement gives them, published with the registry. Empty until the first rebuild: no table
    # is registered yet, so none is served from a replica.
    replica_routes: ReplicaRoutes = field(default_factory=ReplicaRoutes)
    # REQ-1266/REQ-286: the org's live-query engine -- its prod runtime's only; an environment's
    # live config runs once promoted. None until the runtime is built, and for an environment.
    live_engine: Any = None
    relationships: list[dict] = field(default_factory=list)
    # REQ-1317: config-declared metric registry (name → Metric), published alongside tables
    # so the raw-SQL path can expand `metrics.<name>` queries before governance.
    metrics: dict[str, Any] = field(default_factory=dict)
    # Raw registry rows (domains, tables, column_types, …) the on-demand domain-filtered schema
    # builder reads. Per-org because domains ARE per-org: a process-global cache is overwritten by
    # whichever org rebuilt last, so /data/domains hands one org's domain list to another.
    schema_build_cache: dict = field(default_factory=dict)
    # What any role's surface is built from in this generation (app_loaders.register_role_surface).
    role_build_inputs: dict = field(default_factory=dict)
    # meta-role id → the held roles it acts as (security/meta_role.py), this generation.
    meta_roles: dict = field(default_factory=dict)

    # REQ-1349: this org's rows from its ``org_settings`` table, read once at build time and
    # refreshed by the settings router when the org writes one. Cached here rather than read per
    # request because the query path consults it (response-cache TTL, large-result redirect) and a
    # control-plane round trip per query is not a cost those readers can carry. Empty for an org
    # that has overridden nothing — every read then resolves the deployment value.
    settings_overrides: dict[str, Any] = field(default_factory=dict)
    # REQ-1922: the org's own response cache and Hot counts, on the cache store its region names.
    # None on a runtime that has none of its own: it is served the deployment's, held on the
    # default runtime (AppState.response_cache_store / hot_counts).
    response_cache_store: Any = None
    hot_counts: Any = None

    # REQ-1914: the config stamps this runtime's copies were loaded at — the tenant plane's
    # ``model`` stamp read before the schema build read the model, and its ``settings`` stamp read
    # before ``settings_overrides`` was. ``None`` until the first load. The process's config
    # watcher (provisa/api/model_reload.py) compares each with the stored stamp and reloads the
    # copy when they differ, so a change made through another worker process or instance is in
    # force here within the reload interval; no request reads the control plane for it.
    model_stamp: int | None = None
    settings_stamp: int | None = None
    # REQ-826: the tenant plane's ``replica`` stamp ``replica_routes`` was published at (read
    # before the registry was). The stamp moves when a busy table is promoted or demoted and
    # when a promoted table's first replica completes; the watcher then republishes the routes
    # (``replica_routes.generation`` moves only when they differ).
    replica_stamp: int | None = None
    # REQ-1914: what each ``sources`` row held when this runtime last built its per-source state
    # (pool, dialect, catalog name), so a reload rebuilds that state only for a source whose row
    # was added, changed or removed. ``None`` until the first schema build.
    source_rows: dict[str, tuple] | None = None


class OrgRegistry:
    """Lazily-built ``dict[org_id, OrgRuntime]`` with a per-org build lock.

    ``get_or_build`` is double-checked: a hit returns immediately; a miss takes
    the org's lock, re-checks (a concurrent request may have built it), then runs
    the async ``builder`` exactly once. No TTL — a runtime is rebuilt only on an
    explicit reload (config change), never on a timer.
    """

    def __init__(self) -> None:
        self._runtimes: dict[str, OrgRuntime] = {}
        # REQ-1882 (amended 2026-09-29): pgwire/Bolt/Flight build and read runtimes from their
        # connection threads' own loops, so the per-org lock must work across loops and threads;
        # setdefault is atomic, so two threads never each install a different lock for one org.
        self._locks: dict[str, CrossLoopLock] = {}

    def _lock_for(self, org_id: str) -> CrossLoopLock:
        return self._locks.setdefault(org_id, CrossLoopLock())

    def get(self, org_id: str) -> OrgRuntime | None:
        return self._runtimes.get(org_id)

    def set(self, org_id: str, runtime: OrgRuntime) -> None:
        old = self._runtimes.get(org_id)
        self._runtimes[org_id] = runtime
        if old is not None and old is not runtime:
            _retire(old)

    def invalidate(self, org_id: str) -> None:
        old = self._runtimes.pop(org_id, None)
        if old is not None:
            _retire(old)

    def live_engines(self) -> list[Any]:
        """Every org's running live-query engine (REQ-1266), for shutdown."""
        return [rt.live_engine for rt in self._runtimes.values() if rt.live_engine is not None]

    def invalidate_org(self, org_id: str) -> None:
        """Drop the org's runtime AND every environment runtime built from it (REQ-1488).

        An org is deleted or re-provisioned as a whole: its branches go with it, and a branch
        runtime left behind holds pools and compiled schemas for a schema that no longer exists.
        ``invalidate`` alone reaches only prod, because prod is keyed on the bare org id.
        """
        prefix = f"{org_id}_env_"
        for key in [k for k in self._runtimes if k.startswith(prefix)]:
            self.invalidate(key)
        self.invalidate(org_id)

    def env_keys(self, org_id: str) -> list[str]:
        """Every registry key currently held for one org — its prod key and each environment's."""
        prefix = f"{org_id}_env_"
        return [k for k in self._runtimes if k == org_id or k.startswith(prefix)]

    def all_org_ids(self) -> list[str]:
        return list(self._runtimes.keys())

    async def get_or_build(
        self, org_id: str, builder: Callable[[str], Awaitable[OrgRuntime]]
    ) -> OrgRuntime:
        # No lock-free fast path: the builder registers a runtime under this same key BEFORE it
        # finishes building (build_org_runtime needs the entry early so the AppState property
        # shims can route build-time writes onto it), so a bare dict.get() here could return a
        # half-built runtime — schemas still empty — to a concurrent caller. Always taking the
        # lock serializes against that build; an already-built entry is a cheap uncontended
        # acquire, not a rebuild.
        async with self._lock_for(org_id):
            existing = self._runtimes.get(org_id)
            if existing is not None:
                return existing
            runtime = await builder(org_id)
            self._runtimes[org_id] = runtime
            return runtime

    async def rebuild(
        self, org_id: str, builder: Callable[[str], Awaitable[OrgRuntime]]
    ) -> OrgRuntime:
        """Run ``builder`` unconditionally, under the SAME per-org lock as ``get_or_build``.

        REQ-1322: provisioning must build an org's runtime even when one is already cached, but it
        may not do so concurrently with a request that lazily builds the same org. Both paths run
        the seeding DDL (``DROP VIEW IF EXISTS`` / ``CREATE VIEW``), and interleaving them loses the
        DROP — Postgres rejects the second CREATE with a duplicate ``pg_type`` key and provisioning
        fails with the org half-seeded. The lock is the whole point: build here, never at the call
        site.
        """
        async with self._lock_for(org_id):
            runtime = await builder(org_id)
            self._runtimes[org_id] = runtime
            return runtime


class ActiveOrgPool:  # REQ-1266
    """A tenant-control-plane handle that resolves to whichever org is bound *at the moment of use*.

    ``AppState.tenant_db`` is a property routed by the ``current_org`` ContextVar. Reading it once
    at wiring time — which is what passing ``state.tenant_db`` into a long-lived object does —
    freezes that reader to the default org's schema, so a member's ``user_role_assignments`` row in
    any other org is invisible no matter which org the request binds. Holding this handle instead
    defers the read to each call.

    Truthiness reports whether a tenant plane is bound at all, so callers gating on "is there a
    tenant control plane to read" keep working when there is none (single-org boot, unsecured).
    """

    __slots__ = ()

    def resolve(self) -> "Database | None":
        from provisa.api.app import state

        # REQ-1919: what it reads (user_role_assignments) is the org's model.
        return getattr(state, "model_db", None)

    def __bool__(self) -> bool:
        return self.resolve() is not None

    def acquire(self):
        db = self.resolve()
        if db is None:
            raise RuntimeError(
                "No tenant control plane is bound for the active org. The org runtime must be "
                "built (ensure_org_runtime) before its control plane is read."
            )
        return db.acquire()


def _retire(runtime: OrgRuntime) -> None:
    """A runtime no longer served stops its live-query engine: its polls are its org's, and a
    replaced runtime's subscribers end with it (the replacement starts its own)."""
    engine = runtime.live_engine
    if engine is None:
        return
    runtime.live_engine = None
    from provisa.core.connection_loop import spawn_background

    spawn_background(engine.stop(), name=f"live-engine-stop-{runtime.org_id}")

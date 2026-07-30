# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
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

import asyncio
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from provisa.executor.pool import SourcePool

if TYPE_CHECKING:
    import graphql

    from provisa.compiler.rls import RLSContext
    from provisa.compiler.sql_gen import CompilationContext
    from provisa.core.database import Database

# The org selected for the current request/task. Unset (None) at startup, on
# background-boot paths, and in single-org tests — the AppState shims then
# resolve the default-org runtime. A tenant-data entrypoint that sees None must
# raise (see require_current_org); it must never silently pick an org.
current_org: ContextVar[str | None] = ContextVar("current_org", default=None)


@dataclass
class OrgRuntime:
    """The per-org slice of the data plane.

    One instance per org, built lazily by the registry. Everything here is keyed
    within a single org's namespace (source_id / role_id / table_name); the
    genuinely-global handles (admin_db, federation_engine, response cache, mv
    registry, servers) stay on ``AppState``.
    """

    org_id: str

    # REQ-1043/REQ-1067/REQ-1244: per-org federation engine. ``None`` means this org runs on the
    # SHARED engine (the pooled lane, REQ-1243 lane a — every org starts here); an isolated-engine
    # org (orgs.isolated_engine) carries its OWN EngineRuntime plus its own terminal-connection
    # state, so its queries never touch the shared coordinator. The AppState shims route
    # ``state.federation_engine`` / ``state.engine_conn`` / ``state.engine_conn_kwargs`` here
    # whenever ``federation_engine`` is bound, and to the default-org (shared) runtime otherwise.
    isolated_engine: bool = False
    federation_engine: Any = None
    engine_conn: Any = None
    engine_conn_kwargs: dict = field(default_factory=dict)

    # Per-org control plane: Database bound to the org_{id} schema.
    tenant_db: "Database | None" = None

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
    rls_contexts: dict[str, "RLSContext"] = field(default_factory=dict)

    # Governance / masking. (table_id, role_id) → {col: (rule, dtype)}.
    masking_rules: dict[Any, Any] = field(default_factory=dict)

    # Raw-SQL governance inputs (published once per org at schema-load time).
    tables: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    # REQ-1317: config-declared metric registry (name → Metric), published alongside tables
    # so the raw-SQL path can expand `metrics.<name>` queries before governance.
    metrics: dict[str, Any] = field(default_factory=dict)
    # Raw registry rows (domains, tables, column_types, …) the on-demand domain-filtered schema
    # builder reads. Per-org because domains ARE per-org: a process-global cache is overwritten by
    # whichever org rebuilt last, so /data/domains hands one org's domain list to another.
    schema_build_cache: dict = field(default_factory=dict)


class OrgRegistry:
    """Lazily-built ``dict[org_id, OrgRuntime]`` with a per-org build lock.

    ``get_or_build`` is double-checked: a hit returns immediately; a miss takes
    the org's lock, re-checks (a concurrent request may have built it), then runs
    the async ``builder`` exactly once. No TTL — a runtime is rebuilt only on an
    explicit reload (config change), never on a timer.
    """

    def __init__(self) -> None:
        self._runtimes: dict[str, OrgRuntime] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, org_id: str) -> asyncio.Lock:
        lock = self._locks.get(org_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[org_id] = lock
        return lock

    def get(self, org_id: str) -> OrgRuntime | None:
        return self._runtimes.get(org_id)

    def set(self, org_id: str, runtime: OrgRuntime) -> None:
        self._runtimes[org_id] = runtime

    def invalidate(self, org_id: str) -> None:
        self._runtimes.pop(org_id, None)

    def all_org_ids(self) -> list[str]:
        return list(self._runtimes.keys())

    async def get_or_build(
        self, org_id: str, builder: Callable[[str], Awaitable[OrgRuntime]]
    ) -> OrgRuntime:
        existing = self._runtimes.get(org_id)
        if existing is not None:
            return existing
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


def set_current_org(org_id: str) -> Token[str | None]:
    """Bind the active org for the current context; returns a reset token."""
    return current_org.set(org_id)


def reset_current_org(token: Token[str | None]) -> None:
    current_org.reset(token)


def require_current_org() -> str:
    """The active org id, or raise if none is bound.

    A tenant-data path that reaches this with no org selected is a routing bug
    (or an unauthenticated request that slipped past the org gate) — never a
    case to paper over with a default. Callers on the default-org fast path use
    the AppState shims instead, which fall back explicitly to the default org.
    """
    org_id = current_org.get()
    if org_id is None:
        raise RuntimeError(
            "No active org bound (current_org unset). A tenant-data path must "
            "set_current_org before use; this is a routing defect, not a "
            "condition to default around."
        )
    return org_id


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

        return getattr(state, "tenant_db", None)

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

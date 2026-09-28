# Copyright (c) 2026 Kenneth Stott
# Canary: 9f2b6c1a-3d5e-4b8f-9a7c-1e6d4f0b8c2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In-memory, per-org, TTL-evicted cache of compiled query outcomes.

Scope (READ BEFORE EXTENDING): this caches the outcome of `validate_sql` + the domain-access
check in `_govern_and_route_planned` (`provisa/pgwire/_pipeline.py`) — the relationship-guard,
row-level SQL validation and per-table domain-access walk — for the raw-SQL pipeline. Default
TTL 60s, configurable via `PROVISA_COMPILED_QUERY_CACHE_TTL_SECONDS`.

WHY NOT ALSO THE ROUTING DECISION / PHYSICAL SQL, despite that being the original ask: read
verified against the current code (2026-09-28), not the larger out-of-scope plan this task cites
as background. `_optimize_and_route` (`provisa/pgwire/_pipeline.py`) — the function that produces
the routing `Route`/`source_id`/`dialect` AND the physical SQL — calls
`_materialize_api_to_engine_cache` (`provisa/api/data/materialization.py`), which reads
`state.hot_manager`: LIVE, time-varying hot-table/API-cache state that is NOT bumped by
`schema_version` and can flip the route (DIRECT/API -> ENGINE, or reduce a multi-source query to
single-source via VALUES-CTE inlining) between two calls of the identical SQL shape, same role,
same schema generation. Both this task's own instructions and the background plan explicitly list
"tier-cap/hot-table lookups" as a signal that must be rechecked on EVERY call, hit or miss alike —
never cached. Caching `_optimize_and_route`'s output (route or physical SQL) would violate that
constraint and risk silently serving a stale route/physical-SQL pair for up to one TTL window.
Doing this safely needs `_optimize_and_route` split into a live, always-fresh half (the hot-table
check) and a structural, cacheable half (`decide_route` proper) — real, separate work, not done
here. Flagged as a follow-up (see the requirements tracker entry alongside this one).

What IS safely cacheable, confirmed by reading: `validate_sql` (relationship-guard + row-level
violations) and the standalone domain-access table-walk block right after it are pure functions of
(SQL shape, role, schema generation) — no query-literal dependency (both operate on table/join
structure, never on WHERE-clause literal values) and no live/time-varying dependency (no hot-table
or tier-cap read anywhere in either). A statement that fails either check raises before reaching
the cache-store call below, so a cache HIT means only "an identical shape already validated clean
for this role under this schema generation" — never a stale allow.

Key components (see `compiled_query_cache_key`):
  - a SHAPE hash of the SQL — `sqlglot`-parsed with every `exp.Literal` blanked (modeled on
    `provisa/observability/stage_trace.py`'s `redact_sql`), NOT a hash of the raw text. Provisa's
    pgwire bind-parameter path (`_substitute_params` in `provisa/pgwire/server.py`) inlines `$1`/
    `$2` bind values into literal SQL text before the compile stage ever sees it (confirmed by
    reading `_execute_sql_bound`) — a raw-text key would only ever hit on byte-identical repeats
    and miss the entire "same query, different bind values" case, which is the actual point of a
    compiled-query cache for a JDBC/ADBC/asyncpg-style client. Literals never affect what's
    cached here (see above), so shape-hashing loses nothing.
  - the acting role id
  - the acting person id (`provisa.audit.context.current_audit_identity()`'s `user_id`, or `None`
    when no principal is bound — a background/system call, not a missing value)
  - `state.schema_boot_id` + `state.schema_version` — bumped by `_rebuild_schemas_impl` on every
    schema/masking/RLS/relationship change, and (as of the fix landed alongside REQ-1866, already
    on this branch) also by RLS-rule and role create/delete/mutation — closing the invalidation
    gap the background plan flagged as a prerequisite.
  - whether the relationship guard was bypassed for this call (`_bypass_guard`) — a role- and
    SQL-opt-out-comment-derived boolean that changes `validate_sql`'s own behavior, so it must
    partition the cache exactly as it partitions that function's result.
"""

# Requirements: REQ-1877

from __future__ import annotations

import hashlib
import os
import threading
import time
from dataclasses import dataclass

_DEFAULT_TTL_SECONDS = int(os.environ.get("PROVISA_COMPILED_QUERY_CACHE_TTL_SECONDS", "60"))

# Bounded so a pathological caller issuing endless distinct one-off SQL shapes within one TTL
# window cannot grow this without limit — a pure speed optimization, never a correctness
# dependency, so evicting the "wrong" entry only costs a future cache miss.
_MAX_ENTRIES = 4096


def sql_shape_digest(sql_text: str) -> str:
    """SHA-256 of `sql_text` with every literal blanked — the cache-key SQL component.

    Delegates to `provisa.observability.stage_trace.redact_sql` (same `exp.Literal`-blanking
    transform already used for trace redaction) so two calls differing only in a bind-parameter
    or literal-embedded value hash identically. Raises `sqlglot.errors.SqlglotError` on unparseable
    input — the same statement is about to fail to parse a few lines later in the ordinary
    (uncached) path regardless, so this is not a new failure mode, just an earlier one.
    """
    from provisa.observability.stage_trace import redact_sql

    return hashlib.sha256(redact_sql(sql_text).encode()).hexdigest()


@dataclass(frozen=True)
class CompiledOutcome:
    """The cached result: `validate_sql` + the domain-access check passed clean for this key.

    A dataclass (not a bare sentinel) so a future extension — once `_optimize_and_route` is split
    into its live and structural halves (see module docstring) — has somewhere to add the
    structural routing/physical-SQL fields without changing every call site's cache-hit handling.
    """

    validated: bool = True


def compiled_query_cache_key(
    sql_text: str,
    role_id: str,
    person_id: str | None,
    schema_boot_id: str,
    schema_version: int,
    bypass_relationship_guard: bool,
) -> str:
    """Build the cache key described in the module docstring. `sql_text` is shape-hashed
    (unbounded caller-authored length, and literal-independent by design); every other component
    is small and included verbatim."""
    return "\x00".join(
        [
            schema_boot_id,
            str(schema_version),
            role_id,
            person_id or "",
            "g1" if bypass_relationship_guard else "g0",
            sql_shape_digest(sql_text),
        ]
    )


@dataclass
class _Entry:
    outcome: CompiledOutcome
    expires_at: float


class CompiledQueryCache:
    """A per-org, in-memory, TTL-evicted cache of `CompiledOutcome`.

    One instance lives on each `OrgRuntime` (mirroring how `contexts`/`rls_contexts` are scoped
    per org — see `provisa/api/org_runtime.py`), so a compiled outcome for one org's role never
    leaks into another org's cache. Thread-safe: pgwire's socketserver worker calls the pipeline
    from worker threads via `asyncio.run_coroutine_threadsafe`.
    """

    def __init__(self, ttl_seconds: int | None = None) -> None:
        self._ttl = ttl_seconds if ttl_seconds is not None else _DEFAULT_TTL_SECONDS
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}

    def get(self, key: str) -> CompiledOutcome | None:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[key]
                return None
            return entry.outcome

    def put(self, key: str, outcome: CompiledOutcome) -> None:
        now = time.monotonic()
        with self._lock:
            if len(self._entries) >= _MAX_ENTRIES and key not in self._entries:
                self._evict_for_room(now)
            self._entries[key] = _Entry(outcome=outcome, expires_at=now + self._ttl)

    def _evict_for_room(self, now: float) -> None:
        """Called with the lock held. Drop expired entries first; if still at capacity, drop one
        arbitrary (insertion-order-oldest) entry — never a correctness concern, only a future
        cache miss (see module docstring)."""
        expired = [k for k, e in self._entries.items() if e.expires_at <= now]
        for k in expired:
            del self._entries[k]
        if len(self._entries) >= _MAX_ENTRIES:
            oldest_key = next(iter(self._entries))
            del self._entries[oldest_key]

    def clear(self) -> None:
        """Drop every cached entry. Exposed for tests and for an explicit invalidation caller;
        production invalidation is generation-keyed (see module docstring) so this is never called
        from request-serving code."""
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

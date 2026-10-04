# Copyright (c) 2026 Kenneth Stott
# Canary: 7636d952-e031-4cbd-9ae1-b115955017e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How busy each registered table is: the count Hot-N replication is decided on (REQ-826).

A table left at Default, or set to Hot-N, is read live until it is busy: once the governed
statements that read it pass its threshold within the interval (``replication.hot_interval``),
it is replicated and served from its replica; when they fall below half the threshold it goes
back to live.

The count is DERIVED STATE (REQ-1920): it lives in Redis, expires on its own, and losing it
loses no information — the audit log is the record of what ran. It is counted where a
statement's audit row is written (``audit/writer.py``), after the row has landed:

* a statement counts a table ONCE, however many times it reads it;
* a response answered from the response cache reached no data and does not count — it put no
  load on the source, which is what the threshold is about;
* a statement governance refused does not count.

The window is two fixed buckets of one interval each. A table's count is the current bucket
plus the part of the previous bucket still inside the last interval, so the count neither drops
to zero at a bucket boundary nor needs a per-statement list.
"""

# Requirements: REQ-826, REQ-1920

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from provisa.federation.execution_auth import system_auth

if TYPE_CHECKING:
    from provisa.audit.writer import AuditRecord
    from provisa.federation.policy_summary import HotView

log = logging.getLogger(__name__)

_KEY_PREFIX = "provisa:replica_hot"

# ``AuditRecord.route`` of a statement answered from the response cache. A refused statement
# has no route at all; every other route read data.
_CACHED = "cache"
_FIRST_ERROR_STATUS = 400


def counts_toward_hot(route: str | None, status_code: int) -> bool:
    """Whether a finished statement adds to its tables' counts: it was answered (not refused,
    not failed) and its answer was read from data, not from the response cache."""
    return route is not None and route != _CACHED and status_code < _FIRST_ERROR_STATUS


def counted_tables(table_ids: Iterable[int | str]) -> frozenset[int]:
    """The registered tables one statement counts — each once."""
    return frozenset(t for t in table_ids if isinstance(t, int))


class HotCounts:
    """The per-table statement counts of every org environment, on one Redis.

    With the deployment's Redis the counts are shared by every process that serves requests.
    With the embedded one they are this process's own — right only when this process is the
    only one serving (``promotion_runs``)."""

    def __init__(self, redis_url: str | None, *, clock: Callable[[], float] | None = None) -> None:
        self._url = redis_url
        # None: the store's own clock (below). A test passes its own to place statements in time.
        self._clock = clock
        self._client: Any = None
        self._client_lock = threading.Lock()

    @property
    def shared(self) -> bool:
        """Whether every serving process counts into the same store."""
        return bool(self._url)

    def _redis(self) -> Any:
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    from provisa.core.redis_factory import make_redis

                    self._client = make_redis(self._url, decode_responses=True).client()
        return self._client

    def _now(self, r: Any) -> float:
        if self._clock is not None:
            return self._clock()
        # The store's clock, so every process cuts the buckets at the same instants.
        secs, micros = r.time()
        return float(secs) + float(micros) / 1_000_000

    @staticmethod
    def _key(scope: str, table_id: int, interval: int, bucket: int) -> str:
        return f"{_KEY_PREFIX}:{scope}:{table_id}:{interval}:{bucket}"

    def add(self, hits: Mapping[tuple[str, int], int], interval: int) -> None:
        """Add ``hits`` — ``{(scope, table_id): statements}`` — to the current bucket. One round
        trip for the whole batch."""
        if not hits:
            return
        r = self._redis()
        bucket = int(self._now(r) // interval)
        pipe = r.pipeline(transaction=False)
        for (scope, table_id), n in hits.items():
            key = self._key(scope, table_id, interval, bucket)
            pipe.incrby(key, n)
            # Kept for the bucket it is current in and the one it is the previous of.
            pipe.expire(key, 2 * interval)
        pipe.execute()

    @staticmethod
    def _too_large_key(scope: str, key: tuple[str, str, str]) -> str:
        return f"{_KEY_PREFIX}:too_large:{scope}:{'/'.join(key)}"

    def mark_too_large(self, scope: str, key: tuple[str, str, str], interval: int) -> None:
        """Record that the last evaluation left ``key`` live because it is over the size ceiling
        — for the admin summary to state. Kept for two intervals: an evaluation that no longer
        finds it too large (or no longer judges it) simply lets the mark lapse."""
        self._redis().set(self._too_large_key(scope, key), "1", ex=2 * interval)

    def too_large(self, scope: str, key: tuple[str, str, str]) -> bool:
        return bool(self._redis().exists(self._too_large_key(scope, key)))

    def counts(self, scope: str, table_ids: Iterable[int], interval: int) -> dict[int, float]:
        """Each table's statements in the last ``interval`` seconds."""
        ids = list(table_ids)
        if not ids:
            return {}
        r = self._redis()
        now = self._now(r)
        bucket = int(now // interval)
        # How much of the previous bucket is still inside the last interval.
        carried = 1.0 - (now - bucket * interval) / interval
        keys = [self._key(scope, t, interval, b) for t in ids for b in (bucket, bucket - 1)]
        raw = r.mget(keys)
        out: dict[int, float] = {}
        for i, table_id in enumerate(ids):
            current, previous = raw[2 * i], raw[2 * i + 1]
            out[table_id] = float(current or 0) + carried * float(previous or 0)
        return out


def count_scope(org_id: str, env: str) -> str:
    """The key part naming one org environment in this node's region (REQ-1922: regions may
    share one Redis, and each counts its own reads): registered table ids are its own."""
    from provisa.core import process_region
    from provisa.core.environments import region_part

    return f"{org_id}:{env}{region_part(process_region.region())}"


def promotion_runs(counts: HotCounts, workers: int) -> bool:
    """Whether Hot promotion is decided in this deployment (REQ-826).

    The counts must be every serving process's: they are with a shared Redis, and with the
    embedded one only when a single worker serves requests (the demo, a small single-node
    install). With several workers and no shared Redis each process sees its own share of the
    traffic, so no count is the table's and nothing is promoted — the admin summary says so."""
    return counts.shared or workers == 1


def batch_hits(records: "Iterable[AuditRecord]") -> dict[tuple[str, int], int]:
    """The counts one landed audit batch adds: ``{(scope, table_id): statements}``."""
    hits: dict[tuple[str, int], int] = {}
    for rec in records:
        if not counts_toward_hot(rec.route, rec.status_code):
            continue
        ids = rec.table_ids() if callable(rec.table_ids) else rec.table_ids
        for table_id in counted_tables(ids):
            key = (rec.hot_scope, table_id)
            hits[key] = hits.get(key, 0) + 1
    return hits


# -- the decision ----------------------------------------------------------------------------------

PROMOTE = "promote"
DEMOTE = "demote"

#: Why a table under a threshold is not replicated however busy it is.
NO_CLOCK = "no_clock"  # its change signal is TTL-based and no cache_ttl is declared (REQ-1907)
TOO_LARGE = "too_large"  # it holds more rows than replication.hot_max_rows
HOT_TIER = "hot_tier"  # the Redis hot tier manages it: a table lives in one tier (REQ-241)
NOT_WHOLE = "not_whole"  # it has no whole copy to build (replica_converge.whole_copy)


@dataclass(frozen=True)
class HotCandidate:
    """One registered table Hot promotion judges: its statement count against ``threshold``."""

    key: tuple[str, str, str]  # (source_id, schema_name, table_name): the replica's key
    table_id: int
    threshold: int
    promoted: bool


def threshold_of(source: Any, table: Any, default_threshold: int) -> int | None:
    """The statements per interval at which ``table`` is replicated, or None when its setting
    is not a threshold: Never is never replicated for being busy; Always and load protection
    are replicated outright. A table set to Hot-N uses N; one left at Default (and whose source
    sets none) uses ``default_threshold`` (``replication.hot_threshold``)."""
    from provisa.core.replicate import ALWAYS, resolved_load_protected, resolved_replicate

    if resolved_load_protected(source, table):
        return None
    replicate = resolved_replicate(source, table)
    if replicate is None:
        return default_threshold
    return replicate if replicate > ALWAYS else None


def hot_candidates(
    registered: Iterable[Any],
    sources: Mapping[str, Any],
    promoted: frozenset[tuple[str, str, str]],
    engine: Any,
    default_threshold: int,
    *,
    hot_tier: frozenset[int] = frozenset(),
) -> tuple[list[HotCandidate], dict[tuple[str, str, str], str]]:
    """The tables Hot promotion judges on ``engine``, and those it leaves alone with the reason.
    ``registered``: the registered tables as ``registry_view.registered_tables`` returns them
    (each with its resolved change signal and cache_ttl).

    A candidate is a table under a threshold (``threshold_of``) on a source the engine reads in
    place: a source the engine can only reach through a replica is replicated already, and a
    source floored as a whole has no live read to promote from. A candidate whose change signal
    is TTL-based with no cache_ttl has no clock to refresh a replica by (REQ-1907): it is not
    promoted, and the reason is reported — a table left at Default was never asked for a clock,
    so this is the one place it can be missing. A table the Redis hot tier manages
    (``hot_tier``: ``HotTableManager.managed_tables()``) is not judged either: a table lives in
    at most one tier, and the hot tier wins (REQ-241)."""
    from provisa.federation.replica_converge import builds_here, home_region, whole_copy
    from provisa.federation.replica_routing import has_live_attach
    from provisa.federation.role_ttl import missing_landing_ttl

    candidates: list[HotCandidate] = []
    skipped: dict[tuple[str, str, str], str] = {}
    attach: dict[str, bool] = {}
    for reg in registered:
        source = sources.get(reg.source_id)
        if source is None:
            continue
        threshold = threshold_of(source, reg, default_threshold)
        if threshold is None:
            continue
        if source.id not in attach:
            attach[source.id] = has_live_attach(source, engine)
        if not attach[source.id]:
            continue
        key = (reg.source_id, reg.schema_name, reg.table_name)
        if not builds_here(home_region(reg)):
            continue  # REQ-1922: promoted, built and counted only in the region it names
        if reg.id in hot_tier:
            skipped[key] = HOT_TIER
            continue
        # The one rule for what a build may copy whole (convergence, the read backstop and the
        # build ask it too): a table with a parameter column, or one replicated row by row, has
        # no whole copy, so it is never promoted to one.
        if not whole_copy(source, reg, engine):
            skipped[key] = NOT_WHOLE
            continue
        if (
            missing_landing_ttl(
                reg.change_signal, source.change_signal, reg.cache_ttl, source.cache_ttl
            )
            is not None
        ):
            skipped[key] = NO_CLOCK
            continue
        candidates.append(HotCandidate(key, reg.id, threshold, key in promoted))
    return candidates, skipped


def judge(count: float, threshold: int, *, promoted: bool) -> str | None:
    """What a table's count says: ``PROMOTE`` once it reaches its threshold, ``DEMOTE`` once a
    promoted table falls below half of it, None in between — the gap keeps a table whose
    traffic hovers at its threshold from being replicated and dropped over and over."""
    if not promoted:
        return PROMOTE if count >= threshold else None
    return DEMOTE if count < threshold / 2 else None


def hot_tier_tables(state: Any) -> frozenset[int]:
    """The ids of the tables the Redis hot tier manages in this process (REQ-241); none without
    that tier."""
    manager = state.hot_manager
    return frozenset(manager.managed_tables()) if manager is not None else frozenset()


async def busy_replicas(state: Any) -> list[dict]:
    """The tables promoted for being busy, for the admin's list of what Provisa keeps a copy of:
    ``{table_name, catalog (the source), schema, row_count, serving}``. ``serving`` False: the
    replica is still being built and the table is read live. One control-plane read, for the
    row counts; which tables are promoted is what this process's published routes already say."""
    from provisa.federation import replica_state

    routes = state.replica_routes
    if not routes.promoted:
        return []
    async with state.tenant_db.acquire() as conn:
        records = {r.key: r for r in await replica_state.read_all(conn)}
    out: list[dict] = []
    for key in sorted(routes.promoted):
        record = records.get(key)
        serving = key in routes.serving
        out.append(
            {
                "table_name": key[2],
                "catalog": key[0],
                "schema": key[1],
                "row_count": (record.rows_copied or 0) if record is not None and serving else 0,
                "serving": serving,
            }
        )
    return out


# -- what the admin summary states -----------------------------------------------------------------


def _registered_id(state: Any, key: tuple[str, str, str]) -> int | None:
    """The id of the registered table ``key`` (source, schema, table) names; None for a table not
    registered (a draft in the editor)."""
    for row in state.tables:
        if (row["source_id"], row["schema_name"], row["table_name"]) == key:
            return int(row["id"])
    return None


def hot_view(state: Any, source: Any, table: Any) -> "HotView":
    """Where ``table`` stands with Hot replication in this deployment (REQ-826): the settings in
    force, whether promotion is decided here at all, and whether the table is promoted, served
    from its replica, or left live for a stated reason. Read from what this process already
    holds — the published routes and the count store — so a list of tables costs no
    control-plane read."""
    from provisa.core import settings_registry
    from provisa.core.boot_lock import expected_workers
    from provisa.core.environments import PROD
    from provisa.core.request_context import current_env, current_org
    from provisa.federation.policy_summary import HotView
    from provisa.federation.replica_converge import whole_copy
    from provisa.federation.role_ttl import missing_landing_ttl

    interval = settings_registry.value("replication.hot_interval")
    default_threshold = settings_registry.value("replication.hot_threshold")
    routes = state.replica_routes
    key = (source.id, table.schema_name, table.table_name)
    skipped: str | None = None
    if threshold_of(source, table, default_threshold) is not None:
        scope = count_scope(current_org.get() or state.org_id, current_env.get() or PROD)
        if _registered_id(state, key) in hot_tier_tables(state):
            skipped = HOT_TIER
        elif not whole_copy(source, table, state.federation_engine):
            skipped = NOT_WHOLE
        elif (
            missing_landing_ttl(
                table.change_signal, source.change_signal, table.cache_ttl, source.cache_ttl
            )
            is not None
        ):
            skipped = NO_CLOCK
        elif state.hot_counts.too_large(scope, key):
            skipped = TOO_LARGE
    return HotView(
        default_threshold=default_threshold,
        interval=interval,
        max_rows=settings_registry.value("replication.hot_max_rows"),
        runs=promotion_runs(state.hot_counts, expected_workers()),
        promoted=key in routes.promoted,
        serving=key in routes.serving,
        skipped=skipped,
    )


# -- the evaluation (one holder per deployment) ----------------------------------------------------


@dataclass(frozen=True)
class Evaluation:
    """What one evaluation of one org environment did."""

    promoted: tuple[tuple[str, str, str], ...] = ()
    demoted: tuple[tuple[str, str, str], ...] = ()
    skipped: Mapping[tuple[str, str, str], str] = field(default_factory=dict)
    ran: bool = True  # False: promotion is not decided in this deployment (promotion_runs)


class _SizeChecks:
    """The size checks that failed, per table, so a failure is logged when it starts and when it
    changes — not on every evaluation — and its recovery is logged once."""

    def __init__(self) -> None:
        self._failed: dict[tuple[str, tuple[str, str, str]], str] = {}

    def failed(self, scope: str, key: tuple[str, str, str], cause: BaseException) -> None:
        said = f"{type(cause).__name__}: {cause}"
        if self._failed.get((scope, key)) != said:
            log.error(
                "Hot promotion of %s could not size the table (%s). It stays live, is sized "
                "again at every evaluation, and is reported again only when the outcome changes.",
                ".".join(key),
                said,
                exc_info=cause,
            )
        self._failed[(scope, key)] = said

    def succeeded(self, scope: str, key: tuple[str, str, str]) -> None:
        was = self._failed.pop((scope, key), None)
        if was is not None:
            log.info("Hot promotion can size %s again (was: %s)", ".".join(key), was)


_size_checks = _SizeChecks()


async def _row_count(state: Any, key: tuple[str, str, str]) -> int:
    """How many rows the table ``key`` (source, schema, table) holds, counted through the engine
    where it reads the table now."""
    from provisa.mv.models import TableIdentity

    ref = await state.federation_engine.read_ref(TableIdentity(*key))
    result = await state.federation_engine.execute_engine(
        f"SELECT COUNT(*) FROM {ref}", authorization=system_auth("replica row count")
    )
    return int(result.rows[0][0])


async def evaluate(state: Any, *, workers: int) -> Evaluation:
    """Judge every table under a threshold in the bound org environment, once (REQ-826).

    A table at or past its threshold is promoted — unless it is larger than
    ``replication.hot_max_rows`` — and its build is requested in the same transaction; a promoted
    table below half its threshold is demoted, and so is one that is no longer under a threshold
    at all (its setting changed, its table or source is gone, the hot tier took it). Demotion
    only clears the flag: reads return to live at once, and the replicator retires the replica.

    Runs under the single scheduler holder: a promotion is a decision that must be taken once
    per deployment. ``workers``: how many processes serve requests (``promotion_runs``)."""
    from provisa.core import settings_registry
    from provisa.core.environments import PROD
    from provisa.core.request_context import current_env, current_org
    from provisa.federation import replica_state
    from provisa.federation.registry_view import registered_sources, registered_tables
    from provisa.federation.replica_builds import store_identity

    counts = state.hot_counts
    if not promotion_runs(counts, workers):
        return Evaluation(ran=False)
    interval = settings_registry.value("replication.hot_interval")
    max_rows = settings_registry.value("replication.hot_max_rows")
    default_threshold = settings_registry.value("replication.hot_threshold")
    scope = count_scope(current_org.get() or state.org_id, current_env.get() or PROD)

    # REQ-1922: the registry is the model (model store); what is promoted is this region's state.
    async with state.model_db.acquire() as conn:
        registered = await registered_tables(state, conn)
        sources = {s.id: s for s in await registered_sources(state, conn)}
    async with state.tenant_db.acquire() as conn:
        promoted_now = await replica_state.promoted_keys(conn)
    candidates, skipped = hot_candidates(
        registered,
        sources,
        promoted_now,
        state.federation_engine.engine,
        default_threshold,
        hot_tier=hot_tier_tables(state),
    )
    seen = counts.counts(scope, [c.table_id for c in candidates], interval)
    reasons = dict(skipped)
    promote: list[HotCandidate] = []
    demote = [key for key in promoted_now if key not in {c.key for c in candidates}]
    for candidate in candidates:
        verdict = judge(seen[candidate.table_id], candidate.threshold, promoted=candidate.promoted)
        if verdict == DEMOTE:
            demote.append(candidate.key)
        elif verdict == PROMOTE:
            try:
                rows = await _row_count(state, candidate.key)
            except Exception as exc:  # allow-ble: an engine or driver error of any type IS this table's size-check outcome — it is recorded and logged by _size_checks, the table stays live, and the other tables are still judged
                _size_checks.failed(scope, candidate.key, exc)
                continue
            _size_checks.succeeded(scope, candidate.key)
            if rows > max_rows:
                counts.mark_too_large(scope, candidate.key, interval)
                reasons[candidate.key] = TOO_LARGE
                continue
            promote.append(candidate)

    async with state.tenant_db.acquire() as conn:
        for candidate in promote:
            # The flag and the build request are one transaction: a table is never promoted with
            # no build on its way. THE call site of a Hot build (the replicator builds it). A
            # table promoted again while its replica still stands in this store (it was demoted
            # and the replicator has not dropped it) serves from that replica at once: nothing
            # is requested, and its refresh stays on its own clock.
            async with conn.transaction():
                await replica_state.set_promoted(conn, candidate.key, True)
                record = await replica_state.read(conn, candidate.key)
                # The store a standing replica would be in — asked only when a table is
                # promoted: a deployment with nothing to promote needs no store.
                standing = record is not None and record.exists_in(store_identity(state))
                if not standing:
                    await replica_state.request_build(conn, candidate.key, replica_state.REASON_HOT)
            log.info(
                "Hot promotion: %s passed %d statements per %ds; %s",
                ".".join(candidate.key),
                candidate.threshold,
                interval,
                "its standing replica serves it" if standing else "its replica is requested",
            )
        for key in demote:
            await replica_state.set_promoted(conn, key, False)
            log.info("Hot demotion: %s is read live again", ".".join(key))
    return Evaluation(
        promoted=tuple(c.key for c in promote), demoted=tuple(demote), skipped=reasons
    )


async def evaluation_loop(state: Any, *, should_run: Callable[[], bool], workers: int) -> None:
    """Evaluate every org environment this process serves, every ``replication.hot_interval``
    seconds, while ``should_run()`` — the server passes its scheduler holder's ``holds``, so of
    the processes of a deployment one decides. The interval is read at every pass: a change to
    the setting applies to the next one."""
    from provisa.core import settings_registry
    from provisa.core.environments import PROD
    from provisa.core.request_context import (
        reset_current_env,
        reset_current_org,
        set_current_env,
        set_current_org,
    )

    while True:
        await asyncio.sleep(settings_registry.value("replication.hot_interval"))
        if not should_run():
            continue
        for key in state.org_registry.all_org_ids():
            rt = state.org_registry.get(key)
            if rt is None or rt.tenant_db is None:
                continue  # dropped since listed, or registered and still being built
            org_token = set_current_org(rt.org_id)
            env_token = set_current_env(None if rt.env == PROD else rt.env)
            try:
                await evaluate(state, workers=workers)
            except Exception:  # allow-ble: one org environment's evaluation failing (its control plane or engine is unreachable) must not end the loop that evaluates every other one; it is logged with its traceback and judged again at the next interval
                log.exception("Hot promotion evaluation failed for %s", key)
            finally:
                reset_current_env(env_token)
                reset_current_org(org_token)

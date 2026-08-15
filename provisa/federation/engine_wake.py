# Copyright (c) 2026 Kenneth Stott
# Canary: 8f41c60d-27ab-4e93-9f15-5c8ad3b41e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1448: idle-to-zero for the engine shards, and the wake a query pays for.

A shard's Deployment sits at zero replicas while nobody is querying it, and Autopilot bills pod
requests, so a shard with no pod costs nothing (REQ-1464). That resting state is what keeps the
zero-customer floor where it is; it also means the coordinator a query needs may not exist when the
query arrives, and this module is the seam that reconciles the two.

Three things follow from a shard that can be absent, and all three are handled here rather than at
any call site:

* **The wake happens BEFORE the terminal is dispatched, not as a retry.** ``execute_trino``'s retry
  budget is 30s (``PROVISA_RETRY_BUDGET_SECS``) and a cold shard is ~90-120s of node provision plus
  Trino start. A wake left to the retry loop therefore cannot succeed — the budget expires while
  Autopilot is still bringing a node up for the pod. :func:`ensure_engine_awake` runs at the top of ``_execute_plan``, so
  the query waits on the wake and then dispatches once, with its full retry budget intact for the
  failures retries are actually for.
* **A resumed shard has NO catalogs.** It runs ``catalog.management=dynamic`` over an ``emptyDir``,
  so the ``CREATE CATALOG`` statements an org's runtime issued are gone with the old pod. Every cold
  start bumps that shard's generation; an org runtime stamped with an older generation is rebuilt
  before its query runs, which is what reissues them.
* **The reaper and the wake race by construction.** A stop is a drain plus the scale-down that
  follows it, minutes long, and a query can arrive in the middle of it. The wake CANCELS an in-flight stop rather than
  waiting it out, and then treats the shard as cold — the pod may already be gone.

Everything here no-ops unless the deployment can actually provision (``provisioning_available``),
which is true only on the hosted control plane. A desktop or self-hosted install runs an engine that
is simply always on, and must not pay a status check per query to discover that.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any

from provisa.federation import k8s_provisioner as k8s

log = logging.getLogger(__name__)

# Per shard: the lock that makes a wake happen once for N concurrent queries, when the shard was
# last SEEN ready, the number of cold starts it has had, when it last served traffic, and the
# in-flight stop task the reaper started.
_locks: dict[str, asyncio.Lock] = {}
_ready_seen: dict[str, float] = {}
_generation: dict[str, int] = {}
_last_activity: dict[str, float] = {}
_stop_tasks: dict[str, asyncio.Task] = {}
# Per org (None = the deployment's own org): the in-flight sign-in prewarm, so a second sign-in
# does not start one alongside it. REQ-1471.
_prewarm_tasks: dict[str | None, asyncio.Task] = {}


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return int(raw)


def _lock_for(shard: str) -> asyncio.Lock:
    lock = _locks.get(shard)
    if lock is None:
        lock = asyncio.Lock()
        _locks[shard] = lock
    return lock


def boot_shard() -> str:
    """The shard this control plane's own terminal is bound to.

    ``TRINO_HOST`` names WHERE that shard answers; this names WHICH shard it is, which is what the
    provisioner needs to bring it back. Both are written by the same terraform apply
    (``terraform/gcp-saas/main.tf``), so a deployment that can provision but cannot say which shard
    it is querying is a misconfiguration, not a default to be guessed.
    """
    shard = os.environ.get("PROVISA_ENGINE_SHARD")
    if not shard:
        raise k8s.K8sProvisioningError(
            "this deployment provisions engines on Kubernetes but PROVISA_ENGINE_SHARD is unset, "
            "so the shard behind TRINO_HOST cannot be woken (REQ-1448)"
        )
    return shard


def generation(shard: str) -> int:
    """How many times this process has cold-started ``shard``.

    An org runtime carries the generation its ``CREATE CATALOG`` statements were issued under. A
    mismatch means the coordinator that held those catalogs is gone.
    """
    return _generation.get(shard, 0)


def note_activity(shard: str) -> None:
    """Record that the shard just served traffic, which is what the reaper measures idleness from."""
    _last_activity[shard] = time.monotonic()


async def ensure_shard_awake(shard: str) -> bool:
    """Bring ``shard`` up if it is not already serving. Returns whether it was a COLD start.

    Cheap on the warm path: within the recheck window a query costs nothing at all, and outside it a
    single Deployment GET. The window exists because the alternative — a status call per query — puts
    a GKE API round trip on every statement the platform runs.
    """
    note_activity(shard)

    stop_task = _stop_tasks.get(shard)
    recheck = _int_env("PROVISA_ENGINE_READY_RECHECK_SECONDS", 60)
    seen = _ready_seen.get(shard)
    if stop_task is None and seen is not None and time.monotonic() - seen < recheck:
        return False

    async with _lock_for(shard):
        # Re-read under the lock: while this query waited, another may have done the whole wake.
        seen = _ready_seen.get(shard)
        if _stop_tasks.get(shard) is None and seen is not None:
            if time.monotonic() - seen < recheck:
                return False

        cold = False
        stop_task = _stop_tasks.pop(shard, None)
        if stop_task is not None and not stop_task.done():
            # Cancelling mid-drain is the point: the reaper's drain wait is minutes long, and a
            # query must not sit behind a shutdown it can simply abandon. The pod is already at zero
            # replicas by then, so what comes back is a new coordinator either way.
            log.info("cancelling in-flight stop of engine shard %s: a query arrived", shard)
            stop_task.cancel()
            # The task only ever ends cancelled here; awaiting it is how the cancellation is
            # collected before the pool is resized back up underneath it.
            with contextlib.suppress(asyncio.CancelledError):
                await stop_task
            cold = True

        if not cold:
            status = await k8s.shard_status(shard)
            if status["state"] == "ready":
                _ready_seen[shard] = time.monotonic()
                return False
            cold = True

        log.info("waking engine shard %s", shard)
        await k8s.ensure_shared_shard(shard)
        _generation[shard] = _generation.get(shard, 0) + 1
        _ready_seen[shard] = time.monotonic()
        note_activity(shard)
        return True


async def converge_boot_shard() -> str:
    """Apply this control plane's shard manifests and wait for it to serve. Returns the shard.

    Boot APPLIES where the query path probes. The shard's manifests ship with the control plane —
    engine image, the Flight sidecar, Trino config — so a release that changes them has to roll the
    pod, and the warm path's "it is already ready, return" would leave the previous release's
    coordinator serving until something else happened to restart it. A boot whose manifests match
    what is running is a no-op apply plus the ready check the wake would have done anyway.
    """
    shard = boot_shard()
    async with _lock_for(shard):
        log.info("converging engine shard %s on boot", shard)
        await k8s.ensure_shared_shard(shard)
        _generation[shard] = _generation.get(shard, 0) + 1
        _ready_seen[shard] = time.monotonic()
        note_activity(shard)
    return shard


async def restore_shared_terminal(state: Any, shard: str) -> None:
    """Re-establish on ``shard``'s new coordinator everything the old one held for the shared lane.

    Boot builds the shared terminal in three steps and a cold start voids all three: the dbapi
    connection points at a pod IP that is now unrouted, the system catalogs (``provisa_admin``,
    ``otel``, ``results``) and Flight/object-store wiring live in the coordinator's dynamic catalog
    over an ``emptyDir``, and so do the default org's source catalogs. ``provision`` re-resolves the
    endpoint through :func:`k8s.shard_endpoint` — which is why the wake must precede this — and
    reconnects; the source catalogs are reissued from ``state.config``, exactly as boot issues them.

    Every org on the shared lane dispatches through THIS terminal (``AppState._engine_runtime``
    hands out the default org's engine to anyone without a dedicated one), so a tenant org's own
    runtime rebuild is not enough on its own — without this its catalogs are reissued over a
    connection to the pod that is gone.
    """
    from provisa.api.startup_seed import _OPS_VIEWS
    from provisa.core.config_loader import load_config

    log.info("re-establishing the shared engine terminal: shard %s restarted", shard)
    state.engine_conn = None
    state.federation_engine.provision(
        _OPS_VIEWS, getattr(state, "otel_snapshot_retention_hours", None)
    )
    await state.federation_engine.provision_infra()

    default = state.org_registry.get(state.org_id)
    if default is None:
        raise RuntimeError(
            "the default org has no built runtime, so the shared terminal cannot be restored "
            "after shard %s restarted (REQ-1448)" % shard
        )
    config = getattr(state, "config", None)
    if config is not None:
        async with state.tenant_db.acquire() as conn:
            await load_config(
                config,
                conn,
                state.federation_engine,
                replace=False,
                catalog_names=default.source_catalogs,
            )
    # Materialized sources read through landing tables in the materialization store, and their
    # schemas and read views are dynamic catalog state too — a resumed coordinator answers
    # "Schema 'org_default' does not exist" for every materialized table until this reconvenes them
    # (REQ-846/932). Boot swallows a failure here so a store hiccup cannot brick startup; a query
    # cannot, because the query is what would then fail.
    landed = await state.federation_engine.reconcile_landed_tables()
    if landed:
        log.info("reconciled %d landed table(s) after shard %s restarted", len(landed), shard)

    default.engine_generation = generation(shard)


def prewarm_engine(state: Any, org_id: str | None) -> None:
    """REQ-1471: start the shard's cold start at SIGN-IN, so the first query does not pay for it.

    A cold start is ~90-120s of Autopilot node provision plus Trino start, and the query path pays
    every second of it inside the request. Signing in is the earliest moment the platform knows
    which shard a session is about to use, and it is followed by seconds-to-minutes of the operator
    reading schemas and composing a query — which is exactly the window the node needs. This kicks
    the same wake off there and returns immediately: ``/auth/me`` must not block on a node.

    The work is the query path's own :func:`ensure_engine_awake`, so a cold start reissues catalogs
    here exactly as it would there, and the per-shard lock means a query that arrives mid-warm waits
    on this one rather than starting a second.
    """
    if not k8s.provisioning_available():
        return

    # /auth/me reports the deployment's own org by NAME, but _OrgRoutingMiddleware binds
    # current_org only for a non-default org (auth/middleware.py:601-604) — unset IS the default
    # org, and ensure_engine_awake's unset branch is the one that serves it. Binding the name here
    # instead sent the default org down the tenant branch, which invalidates the registry entry and
    # rebuilds the runtime; that rebuild replaced the org's compiled state with a build the boot
    # path had assembled differently, and every surface answered "No schema available for role".
    if org_id == state.org_id:
        org_id = None

    async def _run() -> None:
        from provisa.api.org_runtime import reset_current_org, set_current_org

        # ensure_engine_awake reads the active org off the ContextVar, and this task does not
        # inherit the request's binding — the middleware resets it before the response. None is
        # meaningful there (the deployment's own org), so it is passed through, not defaulted.
        token = set_current_org(org_id) if org_id is not None else None
        try:
            await ensure_engine_awake(state)
        except Exception:
            # A prewarm is an optimization on a path that has its own wake: swallowing here costs
            # the session nothing but the head start, whereas raising would fail a sign-in over an
            # engine the user has not yet asked for. The query path re-runs the same wake and
            # surfaces whatever this hit, so the failure is reported where it can be acted on.
            log.exception("prewarming the engine for org %r failed", org_id)
        finally:
            if token is not None:
                reset_current_org(token)
            _prewarm_tasks.pop(org_id, None)

    if _prewarm_tasks.get(org_id) is not None:
        return
    # Held in module state for the task's lifetime: a bare create_task is only weakly referenced by
    # the loop, so an unheld prewarm can be garbage-collected mid-wake.
    _prewarm_tasks[org_id] = asyncio.create_task(_run())


async def ensure_engine_awake(state: Any) -> None:
    """The query path's wake: the active org's shard is serving, and its catalogs are on it.

    Called at the top of ``_execute_plan``, the one terminal every surface reaches — a per-surface
    wake would leave whichever surface was added next dispatching at a coordinator that is not there.
    """
    if not k8s.provisioning_available():
        return

    from provisa.api.org_runtime import current_org

    org_id = current_org.get()
    if org_id is None:
        # NOT only boot and background: _OrgRoutingMiddleware binds current_org only when a
        # NON-default org is selected, so the deployment's own org — the one a single-tenant
        # install and every unselected request queries — arrives here. It is served by the control
        # plane's own shard, and it owns the shared terminal, so both the wake and the restore are
        # this branch's to do. Treating it as "boot already handled it" is what left the terminal
        # dialing a pod IP that had been released hours earlier.
        shard = boot_shard()
        await ensure_shard_awake(shard)
        default = state.org_registry.get(state.org_id)
        if default is None:
            raise RuntimeError(
                "the default org has no built runtime, so the engine this query dispatches to "
                "cannot be resolved (REQ-1448)"
            )
        if default.engine_generation != generation(shard):
            await restore_shared_terminal(state, shard)
        return

    runtime = state.org_registry.get(org_id)
    if runtime is None:
        raise RuntimeError(
            f"org {org_id!r} is bound for this query but has no built runtime — the shard it "
            "queries cannot be resolved (REQ-1448)"
        )
    # REQ-1412/REQ-1418: an org that runs its own coordinator is not on a shard this control plane
    # operates, so there is nothing here to wake.
    if runtime.engine_endpoint is not None or runtime.engine_url is not None:
        return
    shard = runtime.shard
    if not shard:
        raise RuntimeError(
            f"org {org_id!r} has no shard recorded on its runtime, so the engine it queries cannot "
            "be woken (REQ-1450)"
        )

    await ensure_shard_awake(shard)
    if runtime.engine_generation == generation(shard):
        return

    # The coordinator that held this org's catalogs is gone. Rebuilding under the registry's own
    # per-org lock is what reissues them; a query released before that runs against an engine that
    # is perfectly healthy and has never heard of the org's sources.
    from provisa.api.app import ensure_org_runtime

    # This org has no engine of its own, so it dispatches through the shared terminal — which is
    # still connected to the pod that just went away. Restoring it first is what makes the CREATE
    # CATALOG statements below reach the new coordinator instead of timing out at the old address.
    # The shared terminal is bound to the control plane's OWN shard (configured_engine_endpoint),
    # which is the shard a pooled org is placed on, so that is the generation to compare.
    boot = boot_shard()
    if state.org_registry.get(state.org_id).engine_generation != generation(boot):
        await restore_shared_terminal(state, boot)

    state.org_registry.invalidate(org_id)
    log.info("rebuilding org %s runtime: engine shard %s restarted", org_id, shard)
    await ensure_org_runtime(org_id)


# ── Reaper ──────────────────────────────────────────────────────────────────────


async def _stop_shard(shard: str) -> None:
    try:
        await k8s.scale_shard_to_zero(shard)
    finally:
        # Whatever the outcome — stopped, cancelled by a wake, or failed — the shard is no longer
        # known to be serving, so the next query re-checks instead of trusting a stale sighting.
        _ready_seen.pop(shard, None)


async def idle_reaper() -> None:
    """Scale to zero any shard that has not served traffic for the idle window.

    ``scale_shard_to_zero`` patches the Deployment to zero replicas and waits for the pod to go, and
    the pod going is what stops the meter — Autopilot bills pod requests, so a shard with no pod is
    free and Autopilot removes the node it had provisioned for it (REQ-1448, REQ-1464).
    """
    interval = _int_env("PROVISA_ENGINE_IDLE_CHECK_SECONDS", 60)
    idle_after = _int_env("PROVISA_ENGINE_IDLE_SECONDS", 900)
    while True:
        await asyncio.sleep(interval)
        for shard, last in list(_last_activity.items()):
            if time.monotonic() - last < idle_after:
                continue
            if _stop_tasks.get(shard) is not None:
                continue
            lock = _lock_for(shard)
            if lock.locked():
                # A wake is in progress; this shard is about to be busy by definition.
                continue
            log.info("engine shard %s idle for %ds — scaling it to zero", shard, idle_after)
            _stop_tasks[shard] = asyncio.create_task(_stop_shard(shard))


def start_idle_reaper(state: Any) -> None:
    """Start the reaper, on deployments that can actually scale a shard down."""
    if not k8s.provisioning_available():
        return
    # The reaper measures idleness from _last_activity, which is process state: after a control
    # plane restart it is empty, so a shard whose pod is up but which no query touches was never
    # considered for release and billed indefinitely. Seeding the boot shard here starts its idle
    # window at the restart, so an untouched shard is released one window later (REQ-1463).
    note_activity(boot_shard())
    state._engine_reaper_task = asyncio.create_task(idle_reaper())
    log.info("engine idle reaper started")

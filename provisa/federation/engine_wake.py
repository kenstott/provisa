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

A shard's node pool sits at zero while nobody is querying it, because a GKE node bills in full
whether or not a pod is on it. That resting state is what keeps the zero-customer floor where it is;
it also means the coordinator a query needs may not exist when the query arrives, and this module is
the seam that reconciles the two.

Three things follow from a shard that can be absent, and all three are handled here rather than at
any call site:

* **The wake happens BEFORE the terminal is dispatched, not as a retry.** ``execute_trino``'s retry
  budget is 30s (``PROVISA_RETRY_BUDGET_SECS``) and a cold shard is ~90-120s of node provision plus
  Trino start. A wake left to the retry loop therefore cannot succeed — the budget expires while the
  node is still being created. :func:`ensure_engine_awake` runs at the top of ``_execute_plan``, so
  the query waits on the wake and then dispatches once, with its full retry budget intact for the
  failures retries are actually for.
* **A resumed shard has NO catalogs.** It runs ``catalog.management=dynamic`` over an ``emptyDir``,
  so the ``CREATE CATALOG`` statements an org's runtime issued are gone with the old pod. Every cold
  start bumps that shard's generation; an org runtime stamped with an older generation is rebuilt
  before its query runs, which is what reissues them.
* **The reaper and the wake race by construction.** A stop is a drain plus a pool resize, minutes
  long, and a query can arrive in the middle of it. The wake CANCELS an in-flight stop rather than
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
        # Boot and background paths run before an org is bound; the shard they use is the control
        # plane's own, woken once in _load_and_build rather than per statement.
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
    """Release the node under any shard that has not served traffic for the idle window.

    The DEPLOYMENT scaling to zero saves nothing: an empty node bills exactly as much as a busy one.
    ``scale_shard_to_zero`` drains the pod and then takes the node, and taking the node is the part
    that stops the meter (REQ-1448).
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
            log.info("engine shard %s idle for %ds — releasing its node", shard, idle_after)
            _stop_tasks[shard] = asyncio.create_task(_stop_shard(shard))


def start_idle_reaper(state: Any) -> None:
    """Start the reaper, on deployments that can actually release a node."""
    if not k8s.provisioning_available():
        return
    state._engine_reaper_task = asyncio.create_task(idle_reaper())
    log.info("engine idle reaper started")

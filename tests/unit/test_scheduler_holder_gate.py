# Copyright (c) 2026 Kenneth Stott
# Canary: d7e2b094-5f18-4c6a-8a31-9b0c4e6f2d75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which scheduled jobs a worker runs when it is, or is not, the scheduler holder (REQ-1900)."""

# Requirements: REQ-1900

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.scheduler import executor
from provisa.scheduler.executor import PER_WORKER_JOB_IDS, BackgroundJobExecutor
from provisa.scheduler.holder import Holders, SchedulerHolder


class _Holder:
    def __init__(self, holds: bool) -> None:
        self._holds = holds
        self.asked = 0

    def holds(self) -> bool:
        self.asked += 1
        return self._holds


def _submit(holder, job_id: str) -> tuple[list[str], list]:
    """Submit one sync job through the executor; return (what ran, what was reported)."""
    ran: list[str] = []
    reported: list = []
    ex = BackgroundJobExecutor(None if holder is None else Holders(holder, holder))
    ex._logger = SimpleNamespace(name="test")
    ex._run_job_success = lambda jid, events: reported.append(("success", jid, events))
    ex._run_job_error = lambda jid, *exc: reported.append(("error", jid, exc))
    job = SimpleNamespace(id=job_id, func=lambda: ran.append(job_id), _jobstore_alias="default")

    def _run_now(coro, name):  # noqa: ARG001 - spawn_background's signature
        asyncio.run(coro)

    with (
        patch.object(executor, "spawn_background", _run_now),
        patch.object(executor, "run_job", lambda job, *_a: (job.func(), ["event"])[1]),
    ):
        ex._do_submit_job(job, [])
    return ran, reported


def test_the_holder_runs_a_deployment_job():
    holder = _Holder(True)
    assert _submit(holder, "otel_compact") == (
        ["otel_compact"],
        [("success", "otel_compact", ["event"])],
    )
    assert holder.asked == 1


def test_a_worker_that_is_not_the_holder_skips_it_and_frees_the_job_slot():
    ran, reported = _submit(_Holder(False), "otel_compact")
    assert ran == []
    # Reported as a run with no events: the scheduler's max_instances slot is released.
    assert reported == [("success", "otel_compact", [])]


@pytest.mark.parametrize("job_id", sorted(PER_WORKER_JOB_IDS))
def test_a_process_own_job_runs_in_every_worker_without_asking(job_id):
    holder = _Holder(False)
    ran, _ = _submit(holder, job_id)
    assert ran == [job_id]
    assert holder.asked == 0


def test_a_scheduler_with_no_holder_runs_every_job():
    assert _submit(None, "live_poll")[0] == ["live_poll"]


def test_a_file_control_plane_process_is_the_holder(tmp_path):
    holder = SchedulerHolder(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}", "default")
    try:
        assert holder.holds()
    finally:
        holder.close()


def test_the_servers_scheduler_is_the_one_given_the_holder():
    import inspect

    from provisa.api import app_startup
    from provisa.live import engine as live_engine

    assert "new_scheduler(_scheduler_holders(state))" in inspect.getsource(
        app_startup._start_scheduler
    )
    # The live-query engine's polls feed THIS process's subscribers: never gated.
    assert "BackgroundJobExecutor()" in inspect.getsource(live_engine)


# --- the long-lived loops outside the scheduler (REQ-1900) ---------------------------------------


def _run_one_sweep(should_run) -> list[str]:
    """Run reclamation_loop until its first sleep; return which reclamation steps ran."""
    from provisa.mv import refresh

    ran: list[str] = []

    class _Registry:
        def all(self):
            return [SimpleNamespace(target_catalog="c", target_schema="s", orphan_grace_period=1)]

    async def _detect(*_a, **_k):
        ran.append("detect_orphans")
        return []

    async def _drop(*_a, **_k):
        ran.append("drop_expired_orphans")

    async def _stop(_seconds):
        raise asyncio.CancelledError

    with (
        patch.object(refresh, "detect_orphans", _detect),
        patch.object(refresh, "drop_expired_orphans", _drop),
        patch.object(refresh.asyncio, "sleep", _stop),
        pytest.raises(asyncio.CancelledError),
    ):
        asyncio.run(refresh.reclamation_loop(object(), _Registry(), should_run=should_run))
    return ran


def test_mv_reclamation_sweeps_only_in_the_holder():
    """It drops tables in the shared store: N workers sweeping is N sweeps of one store."""
    assert _run_one_sweep(lambda: True) == ["detect_orphans", "drop_expired_orphans"]
    assert _run_one_sweep(lambda: False) == []
    assert _run_one_sweep(None) == ["detect_orphans", "drop_expired_orphans"]


def test_the_server_gives_its_shared_loops_the_holder():
    import inspect

    from provisa.api import app_startup

    src = inspect.getsource(app_startup._start_background_tasks)
    assert "should_run=_holder.holds" in src
    # The hot-table refresh rewrites Redis: shared when a Redis is configured, this process's own
    # embedded one when it is not — only the shared case is the holder's.
    assert "state.redis_url is not None and not _holder.holds()" in src
    # REQ-826: which busy tables are replicated is decided once per deployment — it writes the
    # shared replica state and requests builds: the holder's.
    hot = src[src.index("_hot_evaluation_loop(") :]
    assert "should_run=_holder.holds," in hot[: hot.index('name="replica-hot"')]


# --- a region's work runs under its region's claim (REQ-1922) ------------------------------------


@pytest.mark.parametrize(
    "job_id", ["events:tick", "events:boot:org_acme", "poll:orders", "row_materialize:reap:x"]
)
def test_a_region_job_asks_the_regions_claim_not_the_deployments(job_id):
    deployment, region = _Holder(False), _Holder(True)
    ran: list[str] = []
    ex = BackgroundJobExecutor(Holders(deployment, region))
    ex._logger = SimpleNamespace(name="test")
    ex._run_job_success = lambda jid, events: None
    job = SimpleNamespace(id=job_id, func=lambda: ran.append(job_id), _jobstore_alias="default")

    def _run_now(coro, name):  # noqa: ARG001 - spawn_background's signature
        asyncio.run(coro)

    with (
        patch.object(executor, "spawn_background", _run_now),
        patch.object(executor, "run_job", lambda job, *_a: (job.func(), [])[1]),
    ):
        ex._do_submit_job(job, [])
    assert ran == [job_id]
    assert (deployment.asked, region.asked) == (0, 1)


def test_each_region_holds_its_own_claim_and_one_region_holds_it_once(tmp_path):
    """Two regions on one control plane each hold their region's claim; a second worker of a
    region does not."""
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    eu, us, eu_again = (
        SchedulerHolder(url, scope) for scope in ("acme_rg_eu", "acme_rg_us", "acme_rg_eu")
    )
    try:
        assert eu.holds() and us.holds()
        assert not eu_again.holds()
    finally:
        for holder in (eu, us, eu_again):
            holder.close()


@pytest.mark.parametrize(("region", "scope"), [(None, None), ("eu", "acme_rg_eu")])
def test_a_nodes_region_claim_is_its_regions_and_with_no_regions_the_deployments(
    tmp_path, monkeypatch, region, scope
):
    from provisa.api import app_startup
    from provisa.core import process_region

    platform = {"regions": [{"id": "eu", "address": "https://eu.example.com"}]} if region else None
    was = process_region._region
    process_region.bind_launch(platform, requested=region)
    url = f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"
    monkeypatch.setattr(
        "provisa.core.config_loader.load_control_plane",
        lambda _path: SimpleNamespace(
            resolved_platform_url=lambda: url, resolved_org_id=lambda: "acme"
        ),
    )
    monkeypatch.setattr(app_startup, "config_path_str", lambda: "config.yaml")
    state = SimpleNamespace(_scheduler_holder=None, _region_holder=None)
    try:
        holders = app_startup._scheduler_holders(state)
        assert holders.deployment._scope == "acme"
        if scope is None:
            assert holders.region is holders.deployment
        else:
            assert holders.region._scope == scope
            assert holders.deployment.holds() and holders.region.holds()
    finally:
        process_region._region = was
        if state._region_holder is not state._scheduler_holder:
            state._region_holder.close()
        state._scheduler_holder.close()

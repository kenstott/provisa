# Copyright (c) 2026 Kenneth Stott
# Canary: e3e273f3-5810-4c60-bb5a-d47be7548b66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1448: the query-path wake and the idle reaper.

The GKE calls are stubbed — what is under test is the decision logic around them: how many times a
shard is woken for N concurrent queries, what a cold start does to the generation counter, and what
happens when a query lands in the middle of a stop.
"""

from __future__ import annotations

import asyncio
import contextlib

from types import SimpleNamespace

import pytest

from provisa.federation import engine_wake


@pytest.fixture(autouse=True)
def _clean_module_state():
    """The wake keeps its per-shard bookkeeping in module globals, so each test starts from empty."""
    for d in (
        engine_wake._locks,
        engine_wake._ready_seen,
        engine_wake._generation,
        engine_wake._last_activity,
        engine_wake._stop_tasks,
    ):
        d.clear()
    yield
    for d in (
        engine_wake._locks,
        engine_wake._ready_seen,
        engine_wake._generation,
        engine_wake._last_activity,
        engine_wake._stop_tasks,
    ):
        d.clear()


class _FakeK8s:
    """The provisioner surface the wake uses, recording what it was asked to do."""

    def __init__(self, state: str = "stopped"):
        self.state = state
        self.status_calls = 0
        self.wakes: list[str] = []
        self.stops: list[str] = []

    async def shard_status(self, shard: str) -> dict:
        self.status_calls += 1
        return {"shard": shard, "state": self.state, "ready_replicas": 0, "replicas": 0}

    async def ensure_shared_shard(self, shard: str) -> None:
        self.wakes.append(shard)
        self.state = "ready"

    async def scale_shard_to_zero(self, shard: str) -> None:
        self.stops.append(shard)
        self.state = "stopped"


@pytest.fixture
def fake_k8s(monkeypatch):
    fake = _FakeK8s()
    monkeypatch.setattr(engine_wake.k8s, "shard_status", fake.shard_status)
    monkeypatch.setattr(engine_wake.k8s, "ensure_shared_shard", fake.ensure_shared_shard)
    monkeypatch.setattr(engine_wake.k8s, "scale_shard_to_zero", fake.scale_shard_to_zero)
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: True)
    return fake


# ── boot_shard ──────────────────────────────────────────────────────────────────


def test_boot_shard_raises_when_unset(monkeypatch):
    """No default: a provisioning deployment that cannot name its shard cannot wake it."""
    monkeypatch.delenv("PROVISA_ENGINE_SHARD", raising=False)
    with pytest.raises(engine_wake.k8s.K8sProvisioningError, match="PROVISA_ENGINE_SHARD"):
        engine_wake.boot_shard()


def test_boot_shard_reads_env(monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_SHARD", "shared_2")
    assert engine_wake.boot_shard() == "shared_2"


# ── ensure_shard_awake ──────────────────────────────────────────────────────────


async def test_cold_start_wakes_and_bumps_generation(fake_k8s):
    assert engine_wake.generation("shared_1") == 0
    assert await engine_wake.ensure_shard_awake("shared_1") is True
    assert fake_k8s.wakes == ["shared_1"]
    assert engine_wake.generation("shared_1") == 1


async def test_already_ready_shard_is_not_woken(fake_k8s):
    fake_k8s.state = "ready"
    assert await engine_wake.ensure_shard_awake("shared_1") is False
    assert fake_k8s.wakes == []
    assert engine_wake.generation("shared_1") == 0


async def test_warm_path_skips_the_status_call(fake_k8s):
    """The second query inside the recheck window costs no GKE round trip at all."""
    fake_k8s.state = "ready"
    await engine_wake.ensure_shard_awake("shared_1")
    assert fake_k8s.status_calls == 1
    await engine_wake.ensure_shard_awake("shared_1")
    assert fake_k8s.status_calls == 1


async def test_recheck_window_expiry_re_reads_status(fake_k8s, monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_READY_RECHECK_SECONDS", "0")
    fake_k8s.state = "ready"
    await engine_wake.ensure_shard_awake("shared_1")
    await engine_wake.ensure_shard_awake("shared_1")
    assert fake_k8s.status_calls == 2


async def test_concurrent_queries_wake_the_shard_once(fake_k8s):
    """N queries arriving at a cold shard produce ONE node provision, not N."""
    results = await asyncio.gather(*(engine_wake.ensure_shard_awake("shared_1") for _ in range(8)))
    assert fake_k8s.wakes == ["shared_1"]
    assert engine_wake.generation("shared_1") == 1
    # Exactly one of them paid for the cold start; the rest found it already up.
    assert sum(1 for r in results if r) == 1


async def test_wake_cancels_an_in_flight_stop(fake_k8s):
    """A query arriving mid-drain abandons the shutdown rather than waiting minutes for it."""
    drain = asyncio.Event()

    async def _slow_stop(shard: str) -> None:
        await drain.wait()  # never set: the wake is what ends this
        fake_k8s.stops.append(shard)

    engine_wake._stop_tasks["shared_1"] = asyncio.create_task(_slow_stop("shared_1"))
    await asyncio.sleep(0)

    assert await engine_wake.ensure_shard_awake("shared_1") is True
    assert fake_k8s.stops == []  # the drain never completed
    assert fake_k8s.wakes == ["shared_1"]
    assert engine_wake._stop_tasks.get("shared_1") is None


# ── ensure_engine_awake ─────────────────────────────────────────────────────────


def _state_with(runtime, rebuilt: list):
    registry = SimpleNamespace(
        get=lambda oid: runtime,
        invalidate=lambda oid: rebuilt.append(oid),
    )
    return SimpleNamespace(org_registry=registry)


async def test_non_provisioning_deployment_does_nothing(monkeypatch):
    """A desktop install must not pay a status check per query for an engine that is always on."""
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)

    async def _boom(*a, **k):
        raise AssertionError("the provisioner must not be reached")

    monkeypatch.setattr(engine_wake.k8s, "shard_status", _boom)
    await engine_wake.ensure_engine_awake(SimpleNamespace())


async def test_external_engine_org_is_not_woken(fake_k8s):
    """REQ-1412: an org running its own coordinator is not on a shard this control plane operates."""
    from provisa.api.org_runtime import OrgRuntime, current_org

    rt = OrgRuntime(org_id="acme", engine_endpoint=("their-host", 8080), shard="shared_1")
    token = current_org.set("acme")
    try:
        await engine_wake.ensure_engine_awake(_state_with(rt, []))
    finally:
        current_org.reset(token)
    assert fake_k8s.wakes == []


async def test_shared_org_without_a_shard_raises(fake_k8s):
    from provisa.api.org_runtime import OrgRuntime, current_org

    rt = OrgRuntime(org_id="acme", shard="")
    token = current_org.set("acme")
    try:
        with pytest.raises(RuntimeError, match="no shard recorded"):
            await engine_wake.ensure_engine_awake(_state_with(rt, []))
    finally:
        current_org.reset(token)


async def test_cold_start_rebuilds_the_org_runtime(fake_k8s, monkeypatch):
    """A resumed shard has no catalogs, so the runtime that issued them has to be rebuilt."""
    from provisa.api.org_runtime import OrgRuntime, current_org

    rebuilt: list[str] = []
    built: list[str] = []

    async def _ensure(oid: str):
        built.append(oid)

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure, raising=False)

    rt = OrgRuntime(org_id="acme", shard="shared_1", engine_generation=0)
    token = current_org.set("acme")
    try:
        await engine_wake.ensure_engine_awake(_state_with(rt, rebuilt))
    finally:
        current_org.reset(token)

    assert fake_k8s.wakes == ["shared_1"]
    assert rebuilt == ["acme"]
    assert built == ["acme"]


async def test_warm_shard_does_not_rebuild_the_org_runtime(fake_k8s, monkeypatch):
    from provisa.api.org_runtime import OrgRuntime, current_org

    fake_k8s.state = "ready"
    rebuilt: list[str] = []

    async def _boom(oid: str):
        raise AssertionError("a warm shard still holds this org's catalogs")

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _boom, raising=False)

    rt = OrgRuntime(org_id="acme", shard="shared_1", engine_generation=0)
    token = current_org.set("acme")
    try:
        await engine_wake.ensure_engine_awake(_state_with(rt, rebuilt))
    finally:
        current_org.reset(token)

    assert rebuilt == []


async def test_bound_org_without_a_runtime_raises(fake_k8s):
    from provisa.api.org_runtime import current_org

    token = current_org.set("acme")
    try:
        with pytest.raises(RuntimeError, match="no built runtime"):
            await engine_wake.ensure_engine_awake(_state_with(None, []))
    finally:
        current_org.reset(token)


# ── reaper ──────────────────────────────────────────────────────────────────────


async def test_reaper_scales_to_zero_after_the_idle_window(fake_k8s, monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_IDLE_CHECK_SECONDS", "0")
    monkeypatch.setenv("PROVISA_ENGINE_IDLE_SECONDS", "0")
    await engine_wake.ensure_shard_awake("shared_1")

    task = asyncio.create_task(engine_wake.idle_reaper())
    for _ in range(50):
        await asyncio.sleep(0)
        if fake_k8s.stops:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake_k8s.stops == ["shared_1"]


async def test_reaper_leaves_a_recently_queried_shard_alone(fake_k8s, monkeypatch):
    monkeypatch.setenv("PROVISA_ENGINE_IDLE_CHECK_SECONDS", "0")
    monkeypatch.setenv("PROVISA_ENGINE_IDLE_SECONDS", "900")
    await engine_wake.ensure_shard_awake("shared_1")

    task = asyncio.create_task(engine_wake.idle_reaper())
    for _ in range(50):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake_k8s.stops == []


async def test_a_stopped_shard_is_no_longer_seen_as_ready(fake_k8s):
    """After a stop the next query re-checks, rather than trusting the sighting from before it."""
    await engine_wake.ensure_shard_awake("shared_1")
    assert "shared_1" in engine_wake._ready_seen
    await engine_wake._stop_shard("shared_1")
    assert "shared_1" not in engine_wake._ready_seen


def test_reaper_not_started_without_a_provisioner(monkeypatch):
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)
    st = SimpleNamespace()
    engine_wake.start_idle_reaper(st)
    assert not hasattr(st, "_engine_reaper_task")


async def test_the_reaper_start_seeds_the_boot_shard(fake_k8s, monkeypatch):
    """_last_activity is process state. After a control plane restart it is empty, so a shard whose
    pod is up but which no query touches was never a reap candidate and billed forever."""
    monkeypatch.setenv("PROVISA_ENGINE_SHARD", "shared_1")
    engine_wake._last_activity.clear()
    st = SimpleNamespace()

    engine_wake.start_idle_reaper(st)
    try:
        assert "shared_1" in engine_wake._last_activity
    finally:
        st._engine_reaper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await st._engine_reaper_task


# ── boot order ──────────────────────────────────────────────────────────────────


def test_boot_wakes_the_shard_before_anything_asks_for_its_address():
    """A shard's address exists only between a wake and the next idle-to-zero, so every boot step
    that reads it must come after the wake. The seed of the built-in sources reads it first — it
    writes the engine's endpoint into the source rows — and when the wake sat below it the hosted
    control plane died on startup with "shard shared_1 has no address" (REQ-1448)."""
    import inspect

    from provisa.api import app as app_module

    src = inspect.getsource(app_module._load_and_build)
    wake = src.index("converge_boot_shard()")
    for reader in ("_seed_built_in_sources(pg_host", "_apply_server_and_engine_config(raw_config)"):
        assert wake < src.index(reader), f"{reader} runs before the wake"

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
    # The shared terminal's shard: the query path reads it to decide whether the coordinator it is
    # connected to is still the one serving.
    monkeypatch.setenv("PROVISA_ENGINE_SHARD", "shared_1")
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


class _FakeEngine:
    """The engine-lifecycle surface restore_shared_terminal drives."""

    def __init__(self):
        self.provisions = 0
        self.infra = 0
        self.reconciles = 0

    def provision(self, ops_views, retention):
        self.provisions += 1

    async def provision_infra(self):
        self.infra += 1

    async def reconcile_landed_tables(self):
        self.reconciles += 1
        return []


def _state_with(runtime, rebuilt: list, default=None):
    """A state whose default org owns the shared terminal, as AppState._engine_runtime arranges."""
    from provisa.api.org_runtime import OrgRuntime

    if default is None:
        # Stamped with the generation boot leaves behind, so a warm shard reads as unchanged.
        default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)

    def _get(oid):
        return default if oid == "default" else runtime

    registry = SimpleNamespace(get=_get, invalidate=lambda oid: rebuilt.append(oid))
    return SimpleNamespace(
        org_registry=registry,
        org_id="default",
        federation_engine=_FakeEngine(),
        engine_conn=object(),
        config=None,
        tenant_db=None,
    )


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


async def test_default_org_query_wakes_and_restores_the_shared_terminal(fake_k8s):
    """The deployment's own org is NOT bound by the routing middleware, so its queries arrive with
    current_org unset. Treating that as "boot handles it" left the shared lane asleep and the
    terminal dialing a released pod IP: the first query worked and every one after the idle reaper
    ran timed out at the old address (REQ-1448)."""
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    await engine_wake.ensure_engine_awake(state)

    assert fake_k8s.wakes == ["shared_1"]
    assert state.federation_engine.provisions == 1  # reconnected at the new coordinator
    assert state.federation_engine.infra == 1
    # Materialized sources are unreadable until their landing tables are back (REQ-846/932).
    assert state.federation_engine.reconciles == 1
    assert state.engine_conn is None
    assert default.engine_generation == engine_wake.generation("shared_1")


async def test_default_org_query_on_a_warm_shard_leaves_the_terminal_alone(fake_k8s):
    """A coordinator that never went away still holds the terminal's catalogs."""
    from provisa.api.org_runtime import OrgRuntime

    fake_k8s.state = "ready"
    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    await engine_wake.ensure_engine_awake(state)

    assert fake_k8s.wakes == []
    assert state.federation_engine.provisions == 0


async def test_tenant_cold_start_restores_the_shared_terminal_before_reissuing_catalogs(
    fake_k8s, monkeypatch
):
    """A pooled org has no engine of its own — its CREATE CATALOG statements go through the shared
    terminal, which is still connected to the pod that just went away."""
    from provisa.api.org_runtime import OrgRuntime, current_org

    order: list[str] = []
    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    rt = OrgRuntime(org_id="acme", shard="shared_1", engine_generation=0)
    state = _state_with(rt, [], default=default)

    async def _ensure(oid: str):
        order.append(f"rebuild:{oid}")

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure, raising=False)

    class _Recording(_FakeEngine):
        def provision(self, ops_views, retention):
            super().provision(ops_views, retention)
            order.append("restore")

    state.federation_engine = _Recording()

    token = current_org.set("acme")
    try:
        await engine_wake.ensure_engine_awake(state)
    finally:
        current_org.reset(token)

    assert order == ["restore", "rebuild:acme"]


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


def test_org_build_wakes_the_shard_before_anything_asks_for_its_address():
    """Boot's wake covers the default org only. An org built later — the first sign-in after the
    idle reaper released the shared lane — reads the address too: its built-in source rows carry
    the engine endpoint, and its catalogs are issued to the coordinator. Without a wake of its own
    that sign-in 500'd with "shard shared_1 has no address" (REQ-1448)."""
    import inspect

    from provisa.api import app as app_module

    src = inspect.getsource(app_module.build_org_runtime)
    wake = src.index("ensure_shard_awake(")
    assert wake < src.index("_engine_generation(shard)"), "the generation is sampled before the wake"
    assert wake < src.index("_seed_built_in_sources("), "the seed runs before the wake"

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
from provisa.federation import k8s_provisioner as k8s


@pytest.fixture(autouse=True)
def _clean_module_state():
    """The wake keeps its per-shard bookkeeping in module globals, so each test starts from empty."""
    for d in (
        engine_wake._locks,
        engine_wake._ready_seen,
        engine_wake._last_activity,
        engine_wake._stop_tasks,
        engine_wake._prewarm_tasks,
        # The generation is the provisioner's observation of which pod is serving, so resetting it
        # means clearing what the provisioner has seen.
        k8s._pod_ips,
        k8s._pod_uids,
        k8s._coordinator_epoch,
    ):
        d.clear()
    yield
    for d in (
        engine_wake._locks,
        engine_wake._ready_seen,
        engine_wake._last_activity,
        engine_wake._stop_tasks,
        engine_wake._prewarm_tasks,
        # The generation is the provisioner's observation of which pod is serving, so resetting it
        # means clearing what the provisioner has seen.
        k8s._pod_ips,
        k8s._pod_uids,
        k8s._coordinator_epoch,
    ):
        d.clear()


class _FakeK8s:
    """The provisioner surface the wake uses, recording what it was asked to do."""

    def __init__(self, state: str = "stopped"):
        self.state = state
        self.status_calls = 0
        self.wakes: list[str] = []
        self.stops: list[str] = []
        self.pods = 0

    def land_pod(self, shard: str) -> None:
        """What every real provisioner call ends in: the pod now serving ``shard`` is observed.

        The generation the wake reports is that observation, so a fake that skipped it would report
        a shard whose coordinator never changed no matter how often it was restarted.
        """
        self.pods += 1
        k8s._pod_uids[shard] = f"uid-{self.pods}"
        k8s._coordinator_epoch[shard] = k8s._coordinator_epoch.get(shard, 0) + 1
        k8s._pod_ips[shard] = f"10.20.0.{self.pods}"

    async def shard_status(self, shard: str) -> dict:
        self.status_calls += 1
        if self.state == "ready":
            if shard not in k8s._pod_uids:
                self.land_pod(shard)
            else:
                # A ready shard is re-resolved on every status read, which is how an address
                # discarded after a dial that reached nothing comes back (REQ-1448).
                k8s._pod_ips[shard] = f"10.20.0.{self.pods}"
        return {"shard": shard, "state": self.state, "ready_replicas": 0, "replicas": 0}

    async def ensure_shared_shard(self, shard: str) -> None:
        self.wakes.append(shard)
        self.state = "ready"
        self.land_pod(shard)

    async def scale_shard_to_zero(self, shard: str) -> None:
        self.stops.append(shard)
        self.state = "stopped"
        k8s._pod_uids.pop(shard, None)
        k8s._pod_ips.pop(shard, None)


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
    # The shard was not started here, but its pod WAS seen for the first time, and the generation
    # counts pods observed rather than starts driven. A runtime stamped before that first sighting
    # has no evidence its catalogs were issued on this coordinator.
    assert engine_wake.generation("shared_1") == 1


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

    def provision(self, ops_views):
        self.provisions += 1

    async def provision_infra(self):
        self.infra += 1

    async def reconcile_landed_tables(self):
        self.reconciles += 1
        return []


async def _noop_rebuild(oid: str) -> None:
    """Stands in for ensure_org_runtime: a cold start rebuilds the org, which needs a database."""


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

    # Stamped on the coordinator now serving: that is what "warm" means to the query path.
    await engine_wake.ensure_shard_awake("shared_1")
    rt = OrgRuntime(
        org_id="acme", shard="shared_1", engine_generation=engine_wake.generation("shared_1")
    )
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
    await engine_wake.ensure_shard_awake("shared_1")
    default = OrgRuntime(
        org_id="default", shard="shared_1", engine_generation=engine_wake.generation("shared_1")
    )
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
        def provision(self, ops_views):
            super().provision(ops_views)
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
    assert wake < src.index("_engine_generation(effective_shard)"), (
        "the generation is sampled before the wake"
    )
    assert wake < src.index("_seed_built_in_sources("), "the seed runs before the wake"


def test_org_build_restores_the_shared_terminal_before_issuing_its_catalogs():
    """The wake brings up a NEW coordinator; the shared terminal this build issues its CREATE
    CATALOG statements over is still connected to the released pod. ensure_engine_awake restores it
    at _execute_plan, which is AFTER this build — so without a restore here the build itself dialed
    the dead pod IP and every tenant sign-in ended in a connect timeout at /v1/statement while
    schemas (read from Postgres) loaded normally (REQ-1448)."""
    import inspect

    from provisa.api import app as app_module

    src = inspect.getsource(app_module.build_org_runtime)
    restore = src.index("restore_shared_terminal(state,")
    assert src.index("ensure_shard_awake(") < restore, "the terminal is restored before the wake"
    assert restore < src.index("_seed_built_in_sources("), "the seed runs before the restore"
    # The default org OWNS the terminal and is stamped by boot: restoring from inside its own
    # rebuild would re-enter the build it is already in.
    assert "org_id != state.org_id" in src


# ── prewarm (sign-in) ───────────────────────────────────────────────────────────


async def test_prewarm_wakes_the_shard_without_blocking_the_caller(fake_k8s):
    """REQ-1471: sign-in starts the cold start; it does not wait on it. The caller is /auth/me, and
    a node is ~90-120s away."""
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    engine_wake.prewarm_engine(state, None)
    assert fake_k8s.wakes == []  # returned before the wake ran

    await asyncio.gather(*engine_wake._prewarm_tasks.values())
    assert fake_k8s.wakes == ["shared_1"]


async def test_prewarm_binds_the_org_it_was_given(fake_k8s, monkeypatch):
    """The task does not inherit the request's ContextVar — the middleware resets it before the
    response — so the org has to be bound inside the task or the tenant's shard is never the one
    woken."""
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    rt = OrgRuntime(org_id="acme", shard="shared_2", engine_generation=0)
    state = _state_with(rt, [], default=default)
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _noop_rebuild, raising=False)

    engine_wake.prewarm_engine(state, "acme")
    await asyncio.gather(*engine_wake._prewarm_tasks.values())

    assert "shared_2" in fake_k8s.wakes


async def test_prewarm_does_not_bind_the_deployments_own_org(fake_k8s, monkeypatch):
    """/auth/me reports the default org by NAME, but the routing middleware leaves current_org
    unset for it. Binding the name sends it down the tenant branch, which invalidates the registry
    entry and rebuilds the runtime — and every data surface then answers "No schema available"."""
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    rebuilt: list[str] = []
    state = _state_with(None, rebuilt, default=default)

    async def _boom(oid: str) -> None:
        raise AssertionError("the default org's runtime must not be rebuilt by a prewarm")

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _boom, raising=False)

    engine_wake.prewarm_engine(state, "default")
    await asyncio.gather(*engine_wake._prewarm_tasks.values())

    assert fake_k8s.wakes == ["shared_1"]
    assert rebuilt == []


async def test_prewarm_does_not_start_a_second_wake_for_the_same_org(fake_k8s):
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    engine_wake.prewarm_engine(state, None)
    engine_wake.prewarm_engine(state, None)
    assert len(engine_wake._prewarm_tasks) == 1

    await asyncio.gather(*engine_wake._prewarm_tasks.values())
    assert fake_k8s.wakes == ["shared_1"]


async def test_prewarm_failure_does_not_reach_the_caller(fake_k8s, monkeypatch):
    """Sign-in must not fail over an engine the user has not asked for yet; the query path runs the
    same wake and surfaces the failure where it can be acted on."""

    async def _boom(shard: str) -> None:
        raise RuntimeError("no node available")

    monkeypatch.setattr(engine_wake.k8s, "ensure_shared_shard", _boom)
    from provisa.api.org_runtime import OrgRuntime

    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    engine_wake.prewarm_engine(state, None)
    await asyncio.gather(*engine_wake._prewarm_tasks.values())  # does not raise
    assert engine_wake._prewarm_tasks == {}


def test_prewarm_is_a_no_op_without_a_provisioner(monkeypatch):
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)
    engine_wake.prewarm_engine(SimpleNamespace(), None)
    assert engine_wake._prewarm_tasks == {}


def test_sign_in_prewarms_the_engine():
    """/auth/me is the first authenticated call of a session — the IdP owns login — so it is the
    earliest point the platform knows which shard the session will query (REQ-1471)."""
    import inspect

    from provisa.api import auth_router

    src = inspect.getsource(auth_router.me)
    assert "prewarm_engine(state, active_org_id)" in src


# ── the dedicated lane (REQ-1510) ───────────────────────────────────────────────


@pytest.fixture
def fake_isolated_k8s(fake_k8s, monkeypatch):
    """``fake_k8s`` plus the isolated provisioner call, recording the size it was asked for."""
    isolated: list[tuple[str, object]] = []

    async def ensure_isolated_shard(shard: str, size):
        isolated.append((shard, size))
        fake_k8s.state = "ready"
        fake_k8s.land_pod(shard)

    monkeypatch.setattr(engine_wake.k8s, "ensure_isolated_shard", ensure_isolated_shard)
    fake_k8s.isolated = isolated
    return fake_k8s


async def test_a_cold_isolated_shard_wakes_at_its_plan_size(fake_isolated_k8s):
    """The size travels with the wake, not only with the first provision: a dedicated engine resumed
    at the shared shape serves on hardware the org is not invoiced for (REQ-1510)."""
    assert await engine_wake.ensure_shard_awake("org_acme", lane="isolated", size="pro_m") is True
    assert fake_isolated_k8s.isolated == [("org_acme", "pro_m")]
    assert fake_isolated_k8s.wakes == []  # never the shared manifests


async def test_converge_applies_even_when_the_shard_is_already_ready(fake_isolated_k8s):
    """A move between Pro sizes finds the shard serving. The wake would return without touching the
    cluster; converge applies, which is what revises the config and rolls the new machine in."""
    fake_isolated_k8s.state = "ready"
    await engine_wake.converge_shard("org_acme", lane="isolated", size="pro_l")
    assert fake_isolated_k8s.isolated == [("org_acme", "pro_l")]
    assert engine_wake.generation("org_acme") == 1


async def test_converge_bumps_the_generation_so_the_query_path_rebuilds(fake_isolated_k8s):
    await engine_wake.converge_shard("org_acme", lane="isolated", size="pro_s")
    await engine_wake.converge_shard("org_acme", lane="isolated", size="pro_m")
    assert engine_wake.generation("org_acme") == 2


async def test_release_stops_the_shard_and_forgets_it(fake_isolated_k8s):
    """A shard released because its org moved back to the shared lane is not coming back, so the
    reaper must not be left measuring the idleness of something nobody dispatches to."""
    await engine_wake.ensure_shard_awake("org_acme", lane="isolated", size="pro_s")
    assert "org_acme" in engine_wake._ready_seen
    await engine_wake.release_shard("org_acme")
    assert fake_isolated_k8s.stops == ["org_acme"]
    assert "org_acme" not in engine_wake._ready_seen
    assert "org_acme" not in engine_wake._last_activity


def test_the_isolated_shard_name_is_distinct_from_the_orgs_shared_placement():
    """REQ-1450: ``orgs.shard`` keeps the pooled placement across the move, so a return to Starter
    lands back on the shard the org already had."""
    from provisa.federation.k8s_provisioner import isolated_shard, shard_workload_name

    assert isolated_shard("acme") == "org_acme"
    assert isolated_shard("acme") != "shared_1"
    assert shard_workload_name(isolated_shard("acme")) == "trino-org-acme"


def test_a_dedicated_engine_is_dialled_at_its_pods_address_on_a_cluster(monkeypatch):
    """Not through PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE: on a provisioning deployment the shard the
    control plane just woke is the engine, and its pod IP is what the wake recorded (REQ-1510)."""
    from provisa.federation import engine as engine_mod
    from provisa.federation import k8s_provisioner as k8s

    monkeypatch.setattr(k8s, "provisioning_available", lambda: True)
    monkeypatch.setenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", "trino-{org_id}.internal")
    monkeypatch.setitem(k8s._pod_ips, "org_acme", "10.4.2.7")
    monkeypatch.setenv("PROVISA_ENGINE_PORT", "8080")
    assert engine_mod.isolated_engine_endpoint("acme") == ("10.4.2.7", 8080)


def test_the_isolated_lane_is_available_wherever_the_cluster_provisions(monkeypatch):
    """The lane used to require the host template, so a cluster deployment refused an org its plan
    had already sold a dedicated engine to (REQ-1510)."""
    from provisa.federation import engine as engine_mod
    from provisa.federation import k8s_provisioner as k8s

    monkeypatch.delenv("PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE", raising=False)
    monkeypatch.setattr(k8s, "provisioning_available", lambda: True)
    assert engine_mod.isolated_engine_available() is True


def test_an_isolated_org_wakes_its_own_shard_and_skips_the_shared_terminal():
    """An isolated org dispatches over its OWN terminal, so restoring the shared one for it would
    reissue the wrong org's catalogs onto the wrong coordinator."""
    import inspect

    src = inspect.getsource(engine_wake._wake_isolated)
    assert "isolated_shard(org_id)" in src
    assert "restore_shared_terminal" not in src
    assert src.index("ensure_shard_awake(") < src.index("ensure_org_runtime(org_id)")


# ── attach_if_serving ───────────────────────────────────────────────────────────


async def test_a_background_job_does_not_wake_a_resting_shard(fake_k8s):
    """Compaction ticks every minute. If maintenance could wake the shard it would never rest, and
    idle-to-zero would save nothing (REQ-1448, REQ-1464)."""
    assert await engine_wake.attach_if_serving(_state_with(None, [])) is False
    assert fake_k8s.wakes == []


async def test_a_background_job_reattaches_to_a_replaced_coordinator(fake_k8s):
    """The job's handle is the shared terminal, still connected to the pod that went away. Without
    the restore it dials a released address and every statement dies on a connect timeout."""
    from provisa.api.org_runtime import OrgRuntime

    fake_k8s.state = "ready"
    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)

    assert await engine_wake.attach_if_serving(state) is True
    # The pod was observed for the first time here, so generation 1 against the runtime's 0 is the
    # replacement the terminal has to be moved onto.
    assert state.federation_engine.provisions == 1
    assert default.engine_generation == 1


async def test_a_background_job_on_an_unchanged_coordinator_leaves_the_terminal_alone(fake_k8s):
    """A restore per tick would tear down and rebuild a working terminal every minute."""
    fake_k8s.state = "ready"
    state = _state_with(None, [])
    await engine_wake.attach_if_serving(state)
    provisions = state.federation_engine.provisions

    await engine_wake.attach_if_serving(state)
    assert state.federation_engine.provisions == provisions


async def test_a_background_job_does_not_count_as_traffic(fake_k8s):
    """The reaper measures idleness from real traffic. A maintenance tick that stamped activity
    would hold the shard open forever exactly as a wake would."""
    fake_k8s.state = "ready"
    await engine_wake.attach_if_serving(_state_with(None, []))
    assert "shared_1" not in engine_wake._last_activity


async def test_a_non_provisioning_deployment_always_has_its_engine(monkeypatch):
    """A desktop engine is always on, so a background job must neither check nor skip."""
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)

    async def _boom(*a, **k):
        raise AssertionError("the provisioner must not be reached")

    monkeypatch.setattr(engine_wake.k8s, "shard_status", _boom)
    assert await engine_wake.attach_if_serving(SimpleNamespace()) is True


# ── engine_state ────────────────────────────────────────────────────────────────


async def test_a_desktop_engine_reports_always_on(monkeypatch):
    """A process that is simply there has no wake to wait for, and reporting a shard state would
    invite the UI to wait for one."""
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)

    async def _boom(*a, **k):
        raise AssertionError("the provisioner must not be reached")

    monkeypatch.setattr(engine_wake.k8s, "shard_status", _boom)
    assert await engine_wake.engine_state(SimpleNamespace(), None) == "always-on"


async def test_the_default_org_reports_the_boot_shard(fake_k8s):
    fake_k8s.state = "stopped"
    assert await engine_wake.engine_state(_state_with(None, []), None) == "stopped"
    fake_k8s.state = "ready"
    assert await engine_wake.engine_state(_state_with(None, []), "default") == "ready"


async def test_reporting_the_state_never_wakes_or_stamps_activity(fake_k8s):
    """A browser polls this while it waits. A poll that woke the shard — or counted as traffic —
    would let an idle tab hold a pod up indefinitely."""
    await engine_wake.engine_state(_state_with(None, []), None)
    assert fake_k8s.wakes == []
    assert "shared_1" not in engine_wake._last_activity


async def test_a_pro_org_reports_its_own_coordinator(fake_k8s, monkeypatch):
    """REQ-1510: a dedicated engine idles to zero like any other. Reporting the shared shard's
    state would tell a paying org its engine is ready while its own is still provisioning."""
    from provisa.api.org_runtime import OrgRuntime

    monkeypatch.setattr(engine_wake.k8s, "isolated_shard", lambda oid: f"org-{oid}")
    seen: list[str] = []

    async def _status(shard: str) -> dict:
        seen.append(shard)
        return {"shard": shard, "state": "starting", "ready_replicas": 0, "replicas": 1}

    monkeypatch.setattr(engine_wake.k8s, "shard_status", _status)

    rt = OrgRuntime(org_id="acme", shard="shared_1", isolated_engine=True)
    assert await engine_wake.engine_state(_state_with(rt, []), "acme") == "starting"
    assert seen == ["org-acme"]


async def test_a_byo_engine_org_reports_always_on(fake_k8s):
    """REQ-1412: an org running its own coordinator is not this control plane's shard to report."""
    from provisa.api.org_runtime import OrgRuntime

    rt = OrgRuntime(org_id="acme", engine_endpoint=("their-host", 8080), shard="shared_1")
    assert await engine_wake.engine_state(_state_with(rt, []), "acme") == "always-on"


async def test_a_draining_shard_reports_starting(fake_k8s):
    """A stop is a drain plus the scale-down after it, and the Deployment reports ready throughout.
    Reporting ready would clear the UI's waiting state just before the query's wake cancels the stop
    and pays for a cold start anyway."""
    fake_k8s.state = "ready"

    async def _drain() -> None:
        await asyncio.Event().wait()

    engine_wake._stop_tasks["shared_1"] = asyncio.create_task(_drain())
    await asyncio.sleep(0)
    try:
        assert await engine_wake.engine_state(_state_with(None, []), None) == "starting"
    finally:
        engine_wake._stop_tasks["shared_1"].cancel()


async def test_an_org_with_no_runtime_is_an_error_not_a_guess(fake_k8s):
    """Reporting a default state for an org whose engine cannot be resolved would show a ready
    engine for a request that is about to fail."""
    state = _state_with(None, [])
    with pytest.raises(RuntimeError, match="no built runtime"):
        await engine_wake.engine_state(state, "acme")


# ── readdress_lost_coordinator ──────────────────────────────────────────────────


def _timeout_error() -> Exception:
    """The failure a released pod IP produces: a connect timeout, restated by the executor."""
    import requests

    inner = requests.exceptions.ConnectTimeout("connect timeout=10")
    outer = RuntimeError("failed to execute: HTTPConnectionPool(host='10.20.0.16', port=8080)")
    outer.__cause__ = inner
    return outer


async def test_a_moved_coordinator_is_re_resolved_and_the_query_redispatched(fake_k8s):
    """REQ-1448: the recheck window means the recorded address can be up to a window stale, and an
    eviction this process did not drive replaces the pod inside it. The connect failure is the only
    signal that says so, so it drops the address and wakes again — and the caller redispatches."""
    from provisa.api.org_runtime import OrgRuntime

    fake_k8s.state = "ready"
    default = OrgRuntime(org_id="default", shard="shared_1", engine_generation=0)
    state = _state_with(None, [], default=default)
    await engine_wake.ensure_engine_awake(state)
    stale = k8s.recorded_shard_address("shared_1")
    assert stale is not None

    # The cluster now answers with a different pod, as it would after the node was released.
    fake_k8s.land_pod("shared_1")
    k8s._pod_ips["shared_1"] = stale  # what this process still holds

    assert await engine_wake.readdress_lost_coordinator(_timeout_error(), state) is True
    assert k8s.recorded_shard_address("shared_1") != stale


async def test_an_unmoved_coordinator_is_not_redispatched(fake_k8s):
    """A shard that comes back at the same address was reachable all along, so the timeout was the
    engine's own. Dispatching again would make a genuinely-down coordinator cost two waits."""
    from provisa.api.org_runtime import OrgRuntime

    fake_k8s.state = "ready"
    state = _state_with(None, [], default=OrgRuntime(org_id="default", shard="shared_1"))
    await engine_wake.ensure_engine_awake(state)

    assert await engine_wake.readdress_lost_coordinator(_timeout_error(), state) is False


async def test_a_statement_error_does_not_re_resolve(fake_k8s):
    """A query the coordinator ACCEPTED and failed says nothing about the address, and re-resolving
    would rebuild every org runtime over a syntax error."""
    from provisa.api.org_runtime import OrgRuntime

    fake_k8s.state = "ready"
    state = _state_with(None, [], default=OrgRuntime(org_id="default", shard="shared_1"))
    await engine_wake.ensure_engine_awake(state)
    before = fake_k8s.status_calls

    assert await engine_wake.readdress_lost_coordinator(ValueError("SYNTAX_ERROR"), state) is False
    assert fake_k8s.status_calls == before


async def test_non_provisioning_deployment_never_re_resolves(monkeypatch):
    """An operator-run coordinator has an address this control plane did not resolve and cannot
    replace."""
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)
    assert (
        await engine_wake.readdress_lost_coordinator(_timeout_error(), SimpleNamespace()) is False
    )


# ── surfaces that execute outside _execute_plan ─────────────────────────────────


def test_graphql_endpoint_wakes_the_engine_before_dispatch():
    """REQ-1448: /data/graphql compiles and executes on its own path — it never reaches
    _execute_plan, where the SQL pipeline's wake sits. Without a wake of its own, a shard that had
    scaled to zero stayed at zero for as long as only GraphQL traffic arrived, and every query
    failed with a connect timeout at /v1/statement against the retired coordinator's address."""
    import inspect

    from provisa.api.data import endpoint as endpoint_module

    src = inspect.getsource(endpoint_module.graphql_endpoint)
    wake = src.index("ensure_engine_awake(state)")
    # Introspection answers from the GraphQL schema alone and returns before this point.
    assert src.index("_detect_introspection(document)") < wake, (
        "introspection wakes the engine it never queries"
    )
    for dispatch in ("_handle_query(", "_handle_mutation(", "handle_subscription_sse("):
        assert wake < src.index(dispatch), f"{dispatch} runs before the wake"


def test_graphql_field_execution_redispatches_a_moved_coordinator():
    """REQ-1448: the wake answers from a recheck window, so a pod replaced inside that window
    leaves the recorded address stale. The connect failure is the signal to re-resolve, and the
    statement is worth one more dispatch only when the shard actually moved."""
    import inspect

    from provisa.api.data import endpoint as endpoint_module

    src = inspect.getsource(endpoint_module._execute_one_field)
    assert "readdress_lost_coordinator(exc, state)" in src, (
        "a lost coordinator is never re-resolved on the GraphQL path"
    )
    assert src.count("await _dispatch()") == 2, "the redispatch after re-resolution is missing"

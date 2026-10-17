# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Trino-only restart under a running app: the next query reissues the catalogs and answers.

The engine keeps its catalogs in memory (catalog.store=memory), so a coordinator that restarts while
Provisa keeps running has none of them. Outside provisioning the query path learns it from the
coordinator's start (Trino's coordinatorId) and reissues what each process built: the shared
terminal's catalogs and the querying org's runtime. Two processes serving two orgs each notice on
their own next query; the coordinator here is one fake both reach.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace

import httpx
import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.federation import engine_wake, trino_lifecycle

_SYSTEM = {"provisa_admin", "otel", "results"}


class _Coordinator:
    """One Trino coordinator: its start's id and the catalogs it holds in memory."""

    def __init__(self) -> None:
        self.starts = 1
        self.catalogs: set[str] = set()

    @property
    def coordinator_id(self) -> str:
        return f"start{self.starts}"

    def restart(self) -> None:
        self.starts += 1
        self.catalogs = set()


class _Engine:
    """The shared terminal's engine as restore_shared_terminal drives it."""

    def __init__(self, coordinator: _Coordinator) -> None:
        self.coordinator = coordinator
        self.provisions = 0

    def coordinator_start(self, *, fresh: bool = False) -> str:
        return f"trino:8080/{self.coordinator.coordinator_id}"

    def provision(self, ops_views) -> None:
        self.provisions += 1
        self.coordinator.catalogs |= _SYSTEM

    async def provision_infra(self) -> None:
        pass

    async def reconcile_landed_tables(self) -> list:
        return []


class _Process:
    """One worker process: its own registry, its own record of coordinator starts."""

    def __init__(self, coordinator: _Coordinator) -> None:
        self.coordinator = coordinator
        self.generations: dict[str, int] = {"": 0}
        self.rebuilds: list[str] = []
        self.runtimes: dict[str, OrgRuntime] = {}
        engine = _Engine(coordinator)
        self.engine = engine
        registry = SimpleNamespace(
            get=self.runtimes.get,
            invalidate=lambda key: self.runtimes.pop(key),
            set=self.runtimes.__setitem__,
        )
        self.state = SimpleNamespace(
            org_registry=registry,
            org_id="default",
            federation_engine=engine,
            engine_conn=object(),
            engine_conn_kwargs={"host": "trino", "port": 8080},
            raw_config=None,
        )

    @contextlib.contextmanager
    def running(self, monkeypatch, org_id: str):
        """Act as this process, serving ``org_id``."""
        from provisa.core.request_context import reset_current_org, set_current_org

        monkeypatch.setattr(engine_wake, "_start_generations", self.generations)
        monkeypatch.setattr(engine_wake, "_locks", {})

        async def _ensure_org_runtime(oid: str, env: str | None = None) -> None:
            # What the org build does: stamp the coordinator start, then issue the catalogs.
            self.rebuilds.append(oid)
            await self.build(oid)

        import provisa.api.app as app_module

        monkeypatch.setattr(app_module, "ensure_org_runtime", _ensure_org_runtime, raising=False)
        token = set_current_org(org_id)
        try:
            yield self
        finally:
            reset_current_org(token)

    async def build(self, org_id: str) -> None:
        runtime = OrgRuntime(org_id=org_id)
        runtime.engine_generation = await engine_wake.coordinator_generation(self.state)
        if org_id == "default":
            self.engine.provision([])
        self.coordinator.catalogs.add(f"org_{org_id}__sales")
        self.runtimes[org_id] = runtime

    def answers(self, org_id: str) -> bool:
        """A query of ``org_id`` finds every catalog it reads."""
        return (_SYSTEM | {f"org_{org_id}__sales"}) <= self.coordinator.catalogs


@pytest.fixture
def no_provisioner(monkeypatch):
    monkeypatch.setattr(engine_wake.k8s, "provisioning_available", lambda: False)

    async def _boom(*a, **k):
        raise AssertionError("the provisioner must not be reached")

    monkeypatch.setattr(engine_wake.k8s, "shard_status", _boom)


async def _boot(process: _Process, monkeypatch, *orgs: str) -> None:
    for org in ("default", *orgs):
        with process.running(monkeypatch, org):
            await process.build(org)


async def test_a_trino_only_restart_is_repaired_by_each_process_on_its_next_query(
    no_provisioner, monkeypatch
):
    coordinator = _Coordinator()
    first, second = _Process(coordinator), _Process(coordinator)
    await _boot(first, monkeypatch, "acme")
    await _boot(second, monkeypatch, "globex")
    assert first.answers("acme") and second.answers("globex")

    coordinator.restart()
    assert not first.answers("acme") and not second.answers("globex")

    with first.running(monkeypatch, "acme"):
        await engine_wake.ensure_engine_awake(first.state)
    assert first.answers("acme")
    assert first.rebuilds == ["acme"]

    with second.running(monkeypatch, "globex"):
        await engine_wake.ensure_engine_awake(second.state)
    assert second.answers("globex")
    assert second.rebuilds == ["globex"]

    # Each process stamped everything it reissued with the coordinator start now serving.
    for process in (first, second):
        current = process.generations[f"trino:8080/{coordinator.coordinator_id}"]
        assert {rt.engine_generation for rt in process.runtimes.values()} == {current}


async def test_a_coordinator_that_did_not_restart_is_left_alone(no_provisioner, monkeypatch):
    coordinator = _Coordinator()
    process = _Process(coordinator)
    await _boot(process, monkeypatch, "acme")
    provisions = process.engine.provisions

    with process.running(monkeypatch, "acme"):
        await engine_wake.ensure_engine_awake(process.state)
    with process.running(monkeypatch, "default"):
        await engine_wake.ensure_engine_awake(process.state)

    assert process.rebuilds == []
    assert process.engine.provisions == provisions


async def test_the_deployment_orgs_query_restores_the_shared_terminal_alone(
    no_provisioner, monkeypatch
):
    coordinator = _Coordinator()
    process = _Process(coordinator)
    await _boot(process, monkeypatch, "acme")
    coordinator.restart()

    with process.running(monkeypatch, "default"):
        await engine_wake.ensure_engine_awake(process.state)

    assert _SYSTEM <= coordinator.catalogs
    # The tenant is rebuilt by its own next query, not by the deployment org's.
    assert process.rebuilds == []


async def test_concurrent_queries_reissue_once(no_provisioner, monkeypatch):
    import asyncio

    coordinator = _Coordinator()
    process = _Process(coordinator)
    await _boot(process, monkeypatch, "acme")
    coordinator.restart()

    with process.running(monkeypatch, "acme"):
        await asyncio.gather(*(engine_wake.ensure_engine_awake(process.state) for _ in range(5)))

    assert process.rebuilds == ["acme"]
    assert process.answers("acme")


# ── trino_lifecycle.coordinator_start ───────────────────────────────────────────


@pytest.fixture
def info_server(monkeypatch):
    """/v1/info as a coordinator answers it; ``refusals`` connection refusals come first."""
    server = SimpleNamespace(coordinator_id="abcde", calls=0, refusals=0)

    def _get(url, timeout):
        server.calls += 1
        if server.refusals:
            server.refusals -= 1
            raise httpx.ConnectError("connection refused")
        return httpx.Response(
            200,
            json={"coordinator": True, "coordinatorId": server.coordinator_id},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx, "get", _get)
    monkeypatch.setattr(trino_lifecycle, "_coordinator_seen", {})
    monkeypatch.setattr("time.sleep", lambda s: None)
    return server


_KWARGS = {"host": "trino", "port": 8080, "http_scheme": "http"}


def test_the_identity_is_the_endpoint_and_the_coordinators_start(info_server):
    assert trino_lifecycle.coordinator_start(_KWARGS, 10) == "trino:8080/abcde"


def test_the_identity_is_asked_once_per_recheck_window_unless_fresh(info_server):
    trino_lifecycle.coordinator_start(_KWARGS, 10)
    info_server.coordinator_id = "fghij"
    assert trino_lifecycle.coordinator_start(_KWARGS, 10) == "trino:8080/abcde"
    assert info_server.calls == 1
    assert trino_lifecycle.coordinator_start(_KWARGS, 10, fresh=True) == "trino:8080/fghij"


def test_a_restarting_coordinator_is_waited_for(info_server):
    info_server.refusals = 2
    assert trino_lifecycle.coordinator_start(_KWARGS, 10) == "trino:8080/abcde"
    assert info_server.calls == 3


def test_a_coordinator_that_never_answers_is_raised(info_server):
    info_server.refusals = 10**6
    with pytest.raises(httpx.ConnectError):
        trino_lifecycle.coordinator_start(_KWARGS, 0)

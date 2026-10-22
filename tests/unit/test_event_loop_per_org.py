# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7e4f19-8c3a-4d56-a1f0-6e9d2c5b8a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Each org's event loop is its own, in a process that serves several (REQ-1266).

Two orgs wire their loops in one process, then the first registers a table. Its new node is
drained by its own tick, reads its own source as itself, and writes to its own store; the other
org's tick and events are untouched."""

from __future__ import annotations

import contextlib
import logging
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.org_runtime import OrgRuntime
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.events import supervisor

pytestmark = pytest.mark.unbound

_LOG = logging.getLogger(__name__)
_ACME, _GLOBEX = "acme", "globex"


class _Db:
    """An org's event store: what a connection drawn from it is."""

    def __init__(self, org: str) -> None:
        self.org = org

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield SimpleNamespace(org=self.org)


class _Processor:
    """A node's processor: a drain fetches its table's rows and writes them where its engine
    lands, on the connection the tick gives it."""

    def __init__(self, node: str, fetch: Any, engine: Any, drained: list) -> None:
        self.node, self._fetch, self._engine, self._drained = node, fetch, engine, drained

    async def process_pending(self, conn: Any) -> None:
        rows = await self._fetch([])
        self._drained.append(
            {"node": self.node, "events_of": conn.org, "rows": rows,
             "written_to": self._engine.materialize_store_dsn()}
        )  # fmt: skip

    def register_poll_job(self, *_a: Any, **_kw: Any) -> None:
        pass


def _in(org: str):
    @contextlib.contextmanager
    def bound():
        token = set_current_org(org)
        try:
            yield
        finally:
            reset_current_org(token)

    return bound()


@pytest.fixture
def two_orgs(monkeypatch):
    """Two orgs in one process, each with an API source ``crm`` of its own (its own address and
    credential) and a table ``orders`` of it, on the deployment's one engine."""
    from provisa.api import app
    from provisa.api.app import AppState
    from provisa.events import app_wiring, boot
    from provisa.federation import registry_view
    from provisa.federation.engine import build_engine
    from provisa.events import source_loader
    from provisa.api_source import caller

    state = AppState()
    state.org_id = "root"
    state.config = SimpleNamespace(sources=[], tables=[])
    state._scheduler = SimpleNamespace(add_job=lambda *a, **k: None)
    engine = build_engine("duckdb")
    shared = SimpleNamespace(engine=engine, materialize_store_dsn=engine.materialize_store)
    for org in (_ACME, _GLOBEX):
        rt = OrgRuntime(org_id=org)
        rt.tenant_db = _Db(org)
        rt.federation_engine = shared
        rt.api_endpoints = {("crm", "orders"): SimpleNamespace(default_params={})}
        rt.api_sources = {
            "crm": SimpleNamespace(base_url=f"https://{org}.example", auth=org, headers={})
        }
        state.org_registry.set(org, rt)
    monkeypatch.setattr(app, "state", state)

    async def _call_api(endpoint, params, *, base_url, auth, source_headers):
        return (base_url, auth)

    monkeypatch.setattr(caller, "call_api", _call_api)
    monkeypatch.setattr(
        caller, "answer_rows", lambda _e, answer: ([{"url": answer[0], "as": answer[1]}], None)
    )
    monkeypatch.setattr(
        "provisa.storage.byo.org_store_dsn", lambda org: f"postgresql://{org}-store/data"
    )
    src = SimpleNamespace(id="crm", type=SimpleNamespace(value="openapi"))
    tbl = SimpleNamespace(source_id="crm", schema_name="api", table_name="orders")

    async def _sources(_state):
        return [src]

    async def _tables(_state):
        return [tbl]

    monkeypatch.setattr(registry_view, "registered_sources", _sources)
    monkeypatch.setattr(registry_view, "registered_tables", _tables)
    monkeypatch.setattr(
        app_wiring,
        "build_adapter_loaders",
        lambda st, _e: {"openapi": source_loader.make_openapi_loader(st)},
    )
    monkeypatch.setattr(app_wiring, "build_keyed_adapter_loaders", lambda *_a, **_k: {})
    monkeypatch.setattr(app_wiring, "make_source_probe", lambda *_a, **_k: None)
    drained: list = []

    def _spec(tbl, src, *, engine_runtime, source_fetch, **_kw):
        return SimpleNamespace(
            handle=SimpleNamespace(),
            poll_seconds=60,
            probe_factory=object(),
            fetch=source_fetch(src, tbl),
            engine=engine_runtime,
            node=f"{src.id}/{tbl.schema_name}.{tbl.table_name}",
        )

    monkeypatch.setattr(boot, "build_source_node_spec", _spec)
    monkeypatch.setattr(
        boot,
        "build_processors",
        lambda specs, **_kw: [_Processor(s.node, s.fetch, s.engine, drained) for s in specs],
    )
    return state, drained


async def _wire(state, org: str) -> list:
    """``org``'s loop as wire_event_loop leaves it: its processors, the list its tick drains."""
    with _in(org):
        state.event_loop_processors = processors = []
    return processors


@pytest.mark.asyncio
async def test_a_table_registered_after_another_org_wired_is_drained_as_its_own(two_orgs):
    from provisa.events.app_wiring import wire_new_poll_jobs

    state, drained = two_orgs
    acme_loop = await _wire(state, _ACME)
    globex_loop = await _wire(state, _GLOBEX)  # wired last

    with _in(_ACME):  # acme registers crm.orders
        assert await wire_new_poll_jobs(state=state, log=_LOG) == 1

    for org, loop in ((_ACME, acme_loop), (_GLOBEX, globex_loop)):
        with _in(org):  # each org's tick, as register_runtime fires it
            await supervisor.tick(state.tenant_db, loop)

    assert drained == [
        {
            "node": "crm/api.orders",
            "events_of": _ACME,
            "rows": [{"url": "https://acme.example", "as": _ACME}],
            "written_to": "postgresql://acme-store/data",
        }
    ]


@pytest.mark.asyncio
async def test_a_nodes_fetch_reads_its_own_orgs_source_whichever_org_is_bound(two_orgs):
    from provisa.events.app_wiring import wire_new_poll_jobs

    state, drained = two_orgs
    await _wire(state, _ACME)
    await _wire(state, _GLOBEX)
    with _in(_ACME):
        await wire_new_poll_jobs(state=state, log=_LOG)
        [processor] = state.event_loop_processors
    with _in(_GLOBEX):  # a job of another org's fires it
        rows = await processor._fetch([])
    assert rows == [{"url": "https://acme.example", "as": _ACME}]


@pytest.mark.asyncio
async def test_each_org_registers_the_poll_jobs_of_its_own_nodes(two_orgs):
    from provisa.events.app_wiring import wire_new_poll_jobs

    state, _drained = two_orgs
    await _wire(state, _ACME)
    await _wire(state, _GLOBEX)
    for org in (_ACME, _GLOBEX):  # each registers its own crm.orders
        with _in(org):
            assert await wire_new_poll_jobs(state=state, log=_LOG) == 1
    for org in (_ACME, _GLOBEX):
        with _in(org):
            assert state.poll_jobs_registered == {"crm/api.orders"}
            assert [p.node for p in state.event_loop_processors] == ["crm/api.orders"]


def test_a_nodes_triggers_fire_in_the_org_whose_table_it_is(two_orgs, monkeypatch):
    from provisa.core.request_context import current_org
    from provisa.events.app_wiring import node_event_triggers

    state, _drained = two_orgs
    fired: list = []
    monkeypatch.setattr(
        "provisa.scheduler.table_triggers.table_event",
        lambda _state, key, event, _payload: fired.append((current_org.get(), key, event)),
    )
    with _in(_ACME):
        triggers = node_event_triggers(state)
    with _in(_GLOBEX):  # another org's job drains the node
        triggers("crm/api.orders", "quarantine", {"reason": "a check failed"})
    assert fired == [(_ACME, ("crm", "api", "orders"), "quarantine")]


def test_a_landed_input_wires_only_its_own_orgs_waiting_views(two_orgs):
    from provisa.events.app_wiring import _wire_views_reading

    state, _drained = two_orgs
    jobs: list[str] = []
    sched = SimpleNamespace(add_job=lambda _fn, id, replace_existing: jobs.append(id))
    for org in (_ACME, _GLOBEX):  # each org's views wait on its own crm.orders
        with _in(org):
            state.views_awaiting_input = {f"{org}.mv_view_daily": {"crm/api.orders"}}
            _wire_views_reading(sched, state, _LOG, org)
    with _in(_ACME):  # acme's replica lands
        state.on_input_landed("crm/api.orders")
    assert jobs == ["events:wire-awaiting:org_acme"]

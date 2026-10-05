# Copyright (c) 2026 Kenneth Stott
# Canary: ec83399d-28c8-48e2-a7c8-3ef4446c50fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Work that no request starts binds the org it is for, by itself (REQ-1266).

A scheduled job, a live-query poll and an Airport RPC have no request to bind an org for them, and
the state refuses a per-org read with none bound. Each binds its own org -- and a scheduled job
never inherits whichever org last re-armed the scheduler. These run with nothing bound.
"""

# Requirements: REQ-1266

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

from provisa.core.request_context import current_org, reset_current_org, set_current_org

pytestmark = pytest.mark.unbound


# --- the scheduler's job executor ----------------------------------------------------------------


async def test_a_scheduled_job_never_inherits_the_org_that_re_armed_the_scheduler():
    """add_job from inside a tenant request copies that request's context into the wakeup chain;
    the job still fires with no org bound, so it serves only the org it binds itself."""
    from apscheduler.triggers.date import DateTrigger

    from provisa.scheduler.executor import background_scheduler

    seen: list[str | None] = []
    fired = asyncio.Event()
    loop = asyncio.get_running_loop()

    async def _job() -> None:
        seen.append(current_org.get())
        loop.call_soon_threadsafe(fired.set)

    scheduler = background_scheduler()
    scheduler.start()
    token = set_current_org("acme")  # a tenant request registers the job
    try:
        scheduler.add_job(_job, trigger=DateTrigger(), id="probe")
    finally:
        reset_current_org(token)
    try:
        await asyncio.wait_for(fired.wait(), 10)
    finally:
        scheduler.shutdown(wait=False)
    assert seen == [None]


async def test_the_deployments_platform_jobs_run_as_the_deployment_org(monkeypatch):
    from provisa.scheduler import jobs

    seen: list[str | None] = []
    engine = SimpleNamespace(watchdog=lambda: _record(seen))
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(org_id="root", shared_federation_engine=engine),
        raising=False,
    )
    await jobs.watch_engine()
    assert seen == ["root"]
    assert current_org.get() is None


async def _record(seen: list[str | None]) -> None:
    seen.append(current_org.get())


# --- row-materialize jobs ------------------------------------------------------------------------


async def test_row_materialize_jobs_bind_and_name_the_org_they_were_wired_for(monkeypatch):
    from provisa.events import row_materialize_lifecycle as rml

    table = SimpleNamespace(
        source_id="src",
        schema_name="public",
        table_name="orders",
        columns=[SimpleNamespace(name="id", is_primary_key=True)],
    )

    async def _tables(_state):
        return {"orders": table}

    seen: list[str | None] = []

    async def _refresh(_state, **_kw):
        seen.append(current_org.get())

    monkeypatch.setattr(
        "provisa.federation.query_residency.row_materialized_tables_by_name", _tables
    )
    monkeypatch.setattr(rml, "process_row_refresh_events", _refresh)
    jobs: dict[str, Any] = {}
    scheduler = SimpleNamespace(add_job=lambda fn, **kw: jobs.update({kw["id"]: fn}))
    state = SimpleNamespace(tenant_db=object(), federation_engine=object())

    token = set_current_org("acme")
    try:
        wired = await rml.wire_row_materialize_background(
            scheduler, state=state, log=SimpleNamespace(warning=print), reap_grace_period=60.0
        )
    finally:
        reset_current_org(token)

    assert wired == 1
    assert set(jobs) == {
        "row_materialize:refresh:src/public.orders:org_acme",
        "row_materialize:reap:src/public.orders:org_acme",
    }
    await jobs["row_materialize:refresh:src/public.orders:org_acme"]()  # fires unbound
    assert seen == ["acme"]


async def test_row_materialize_wiring_refuses_with_no_org_bound():
    from provisa.events import row_materialize_lifecycle as rml

    with pytest.raises(RuntimeError, match="No active org bound"):
        await rml.wire_row_materialize_background(
            SimpleNamespace(add_job=lambda *a, **k: None),
            state=SimpleNamespace(tenant_db=object(), federation_engine=object()),
            log=SimpleNamespace(warning=print),
            reap_grace_period=60.0,
        )


# --- the live-query engine -----------------------------------------------------------------------


async def test_a_live_poll_runs_as_the_engines_org(monkeypatch):
    from provisa.live.engine import LiveEngine

    engine = LiveEngine(tenant_db=None, engine=None, org_id="root")
    seen: list[str | None] = []

    async def _bound(_query_id: str) -> None:
        seen.append(current_org.get())

    monkeypatch.setattr(engine, "_poll_bound", _bound)
    await engine._poll("q1")
    assert seen == ["root"]
    assert current_org.get() is None


# --- Airport -------------------------------------------------------------------------------------


def _airport(multitenancy: bool):
    from provisa.api.airport.server import ProvisaAirportServer

    server = ProvisaAirportServer.__new__(ProvisaAirportServer)
    server._state = cast("Any", SimpleNamespace(org_id="root", multitenancy=multitenancy))
    return server


def test_a_single_tenant_airport_rpc_is_served_in_the_deployment_org():
    assert _airport(False)._in_serving_org(current_org.get) == "root"
    assert current_org.get() is None


def test_an_airport_rpc_under_multitenancy_is_refused_by_name():
    import pyarrow.flight as flight

    with pytest.raises(flight.FlightServerError, match="names no org under multitenancy"):
        _airport(True)._in_serving_org(current_org.get)


def test_airport_stream_batches_are_pulled_in_the_serving_org():
    from provisa.api.airport.server import _bound_batches

    def _batches():
        yield current_org.get()
        yield current_org.get()

    assert list(_bound_batches("root", _batches())) == ["root", "root"]
    assert current_org.get() is None


async def test_the_live_engine_reconciles_only_from_its_own_orgs_model(monkeypatch):
    """Another org's rebuild must not hand the deployment org's engine its live tables (they would
    be polled in the engine's org and delivered to the other org's outputs) nor drop its jobs."""
    from provisa.api import app_rebuild
    from provisa.live.engine import LiveEngine

    engine = LiveEngine(tenant_db=None, engine=None, org_id="root")
    reconciled: list[str | None] = []

    async def _reconcile(_conn, _engine):
        reconciled.append(current_org.get())

    monkeypatch.setattr("provisa.live.reconcile.reconcile_live_engine", _reconcile)
    monkeypatch.setattr("provisa.api.app.state", SimpleNamespace(live_engine=engine), raising=False)

    for org in ("acme", "root"):
        token = set_current_org(org)
        try:
            await app_rebuild._reconcile_live_engine(cast("Any", None))
        finally:
            reset_current_org(token)
    assert reconciled == ["root"]

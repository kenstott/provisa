# Copyright (c) 2026 Kenneth Stott
# Canary: e122d5f0-ff2a-46b6-a5e6-4bd94f9fbcdb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-941: wire_event_loop — best-effort boot wiring of the event loop onto the scheduler."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from provisa.events.app_wiring import wire_event_loop
from provisa.federation.engine import build_duckdb_engine

_LOG = logging.getLogger("test")


@pytest.fixture(autouse=True)
def replica_runner_wired(monkeypatch):
    """The build runner's registration, recorded: these tests are about the event loop, and the
    runner's own registration is tested in test_replica_builds.py."""
    wired: list = []

    def _wire(scheduler, *, state, platform_url):
        wired.append((scheduler, state))

    monkeypatch.setattr("provisa.federation.replica_builds.wire_replica_runner", _wire)
    monkeypatch.setattr(
        "provisa.core.config_loader.load_control_plane",
        lambda path: SimpleNamespace(resolved_platform_url=lambda: "sqlite+pysqlite://"),
    )
    return wired


@pytest.fixture(autouse=True)
def _patch_fetch_tables(monkeypatch):
    # wire_event_loop drives off the REGISTERED tables (control plane); the fake conn carries them.
    async def _fetch(conn):
        return getattr(conn, "registered", [])

    # REQ-1919: the sources it drives off are the store's rows too, never the config's.
    async def _source_rows(conn):
        return [
            {"id": s.id, "type": s.type.value, "change_signal": s.change_signal, "password_ref": ""}
            for s in getattr(conn, "sources", ())
        ]

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch)
    monkeypatch.setattr("provisa.core.repositories.source.list_all", _source_rows)


class _Sched:
    def __init__(self):
        self.jobs: list[str] = []

    def add_job(self, fn, trigger=None, id="", replace_existing=None):
        self.jobs.append(id)


class _Conn:
    def __init__(self, registered, sources=()):
        self.registered = registered
        self.sources = sources

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def execute_core(self, stmt):
        # REQ-962: wire_event_loop loads the calendar registry from the `calendars` table; these
        # tests declare no calendars, so the query returns an empty set (an empty registry).
        return SimpleNamespace(fetchall=lambda: [])


def _fake_db(registered, sources=()):
    return SimpleNamespace(acquire=lambda: _Conn(registered, sources))


def _rcol(name, dt="bigint", pk=False):
    return {"column_name": name, "data_type": dt, "is_primary_key": pk, "native_filter_type": None}


async def _noop_reconcile(**_kw):
    return "mat.noop"


async def _noop_land(**_kw):
    return "mat.noop"


def _replica_address(*, source_id, schema_name, table_name):
    """As ``EngineRuntime.replica_address``: the one replica address, for the test org."""
    from provisa.federation.replica_address import replica_address

    return replica_address(
        org_id="test", source_id=source_id, schema_name=schema_name, table_name=table_name
    )


def _state(*, ready=True):
    if not ready:
        return SimpleNamespace(model_db=None, tenant_db=None, federation_engine=None, config=None)
    engine = SimpleNamespace(
        engine=build_duckdb_engine(),
        materialize_store_dsn=lambda: "sqlite://",
        reconcile_mv_table=_noop_reconcile,
        land_source_table=_noop_land,
        persist_mv_table=_noop_land,
        replica_address=_replica_address,
    )
    config = SimpleNamespace(
        sources=[
            SimpleNamespace(id="api", type=SimpleNamespace(value="openapi"), change_signal="ttl")
        ],
        tables=[
            SimpleNamespace(
                source_id="api",
                schema_name="default",
                table_name="events",
                change_signal=None,
                watermark_column=None,
                live=None,
                cache_ttl=300,
            )
        ],
    )
    registered = [
        {
            "id": 1,
            "source_id": "api",
            "schema_name": "default",
            "table_name": "events",
            "columns": [_rcol("id", "bigint", pk=True)],
            "dq_contract": None,
            "role_ttl": {},  # REQ-1907
            "pagination": None,  # REQ-318: the table sets no paging
            "replicate": None,
            "load_protected": None,
            "change_signal": None,  # REQ-929: the table sets none
            "region": None,  # REQ-1921: it names no region
            # REQ-1919: the landing settings are the row's, as the seed stored them.
            "cache_ttl": 300,
            "live": None,
            "watermark_column": None,
            "probe_type": None,
        }
    ]
    registry = SimpleNamespace(get_enabled=lambda: [])
    return SimpleNamespace(
        model_db=(_one_db := _fake_db(registered, config.sources)),
        tenant_db=_one_db,
        federation_engine=engine,
        config=config,
        mv_registry=registry,
        # the model a view's inputs resolve against (events.nodes.lineage_graph); no views here
        tables=[],
        source_catalogs={},
        contexts={},
    )


@pytest.fixture(autouse=True)
def _boot_org():
    """The boot wires the event loop with the deployment's org bound (REQ-1266)."""
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org("default")
    yield
    reset_current_org(token)


@pytest.mark.asyncio
async def test_skips_when_prerequisites_missing():
    sched = _Sched()
    n = await wire_event_loop(sched, state=_state(ready=False), log=_LOG)
    assert n == 0 and sched.jobs == []  # no db/engine/config → no-op, boot unharmed


@pytest.mark.asyncio
async def test_the_build_runner_is_registered_with_the_event_loop(replica_runner_wired):
    """REQ-1915: wiring the event loop registers this process's replica build pass for the org,
    and a failure to register it does not stop the event loop from being wired."""
    sched = _Sched()
    state = _state()
    assert await wire_event_loop(sched, state=state, log=logging.getLogger("test")) >= 1
    assert replica_runner_wired == [(sched, state)]


async def test_a_runner_that_cannot_be_registered_leaves_the_event_loop_wired(monkeypatch, caplog):
    def _boom(scheduler, *, state, platform_url):
        raise RuntimeError("no platform control plane")

    monkeypatch.setattr("provisa.federation.replica_builds.wire_replica_runner", _boom)
    with caplog.at_level(logging.ERROR):
        wired = await wire_event_loop(_Sched(), state=_state(), log=logging.getLogger("test"))
    assert wired >= 1
    assert "replica build runner could not be registered" in caplog.text


async def test_registers_source_node_and_runtime_jobs():
    sched = _Sched()
    n = await wire_event_loop(sched, state=_state(), log=_LOG)
    assert n == 1  # the one MATERIALIZED source table (openapi) → a source node
    # the org the boot serves names its jobs (REQ-1266: it is bound, like every other org)
    assert "events:tick:org_default" in sched.jobs and "events:reaper:org_default" in sched.jobs


def _state_with_mv(*, column_types):
    """State with one enabled MV; its engine probe returns typed output columns."""
    from provisa.executor.result import QueryResult

    async def _execute_engine(sql, *a, **k):
        return QueryResult(rows=[], column_names=["d", "n"], column_types=column_types)

    engine = SimpleNamespace(
        engine=build_duckdb_engine(),
        materialize_store_dsn=lambda: "sqlite://",
        execute_engine=_execute_engine,
        address_replicas=lambda sql: sql,  # this stand-in serves no table from a replica
        reconcile_mv_table=_noop_reconcile,
        land_source_table=_noop_land,
        persist_mv_table=_noop_land,
        replica_address=_replica_address,
    )
    mv = SimpleNamespace(
        id="daily",
        target_catalog="store",
        target_schema="analytics",
        target_table="daily",
        sql="SELECT day AS d, count(*) AS n FROM orders GROUP BY day",
        freshness_mode="ttl",
        refresh_interval=600,
        debounce_quiet=0.0,
        debounce_max_delay=None,
        region=None,  # REQ-1921: names no region — built by every region
    )
    return SimpleNamespace(
        model_db=(_one_db := _fake_db([])),
        tenant_db=_one_db,
        federation_engine=engine,
        config=SimpleNamespace(sources=[], tables=[]),
        mv_registry=SimpleNamespace(get_enabled=lambda: [mv], get=lambda _id: None),
        # the model the view's input ``orders`` resolves against (events.nodes.lineage_graph)
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "alias": None,
            }
        ],
        source_catalogs={"pg": "pg"},
        contexts={},
    )


@pytest.mark.asyncio
async def test_mv_columns_introspected_and_node_registered():
    # LIMIT-0 probe yields typed columns → translated native→IR → the MV node registers.
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "bigint"]), log=_LOG
    )
    assert n == 1  # the one MV node


@pytest.mark.asyncio
async def test_mv_with_unmapped_output_type_skipped():
    # an unmapped output type is an IR vocabulary gap → the MV is skipped (not silently defaulted).
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "geometry"]), log=_LOG
    )
    assert n == 0  # no node — its columns did not resolve to IR


@pytest.mark.asyncio
async def test_registered_checker_table_carries_its_contract_to_the_loop(monkeypatch):
    # REQ-1443: a checker table's rows are the results of RUNNING its registered contract, so the
    # table handed to the node binder (and on to make_dq_loader) must carry dq_contract — without
    # it the DQ loader has nothing to run and every poll/forced regen lands nothing.
    seen: dict = {}

    def _capture(**kw):
        seen["tables"] = kw["tables"]
        return []

    monkeypatch.setattr("provisa.events.app_wiring.specs_from_config", _capture)
    st = _state()
    contract = "dataset: provisa/pet_store/pets\nchecks: []\n"
    st.tenant_db = _fake_db(
        [
            {
                "id": 1,
                "source_id": "dq",
                "schema_name": "quality",
                "table_name": "pets_scan",
                "columns": [_rcol("scan_id", "varchar", pk=True)],
                "dq_contract": contract,
                "role_ttl": {},  # REQ-1907
                "pagination": None,  # REQ-318: the table sets no paging
                "replicate": None,
                "load_protected": None,
                "change_signal": None,  # REQ-929: the table sets none
                "region": None,  # REQ-1921: it names no region
                "cache_ttl": None,
                "live": None,
                "watermark_column": None,
                "probe_type": None,
            }
        ]
    )
    st.model_db = st.tenant_db
    await wire_event_loop(_Sched(), state=st, log=_LOG)
    assert [t.dq_contract for t in seen["tables"]] == [contract]


@pytest.mark.asyncio
async def test_never_raises_into_boot():
    # a malformed state (missing attrs) must be swallowed, not propagated
    n = await wire_event_loop(
        _Sched(),
        state=SimpleNamespace(
            model_db=(_one_db := object()),
            tenant_db=_one_db,
            federation_engine=SimpleNamespace(),
            config=SimpleNamespace(),
        ),
        log=_LOG,
    )
    assert n == 0


@pytest.mark.asyncio
async def test_a_view_whose_input_does_not_resolve_fails_wiring_naming_it(caplog):
    """A view that reads a name the model does not hold was refused when it was declared;
    meeting one at wiring is a defect — the wiring raises rather than wiring the view with a
    missing edge, the error is logged naming the view and the reference, and the view is marked
    failed with that reason, so the admin's view list says why it does not refresh."""
    st = _state_with_mv(column_types=["date", "bigint"])
    st.tables = []
    failed: dict[str, str] = {}
    st.mv_registry.mark_refresh_failed = failed.__setitem__
    with caplog.at_level("ERROR"):
        n = await wire_event_loop(_Sched(), state=st, log=_LOG)
    assert n == 0
    said = "not wired into the event loop: materialized view 'daily' reads 'orders'"
    assert failed["daily"].startswith(said)
    assert any(r.levelname == "ERROR" and said in r.getMessage() for r in caplog.records)


def test_an_unresolved_input_is_not_taken_for_a_lineage_cycle():
    """The poll-job wiring rejects a cycle with a warning; an input that does not resolve is
    not a cycle — it raises out of the lineage step with the view marked failed."""
    from provisa.events.app_wiring import _lineage
    from provisa.mv.view_inputs import InputUnresolved

    st = _state_with_mv(column_types=["date", "bigint"])
    st.tables = []
    failed: dict[str, str] = {}
    st.mv_registry.mark_refresh_failed = failed.__setitem__
    with pytest.raises(InputUnresolved):
        _lineage(st.mv_registry.get_enabled(), st, _LOG)
    assert list(failed) == ["daily"]

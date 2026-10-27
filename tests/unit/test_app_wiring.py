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
from tests.helpers import build_through_a_stand_in_pipeline
from tests.role_state import RoleState

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
    return {
        "column_name": name,
        "data_type": dt,
        "is_primary_key": pk,
        "native_filter_type": None,
        "path": None,
        "embedding": False,
        # REQ-1965: the hidden attributes fetch_tables reads with every column.
        "array_depth": 0,
        "element_type": None,
        "native_type": None,
        "native_platform": None,
    }


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
            "landing_filter": None,
            "columns": [_rcol("id", "bigint", pk=True)],
            "dq_contract": None,
            "query_template": None,
            "role_ttl": {},  # REQ-1907
            "pagination": None,  # REQ-318: the table sets no paging
            "replicate": None,
            "load_protected": None,
            "off_peak_window": None,
            "off_peak_tz": None,
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
    return RoleState(
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


async def test_the_views_over_each_table_are_known_before_its_first_build_runs(monkeypatch):
    """The build pass runs as soon as it is registered, and a table that lands once (cache_ttl
    -1) is built by that one pass: the views that read it are published to it first, or its one
    landing told no view and they were never built."""
    known_then: list = []

    def _wire(scheduler, *, state, platform_url):
        known_then.append(state.replica_dependents_of("nothing reads this"))

    monkeypatch.setattr("provisa.federation.replica_builds.wire_replica_runner", _wire)
    await wire_event_loop(_Sched(), state=_state(), log=_LOG)
    assert known_then == [[]]


async def test_a_lineage_cycle_still_registers_the_build_pass(monkeypatch, replica_runner_wired):
    """A cycle among the views skips the event loop; the tables are still built."""

    def _cycle(_graph):
        raise ValueError("MV lineage has a cycle: a -> b -> a")

    monkeypatch.setattr("provisa.events.supervisor.dependents_of", _cycle)
    assert await wire_event_loop(_Sched(), state=_state(), log=_LOG) == 0
    assert len(replica_runner_wired) == 1


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


def _state_with_mv(*, column_types, probed: list | None = None):
    """State with one enabled MV; its engine probe returns typed output columns. ``probed`` gets
    each statement the engine is asked."""
    from provisa.executor.result import QueryResult

    async def _execute_engine(sql, *a, **k):
        if probed is not None:
            probed.append(sql)
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
        product_id=None,  # REQ-2133
        hold_on_failing_checks=False,  # REQ-2133
        table_id=None,  # REQ-2133
        region=None,  # REQ-1921: names no region — built by every region
    )
    return RoleState(
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
                "landing_filter": None,
                "alias": None,
            }
        ],
        source_catalogs={"pg": "pg"},
        contexts={},
    )


@pytest.mark.asyncio
async def test_mv_columns_introspected_and_node_registered(monkeypatch):
    # LIMIT-0 probe yields typed columns → translated native→IR → the MV node registers. The
    # probe reads the statement the view's governed build reads (REQ-1922, #257).
    build_through_a_stand_in_pipeline(monkeypatch, lambda _mv: "SELECT d, n FROM governed")
    probed: list[str] = []
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "bigint"], probed=probed), log=_LOG
    )
    assert n == 1  # the one MV node
    assert "SELECT * FROM (SELECT d, n FROM governed) AS _mv_probe LIMIT 0" in probed


@pytest.mark.asyncio
async def test_mv_with_unmapped_output_type_skipped(monkeypatch):
    build_through_a_stand_in_pipeline(monkeypatch)
    # an unmapped output type is an IR vocabulary gap → the MV is skipped (not silently defaulted).
    n = await wire_event_loop(
        _Sched(), state=_state_with_mv(column_types=["date", "hierarchyid"]), log=_LOG
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
                "landing_filter": None,
                "columns": [_rcol("scan_id", "varchar", pk=True)],
                "dq_contract": contract,
                "query_template": None,
                "role_ttl": {},  # REQ-1907
                "pagination": None,  # REQ-318: the table sets no paging
                "replicate": None,
                "load_protected": None,
                "off_peak_window": None,
                "off_peak_tz": None,
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


def _with_an_unresolved_view(st):
    """``st``'s view ``daily`` beside a view ``stale`` that reads a table the model no longer
    holds -- what a view left registered after its input was deleted looks like."""
    (daily,) = st.mv_registry.get_enabled()
    stale = SimpleNamespace(**{**vars(daily), "id": "stale", "target_table": "stale"})
    stale.sql = "SELECT d FROM gone"
    st.mv_registry = SimpleNamespace(get_enabled=lambda: [daily, stale], get=lambda _id: None)
    failed: dict[str, str] = {}
    st.mv_registry.mark_refresh_failed = failed.__setitem__
    return failed


@pytest.mark.asyncio
async def test_a_view_whose_input_does_not_resolve_is_refused_by_name_and_the_rest_are_wired(
    monkeypatch, caplog
):
    """A view whose input does not resolve is refused alone: it is marked failed with the reason
    (the admin's view list and refresh status say why it does not refresh), the error names the
    view and the reference, and every other view is wired -- one view never fails the org's
    wiring."""
    build_through_a_stand_in_pipeline(monkeypatch, lambda _mv: "SELECT d, n FROM governed")
    st = _state_with_mv(column_types=["date", "bigint"])
    failed = _with_an_unresolved_view(st)
    with caplog.at_level("ERROR"):
        n = await wire_event_loop(_Sched(), state=st, log=_LOG)
    assert n == 1  # daily's node
    said = "not wired into the event loop: materialized view 'stale' reads 'gone'"
    assert list(failed) == ["stale"]
    assert failed["stale"].startswith(said)
    assert any(r.levelname == "ERROR" and said in r.getMessage() for r in caplog.records)


def test_an_unresolved_input_leaves_only_its_own_view_out_of_the_lineage():
    """The poll-job wiring rejects a cycle with a warning; an input that does not resolve is not
    a cycle -- its view is marked failed and left out, and the lineage of the others stands."""
    from provisa.events.app_wiring import _lineage

    st = _state_with_mv(column_types=["date", "bigint"])
    failed = _with_an_unresolved_view(st)
    graph = _lineage(st.mv_registry.get_enabled(), st, _LOG)
    assert graph == {"analytics.daily": {"pg/public.orders"}}
    assert list(failed) == ["stale"]


@pytest.mark.asyncio
async def test_a_sources_own_probe_runs_with_its_orgs_vault_bound(monkeypatch):
    """REQ-1695: the probe dials the source with the source's own credential."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    from provisa.events import app_wiring
    from provisa.federation import source_vault

    order: list[str] = []

    @asynccontextmanager
    async def vault(state, sources):
        order.append(f"bound:{len(sources)}")
        yield
        order.append("released")

    async def token() -> str:
        order.append("asked")
        return "t"

    def loader(*_a):
        return []

    loader.freshness_probe = lambda source, table: token if table.table_name == "messages" else None
    monkeypatch.setattr(source_vault, "org_vault", vault)
    source = SimpleNamespace(id="mail", type="google_workspace")
    source_probe = app_wiring.make_source_probe(
        SimpleNamespace(), {"google_workspace": loader}, [source]
    )
    probe = source_probe(source, SimpleNamespace(table_name="messages"))
    assert await probe() == "t"
    assert order == ["bound:1", "asked", "released"]
    assert source_probe(source, SimpleNamespace(table_name="events")) is None


def _state_with_a_view_over_a_view(built: set[str], probed: list[str]):
    """``margins`` reads the table ``orders``; ``close`` reads the view ``margins``. A probe of a
    view's build reads the views it reads from their store, so ``close``'s fails until ``margins``
    has been built (``built``)."""
    from provisa.core.models import DERIVED_SOURCE_ID
    from provisa.executor.result import QueryResult

    async def _execute_engine(sql, *a, **k):
        probed.append(sql)
        if "close" in sql and "margins" not in built:
            raise RuntimeError("schema org_x_mv_cache does not exist")
        return QueryResult(rows=[], column_names=["d", "n"], column_types=["date", "bigint"])

    engine = SimpleNamespace(
        engine=build_duckdb_engine(),
        materialize_store_dsn=lambda: "sqlite://",
        execute_engine=_execute_engine,
        address_replicas=lambda sql: sql,
        reconcile_mv_table=_noop_reconcile,
        land_source_table=_noop_land,
        persist_mv_table=_noop_land,
        replica_address=_replica_address,
    )

    def _mv(name: str, sql: str) -> SimpleNamespace:
        return SimpleNamespace(
            id=f"view-view_{name}",
            target_catalog="store",
            target_schema="analytics",
            target_table=f"mv_view_{name}",
            sql=sql,
            freshness_mode="ttl",
            refresh_interval=600,
            debounce_quiet=0.0,
            debounce_max_delay=None,
            product_id=None,  # REQ-2133
            hold_on_failing_checks=False,  # REQ-2133
            table_id=None,  # REQ-2133
            region=None,
            last_refresh_at=None,
        )

    margins = _mv("margins", "SELECT day AS d, count(*) AS n FROM orders GROUP BY day")
    close = _mv("close", "SELECT d, n FROM view_margins")
    views = {mv.id: mv for mv in (margins, close)}
    row = {"domain_id": "sales", "landing_filter": None, "alias": None}
    return RoleState(
        model_db=(_one_db := _fake_db([])),
        tenant_db=_one_db,
        federation_engine=engine,
        config=SimpleNamespace(sources=[], tables=[]),
        mv_registry=SimpleNamespace(
            get_enabled=lambda: [margins, close],
            get=views.get,
            mark_refreshed=lambda mv_id, *_a: setattr(views[mv_id], "last_refresh_at", 1.0),
        ),
        tables=[
            {**row, "id": 1, "source_id": "pg", "schema_name": "public", "table_name": "orders"},
            {
                **row,
                "id": 2,
                "source_id": DERIVED_SOURCE_ID,
                "schema_name": "sales",
                "table_name": "view_margins",
                "view_sql": margins.sql,
            },
        ],
        source_catalogs={"pg": "pg"},
        contexts={},
    ), margins


class _RunnableSched(_Sched):
    def __init__(self):
        super().__init__()
        self.fns: dict[str, object] = {}

    def add_job(self, fn, trigger=None, id="", replace_existing=None):
        super().add_job(fn, trigger, id, replace_existing)
        self.fns[id] = fn


@pytest.mark.asyncio
async def test_a_view_over_a_view_not_yet_built_is_wired_and_built_once_its_input_lands(
    monkeypatch,
):
    """A view whose input view is not built yet is skipped at boot -- its build reads nothing yet
    -- and is wired onto the loop when that input's first build lands, then built as that input's
    change: never left off the loop for the life of the process."""
    build_through_a_stand_in_pipeline(monkeypatch, lambda mv: f"SELECT d, n FROM {mv.id}")
    monkeypatch.setattr("provisa.mv.coordination.ensure_mv_row", _noop_land_any)
    monkeypatch.setattr("provisa.mv.coordination.record_build", _noop_land_any)
    marked: list = []
    real_specs = __import__("provisa.events.app_wiring", fromlist=["x"]).specs_from_config

    def _specs(**kw):
        marked.append(kw["mv_refreshed"])
        return real_specs(**kw)

    monkeypatch.setattr("provisa.events.app_wiring.specs_from_config", _specs)
    posted: list = []

    async def _post_event(conn, *, source_table, event_type, payload):
        posted.append((source_table, event_type))
        return 7

    async def _fan_out(conn, event_id, dependents):
        posted.append((event_id, list(dependents)))
        return len(dependents)

    monkeypatch.setattr("provisa.events.queue.post_event", _post_event)
    monkeypatch.setattr("provisa.events.queue.fan_out", _fan_out)
    built: set[str] = set()
    state, margins = _state_with_a_view_over_a_view(built, probed := [])
    sched = _RunnableSched()

    assert await wire_event_loop(sched, state=state, log=_LOG) == 1  # margins; close skipped
    assert state.views_awaiting_input == {"analytics.mv_view_close": {"analytics.mv_view_margins"}}

    built.add("margins")
    await marked[0](margins)(("replace", {"rows": 3}))  # margins' first build lands
    await sched.fns["events:wire-awaiting:org_default"]()

    assert {p.node for p in state.event_loop_processors} == {
        "analytics.mv_view_margins",
        "analytics.mv_view_close",
    }
    assert state.views_awaiting_input == {}
    assert posted == [("analytics.mv_view_margins", "replace"), (7, ["analytics.mv_view_close"])]


@pytest.mark.asyncio
async def test_a_view_over_an_unbuilt_view_is_not_probed_and_logs_no_error(monkeypatch, caplog):
    """A view whose input view has never been built is not probed at boot: its build would read a
    store table that does not exist yet. It goes straight to awaiting that input, with no query
    and no error logged, and is probed once the input's first build lands."""
    build_through_a_stand_in_pipeline(monkeypatch, lambda mv: f"SELECT d, n FROM {mv.id}")
    monkeypatch.setattr("provisa.mv.coordination.ensure_mv_row", _noop_land_any)
    monkeypatch.setattr("provisa.mv.coordination.record_build", _noop_land_any)
    marked: list = []
    real_specs = __import__("provisa.events.app_wiring", fromlist=["x"]).specs_from_config

    def _specs(**kw):
        marked.append(kw["mv_refreshed"])
        return real_specs(**kw)

    monkeypatch.setattr("provisa.events.app_wiring.specs_from_config", _specs)

    async def _post_event(conn, *, source_table, event_type, payload):
        return 7

    async def _fan_out(conn, event_id, dependents):
        return len(dependents)

    monkeypatch.setattr("provisa.events.queue.post_event", _post_event)
    monkeypatch.setattr("provisa.events.queue.fan_out", _fan_out)
    built: set[str] = set()
    probed: list[str] = []
    state, margins = _state_with_a_view_over_a_view(built, probed)
    sched = _RunnableSched()

    with caplog.at_level(logging.INFO):
        await wire_event_loop(sched, state=state, log=_LOG)

    assert not [sql for sql in probed if "close" in sql]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert state.views_awaiting_input == {"analytics.mv_view_close": {"analytics.mv_view_margins"}}

    built.add("margins")
    await marked[0](margins)(("replace", {"rows": 3}))  # margins' first build lands
    await sched.fns["events:wire-awaiting:org_default"]()

    assert [sql for sql in probed if "close" in sql]
    assert "analytics.mv_view_close" in {p.node for p in state.event_loop_processors}


async def _noop_land_any(*_a, **_kw):
    return None


@pytest.mark.asyncio
async def test_a_view_over_a_replica_not_yet_landed_is_wired_and_built_once_it_lands(monkeypatch):
    """REQ-2068: a demo template's views read replicas that land after its loop is wired. A view
    whose input replica has not landed is skipped -- its build reads nothing yet -- and is wired
    when that replica's build lands (replica_builds.run_build tells ``state.on_input_landed``),
    then built as that replica's change: never left off the loop, its region's seed never
    written."""
    from provisa.executor.result import QueryResult

    build_through_a_stand_in_pipeline(monkeypatch, lambda mv: f"SELECT d, n FROM {mv.id}")
    posted: list = []

    async def _post_event(conn, *, source_table, event_type, payload):
        posted.append((source_table, event_type))
        return 7

    async def _fan_out(conn, event_id, dependents):
        posted.append((event_id, list(dependents)))
        return len(dependents)

    monkeypatch.setattr("provisa.events.queue.post_event", _post_event)
    monkeypatch.setattr("provisa.events.queue.fan_out", _fan_out)
    landed: set[str] = set()
    state, _margins = _state_with_a_view_over_a_view(set(), [])

    async def _execute_engine(sql, *a, **k):
        if "orders" not in landed:
            raise RuntimeError("eu-shop.public.orders has not landed yet")
        return QueryResult(rows=[], column_names=["d", "n"], column_types=["date", "bigint"])

    state.federation_engine.execute_engine = _execute_engine
    sched = _RunnableSched()

    await wire_event_loop(sched, state=state, log=_LOG)
    orders = "pg/public.orders"
    assert orders in state.views_awaiting_input["analytics.mv_view_margins"]

    landed.add("orders")
    state.on_input_landed(orders)  # the replica's first build lands
    await sched.fns["events:wire-awaiting:org_default"]()

    assert "analytics.mv_view_margins" in {p.node for p in state.event_loop_processors}
    assert (orders, "replace") in posted and (7, ["analytics.mv_view_margins"]) in posted


@pytest.mark.asyncio
async def test_two_inputs_landing_before_the_rewire_runs_each_give_their_views_a_first_build(
    monkeypatch,
):
    """Two inputs land before the one rewire job (``events:wire-awaiting``) has run: the job
    the second landing schedules replaces the first's, and must still give the first landing's
    views their first build, not only the second's."""
    from provisa.executor.result import QueryResult

    build_through_a_stand_in_pipeline(monkeypatch, lambda mv: f"SELECT d, n FROM {mv.id}")
    posted: list = []

    async def _post_event(conn, *, source_table, event_type, payload):
        posted.append((source_table, event_type))
        return len(posted)

    async def _fan_out(conn, event_id, dependents):
        posted.append((event_id, list(dependents)))
        return len(dependents)

    monkeypatch.setattr("provisa.events.queue.post_event", _post_event)
    monkeypatch.setattr("provisa.events.queue.fan_out", _fan_out)
    landed: set[str] = set()
    built: set[str] = set()
    state, _margins = _state_with_a_view_over_a_view(built, [])

    async def _execute_engine(sql, *a, **k):
        if "orders" not in landed or ("close" in sql and "margins" not in built):
            raise RuntimeError("an input has not landed yet")
        return QueryResult(rows=[], column_names=["d", "n"], column_types=["date", "bigint"])

    state.federation_engine.execute_engine = _execute_engine
    sched = _RunnableSched()
    await wire_event_loop(sched, state=state, log=_LOG)
    orders = "pg/public.orders"
    assert state.views_awaiting_input == {
        "analytics.mv_view_margins": {orders},
        "analytics.mv_view_close": {"analytics.mv_view_margins"},
    }

    landed.add("orders")
    state.on_input_landed(orders)  # the replica lands: margins is to be wired and built
    built.add("margins")
    state.on_input_landed("analytics.mv_view_margins")  # ... and close, before that job ran
    await sched.fns["events:wire-awaiting:org_default"]()

    fanned = {node for entry in posted if isinstance(entry[1], list) for node in entry[1]}
    assert "analytics.mv_view_margins" in {p.node for p in state.event_loop_processors}
    assert "analytics.mv_view_margins" in fanned, posted  # wired, and given its first build

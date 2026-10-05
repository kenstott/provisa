# Copyright (c) 2026 Kenneth Stott
# Canary: e636ea5a-9397-4497-bbae-3dd105432540
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-990 PIPELINE_LAND for Kafka on a SingleStore store: the table lands through a continuous
pipeline the store runs (starting at the latest offsets, as the relay does), an unchanged one keeps
running, a changed definition replaces it, a table no longer wired loses it, only this org and
region's Kafka pipelines are ever swept, and each new successful batch ripples the table's change
and freshness through the one ripple helper."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine

from provisa.federation import singlestore_kafka as sk
from provisa.federation import singlestore_pipeline as sp

_SCHEMA = "org_a_replicas"
_TABLE = "kafka_src__s__orders"
_COLUMNS = [("id", "bigint"), ("amount", "double")]
_ORIGIN = sp.LandOrigin("kafka", "orders", "json")


class _Error(Exception):
    def __init__(self, errno: int) -> None:
        super().__init__(f"error {errno}")
        self.errno = errno


class _Raw:
    """The singlestoredb connection: records statements, answers the pipeline listing."""

    def __init__(self, running: dict[str, str], *, start_errno: int | None = None) -> None:
        self.running = dict(running)
        self.statements: list[str] = []
        self._start_errno = start_errno
        self._rows: list[tuple] = []

    def cursor(self) -> _Raw:
        return self

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if sql.startswith("SELECT PIPELINE_NAME, STATE"):
            self._rows = list(self.running.items())
        elif sql.startswith("START PIPELINE") and self._start_errno is not None:
            raise _Error(self._start_errno)
        elif sql.startswith("DROP PIPELINE IF EXISTS"):
            name = sql.rsplit(".", 1)[1].strip("`")
            self.running.pop(name, None)

    def fetchall(self) -> list[tuple]:
        return self._rows

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass


class _Store:
    """A SQLAlchemy engine whose connections hand out ``raw`` (the real singlestoredb dialect)."""

    def __init__(self, raw: _Raw) -> None:
        self.dialect = create_engine("singlestoredb://u:p@h:1/d").dialect
        self._raw = raw

    @contextmanager
    def connect(self):
        conn = SimpleNamespace(
            connection=SimpleNamespace(driver_connection=self._raw), commit=lambda: None
        )
        yield conn


@pytest.fixture(autouse=True)
def _no_table_ddl(monkeypatch):
    # The keyed table is created by the same helpers the engine's own land uses.
    monkeypatch.setattr("provisa.federation.sqlalchemy_runtime._ensure_schema", lambda c, s: None)
    monkeypatch.setattr("provisa.federation.sqlalchemy_runtime._ensure_table", lambda c, t: "kept")


def _ensure(raw: _Raw, **overrides) -> str:
    args = dict(
        schema=_SCHEMA,
        table=_TABLE,
        columns=_COLUMNS,
        pk_columns=["id"],
        origin=_ORIGIN,
        bootstrap="broker:9092",
    )
    args.update(overrides)
    return sk.ensure(_Store(raw), **args)


def _expected_name(**overrides) -> str:
    parts = dict(
        origin=_ORIGIN,
        columns=_COLUMNS,
        pk_columns=["id"],
        field_mapping=None,
        schema_registry=None,
        bootstrap="broker:9092",
    )
    parts.update(overrides)
    return sp.kafka_pipeline_name(_SCHEMA, _TABLE, sp.kafka_definition(**parts))


def test_a_new_table_gets_a_pipeline_that_starts_at_the_latest_offsets():
    raw = _Raw({})
    name = _ensure(raw)

    assert name == _expected_name() and name.startswith(sp.KAFKA_PIPELINE_PREFIX)
    ddl = [s for s in raw.statements if not s.startswith("SELECT")]
    assert ddl[0].startswith(f"CREATE OR REPLACE PROCEDURE `{_SCHEMA}`.`provisa_kq_")
    assert ddl[1].startswith(f"CREATE PIPELINE `{_SCHEMA}`.`{name}` AS LOAD DATA KAFKA")
    assert ddl[2:] == [sp.offsets_latest(_SCHEMA, name), sp.start(_SCHEMA, name)]


def test_an_unchanged_table_keeps_its_running_pipeline_and_its_offsets():
    name = _expected_name()
    raw = _Raw({name: "Running"}, start_errno=sp.ALREADY_RUNNING)
    assert _ensure(raw) == name
    ddl = [s for s in raw.statements if not s.startswith("SELECT")]
    assert ddl == [sp.start(_SCHEMA, name)]  # "already running" is passed over


def test_a_changed_definition_replaces_the_pipeline():
    old = _expected_name(columns=[("id", "bigint")])
    raw = _Raw({old: "Running"})
    new = _ensure(raw)
    assert new != old
    assert sp.teardown(_SCHEMA, old, procedure=sp.kafka_procedure_for(old))[0] in raw.statements
    assert any(s.startswith(f"CREATE PIPELINE `{_SCHEMA}`.`{new}`") for s in raw.statements)


def test_any_other_start_error_raises():
    name = _expected_name()
    raw = _Raw({name: "Error"}, start_errno=1064)
    with pytest.raises(_Error):
        _ensure(raw)


def test_the_sweep_drops_only_unwired_kafka_pipelines_of_this_schema():
    kept, failed, gone = "provisa_kp_aaaa_11111111", "provisa_kp_bbbb_22222222", "provisa_kp_cccc_3"
    raw = _Raw({kept: "Running", failed: "Running", gone: "Running"})
    dropped = sk.sweep(_Store(raw), schema=_SCHEMA, keep={kept}, keep_prefixes={"provisa_kp_bbbb_"})
    assert dropped == [gone]
    listing = raw.statements[0]
    assert f"DATABASE_NAME = '{_SCHEMA}'" in listing  # this org and region's schema only
    assert "LIKE 'provisa\\\\_kp\\\\_%'" in listing  # Kafka pipelines only, never a file build's


def test_new_batches_sums_changed_rows_per_pipeline():
    raw = _Raw({})
    raw._rows = [("p1", 5, 2, 1, 0), ("p1", 6, 0, 0, 1), ("p2", 9, 0, 0, 0)]
    raw.execute = lambda sql: raw.statements.append(sql)  # type: ignore[method-assign]
    found = sk.new_batches(_Store(raw), schema=_SCHEMA, since={"p1": 4, "p2": 8})
    assert found == {"p1": (6, 4), "p2": (9, 0)}
    assert "BATCH_STATE = 'Succeeded'" in raw.statements[0]


@asynccontextmanager
async def _queue_db(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_org import event_status, events, node_freshness_state

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'q.db'}")
    with engine.begin() as c:
        events.metadata.create_all(c, tables=[events, event_status, node_freshness_state])
    try:
        yield Database(engine, name="q")
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_the_batch_poll_ripples_a_changed_table_once_and_remembers_the_batch(
    tmp_path, monkeypatch
):
    from provisa.events import queue
    from provisa.events.processor import SourceTableProcessor

    node, view = "kafka_src/s.orders", "view-orders_by_day"
    async with _queue_db(tmp_path) as db:
        processor = SourceTableProcessor(
            node,
            change_signal="kafka",
            watermark_column=None,
            dependents_of=lambda n: [view] if n == node else [],
            db=db,
            name="box-1",
            land=lambda *a, **k: None,
        )
        state = SimpleNamespace(
            tenant_db=db, event_loop_processors=[processor], kafka_pipelines={node: "p1"}
        )
        batches = [{"p1": (6, 3)}, {}]
        seen_since: list[dict] = []

        def _new_batches(store, *, schema, since):
            seen_since.append(dict(since))
            return batches.pop(0)

        monkeypatch.setattr(sk, "new_batches", _new_batches)
        assert await sk.ripple_new_batches(state, object(), schema=_SCHEMA) == 1
        assert await sk.ripple_new_batches(state, object(), schema=_SCHEMA) == 0

        async with db.acquire() as conn:
            pending = await queue.peek_pending(conn, dependent_table=view)
            posted = await queue.get_events(conn, [p["event_id"] for p in pending])
            node_state = await queue.get_node_state(conn, node)
    assert seen_since == [{"p1": -1}, {"p1": 6}]  # the cursor is the node's own state
    assert [(e["source_table"], e["event_type"]) for e in posted] == [(node, "delta")]
    assert posted[0]["payload"] == {"pipeline": "p1", "batch": 6, "rows": 3}
    assert node_state["last_refresh_ok"] and node_state["probe_token"] == "6"


def test_a_kafka_table_on_a_singlestore_store_is_wired_to_a_pipeline_not_a_listener(monkeypatch):
    from provisa.core.models import Source, SourceType
    from provisa.events import push_wiring
    from provisa.federation.engine import FederationEngine, build_sqlalchemy_engine

    monkeypatch.setattr(
        FederationEngine, "materialize_store", lambda self: "singlestoredb://u:p@h:3306/db"
    )
    bare = build_sqlalchemy_engine("singlestoredb://u:p@h:3306/db")
    engine = SimpleNamespace(
        engine=bare,
        replica_address=lambda **k: SimpleNamespace(schema=_SCHEMA, table=_TABLE),
        materialize_store_dsn=lambda: "singlestoredb://u:p@h:3306/db",
    )
    src = Source(id="kafka_src", type=SourceType.kafka, host="broker", port=9092)
    json_table = {"schema_name": "s", "table_name": "orders", "live": {"kafka": {"topic": "o"}}}
    protobuf = {**json_table, "live": {"kafka": {"topic": "o", "format": "protobuf"}}}
    assert push_wiring._lands_by_pipeline(engine, src, json_table)
    assert not push_wiring._lands_by_pipeline(engine, src, protobuf)  # keeps the relay

    ensured: list[dict] = []

    def _ensure_fake(store, **k):
        ensured.append(k)
        definition = sp.kafka_definition(
            k["origin"],
            k["columns"],
            k["pk_columns"],
            k["field_mapping"],
            k["schema_registry"],
            k["bootstrap"],
        )
        return sp.kafka_pipeline_name(k["schema"], k["table"], definition)

    monkeypatch.setattr(sk, "ensure", _ensure_fake)
    state = SimpleNamespace(kafka_pipeline_store=object())
    log = SimpleNamespace(info=lambda *a, **k: None, exception=lambda *a, **k: None)
    for _ in range(2):
        asyncio.run(
            push_wiring._wire_pipeline(
                state,
                engine,
                src,
                json_table,
                node="kafka_src/s.orders",
                columns=_COLUMNS,
                pk_columns=["id"],
                log=log,
            )
        )
    assert len(ensured) == 1, "an unchanged, recorded pipeline is not re-ensured"
    assert ensured[0]["bootstrap"] == "broker:9092" and ensured[0]["origin"].format == "json"


def test_settling_forgets_unwired_tables_and_sweeps_this_org_schema(monkeypatch):
    from provisa.events import push_wiring
    from provisa.federation.engine import build_sqlalchemy_engine

    engine = SimpleNamespace(engine=build_sqlalchemy_engine("singlestoredb://u:p@h:3306/db"))
    swept: list[dict] = []
    monkeypatch.setattr(sk, "sweep", lambda store, **k: swept.append(k) or ["provisa_kp_gone_1"])
    monkeypatch.setattr(
        "provisa.federation.replica_address.replica_schema", lambda org, region=None: _SCHEMA
    )
    state = SimpleNamespace(
        org_id="org_a",
        kafka_pipeline_store=object(),
        kafka_pipelines={"a": "provisa_kp_a_1", "b": None, "gone": "provisa_kp_gone_1"},
        kafka_pipeline_delays={"a": 5.0, "b": 2.0, "gone": 1.0},
        kafka_pipeline_tables={"a": (_SCHEMA, "ta"), "b": (_SCHEMA, "tb"), "gone": (_SCHEMA, "tg")},
    )
    log = SimpleNamespace(info=lambda *a, **k: None)
    from provisa.core.request_context import reset_current_org, set_current_org

    tok = set_current_org("org_a")  # wiring runs with its org bound (REQ-1935)
    try:
        asyncio.run(push_wiring._settle_pipelines(state, engine, {"a", "b"}, log=log))
    finally:
        reset_current_org(tok)

    assert set(state.kafka_pipelines) == {"a", "b"}
    assert swept == [
        {
            "schema": _SCHEMA,
            "keep": {"provisa_kp_a_1"},
            "keep_prefixes": {sp.kafka_table_prefix(_SCHEMA, "tb")},
        }
    ]


@pytest.mark.unbound
def test_settling_with_no_org_bound_is_refused_by_name(monkeypatch):
    from provisa.events import push_wiring
    from provisa.federation.engine import build_sqlalchemy_engine

    engine = SimpleNamespace(engine=build_sqlalchemy_engine("singlestoredb://u:p@h:3306/db"))
    state = SimpleNamespace(kafka_pipeline_store=object(), kafka_pipelines={})
    with pytest.raises(RuntimeError, match="No active org bound"):
        asyncio.run(
            push_wiring._settle_pipelines(
                state, engine, set(), log=SimpleNamespace(info=lambda *a, **k: None)
            )
        )


@pytest.mark.unbound
def test_the_scheduled_batch_poll_binds_the_org_it_was_wired_for(monkeypatch):
    from provisa.core.request_context import current_org
    from provisa.events import push_wiring

    jobs: dict = {}
    scheduler = SimpleNamespace(
        add_job=lambda fn, trigger, id, replace_existing: jobs.update({id: (fn, trigger)}),
        get_job=lambda id: jobs.get(id),
        remove_job=lambda id: jobs.pop(id),
    )
    seen: list = []

    async def _ripple(state, store, *, schema):
        seen.append((current_org.get(None), schema))
        return 0

    monkeypatch.setattr(sk, "ripple_new_batches", _ripple)
    state = SimpleNamespace(_scheduler=scheduler)
    push_wiring._schedule_batch_poll(state, object(), _SCHEMA, {"n": 2.0}, org_id="acme")

    fire, trigger = jobs["poll:singlestore-kafka-pipelines:org_acme"]
    assert trigger.interval.total_seconds() == 2.0
    assert current_org.get(None) is None  # the fire runs outside any request
    asyncio.run(fire())
    assert seen == [("acme", _SCHEMA)]
    assert current_org.get(None) is None  # and leaves nothing bound behind

    push_wiring._schedule_batch_poll(state, object(), _SCHEMA, {}, org_id="acme")
    assert jobs == {}  # no pipeline left: the poll is removed

# Copyright (c) 2026 Kenneth Stott
# Canary: 2e8b6d41-7c3f-4a95-b1d6-9f0e4a27c5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1661: the query path lands a MATERIALIZED source it reads when that source has never
landed or has gone stale, through the engine's own materialize_pending, and stamps the node
freshness state the event loop reads."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.query_residency import ensure_resident, is_stale_of, stale_sources

pytestmark = pytest.mark.unit


def _source(sid, **kw):
    base = dict(
        id=sid,
        type=SimpleNamespace(value="sqlite"),
        change_signal="ttl",
        cache_ttl=None,
        freshness_gate=False,
        prefer_materialized=False,
        load_protected=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _table(sid, name, schema="pet_store", row_materialize=False, columns=None, cache_ttl=300):
    return SimpleNamespace(
        source_id=sid,
        schema_name=schema,
        table_name=name,
        row_materialize=row_materialize,
        columns=columns or [],
        cache_ttl=cache_ttl,
    )


def test_stale_sources_reports_never_landed_and_failed_lands():
    sources = [_source("a"), _source("b"), _source("c")]
    tables = {"a": [_table("a", "pets"), _table("a", "vets")], "b": [_table("b", "x")], "c": []}
    states = {
        "pet_store.pets": {"last_refresh_at": 100.0, "last_refresh_ok": True},
        "pet_store.vets": {"last_refresh_at": 50.0, "last_refresh_ok": False},
        "pet_store.x": None,
    }
    stamps, oks = stale_sources(sources, tables, states)
    assert stamps == {"a": 50.0, "b": None, "c": None}
    assert oks == {"a": False, "b": True, "c": True}


def test_is_stale_of_honours_never_landed_failure_and_ttl():
    sources = [_source("fresh"), _source("ttl", cache_ttl=60), _source("bad"), _source("never")]
    stamps = {"fresh": 1000.0, "ttl": 900.0, "bad": 1000.0, "never": None}
    oks = {"fresh": True, "ttl": True, "bad": False, "never": True}
    is_stale = is_stale_of(sources, stamps, oks, now=1000.0)
    assert not is_stale("fresh")
    assert is_stale("ttl")  # 100s old against a 60s ttl
    assert is_stale("bad")
    assert is_stale("never")


class _Conn:
    def __init__(self, states, recorded):
        self._states = states
        self._recorded = recorded


class _Db:
    def __init__(self, states):
        self.states = states
        self.recorded: list[tuple[str, bool]] = []

    def acquire(self):
        db = self

        class _Ctx:
            async def __aenter__(self):
                return _Conn(db.states, db.recorded)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Backend:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail
        self.dialect = "postgres"
        self._landed_this_process: set[str] = set()

    def require_reconciled(self, source_ids) -> None:
        """As EngineBackend.require_reconciled: every replica here reconciled."""
        del source_ids

    def is_first_touch(self, source_id: str) -> bool:
        return source_id not in self._landed_this_process

    def mark_landed(self, source_id: str) -> None:
        self._landed_this_process.add(source_id)

    async def materialize_pending(self, state, *, loader, is_stale, source_ids, **kw):
        self.calls.append((set(source_ids), kw["now"]))
        if self.fail:
            raise RuntimeError("adapter down")
        landed = []
        for sid in source_ids:
            if is_stale(sid):
                landed += [
                    (sid, t.table_name)
                    for t in state.config.tables
                    if t.source_id == sid and not getattr(t, "row_materialize", False)
                ]
        return landed

    def landing_target(self, *, store_schema, source_id, source_type, schema_name, table_name):
        # REQ-1730: a stand-in for the default (catalog_qualified=True) fold — the registered
        # address, unchanged, matching this fake's own snowflake native_store.
        del store_schema, source_id, source_type
        return schema_name, table_name


def _state(sources, tables, backend):
    engine = SimpleNamespace(
        engine=SimpleNamespace(
            backend=backend,
            native_store="snowflake",
            materialize_store=lambda: "postgresql://localhost/materialize",
        )
    )
    return SimpleNamespace(
        federation_engine=engine,
        config=SimpleNamespace(sources=sources, tables=tables),
        tenant_db=_Db({}),
    )


@pytest.fixture
def wiring(monkeypatch):
    async def get_node_state(conn, node):
        return conn._states.get(node)

    async def record_refresh(conn, node, *, at, ok):
        conn._recorded.append((node, ok))

    monkeypatch.setattr("provisa.events.queue.get_node_state", get_node_state)
    monkeypatch.setattr("provisa.events.queue.record_refresh", record_refresh)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda state, engine: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda state, engine=None: {}
    )
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: object(),
    )
    monkeypatch.setattr("provisa.events.land_lock._locks", {})
    # REQ-1674: ensure_resident reads the registry view; here the config IS the registry
    # (these tests are about residency, not about where the rows come from).

    async def _sources(state, conn=None):
        return list(state.config.sources)

    async def _tables(state, conn=None):
        return list(state.config.tables)

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)


@pytest.mark.asyncio
async def test_a_never_landed_source_is_landed_and_stamped(wiring):
    backend = _Backend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    landed = await ensure_resident(state, {"pets-db"})
    assert landed == [("pets-db", "pets")]
    assert backend.calls and backend.calls[0][0] == {"pets-db"}
    assert state.tenant_db.recorded == [("pet_store.pets", True)]


@pytest.mark.asyncio
async def test_row_materialize_table_never_swept_into_the_whole_source_land(wiring):
    """REQ-1865: a row_materialize=True table's residency is governed exclusively by
    ensure_rows_resident (called alongside this function at every real call site) -- it must
    never also be landed here. Confirmed live: this whole-source sweep landing a row_materialize
    table before ensure_rows_resident got a chance to help negated the entire point of the row
    cache (a keyed lookup paid the same full-table land cost row_materialize exists to avoid)."""
    backend = _Backend()
    state = _state(
        [_source("bench-neo4j")],
        [
            _table("bench-neo4j", "bench_order_node", schema="neo4j", row_materialize=True),
            _table("bench-neo4j", "bench_placed_edge", schema="neo4j"),
        ],
        backend,
    )
    landed = await ensure_resident(state, {"bench-neo4j"})
    assert landed == [("bench-neo4j", "bench_placed_edge")]
    assert ("bench-neo4j", "bench_order_node") not in landed


@pytest.mark.asyncio
async def test_row_materialize_table_named_unbound_gets_whole_table_land(wiring, monkeypatch):
    """REQ-1865: unlike the sibling-collateral case above, a row_materialize table THIS query's
    own SQL names directly (``unbound_targets``, e.g. ``neo4j_materialize_cold``'s unfiltered
    ``SELECT count(*) FROM bench_order_node``) has no PK bound for ensure_rows_resident to key
    off and no sibling table to collaterally starve it -- it must get the whole-table fallback
    REQ-1865 documents, or its row-cache table never gets created at all. Confirmed live: this
    exact query hit "relation ... does not exist" on a fresh boot because neither this function
    nor materialize_pending (REQ-1865's OWN blanket row_materialize exclusion) ever landed it."""
    landed_calls = []

    async def fake_ensure(engine, backend, state, schema, name, columns):
        return SimpleNamespace(schema=schema, name=name)

    async def fake_land(
        engine, backend, state, schema, name, cache_table, pk_columns, columns, rows, ttl
    ):
        landed_calls.append((schema, name, pk_columns, rows, ttl))

    class _Loader:
        async def load(self, source, table):
            return [{"order_id": 1}, {"order_id": 2}]

    monkeypatch.setattr("provisa.federation.query_residency._ensure_row_cache_table", fake_ensure)
    monkeypatch.setattr("provisa.federation.query_residency._land_row_cache", fake_land)
    monkeypatch.setattr(
        "provisa.federation.query_residency.resolve_landing_args_for",
        lambda source, table, dialect: SimpleNamespace(columns=[("order_id", "integer")]),
    )
    monkeypatch.setattr(
        "provisa.events.source_loader.SourceRowLoader",
        lambda engine, adapter_loaders=None, keyed_adapter_loaders=None: _Loader(),
    )
    backend = _Backend()
    state = _state(
        [_source("bench-neo4j")],
        [
            _table(
                "bench-neo4j",
                "bench_order_node",
                schema="neo4j",
                row_materialize=True,
                columns=[SimpleNamespace(name="order_id", is_primary_key=True)],
            ),
        ],
        backend,
    )
    landed = await ensure_resident(state, {"bench-neo4j"}, unbound_targets={"bench_order_node"})
    assert landed == [("bench-neo4j", "bench_order_node")]
    assert landed_calls == [
        ("neo4j", "bench_order_node", ["order_id"], [{"order_id": 1}, {"order_id": 2}], 300)
    ]


@pytest.mark.asyncio
async def test_a_resident_source_is_left_alone(wiring):
    backend = _Backend()
    # REQ-1730: staleness is the persisted stamp OR this backend INSTANCE's own first-touch
    # signal (a genuine reboot reads as stale even with a fresh stamp, since THIS engine has
    # never held the row) — "already resident" here means both: a fresh stamp AND this process
    # has already landed it once.
    backend.mark_landed("pets-db")
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    state.tenant_db.states["pet_store.pets"] = {"last_refresh_at": 1.0, "last_refresh_ok": True}
    assert await ensure_resident(state, {"pets-db"}) == []
    assert state.tenant_db.recorded == []


@pytest.mark.asyncio
async def test_only_the_sources_the_plan_names_are_considered(wiring):
    backend = _Backend()
    state = _state(
        [_source("a"), _source("b")], [_table("a", "pets"), _table("b", "vets")], backend
    )
    assert await ensure_resident(state, {"b"}) == [("b", "vets")]
    assert await ensure_resident(state, set()) == []
    assert await ensure_resident(state, {"unknown"}) == []


@pytest.mark.asyncio
async def test_a_failed_land_is_stamped_not_ok_and_fails_the_read(wiring):
    """REQ-1661 (amended 2026-09-30): a failed land raises its own cause -- the query never reads
    the stale replica. The node is still stamped not ok, so the next query retries the land."""
    backend = _Backend(fail=True)
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)
    with pytest.raises(RuntimeError, match="adapter down"):
        await ensure_resident(state, {"pets-db"})
    assert state.tenant_db.recorded == [("pet_store.pets", False)]


@pytest.mark.asyncio
async def test_without_an_engine_or_store_nothing_happens():
    assert await ensure_resident(SimpleNamespace(), {"pets-db"}) == []


# -- one land per stale table, shared by the requests that need it (REQ-1661, REQ-1882) ---------


class _SlowBackend(_Backend):
    """Lands like _Backend, but holds each land open and counts the lands that did real work."""

    def __init__(self, hold: float = 0.3, both_inside=None):
        super().__init__()
        self.hold = hold
        self.both_inside = both_inside
        self.landed_calls: list[tuple[str, int]] = []

    async def materialize_pending(self, state, *, loader, is_stale, source_ids, **kw):
        import threading
        import time as _time

        landed = await super().materialize_pending(
            state, loader=loader, is_stale=is_stale, source_ids=source_ids, **kw
        )
        if landed:
            self.landed_calls += [(sid, threading.get_ident()) for sid, _ in landed]
            if self.both_inside is not None:
                self.both_inside.wait(timeout=10)
            else:
                _time.sleep(self.hold)  # blocks this request's thread, as a real land does
        return landed


def _stamping_refresh(monkeypatch):
    """record_refresh that writes the freshness state get_node_state reads, as the real one does."""

    async def record_refresh(conn, node, *, at, ok):
        conn._recorded.append((node, ok))
        conn._states[node] = {"last_refresh_at": at.timestamp(), "last_refresh_ok": ok}

    monkeypatch.setattr("provisa.events.queue.record_refresh", record_refresh)


def _on_request_threads(calls):
    """Run each call on its own thread and connection loop, started together; return results."""
    import threading

    from provisa.core.connection_loop import connection_loop

    start = threading.Barrier(len(calls))
    results: list = [None] * len(calls)
    errors: list[BaseException] = []

    def _request(i, make_coro):
        try:
            start.wait(timeout=10)
            with connection_loop() as cl:
                results[i] = cl.run(make_coro())
        except BaseException as exc:  # reported by the caller's assertion with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(i, c)) for i, c in enumerate(calls)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors
    return results


def test_two_requests_reading_one_stale_table_share_one_land(wiring, monkeypatch):
    """Both requests find the table stale. One lands it; the other waits for that land and then
    reads the fresh copy — it does not land the table a second time."""
    _stamping_refresh(monkeypatch)
    backend = _SlowBackend()
    state = _state([_source("pets-db")], [_table("pets-db", "pets")], backend)

    results = _on_request_threads(
        [lambda: ensure_resident(state, {"pets-db"}), lambda: ensure_resident(state, {"pets-db"})]
    )

    assert len(backend.landed_calls) == 1, f"the table was landed {len(backend.landed_calls)} times"
    assert sorted(results, key=len) == [[], [("pets-db", "pets")]]


def test_two_requests_reading_different_stale_tables_land_at_the_same_time(wiring, monkeypatch):
    import threading

    _stamping_refresh(monkeypatch)
    both_inside = threading.Barrier(2)
    backend = _SlowBackend(both_inside=both_inside)
    state = _state(
        [_source("pets-db"), _source("vets-db")],
        [_table("pets-db", "pets"), _table("vets-db", "vets")],
        backend,
    )

    results = _on_request_threads(
        [lambda: ensure_resident(state, {"pets-db"}), lambda: ensure_resident(state, {"vets-db"})]
    )

    assert both_inside.broken is False, "the two lands did not overlap"
    assert sorted(results) == [[("pets-db", "pets")], [("vets-db", "vets")]]
    assert len({ident for _, ident in backend.landed_calls}) == 2

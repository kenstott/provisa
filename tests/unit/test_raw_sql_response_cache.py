# Copyright (c) 2026 Kenneth Stott
# Canary: 7b3e9d41-2f6a-4c85-a1d0-5e8c2b7f4a96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1897 (#127): raw-SQL surfaces write and read their own response-cache namespace.

Covers the write-through tee (forwards each batch at once, buffers only within the bound, writes
only a stream drained to its end), the key (disjoint from GraphQL's; partitioned by role and bound
params), the cache policy (TTL/source-disable/table index), the kind-tagged entries across
surfaces (rows <-> Arrow, exact DECIMAL(18,2) and timezone-aware timestamps), and write
invalidation.
"""

from __future__ import annotations

import datetime
import decimal
import time
from types import SimpleNamespace

import pyarrow as pa
import pytest

from provisa.audit.pipeline import PendingAudit
from provisa.cache.raw_sql import (
    KIND_ARROW_IPC,
    KIND_ROWS,
    entry_as_arrow,
    entry_as_result,
)
from provisa.executor.result import QueryResult, StreamingQueryResult
from provisa.pgwire._pipeline import (
    _Plan,
    _response_cache_key,
    check_response_cache,
    check_response_cache_arrow,
    finalize_audit,
    response_cache_tee,
)
from provisa.transpiler.router import Route
from tests.unit.test_response_cache_shared import FakeCacheStore


@pytest.fixture(autouse=True)
def _bound_to_org_a(bind_org):
    """The work is org-a's, the org the test state serves, bound as its entrypoint binds it (REQ-1266)."""
    bind_org("org-a")


_TS = datetime.datetime(2026, 1, 2, 3, 4, 5, 678000, tzinfo=datetime.UTC)
_ROWS = [(1, decimal.Decimal("0.50"), _TS), (2, decimal.Decimal("123456789.01"), _TS)]
_NAMES = ["id", "amount", "ts"]
_TYPES = ["BIGINT", "DECIMAL(18,2)", "TIMESTAMP WITH TIME ZONE"]


def _plan(sql: str = "SELECT id, amount, ts FROM t", *, params=None, role="analyst", **kw) -> _Plan:
    audit = PendingAudit(
        user_id="u1",
        surface="pgwire",
        role_id=role,
        query_text=sql,
        table_ids=[7, 8],
        started=time.time(),
        model_stamp=1,
        enforced={},
    )
    return _Plan(
        route=Route.ENGINE,
        sql=sql,
        source_id="engine",
        dialect="duckdb",
        exec_params=params,
        audit=audit,
        role_id=role,
        table_ids=(7, 8),
        response_cacheable=kw.pop("response_cacheable", True),
        cache_opt_in=kw.pop("cache_opt_in", True),  # REQ-544 (amended): opted-in reads
        **kw,
    )


def _state(store, *, source_cache=None, table_cache=None, default_ttl=300):
    return SimpleNamespace(
        response_cache_store=store,
        model_db="fake",
        tenant_db="fake",
        org_id="org-a",
        model_stamp=1,
        contexts={
            "analyst": SimpleNamespace(
                tables={
                    "a": SimpleNamespace(table_id=7, source_id="pg"),
                    "b": SimpleNamespace(table_id=8, source_id="ch"),
                }
            )
        },
        source_cache=source_cache if source_cache is not None else {},
        table_cache=table_cache if table_cache is not None else {},
        response_cache_default_ttl=default_ttl,
    )


@pytest.fixture(autouse=True)
def _no_audit_writes(monkeypatch):
    async def _noop(pending, status_code, state=None, **outcome):
        return None

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", _noop)


@pytest.fixture
def bound(monkeypatch):
    def _set(n: int) -> None:
        monkeypatch.setattr("provisa.pgwire._pipeline._response_cache_bound", lambda: n)

    _set(100)
    return _set


def _stream(batches: list[list[tuple]], pulls: list[int] | None = None, fail_at: int | None = None):
    def _gen():
        for i, b in enumerate(batches):
            if fail_at == i:
                raise TimeoutError("request exceeded its 2s budget")
            if pulls is not None:
                pulls.append(i)
            yield b

    return StreamingQueryResult(_gen(), column_names=_NAMES, column_types=_TYPES)


# -- key ------------------------------------------------------------------------------------------


def test_raw_sql_key_is_disjoint_from_graphql_and_partitioned_by_role_and_params():
    from provisa.cache.key import cache_key, raw_sql_cache_key

    sql = "SELECT id FROM t WHERE id = $1"
    assert raw_sql_cache_key(sql, [1], "r", wire_formats=None) != cache_key(sql, [1], "r", {})
    assert raw_sql_cache_key(sql, [1], "r", wire_formats=None) != raw_sql_cache_key(
        sql, [2], "r", wire_formats=None
    )
    assert raw_sql_cache_key(sql, [1], "r", wire_formats=None) != raw_sql_cache_key(
        sql, [1], "other", wire_formats=None
    )


def test_a_plan_without_an_audit_record_is_still_keyed_by_its_role():
    """An unsecured Flight ticket has no acting principal (no audit record); its plan still
    carries the governed role and tables, so it is cacheable under that role."""
    plan = _plan()
    plan.audit = None
    assert _response_cache_key(plan, wire_formats=None) is not None
    plan.role_id = None
    assert _response_cache_key(plan, wire_formats=None) is None


def test_non_cacheable_plans_have_no_key():
    assert _response_cache_key(_plan(response_cacheable=False), wire_formats=None) is None
    assert (
        _response_cache_key(_plan(sql="SELECT current_setting('provisa.user')"), wire_formats=None)
        is None
    )


# -- tee ------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_miss_store_hit_round_trip_rows(bound):
    store = FakeCacheStore()
    plan, state = _plan(), _state(store)
    assert await check_response_cache(plan, state) is None
    tee = response_cache_tee(plan, state, run=None)
    assert tee is not None
    served = [b for b in tee.rows(_stream([_ROWS[:1], _ROWS[1:]])).batches()]
    await tee.commit()
    assert [r for b in served for r in b] == _ROWS

    hit = await check_response_cache(_plan(), state)
    assert hit is not None
    assert hit.rows == _ROWS and hit.column_names == _NAMES and hit.column_types == _TYPES
    assert isinstance(hit.rows[1][1], decimal.Decimal)


@pytest.mark.asyncio
async def test_tee_forwards_each_batch_before_pulling_the_next(bound):
    pulls: list[int] = []
    tee = response_cache_tee(_plan(), _state(FakeCacheStore()), run=None)
    assert tee is not None
    it = tee.rows(_stream([[(1,)], [(2,)], [(3,)]], pulls)).batches()
    assert next(it) == [(1,)]
    assert pulls == [0]  # the first batch reached the consumer before batch 2 was pulled


@pytest.mark.asyncio
async def test_over_bound_stream_writes_nothing_and_buffers_at_most_the_bound(bound):
    bound(3)
    store = FakeCacheStore()
    plan, state = _plan(), _state(store)
    tee = response_cache_tee(plan, state, run=None)
    assert tee is not None
    batches = [[(i,), (i + 1,)] for i in range(0, 10, 2)]  # 5 batches x 2 rows = 10 rows
    served = [r for b in tee.rows(_stream(batches)).batches() for r in b]
    await tee.commit()
    assert len(served) == 10  # every row still reached the client
    assert tee.peak_buffered_rows <= 3
    assert tee.stored_entry is None
    assert store._data == {}


@pytest.mark.asyncio
async def test_failed_or_deadline_expired_stream_writes_nothing(bound):
    store = FakeCacheStore()
    tee = response_cache_tee(_plan(), _state(store), run=None)
    assert tee is not None
    with pytest.raises(TimeoutError):
        for _ in tee.rows(_stream([[(1,)], [(2,)]], fail_at=1)).batches():
            pass
    await tee.commit()
    assert tee.stored_entry is None and store._data == {}


@pytest.mark.asyncio
async def test_stream_closed_early_writes_nothing(bound):
    store = FakeCacheStore()
    tee = response_cache_tee(_plan(), _state(store), run=None)
    assert tee is not None
    it = tee.rows(_stream([[(1,)], [(2,)]])).batches()
    next(it)
    it.close()  # the client went away mid-stream
    await tee.commit()
    assert tee.stored_entry is None and store._data == {}


@pytest.mark.asyncio
async def test_sync_terminal_run_stores_when_the_drain_ends(bound):
    """A synchronous terminal hands its loop runner; the entry is stored as the stream ends."""
    import asyncio

    store = FakeCacheStore()
    plan, state = _plan(), _state(store)
    loop = asyncio.get_running_loop()
    ran: list[object] = []

    def _run(coro):
        ran.append(loop.create_task(coro))

    tee = response_cache_tee(plan, state, run=_run)
    assert tee is not None
    for _ in tee.rows(_stream([_ROWS])).batches():
        pass
    await asyncio.gather(*ran)  # type: ignore[arg-type]
    assert await check_response_cache(_plan(), state) is not None


# -- policy ---------------------------------------------------------------------------------------


def test_policy_source_disabled_or_zero_ttl_means_no_tee(bound):
    store = FakeCacheStore()
    off = _state(store, source_cache={"ch": {"cache_enabled": False}})
    assert response_cache_tee(_plan(), off, run=None) is None
    zero = _state(store, table_cache={8: 0})
    assert response_cache_tee(_plan(), zero, run=None) is None
    assert response_cache_tee(_plan(response_cacheable=False), _state(store), run=None) is None


@pytest.mark.asyncio
async def test_policy_uses_the_shortest_ttl_and_indexes_every_table(bound, monkeypatch):
    captured: dict = {}
    store = FakeCacheStore()

    async def _set(key, data, ttl, tenant_id=None, table_ids=None):
        captured.update(ttl=ttl, tenant_id=tenant_id, table_ids=table_ids)

    monkeypatch.setattr(store, "set", _set)
    state = _state(store, table_cache={7: 90}, source_cache={"ch": {"cache_ttl": 40}})
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    for _ in tee.rows(_stream([_ROWS])).batches():
        pass
    await tee.commit()
    assert captured == {"ttl": 40, "tenant_id": "org-a:m1", "table_ids": {7, 8}}


# -- kinds across surfaces --------------------------------------------------------------------------


def _arrow_batches() -> tuple[pa.Schema, list[pa.RecordBatch]]:
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("amount", pa.decimal128(18, 2)),
            ("ts", pa.timestamp("us", tz="UTC")),
        ]
    )
    batch = pa.RecordBatch.from_pylist(
        [dict(zip(_NAMES, r, strict=True)) for r in _ROWS], schema=schema
    )
    return schema, [batch]


@pytest.mark.asyncio
async def test_flight_miss_then_pgwire_hit_is_exact(bound):
    """Flight's Arrow terminal writes arrow_ipc; a row surface's hit decodes exact values."""
    store = FakeCacheStore()
    state = _state(store)
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    schema, batches = _arrow_batches()
    assert [b.num_rows for b in tee.arrow(schema, batches)] == [2]
    await tee.commit()
    assert tee.stored_entry is not None and tee.stored_entry["kind"] == KIND_ARROW_IPC

    hit = await check_response_cache(_plan(), state)
    assert hit is not None
    assert hit.rows == _ROWS
    assert isinstance(hit.rows[0][1], decimal.Decimal) and hit.rows[0][2].tzinfo is not None
    flight_hit = await check_response_cache_arrow(_plan(), state)
    assert flight_hit.schema == schema  # Flight miss -> Flight hit: type-identical


@pytest.mark.asyncio
async def test_pgwire_miss_then_flight_hit_is_exact(bound):
    """A row terminal writes rows; Flight's hit builds Arrow losslessly (decimal128(18, 2))."""
    store = FakeCacheStore()
    state = _state(store)
    tee = response_cache_tee(_plan(), state, run=None)
    assert tee is not None
    for _ in tee.rows(_stream([_ROWS])).batches():
        pass
    await tee.commit()
    assert tee.stored_entry is not None and tee.stored_entry["kind"] == KIND_ROWS

    table = await check_response_cache_arrow(_plan(), state)
    assert table.schema.field("amount").type == pa.decimal128(18, 2)
    assert table.column("amount").to_pylist() == [
        decimal.Decimal("0.50"),
        decimal.Decimal("123456789.01"),
    ]
    assert pa.types.is_timestamp(table.schema.field("ts").type)
    assert table.column("ts").to_pylist() == [_TS, _TS]


def test_unknown_kind_raises_for_both_readers():
    with pytest.raises(ValueError, match="unknown kind"):
        entry_as_result({"kind": "x"}, None)
    with pytest.raises(ValueError, match="unknown kind"):
        entry_as_arrow({"kind": "x"}, None)


# -- chokepoint + invalidation --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chokepoint_stores_buffered_result_and_redirect_is_not_stored(bound):
    from provisa.pgwire._pipeline import store_executed_result

    store = FakeCacheStore()
    state = _state(store)
    await store_executed_result(
        _plan(), state, QueryResult(rows=_ROWS, column_names=_NAMES, column_types=_TYPES)
    )
    hit = await check_response_cache(_plan(), state)
    assert hit is not None and hit.rows == _ROWS

    other = FakeCacheStore()
    redirected = QueryResult(rows=[], column_names=[], redirect={"sink": "s3"})
    await store_executed_result(_plan(), _state(other), redirected)
    assert other._data == {}


@pytest.mark.asyncio
async def test_successful_write_invalidates_its_tables_for_the_org(monkeypatch):
    import provisa.kafka.change_events as change_events

    store = FakeCacheStore()
    dropped: list[tuple[int, str | None]] = []

    async def _inv(table_id, tenant_id=None):
        dropped.append((table_id, tenant_id))
        return 1

    monkeypatch.setattr(store, "invalidate_by_table", _inv)
    monkeypatch.setattr(change_events, "emit_change_event", lambda *a, **k: None)

    async def _no_replica(_state, _table_id, _source_id, _reason):
        return False  # no replica store here: the build request has its own tests

    monkeypatch.setattr("provisa.federation.replica_builds.request_if_replicated", _no_replica)
    state = _state(store)
    state.contexts["analyst"].tables["a"].table_name = "t"
    stale: list[str] = []
    state.mv_registry = SimpleNamespace(mark_stale=stale.append)
    state.hot_manager = None
    state.config = SimpleNamespace(tables=[])  # no change-event sinks declared
    write = _plan(
        sql="UPDATE t SET x = 1", response_cacheable=False, writes_tables=True, written_table_id=7
    )
    await finalize_audit(write, 200, state)
    assert sorted(dropped) == [(7, "org-a:m1"), (8, "org-a:m1")]
    assert stale == ["t"]  # the views over the written table, by its name

    failed = _plan(
        sql="UPDATE t SET x = 1", response_cacheable=False, writes_tables=True, written_table_id=7
    )
    dropped.clear()
    stale.clear()
    await finalize_audit(failed, 500, state)
    assert dropped == []
    assert stale == []


@pytest.mark.asyncio
async def test_a_write_plan_without_its_written_table_is_an_error_not_a_silent_skip(monkeypatch):
    store = FakeCacheStore()

    async def _inv(table_id, tenant_id=None):
        return 1

    monkeypatch.setattr(store, "invalidate_by_table", _inv)
    write = _plan(sql="UPDATE t SET x = 1", response_cacheable=False, writes_tables=True)
    with pytest.raises(RuntimeError, match="without the table it wrote"):
        await finalize_audit(write, 200, _state(store))


# -- pgwire passthrough (pg_datarows) + the one streaming read/write-through -------------------------

_DATAROWS = [
    b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x011",
    b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x012",
]


def _passthrough_stream(msgs: list[bytes]):
    from buenavista.core import RawDataRowBytes

    return StreamingQueryResult(
        iter([[RawDataRowBytes(m) for m in msgs]]), column_names=["n"], column_types=["int4"]
    )


@pytest.mark.asyncio
async def test_passthrough_miss_store_then_replay_is_byte_exact_and_undecoded(bound):
    from buenavista.core import RawDataRowBytes

    from provisa.pgwire._pipeline import _cache_tee, check_response_cache_datarows

    store = FakeCacheStore()
    state = _state(store)
    tee = _cache_tee(_plan(), state, None, [1])
    assert tee is not None
    served = [m for b in tee.datarows(_passthrough_stream(_DATAROWS), [1]).batches() for m in b]
    await tee.commit()
    assert served == _DATAROWS

    replay = await check_response_cache_datarows(_plan(), state, [1])
    assert replay is not None
    assert list(replay.rows) == _DATAROWS
    assert all(isinstance(r, RawDataRowBytes) for r in replay.rows)  # forwarded, never decoded
    assert replay.column_names == ["n"] and replay.column_types == ["int4"]
    # Other client format codes are another entry; decoded readers never see pg_datarows.
    assert await check_response_cache_datarows(_plan(), state, [0]) is None
    assert await check_response_cache(_plan(), state) is None


def test_decoded_readers_refuse_a_pg_datarows_entry():
    from provisa.cache.raw_sql import datarows_entry, entry_as_datarows

    entry = datarows_entry(_DATAROWS, ["n"], ["int4"], [1])
    with pytest.raises(ValueError, match="passthrough"):
        entry_as_result(entry, None)
    with pytest.raises(ValueError, match="passthrough"):
        entry_as_arrow(entry, None)
    with pytest.raises(ValueError, match="format codes"):
        entry_as_datarows(entry, [0])
    with pytest.raises(ValueError, match="needs a pg_datarows"):
        entry_as_datarows({"kind": KIND_ROWS}, [1])


@pytest.mark.asyncio
async def test_over_bound_passthrough_writes_nothing(bound):
    from provisa.pgwire._pipeline import _cache_tee

    bound(1)
    store = FakeCacheStore()
    tee = _cache_tee(_plan(), _state(store), None, [1])
    assert tee is not None
    served = [m for b in tee.datarows(_passthrough_stream(_DATAROWS), [1]).batches() for m in b]
    await tee.commit()
    assert served == _DATAROWS and tee.peak_buffered_rows <= 1 and store._data == {}


class _Runner:
    """A terminal's loop runner for sync code paths: runs each coroutine to completion."""

    def __init__(self) -> None:
        import asyncio

        self.loop = asyncio.new_event_loop()

    def __call__(self, coro):
        return self.loop.run_until_complete(coro)

    def close(self) -> None:
        self.loop.close()


def test_serve_stream_through_cache_passthrough_miss_then_hit(bound):
    from provisa.pgwire._pipeline import serve_stream_through_cache

    store, run = FakeCacheStore(), _Runner()
    state = _state(store)
    opened: list[str] = []

    def _open_pt():
        opened.append("passthrough")
        return _passthrough_stream(_DATAROWS)

    def _open_rows():
        opened.append("rows")
        return _stream([_ROWS])

    try:
        kw = dict(run=run, check_rows=True, passthrough=([1], _open_pt), open_rows=_open_rows)
        miss = serve_stream_through_cache(_plan(), state, **kw)
        assert [m for b in miss.batches() for m in b] == _DATAROWS
        hit = serve_stream_through_cache(_plan(), state, **kw)
        assert list(hit.rows) == _DATAROWS
        assert opened == ["passthrough"]  # the hit never dialled the source
    finally:
        run.close()


def test_serve_stream_through_cache_passthrough_declined_uses_the_decoded_path(bound):
    from provisa.pgwire._pipeline import serve_stream_through_cache
    from provisa.pgwire.pg_passthrough import PassthroughError

    store, run = FakeCacheStore(), _Runner()
    state = _state(store)

    def _declined():
        raise PassthroughError("unrecognized column type(s)")

    try:
        kw = dict(run=run, check_rows=True, passthrough=([1], _declined))
        first = serve_stream_through_cache(_plan(), state, open_rows=lambda: _stream([_ROWS]), **kw)
        assert [r for b in first.batches() for r in b] == _ROWS

        def _must_not_run():
            raise AssertionError("the decoded HIT should have been served")

        hit = serve_stream_through_cache(_plan(), state, open_rows=_must_not_run, **kw)
        assert hit.rows == _ROWS
    finally:
        run.close()


def test_serve_stream_through_cache_check_rows_false_skips_the_decoded_read(bound):
    from provisa.pgwire._pipeline import serve_stream_through_cache

    store, run = FakeCacheStore(), _Runner()
    state = _state(store)
    try:
        for _ in range(2):  # the second call would HIT if the decoded read ran
            out = serve_stream_through_cache(
                _plan(),
                state,
                run=run,
                check_rows=False,
                passthrough=None,
                open_rows=lambda: _stream([_ROWS]),
            )
            assert isinstance(out, StreamingQueryResult)
            assert [r for b in out.batches() for r in b] == _ROWS
    finally:
        run.close()


@pytest.mark.parametrize("declared", ["numeric", None])
def test_flight_direct_miss_and_its_hit_build_one_arrow_shape(declared):
    """Flight's DIRECT stream (the miss) and its rows-entry hit build Arrow through one rows->Arrow
    typing: a Decimal stays a decimal on both, and the hit's table has the miss's schema even when
    it holds fewer rows (a Postgres stream declares ``numeric``, with no precision or scale)."""
    from provisa.federation.runtime_support import arrow_batches_from_rows

    rows = [(1, decimal.Decimal("129.99")), (2, decimal.Decimal("19.99"))]
    names = ["id", "amount"]
    types = ["int8", declared] if declared else None
    miss_schema, batches = arrow_batches_from_rows(
        StreamingQueryResult(iter([rows]), column_names=names, column_types=types)
    )
    miss = pa.Table.from_batches(list(batches), schema=miss_schema)
    entry = {"kind": KIND_ROWS, "rows": [list(rows[1])], "column_names": names}
    hit = entry_as_arrow(entry, types)
    assert pa.types.is_decimal(miss.schema.field("amount").type)
    assert hit.schema == miss.schema
    assert hit.to_pylist() == miss.to_pylist()[1:2]

# Copyright (c) 2026 Kenneth Stott
# Canary: 5c8e1b47-2d9f-4a63-b0e5-9f3a7c1d6e28
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A land runs on the thread that asked for it (REQ-1882).

A read that finds its source stale lands it before reading (REQ-1661), and an admin request
converges a landed table's DDL. Either way the store write is part of the request, so it runs on
the request's own thread — never on a worker another request shares. Every engine runtime's
landing terminal is driven here the way a request drives it (a coroutine on the request thread's
connection loop); the store write records the thread it ran on.
"""

# Requirements: REQ-1882

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.core.connection_loop import connection_loop
from provisa.federation.land_guard import LandGuard

pytestmark = pytest.mark.unit

_COLUMNS = [("id", "integer")]
_ROWS = [{"id": 1}]


class _Recorder:
    """Stands in for the store write: records the thread it runs on."""

    def __init__(self, result: Any = "ok") -> None:
        self.idents: list[int] = []
        self._result = result

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        self.idents.append(threading.get_ident())
        return self._result


class _Cursor:
    def execute(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs

    def close(self) -> None:
        pass


class _Connection:
    def cursor(self) -> _Cursor:
        return _Cursor()


def _run_as_request(make_coro: Any) -> Any:
    """Run the coroutine the way a request does: on this thread's own connection loop."""
    with connection_loop() as cl:
        return cl.run(make_coro())


def _assert_ran_here(recorder: _Recorder) -> None:
    assert recorder.idents, "the store write never ran"
    assert set(recorder.idents) == {threading.get_ident()}, (
        "the store write ran on another thread, not the request's"
    )


def _bare(cls: type, **attrs: Any) -> Any:
    rt = cls.__new__(cls)
    rt._land_guard = LandGuard("test store connection")
    for name, value in attrs.items():
        setattr(rt, name, value)
    return rt


def _source() -> SimpleNamespace:
    return SimpleNamespace(id="src", schema_name="sales", table_name="orders")


def _plan() -> SimpleNamespace:
    return SimpleNamespace(tables={}, store_parts={}, edges=[], known_tags=[])


# -- DuckDB ------------------------------------------------------------------------------------


def test_duckdb_land_table_writes_the_store_on_the_request_thread() -> None:
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    write = _Recorder("s.t")
    rt = _bare(
        DuckDBFederationRuntime,
        ensure_materialize_attached=lambda: None,
        _store_is_duckdb=lambda: True,
        _store_broker=SimpleNamespace(land=write),
    )
    _run_as_request(lambda: rt.land_table(schema="s", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


# -- Postgres ----------------------------------------------------------------------------------


def _pg_runtime(write: _Recorder) -> Any:
    from provisa.federation.pg_runtime import PgFederationRuntime

    return _bare(PgFederationRuntime, _con=_Connection(), _ensure_table=write, _insert_rows=write)


def test_pg_land_table_writes_on_the_request_thread() -> None:
    write = _Recorder()
    rt = _pg_runtime(write)
    _run_as_request(lambda: rt.land_table(schema="s", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


def test_pg_apply_cdc_events_writes_on_the_calling_thread() -> None:
    write = _Recorder()
    rt = _pg_runtime(write)
    _run_as_request(
        lambda: rt.apply_cdc_events(
            schema="s", table="t", columns=_COLUMNS, pk_columns=["id"], events=[]
        )
    )
    _assert_ran_here(write)


# -- ClickHouse --------------------------------------------------------------------------------


def test_clickhouse_land_table_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import clickhouse_store
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    write = _Recorder("db.t")
    monkeypatch.setattr(clickhouse_store, "land_clickhouse_native", write)
    rt = _bare(ClickHouseFederationRuntime, _backend=object())
    _run_as_request(lambda: rt.land_table(schema="db", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


def test_clickhouse_reconcile_replica_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import clickhouse_store
    from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime

    write = _Recorder("created")
    monkeypatch.setattr(clickhouse_store, "reconcile_clickhouse_native", write)
    rt = _bare(ClickHouseFederationRuntime, _backend=object())
    _run_as_request(lambda: rt.reconcile_replica(schema="db", table="t", columns=_COLUMNS))
    _assert_ran_here(write)


# -- Snowflake ---------------------------------------------------------------------------------


def _snowflake_runtime() -> Any:
    from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime

    return _bare(
        SnowflakeFederationRuntime,
        _conn=_Connection(),
        ensure_materialize_attached=lambda: "LANDING",
        _phys_parts=lambda source: ("src", "sales", "orders"),
    )


def test_snowflake_land_table_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder("LANDING.s.t")
    monkeypatch.setattr(snowflake_store, "land_snowflake_native", write)
    rt = _snowflake_runtime()
    _run_as_request(lambda: rt.land_table(schema="s", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


def test_snowflake_reconcile_replica_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder("created")
    monkeypatch.setattr(snowflake_store, "reconcile_snowflake_native", write)
    rt = _snowflake_runtime()
    _run_as_request(lambda: rt.reconcile_replica(schema="s", table="t", columns=_COLUMNS))
    _assert_ran_here(write)


def test_snowflake_publish_replica_view_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder(None)
    monkeypatch.setattr(snowflake_store, "expose_view", write)
    rt = _snowflake_runtime()
    _run_as_request(
        lambda: rt.publish_replica_view(_source(), schema="s", table="t", replace=False)
    )
    _assert_ran_here(write)


def test_snowflake_reconcile_landed_metadata_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder(0)
    monkeypatch.setattr(snowflake_store, "reconcile_metadata_native", write)
    rt = _snowflake_runtime()
    _run_as_request(lambda: rt.reconcile_landed_metadata(_plan()))
    _assert_ran_here(write)


def test_snowflake_reconcile_mv_table_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder("created")
    monkeypatch.setattr(snowflake_store, "reconcile_snowflake_native", write)
    rt = _snowflake_runtime()
    _run_as_request(lambda: rt.reconcile_mv_table(schema="s", table="mv", columns=_COLUMNS))
    _assert_ran_here(write)


def test_snowflake_persist_mv_table_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import snowflake_store

    write = _Recorder("LANDING.s.mv")
    monkeypatch.setattr(snowflake_store, "land_snowflake_native", write)
    rt = _snowflake_runtime()
    _run_as_request(
        lambda: rt.persist_mv_table(
            schema="s", table="mv", columns=_COLUMNS, rows=_ROWS, persist="replace"
        )
    )
    _assert_ran_here(write)


# -- Databricks --------------------------------------------------------------------------------


def _databricks_runtime() -> Any:
    from provisa.federation.databricks_runtime import DatabricksFederationRuntime

    return _bare(
        DatabricksFederationRuntime,
        _conn=_Connection(),
        _catalog="main",
        _stage_from_env=lambda: None,
        _phys_parts=lambda source: ("src", "sales", "orders"),
    )


def test_databricks_land_table_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import databricks_store

    write = _Recorder(None)
    monkeypatch.setattr(databricks_store, "land_databricks_native", write)
    rt = _databricks_runtime()
    _run_as_request(lambda: rt.land_table(schema="s", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


def test_databricks_reconcile_replica_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import databricks_store

    write = _Recorder(None)
    monkeypatch.setattr(databricks_store, "reconcile_databricks_native", write)
    rt = _databricks_runtime()
    _run_as_request(lambda: rt.reconcile_replica(schema="s", table="t", columns=_COLUMNS))
    _assert_ran_here(write)


def test_databricks_reconcile_landed_metadata_writes_on_the_request_thread(monkeypatch) -> None:
    from provisa.federation import databricks_store

    write = _Recorder(0)
    monkeypatch.setattr(databricks_store, "reconcile_metadata_native", write)
    rt = _databricks_runtime()
    _run_as_request(lambda: rt.reconcile_landed_metadata(_plan()))
    _assert_ran_here(write)


# -- Fabric / Synapse --------------------------------------------------------------------------


def test_mssql_warehouse_land_table_writes_on_the_request_thread() -> None:
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    write = _Recorder(None)
    rt = _bare(MssqlWarehouseRuntime, _land=write, _database="wh")
    _run_as_request(lambda: rt.land_table(schema="s", table="t", columns=_COLUMNS, rows=_ROWS))
    _assert_ran_here(write)


def test_mssql_warehouse_reconcile_replica_writes_on_the_request_thread() -> None:
    from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime

    write = _Recorder("created")
    rt = _bare(MssqlWarehouseRuntime, _reconcile=write)
    _run_as_request(lambda: rt.reconcile_replica(schema="s", table="t", columns=_COLUMNS))
    _assert_ran_here(write)


# -- the shared connection stays serialized ----------------------------------------------------


@pytest.mark.parametrize("runtime", ["pg", "snowflake"])
def test_lands_from_two_requests_never_overlap_on_the_shared_connection(
    runtime: str, monkeypatch
) -> None:
    """Two requests land different tables at once. Each write runs on its own request's thread,
    and the two never overlap on the runtime's one connection."""
    inside = 0
    overlapped = False
    idents: set[int] = set()
    guard = threading.Lock()
    both_started = threading.Barrier(2)

    def _write(*args: Any, **kwargs: Any) -> str:
        del args, kwargs
        nonlocal inside, overlapped
        with guard:
            inside += 1
            overlapped = overlapped or inside > 1
            idents.add(threading.get_ident())
        threading.Event().wait(0.2)  # hold the connection long enough for the other to arrive
        with guard:
            inside -= 1
        return "ok"

    if runtime == "pg":
        from provisa.federation.pg_runtime import PgFederationRuntime

        rt = _bare(
            PgFederationRuntime, _con=_Connection(), _ensure_table=_write, _insert_rows=_write
        )
    else:
        from provisa.federation import snowflake_store

        monkeypatch.setattr(snowflake_store, "land_snowflake_native", _write)
        rt = _snowflake_runtime()

    errors: list[BaseException] = []
    request_idents: list[int] = []

    def _request(table: str) -> None:
        try:
            request_idents.append(threading.get_ident())
            both_started.wait(timeout=10)
            _run_as_request(
                lambda: rt.land_table(schema="s", table=table, columns=_COLUMNS, rows=_ROWS)
            )
        except BaseException as exc:  # reported by the assertion below with the real cause
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(t,)) for t in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, errors
    assert not overlapped, "two lands ran on the shared connection at the same time"
    assert idents == set(request_idents), "a land ran on a thread that is not its request's"


def test_a_land_waits_for_the_connection_no_longer_than_the_request_budget() -> None:
    """Another land holds the connection. A request with 0.2 s left gives up at its deadline
    with an error naming the store, instead of queuing behind it."""
    import time

    from provisa.core import request_deadline

    guard = LandGuard("Postgres store connection")
    release = threading.Event()
    holding = threading.Event()

    def _holder() -> None:
        def _hold() -> None:
            holding.set()
            release.wait(10)

        _run_as_request(lambda: guard.run(_hold))

    holder = threading.Thread(target=_holder)
    holder.start()
    try:
        assert holding.wait(5)
        t0 = time.monotonic()
        with request_deadline.within(0.2), pytest.raises(TimeoutError, match="Postgres store"):
            _run_as_request(lambda: guard.run(lambda: None))
        assert time.monotonic() - t0 < 2
    finally:
        release.set()
        holder.join(timeout=10)

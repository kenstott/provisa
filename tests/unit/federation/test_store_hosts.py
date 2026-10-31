# Copyright (c) 2026 Kenneth Stott
# Canary: 1de98696-1af9-402e-9940-e2ebe1b76dbf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1901: a store operation attaches the store file to an empty in-memory database kept open
between operations, and holds the file for that one operation only (materialize_broker._take_host).
Opening a database per operation was most of what a graph session's write cost."""

# Requirements: REQ-1901

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from provisa.core import engine_memory
from provisa.federation import materialize_broker
from provisa.federation.materialize_broker import _with_store


@pytest.fixture(autouse=True)
def no_idle_hosts() -> Iterator[None]:
    def empty() -> None:
        while materialize_broker._idle_hosts:
            materialize_broker._idle_hosts.pop()[1].close()

    empty()
    yield
    empty()


@pytest.fixture
def store(tmp_path: Path) -> str:
    path = str(tmp_path / "store.duckdb")
    _with_store(path, lambda con: con.execute("CREATE TABLE mat_store.t (id INTEGER)"))
    return path


def test_one_operation_after_another_opens_one_database(
    store: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[str] = []
    connect = engine_memory.connect

    def counted(*args: object, **kwargs: object) -> object:
        opened.append("opened")
        return connect(*args, **kwargs)

    monkeypatch.setattr(materialize_broker.engine_memory, "connect", counted)
    for n in range(5):
        _with_store(store, lambda con, n=n: con.execute(f"INSERT INTO mat_store.t VALUES ({n})"))
    assert len(opened) == 0  # the one the fixture's operation opened stayed


def test_the_file_is_whole_and_free_between_operations(store: str) -> None:
    _with_store(store, lambda con: con.execute("INSERT INTO mat_store.t VALUES (7)"))
    assert not Path(store + ".wal").exists()
    other = duckdb.connect(store)
    try:
        other.execute("INSERT INTO t VALUES (8)")
    finally:
        other.close()
    rows = _with_store(
        store, lambda con: con.execute("SELECT id FROM mat_store.t ORDER BY id").fetchall()
    )
    assert rows == [(7,), (8,)]


def test_what_an_operation_registers_goes_with_it(store: str) -> None:
    _with_store(store, lambda con: con.register("batch", pa.table({"id": [1]})))
    with pytest.raises(duckdb.CatalogException):
        _with_store(store, lambda con: con.execute("SELECT * FROM batch").fetchall())


def test_a_read_only_operation_cannot_write_and_the_next_can(store: str) -> None:
    with pytest.raises(duckdb.Error):
        _with_store(
            store, lambda con: con.execute("INSERT INTO mat_store.t VALUES (1)"), read_only=True
        )
    _with_store(store, lambda con: con.execute("INSERT INTO mat_store.t VALUES (2)"))
    assert _with_store(
        store, lambda con: con.execute("SELECT id FROM mat_store.t").fetchall(), read_only=True
    ) == [(2,)]


def test_a_failed_operation_closes_its_database(store: str) -> None:
    materialize_broker._idle_hosts.clear()

    def fails(con: duckdb.DuckDBPyConnection) -> None:
        con.execute("INSERT INTO mat_store.t VALUES (1)")
        raise RuntimeError("the operation failed")

    with pytest.raises(RuntimeError, match="the operation failed"):
        _with_store(store, fails)
    assert materialize_broker._idle_hosts == []
    # The store is free again: closing released the file.
    assert _with_store(
        store, lambda con: con.execute("SELECT count(*) FROM mat_store.t").fetchone()
    ) == (1,)


def test_an_open_transaction_does_not_outlive_its_operation(store: str) -> None:
    _with_store(
        store,
        lambda con: (con.execute("BEGIN"), con.execute("INSERT INTO mat_store.t VALUES (1)")),
    )
    assert _with_store(
        store, lambda con: con.execute("SELECT count(*) FROM mat_store.t").fetchone()
    ) == (0,)


def test_a_changed_cap_is_in_force_on_the_next_operation(
    store: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    capped = {"memory_limit": "256MiB"}
    monkeypatch.setattr(materialize_broker.engine_memory, "config", lambda own=None: capped)
    limit = _with_store(
        store, lambda con: con.execute("SELECT current_setting('memory_limit')").fetchone()
    )
    assert limit == ("256.0 MiB",)


def test_idle_databases_are_bounded() -> None:
    cap = engine_memory.config()
    hosts = [engine_memory.connect() for _ in range(materialize_broker._IDLE_HOSTS + 1)]
    for host in hosts:
        materialize_broker._give_back_host(cap, host)
    assert len(materialize_broker._idle_hosts) == materialize_broker._IDLE_HOSTS
    with pytest.raises(duckdb.ConnectionException):
        hosts[-1].execute("SELECT 1")

# Copyright (c) 2026 Kenneth Stott
# Canary: 6a3f9d21-4c8e-4b7a-9e5d-1f2c8a0b7e34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An MV's read target is `mat_store.<schema>.<table>`; on an embedded-DuckDB store the engine
connection never ATTACHes the store file (REQ-1901), so a read of it is served from a local copy
the broker hands over, and only the tables the statement names are copied."""

# Requirements: REQ-1901

from __future__ import annotations

import asyncio

import pytest

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime


@pytest.fixture
def runtime(tmp_path):
    rt = DuckDBFederationRuntime(materialize_dsn=f"duckdb:///{tmp_path / 'mat.duckdb'}")
    assert rt.ensure_materialize_attached() == "mat_store"
    for table, rows in (("mv_a", [{"id": 1, "amount": 10}]), ("mv_b", [{"id": 2, "amount": 20}])):
        asyncio.run(
            rt.persist_mv_table(
                schema="org_x",
                table=table,
                columns=[("id", "INTEGER"), ("amount", "INTEGER")],
                rows=rows,
                persist="replace",
            )
        )
    return rt


def _attached_catalogs(rt) -> set[str]:
    return {
        r[0]
        for r in rt.connection.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    }


def test_a_mat_store_read_returns_the_store_rows(runtime):
    res = asyncio.run(runtime.run('SELECT id, amount FROM mat_store."org_x"."mv_a"'))
    assert res.rows == [(1, 10)]
    assert res.column_names == ["id", "amount"]


def test_a_quoted_catalog_reference_is_served_too(runtime):
    # The compiled MV read (e.g. a bitemporal reconstruction) quotes every part.
    sql = 'SELECT amount FROM (SELECT * FROM "mat_store"."org_x"."mv_b" AS "mv_b") AS "v"'
    assert asyncio.run(runtime.run(sql)).rows == [(20,)]


def test_the_store_file_is_never_attached_to_the_engine_connection(runtime):
    asyncio.run(runtime.run("SELECT * FROM mat_store.org_x.mv_a"))
    assert "mat_store" not in _attached_catalogs(runtime)


def test_only_the_referenced_table_is_copied(runtime):
    asyncio.run(runtime.run('SELECT * FROM mat_store."org_x"."mv_a"'))
    local = {
        r[0]
        for r in runtime.connection.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = '_mat_store_local'"
        ).fetchall()
    }
    assert local == {"org_x__mv_a"}


def test_a_read_sees_rows_persisted_after_the_previous_read(runtime):
    assert asyncio.run(runtime.run("SELECT count(*) FROM mat_store.org_x.mv_a")).rows == [(1,)]
    asyncio.run(
        runtime.persist_mv_table(
            schema="org_x",
            table="mv_a",
            columns=[("id", "INTEGER"), ("amount", "INTEGER")],
            rows=[{"id": 1, "amount": 10}, {"id": 3, "amount": 30}],
            persist="replace",
        )
    )
    assert asyncio.run(runtime.run("SELECT count(*) FROM mat_store.org_x.mv_a")).rows == [(2,)]


def test_a_write_aimed_at_mat_store_still_fails_loudly(runtime):
    # No stand-in catalog: a write that assumed an attached store must not vanish into memory.
    with pytest.raises(Exception, match="mat_store"):
        runtime.connection.execute('CREATE TABLE mat_store."org_x"."stray" (x INTEGER)')


def test_run_sync_and_run_arrow_serve_the_same_read(runtime):
    assert runtime.run_sync("SELECT amount FROM mat_store.org_x.mv_b").rows == [(20,)]
    assert runtime.run_arrow("SELECT amount FROM mat_store.org_x.mv_b").to_pylist() == [
        {"amount": 20}
    ]


def _count_fetches(runtime, monkeypatch) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []
    real = runtime._store_broker.fetch_arrow

    def _spy(schema, table):
        calls.append((schema, table))
        return real(schema, table)

    monkeypatch.setattr(runtime._store_broker, "fetch_arrow", _spy)
    return calls


def test_an_unchanged_store_table_is_copied_once_not_per_query(runtime, monkeypatch):
    """A landed/MV table is copied only when the store changed — never once per statement."""
    calls = _count_fetches(runtime, monkeypatch)
    for _ in range(3):
        assert asyncio.run(runtime.run("SELECT count(*) FROM mat_store.org_x.mv_a")).rows == [(1,)]
    assert calls == [("org_x", "mv_a")]


def test_a_store_write_forces_a_fresh_copy(runtime, monkeypatch):
    calls = _count_fetches(runtime, monkeypatch)
    asyncio.run(runtime.run("SELECT count(*) FROM mat_store.org_x.mv_a"))
    asyncio.run(
        runtime.persist_mv_table(
            schema="org_x",
            table="mv_a",
            columns=[("id", "INTEGER"), ("amount", "INTEGER")],
            rows=[{"id": 1, "amount": 10}, {"id": 3, "amount": 30}],
            persist="replace",
        )
    )
    assert asyncio.run(runtime.run("SELECT count(*) FROM mat_store.org_x.mv_a")).rows == [(2,)]
    assert calls == [("org_x", "mv_a"), ("org_x", "mv_a")]


def test_a_broker_read_leaves_the_store_canary_unchanged(runtime):
    before = runtime._store_broker.canary()
    _tbl, canary = runtime._store_broker.fetch_arrow("org_x", "mv_a")
    assert canary == before == runtime._store_broker.canary()


def test_every_write_advances_the_generation_even_within_one_clock_tick(runtime):
    """Two back-to-back same-shape writes must each be visible — no timestamp granularity."""
    g0 = runtime._store_broker.canary()
    for amount in (11, 12):
        asyncio.run(
            runtime.persist_mv_table(
                schema="org_x",
                table="mv_a",
                columns=[("id", "INTEGER"), ("amount", "INTEGER")],
                rows=[{"id": 1, "amount": amount}],
                persist="replace",
            )
        )
        assert asyncio.run(runtime.run("SELECT amount FROM mat_store.org_x.mv_a")).rows == [
            (amount,)
        ]
    assert runtime._store_broker.canary() == g0 + 2


def test_store_copies_racing_shared_connection_work_never_deadlock(runtime):
    """A store-table copy (register -> CREATE OR REPLACE -> unregister) runs on a private cursor, so
    a peer thread executing on the shared connection while holding the GIL cannot deadlock it (the
    view's Python-reference teardown needs the GIL inside the copying context's lock)."""
    import threading

    stop = threading.Event()
    errors: list[BaseException] = []

    def _writer_reader() -> None:
        try:
            for i in range(25):
                asyncio.run(
                    runtime.persist_mv_table(
                        schema="org_x",
                        table="mv_a",
                        columns=[("id", "INTEGER"), ("amount", "INTEGER")],
                        rows=[{"id": 1, "amount": i}],
                        persist="replace",
                    )
                )
                asyncio.run(runtime.run("SELECT amount FROM mat_store.org_x.mv_a"))
        except BaseException as exc:  # handed to the main thread's assertion
            errors.append(exc)

    def _shared_connection_peer() -> None:
        try:
            while not stop.is_set():
                runtime.connection.execute("SELECT 1").fetchall()
        except BaseException as exc:
            errors.append(exc)

    peers = [threading.Thread(target=_shared_connection_peer, daemon=True) for _ in range(3)]
    workers = [threading.Thread(target=_writer_reader, daemon=True) for _ in range(3)]
    for t in peers + workers:
        t.start()
    for t in workers:
        t.join(timeout=60)
    stop.set()
    for t in peers:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in workers + peers), "deadlocked"
    assert errors == []

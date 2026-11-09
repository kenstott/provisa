# Copyright (c) 2026 Kenneth Stott
# Canary: c603a40e-f806-427b-b840-90376de6df00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2015: each store builds a replica's key and join indexes after the replica lands.

PostgreSQL: each index ``CREATE INDEX CONCURRENTLY`` on a connection of its own, the key as a
unique index so built and then made the primary key; rows that repeat the key are a named
failure, and a landing's swap of the replica cancels a build still running on the table it
replaces. The embedded DuckDB store: through the store broker, the key as the table's PRIMARY
KEY. A store a SQLAlchemy engine names: the dialect's own DDL."""

# Requirements: REQ-2015

from __future__ import annotations

import asyncio
import threading
import time

import psycopg
import pytest

from provisa.federation import replica_indexes as ri
from provisa.federation.replica_target import (
    DuckDBStoreIndexer,
    PostgresIndexer,
    SqlAlchemyIndexer,
    _pg_swap,
)

pytestmark = pytest.mark.asyncio

SCHEMA = "replicas_ix"
TABLE = "shop__public__orders"


def _specs() -> list[ri.IndexSpec]:
    return ri.plan(f"{SCHEMA}.{TABLE}", ["id", "customer_id", "note"], ["id"], [("customer_id",)])


# -- PostgreSQL ------------------------------------------------------------------------------------


@pytest.fixture
def pg(embedded_postgres):
    dsn = embedded_postgres.dsn()
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        con.execute(f"CREATE SCHEMA {SCHEMA}")
        con.execute(f'CREATE TABLE {SCHEMA}."{TABLE}" (id integer, customer_id integer, note text)')
        con.execute(
            f'INSERT INTO {SCHEMA}."{TABLE}" '
            "SELECT g, g % 10, 'n' || g FROM generate_series(1, 1000) g"
        )
    yield dsn
    with psycopg.connect(dsn, autocommit=True) as con:
        con.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")


def _pg_indexes(dsn) -> dict[str, tuple[bool, bool, bool]]:
    """Each index of the replica by name: (valid, unique, primary)."""
    with psycopg.connect(dsn) as con:
        rows = con.execute(
            "SELECT c.relname, i.indisvalid, i.indisunique, i.indisprimary FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = %s::regclass",
            (f'{SCHEMA}."{TABLE}"',),
        ).fetchall()
    return {name: (valid, unique, primary) for name, valid, unique, primary in rows}


async def test_postgres_builds_the_key_and_the_join_index_after_the_replica_reads(pg):
    with psycopg.connect(pg) as con:
        # Readable as it landed, with no index yet.
        assert con.execute(f'SELECT count(*) FROM {SCHEMA}."{TABLE}"').fetchone() == (1000,)
    assert _pg_indexes(pg) == {}
    pk, join = _specs()
    indexer = PostgresIndexer(pg, schema=SCHEMA, table=TABLE)
    await indexer.build(pk)
    await indexer.build(join)
    assert _pg_indexes(pg) == {pk.name: (True, True, True), join.name: (True, False, False)}
    # Built once: asked again, nothing changes.
    await indexer.build(pk)
    await indexer.build(join)
    assert set(_pg_indexes(pg)) == {pk.name, join.name}
    with psycopg.connect(pg) as con:
        plan = con.execute(
            f'EXPLAIN SELECT * FROM {SCHEMA}."{TABLE}" WHERE customer_id = 3'
        ).fetchall()
    assert any(join.name in line[0] for line in plan) or any(
        "Seq Scan" in line[0] for line in plan
    )  # the planner may prefer a scan on a table this small; the index is there to choose


async def test_postgres_names_rows_that_repeat_the_key_and_leaves_no_index(pg):
    with psycopg.connect(pg, autocommit=True) as con:
        con.execute(f"INSERT INTO {SCHEMA}.\"{TABLE}\" VALUES (7, 1, 'again')")
    pk, join = _specs()
    indexer = PostgresIndexer(pg, schema=SCHEMA, table=TABLE)
    with pytest.raises(ri.DuplicateKey) as raised:
        await indexer.build(pk)
    assert raised.value.columns == ("id",)
    assert "(id)" in str(raised.value) and "duplicates" in str(raised.value)
    assert _pg_indexes(pg) == {}  # the invalid unique index it left is gone
    await indexer.build(join)  # the join index is built all the same
    assert set(_pg_indexes(pg)) == {join.name}


async def test_a_swap_cancels_an_index_build_still_running_on_the_table_it_replaces(pg):
    """A transaction holding a write lock on the replica makes CREATE INDEX CONCURRENTLY wait;
    the swap of the next landing cancels it before its DROP, and the build says it was left."""
    join = _specs()[1]
    holder = psycopg.connect(pg)
    holder.execute(f"INSERT INTO {SCHEMA}.\"{TABLE}\" VALUES (5000, 1, 'held')")
    indexer = PostgresIndexer(pg, schema=SCHEMA, table=TABLE)
    outcome: dict = {}

    def run_build() -> None:
        try:
            asyncio.run(indexer.build(join))
            outcome["ok"] = True
        except BaseException as exc:  # noqa: BLE001 -- the outcome is the assertion
            outcome["error"] = exc

    swapped: dict = {}

    def run_swap() -> None:
        with psycopg.connect(pg) as con:
            cur = con.cursor()
            cur.execute(f'CREATE TABLE {SCHEMA}."build__x" (id integer, customer_id integer)')
            _pg_swap(cur, SCHEMA, TABLE, "build__x", [])
            con.commit()
            swapped["done"] = True

    builder = threading.Thread(target=run_build, daemon=True)
    swapper = threading.Thread(target=run_swap, daemon=True)
    try:
        builder.start()
        # Autocommit: pg_stat_activity is read once per transaction, so a probe inside one would
        # never see the build arrive.
        with psycopg.connect(pg, autocommit=True) as look:
            waiting = 0
            for _ in range(100):
                waiting = look.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE position(%s in query) > 0",
                    (f"provisa replica index: {SCHEMA}.{TABLE}",),
                ).fetchone()[0]  # type: ignore[index]
                if waiting:
                    break
                time.sleep(0.05)
        assert waiting, f"the index build never started: {outcome}"
        swapper.start()
        builder.join(10)
    finally:
        holder.rollback()  # the DROP waits only on the writer, not on the cancelled build
        holder.close()
    swapper.join(10)
    assert isinstance(outcome.get("error"), ri.IndexBuildAbandoned), outcome
    assert swapped == {"done": True}
    assert _pg_indexes(pg) == {}  # the new table has none until its own builds run


# -- the embedded DuckDB store, through its broker --------------------------------------------------


@pytest.fixture
def duck_store(tmp_path):
    from provisa.federation.materialize_broker import get_broker

    broker = get_broker(str(tmp_path / "store.duckdb"))

    def _setup(con):
        con.execute(f'CREATE SCHEMA IF NOT EXISTS mat_store."{SCHEMA}"')
        con.execute(
            f'CREATE TABLE mat_store."{SCHEMA}"."{TABLE}" AS '
            "SELECT i AS id, i % 10 AS customer_id, 'n' || i AS note FROM range(1, 1001) t(i)"
        )

    broker._run(_setup)
    return broker


def _duck_state(broker) -> tuple[list, list]:
    def _read(con):
        keys = con.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() WHERE database_name = "
            "'mat_store' AND table_name = ? AND constraint_type = 'PRIMARY KEY'",
            [TABLE],
        ).fetchall()
        indexes = con.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE database_name = 'mat_store' "
            "AND table_name = ?",
            [TABLE],
        ).fetchall()
        return [list(k[0]) for k in keys], sorted(i[0] for i in indexes)

    return broker._run(_read, read_only=True)


async def test_the_duckdb_store_carries_the_key_and_the_join_index(duck_store):
    pk, join = _specs()
    indexer = DuckDBStoreIndexer(duck_store, schema=SCHEMA, table=TABLE)
    assert _duck_state(duck_store) == ([], [])
    await indexer.build(pk)
    await indexer.build(join)
    await indexer.build(pk)  # built once
    await indexer.build(join)
    assert _duck_state(duck_store) == ([["id"]], [join.name])


async def test_a_key_built_after_a_join_index_leaves_both(duck_store):
    """A join index built first (an earlier attempt failed at the key) does not cost the key:
    run 42527446894 logged "Cannot alter entry" for mv_dim_pet on every refresh."""
    pk, join = _specs()
    indexer = DuckDBStoreIndexer(duck_store, schema=SCHEMA, table=TABLE)
    await indexer.build(join)
    await indexer.build(pk)
    assert _duck_state(duck_store) == ([["id"]], [join.name])


async def test_rows_that_repeat_the_key_leave_the_join_index_standing(duck_store):
    pk, join = _specs()
    indexer = DuckDBStoreIndexer(duck_store, schema=SCHEMA, table=TABLE)
    await indexer.build(join)
    duck_store._run(
        lambda con: con.execute(f'INSERT INTO mat_store."{SCHEMA}"."{TABLE}" VALUES (7, 1, \'x\')')
    )
    with pytest.raises(ri.DuplicateKey):
        await indexer.build(pk)
    assert _duck_state(duck_store) == ([], [join.name])


async def test_the_duckdb_store_names_rows_that_repeat_the_key(duck_store):
    duck_store._run(
        lambda con: con.execute(f'INSERT INTO mat_store."{SCHEMA}"."{TABLE}" VALUES (7, 1, \'x\')')
    )
    pk, _join = _specs()
    with pytest.raises(ri.DuplicateKey) as raised:
        await DuckDBStoreIndexer(duck_store, schema=SCHEMA, table=TABLE).build(pk)
    assert raised.value.columns == ("id",)
    assert _duck_state(duck_store) == ([], [])


async def test_the_duckdb_key_on_a_large_replica_holds_the_store_briefly(tmp_path):
    """The key is one broker operation, and the broker serialises the store: measured here so a
    hand-off can say how long it holds other store work (2,000,000 rows)."""
    from provisa.federation.materialize_broker import get_broker

    broker = get_broker(str(tmp_path / "big.duckdb"))

    def _setup(con):
        con.execute(f'CREATE SCHEMA IF NOT EXISTS mat_store."{SCHEMA}"')
        con.execute(
            f'CREATE TABLE mat_store."{SCHEMA}"."{TABLE}" AS SELECT i AS id, i % 1000 AS '
            "customer_id, 'n' || i AS note FROM range(1, 2000001) t(i)"
        )

    broker._run(_setup)
    pk, join = _specs()
    indexer = DuckDBStoreIndexer(broker, schema=SCHEMA, table=TABLE)
    started = time.monotonic()
    await indexer.build(pk)
    key_s = time.monotonic() - started
    started = time.monotonic()
    await indexer.build(join)
    join_s = time.monotonic() - started
    print(f"duckdb 2M-row replica: key {key_s:.2f}s, join index {join_s:.2f}s")
    assert _duck_state(broker)[0] == [["id"]]


# -- a store a SQLAlchemy engine names --------------------------------------------------------------


async def test_a_sqlalchemy_store_takes_the_key_and_the_join_index_by_its_dialect(
    embedded_postgres,
):
    """The DDL of the SQLAlchemy engine's own dialect (a PostgreSQL URL here, its inspector
    reading what is built): the key as a constraint, the join column as an index."""
    from sqlalchemy import create_engine, inspect, text

    engine = create_engine(embedded_postgres.url())
    with engine.begin() as con:
        con.execute(text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
        con.execute(text(f'CREATE SCHEMA "{SCHEMA}"'))
        con.execute(
            text(
                f'CREATE TABLE "{SCHEMA}"."{TABLE}" AS SELECT g AS id, g % 10 AS customer_id '
                "FROM generate_series(1, 100) g"
            )
        )
    try:
        pk, join = _specs()
        indexer = SqlAlchemyIndexer(engine, schema=SCHEMA, table=TABLE)
        await indexer.build(pk)
        await indexer.build(join)
        await indexer.build(pk)  # built once
        await indexer.build(join)
        with engine.connect() as con:
            seen = inspect(con)
            assert seen.get_pk_constraint(TABLE, schema=SCHEMA)["constrained_columns"] == ["id"]
            assert [ix["name"] for ix in seen.get_indexes(TABLE, schema=SCHEMA)] == [join.name]
    finally:
        with engine.begin() as con:
            con.execute(text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
        engine.dispose()

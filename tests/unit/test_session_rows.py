# Copyright (c) 2026 Kenneth Stott
# Canary: 979c6e8b-25e0-4e4d-955c-446ec08af6f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's temporary table written in place (REQ-2176), on the stores that do so: the
embedded DuckDB store through its broker, and a PostgreSQL store on its own connection. Rows are
added and removed by key, a column added and a bigint widened to a double, and the table's key
indexed so no two rows share it."""

# Requirements: REQ-2176

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from provisa.federation.session_rows import DuckDBSessionRows, SqlAlchemySessionRows

pytestmark = pytest.mark.unit

SCHEMA, TABLE = "session_s", "cypher__anchor__Person"
COLUMNS = [("node_id", "text"), ("age", "bigint")]


def _batch(rows: list[tuple]) -> pa.RecordBatch:
    return pa.record_batch(
        [pa.array([r[0] for r in rows], pa.string()), pa.array([r[1] for r in rows], pa.int64())],
        names=["node_id", "age"],
    )


async def _exercise(rows) -> None:
    await rows.index(SCHEMA, TABLE, ("node_id",))
    await rows.insert(SCHEMA, TABLE, COLUMNS, _batch([("e:1", 1), ("e:2", 2), ("e:3", 3)]))
    await rows.add_column(SCHEMA, TABLE, "city", "text")
    await rows.retype(SCHEMA, TABLE, "age", "double")
    await rows.delete(SCHEMA, TABLE, ("node_id",), [("e:2",)])


@pytest.fixture
def duckdb_store(tmp_path):
    from provisa.federation.materialize_broker import _MAT_STORE_ALIAS, get_broker
    from provisa.federation.store_connection import _create_ddl

    broker = get_broker(str(tmp_path / "store.duckdb"))

    def _create(con) -> None:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {_MAT_STORE_ALIAS}."{SCHEMA}"')
        con.execute(_create_ddl(_MAT_STORE_ALIAS, SCHEMA, TABLE, COLUMNS))

    broker._run(_create)

    def read(sql: str) -> list[tuple]:
        return broker._run(
            lambda con: con.execute(
                sql.format(table=f'{_MAT_STORE_ALIAS}."{SCHEMA}"."{TABLE}"')
            ).fetchall(),
            read_only=True,
        )

    return DuckDBSessionRows(broker), read


@pytest.fixture
def postgres_store(control_plane):
    from sqlalchemy import create_engine, text

    engine = create_engine(control_plane.url)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{SCHEMA}"'))
        conn.execute(text(f'CREATE TABLE "{SCHEMA}"."{TABLE}" (node_id TEXT, age BIGINT)'))

    def read(sql: str) -> list[tuple]:
        with engine.connect() as conn:
            return [
                tuple(r)
                for r in conn.execute(text(sql.format(table=f'"{SCHEMA}"."{TABLE}"'))).fetchall()
            ]

    yield SqlAlchemySessionRows(control_plane.url), read
    engine.dispose()


@pytest.mark.parametrize("store", ["duckdb_store", "postgres_store"])
def test_rows_are_added_removed_and_widened_in_place(store, request):
    rows, read = request.getfixturevalue(store)
    asyncio.run(_exercise(rows))
    assert read("SELECT node_id, age, city FROM {table} ORDER BY node_id") == [
        ("e:1", 1.0, None),
        ("e:3", 3.0, None),
    ]
    assert all(isinstance(age, float) for _, age, _ in read("SELECT * FROM {table}"))


@pytest.mark.parametrize("store", ["duckdb_store", "postgres_store"])
def test_the_key_is_indexed_so_no_two_rows_share_it(store, request):
    rows, _read = request.getfixturevalue(store)
    asyncio.run(_exercise(rows))
    with pytest.raises(Exception, match=r"(?i)duplicate|unique|constraint"):
        asyncio.run(rows.insert(SCHEMA, TABLE, COLUMNS, _batch([("e:1", 9)])))

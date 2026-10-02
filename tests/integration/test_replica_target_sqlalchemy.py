# Copyright (c) 2026 Kenneth Stott
# Canary: 8a38d0d5-540c-420d-ad58-fbc86766a3d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in a store reached through SQLAlchemy is filled by batched inserts into a
build table and swapped in atomically, on each dialect that declares the swap."""

from __future__ import annotations

import os
import threading

import pyarrow as pa
import pytest
from sqlalchemy import create_engine, inspect, text

from provisa.federation.data_replicator import TargetWrite
from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_target import SqlAlchemyStoreTarget, build_table_name

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_COLUMNS = [("id", "integer"), ("name", "text"), ("amount", "numeric")]
_SCHEMA = replica_schema("swaptest")
_TABLE = "src__public__orders"


def _postgres_url() -> str:
    return f"postgresql+psycopg2://provisa:provisa@localhost:{os.environ['PG_PORT']}/provisa"


def _mariadb_url() -> str:
    return f"mysql+pymysql://root:provisa@localhost:{os.environ['MARIADB_PORT']}/provisa"


def _sqlserver_url() -> str:
    return (
        f"mssql+pyodbc://sa:Provisa_2026%21@localhost:{os.environ['SQLSERVER_PORT']}/master"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
    )


def _rows(n: int, tag: str) -> list[dict]:
    return [{"id": i, "name": f"{tag}{i}", "amount": i} for i in range(n)]


def _batch(rows: list[dict]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


def _target(sa) -> SqlAlchemyStoreTarget:
    return SqlAlchemyStoreTarget(
        sa, schema=_SCHEMA, table=_TABLE, columns=_COLUMNS, pk_columns=["id"]
    )


def _names(sa) -> list[str]:
    quote = sa.dialect.identifier_preparer.quote
    with sa.connect() as conn:
        found = conn.execute(
            text(f"SELECT {quote('name')} FROM {quote(_SCHEMA)}.{quote(_TABLE)} ORDER BY 1")
        )
        return [row[0] for row in found]


async def _build(sa, rows: list[dict], *, during_build=None) -> None:
    target = _target(sa)
    assert target.caps.writes == {TargetWrite.BULK_BATCH}
    assert target.caps.atomic_swap
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(_batch(part), part)
    if during_build is not None:
        during_build()
    await target.swap()


async def _swap_is_atomic(url: str) -> None:
    sa = create_engine(url)
    try:
        with sa.begin() as conn:
            if inspect(conn).has_table(_TABLE, schema=_SCHEMA):
                quote = sa.dialect.identifier_preparer.quote
                conn.execute(text(f"DROP TABLE {quote(_SCHEMA)}.{quote(_TABLE)}"))

        # First build: no replica stands; the build table becomes it.
        await _build(sa, _rows(5, "a"))
        assert _names(sa) == [f"a{i}" for i in range(5)]

        # Rebuild: a reader sees the previous replica, whole, until the swap, and never a
        # missing or half-filled one while it happens.
        seen: list[list[str]] = []
        failures: list[BaseException] = []
        stop = threading.Event()

        def _reader() -> None:
            try:
                while not stop.is_set():
                    seen.append(_names(sa))
            except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
                failures.append(exc)

        reader = threading.Thread(target=_reader)
        before: list[list[str]] = []
        reader.start()
        try:
            await _build(sa, _rows(3, "b"), during_build=lambda: before.append(_names(sa)))
        finally:
            stop.set()
            reader.join()
        assert not failures, failures
        assert before == [[f"a{i}" for i in range(5)]]
        old, new = [f"a{i}" for i in range(5)], [f"b{i}" for i in range(3)]
        assert all(names in (old, new) for names in seen), [n for n in seen if n not in (old, new)]
        assert _names(sa) == new

        # A build that is abandoned leaves the replica as it was and no build table behind.
        target = _target(sa)
        await target.begin()
        await target.write(_batch(_rows(2, "c")), _rows(2, "c"))
        await target.abort()
        assert _names(sa) == new
        with sa.connect() as conn:
            assert not inspect(conn).has_table(build_table_name(_TABLE), schema=_SCHEMA)
            assert inspect(conn).get_pk_constraint(_TABLE, schema=_SCHEMA)[
                "constrained_columns"
            ] == ["id"]
    finally:
        sa.dispose()


async def test_postgresql_swaps_the_build_table_in_atomically():
    await _swap_is_atomic(_postgres_url())


@pytest.mark.requires_mariadb
async def test_mariadb_swaps_the_build_table_in_atomically():
    await _swap_is_atomic(_mariadb_url())


@pytest.mark.requires_sqlserver
async def test_sqlserver_swaps_the_build_table_in_atomically():
    await _swap_is_atomic(_sqlserver_url())

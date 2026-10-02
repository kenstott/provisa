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
from sqlalchemy.sql.elements import quoted_name

from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_target import (
    LOAD_INSERT,
    LOAD_ODBC_ARRAY,
    LOAD_ORACLE_DIRECT_PATH,
    RENAME_IN_TRANSACTION,
    RENAME_PAIR,
    ROWS_IN_TRANSACTION,
    PostgresStoreTarget,
    SqlAlchemyStoreTarget,
    build_table_name,
    sqlalchemy_store_target,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_COLUMNS = [("id", "integer"), ("name", "text"), ("amount", "numeric")]
_SCHEMA = replica_schema("swaptest")
_TABLE = "src__public__orders"


def _Q(name: str) -> quoted_name:
    """``name`` as the inspector must look it up: by its exact case (Oracle folds otherwise)."""
    return quoted_name(name, quote=True)


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


def _oracle_url() -> str:
    return (
        f"oracle+oracledb://system:provisa@localhost:{os.environ['ORACLE_PORT']}"
        "/?service_name=FREEPDB1"
    )


def _target(sa, **overrides):
    return (
        sqlalchemy_store_target(
            sa, schema=_SCHEMA, table=_TABLE, columns=_COLUMNS, pk_columns=["id"]
        )
        if not overrides
        else SqlAlchemyStoreTarget(
            sa, schema=_SCHEMA, table=_TABLE, columns=_COLUMNS, pk_columns=["id"], **overrides
        )
    )


def _names(sa) -> list[str]:
    quote = sa.dialect.identifier_preparer.quote_identifier
    with sa.connect() as conn:
        found = conn.execute(
            text(f"SELECT {quote('name')} FROM {quote(_SCHEMA)}.{quote(_TABLE)} ORDER BY 1")
        )
        return [row[0] for row in found]


async def _build(sa, rows: list[dict], *, during_build=None, **overrides) -> None:
    target = _target(sa, **overrides)
    assert target.caps.atomic_swap
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(_batch(part), part)
    if during_build is not None:
        during_build()
    await target.swap()


async def _replace_is_atomic(url: str, *, expect: tuple[str, str] | None = None, **overrides):
    """A first build, then a rebuild under a concurrent reader that must see only the whole
    previous rows or the whole new rows, then an abandoned build that must leave no trace."""
    sa = create_engine(url)
    try:
        if expect is not None:
            probe = _target(sa, **overrides)
            assert (probe.load_method, probe.replace_method) == expect
        quote = sa.dialect.identifier_preparer.quote_identifier
        with sa.begin() as conn:
            if inspect(conn).has_table(_Q(_TABLE), schema=_Q(_SCHEMA)):
                conn.execute(text(f"DROP TABLE {quote(_SCHEMA)}.{quote(_TABLE)}"))

        # First build: no replica stands.
        await _build(sa, _rows(5, "a"), **overrides)
        old, new = [f"a{i}" for i in range(5)], [f"b{i}" for i in range(3)]
        assert _names(sa) == old

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
            await _build(
                sa, _rows(3, "b"), during_build=lambda: before.append(_names(sa)), **overrides
            )
        finally:
            stop.set()
            reader.join()
        assert not failures, failures
        assert before == [old]
        assert all(names in (old, new) for names in seen), [n for n in seen if n not in (old, new)]
        assert _names(sa) == new

        # An abandoned build leaves the replica as it was and no build table behind.
        target = _target(sa, **overrides)
        await target.begin()
        await target.write(_batch(_rows(2, "c")), _rows(2, "c"))
        await target.abort()
        assert _names(sa) == new
        with sa.connect() as conn:
            assert not inspect(conn).has_table(_Q(build_table_name(_TABLE)), schema=_Q(_SCHEMA))
            assert inspect(conn).get_pk_constraint(_Q(_TABLE), schema=_Q(_SCHEMA))[
                "constrained_columns"
            ] == ["id"]
    finally:
        sa.dispose()


async def test_postgresql_store_is_written_through_the_held_copy():
    sa = create_engine(_postgres_url())
    try:
        assert isinstance(_target(sa), PostgresStoreTarget)
    finally:
        sa.dispose()
    await _replace_is_atomic(_postgres_url())


@pytest.mark.requires_mariadb
async def test_mariadb_moves_both_names_in_one_rename():
    await _replace_is_atomic(_mariadb_url(), expect=(LOAD_INSERT, RENAME_PAIR))


@pytest.mark.requires_mariadb
async def test_mariadb_with_the_rename_ruled_out_replaces_rows_in_one_transaction():
    await _replace_is_atomic(
        _mariadb_url(), expect=(LOAD_INSERT, ROWS_IN_TRANSACTION), rename=False
    )


@pytest.mark.requires_sqlserver
async def test_sqlserver_loads_by_parameter_arrays_and_renames_in_one_transaction():
    await _replace_is_atomic(_sqlserver_url(), expect=(LOAD_ODBC_ARRAY, RENAME_IN_TRANSACTION))


@pytest.mark.requires_sqlserver
async def test_sqlserver_with_the_rename_ruled_out_replaces_rows_in_one_transaction():
    await _replace_is_atomic(
        _sqlserver_url(), expect=(LOAD_ODBC_ARRAY, ROWS_IN_TRANSACTION), rename=False
    )


@pytest.mark.requires_oracle
async def test_oracle_loads_by_direct_path_and_replaces_rows_in_one_transaction():
    await _replace_is_atomic(_oracle_url(), expect=(LOAD_ORACLE_DIRECT_PATH, ROWS_IN_TRANSACTION))

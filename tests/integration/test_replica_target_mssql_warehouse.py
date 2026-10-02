# Copyright (c) 2026 Kenneth Stott
# Canary: b111ae3e-ad21-4825-a1f1-6a9acc4d865b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in a T-SQL warehouse is filled by bulk inserts into a build table and its
rows replaced from it in one transaction. Proven here on SQL Server, which runs the same T-SQL."""

from __future__ import annotations

import os
import threading

import pyarrow as pa
import pytest

from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_target import build_table_name
from provisa.federation.replica_target_warehouse import MssqlWarehouseStoreTarget

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc/unixODBC not available")

pytestmark = [pytest.mark.integration, pytest.mark.requires_sqlserver, pytest.mark.asyncio]

_COLUMNS = [("id", "integer"), ("name", "text"), ("amount", "numeric")]
_SCHEMA = replica_schema("swaptest")
_TABLE = "src__public__orders"
_REF = f"[{_SCHEMA}].[{_TABLE}]"


def _connect(*, autocommit: bool = False):
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=localhost,{os.environ['SQLSERVER_PORT']};"
        "UID=sa;PWD=Provisa_2026!;Encrypt=yes;TrustServerCertificate=yes",
        autocommit=autocommit,
    )


def _target(*, transactional: bool = True) -> MssqlWarehouseStoreTarget:
    return MssqlWarehouseStoreTarget(
        _connect, schema=_SCHEMA, table=_TABLE, columns=_COLUMNS, transactional=transactional
    )


def _rows(n: int, tag: str) -> list[dict]:
    return [{"id": i, "name": f"{tag}{i}", "amount": i} for i in range(n)]


def _names() -> list[str]:
    conn = _connect(autocommit=True)
    try:
        return [r[0] for r in conn.cursor().execute(f"SELECT name FROM {_REF} ORDER BY 1")]
    finally:
        conn.close()


async def _build(rows: list[dict], *, during_build=None) -> None:
    target = _target()
    assert target.caps.atomic_swap
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(pa.RecordBatch.from_pylist(part), part)
    if during_build is not None:
        during_build()
    await target.swap()


async def test_the_replica_rows_are_replaced_in_one_transaction():
    admin = _connect(autocommit=True)
    try:
        admin.cursor().execute(f"IF OBJECT_ID('{_SCHEMA}.{_TABLE}') IS NOT NULL DROP TABLE {_REF}")
    finally:
        admin.close()

    await _build(_rows(5, "a"))
    old = [f"a{i}" for i in range(5)]
    assert _names() == old

    seen: list[list[str]] = []
    failures: list[BaseException] = []
    stop = threading.Event()

    def _reader() -> None:
        try:
            while not stop.is_set():
                seen.append(_names())
        except BaseException as exc:  # noqa: BLE001 - reported by the assertion below
            failures.append(exc)

    reader = threading.Thread(target=_reader)
    before: list[list[str]] = []
    reader.start()
    try:
        await _build(_rows(3, "b"), during_build=lambda: before.append(_names()))
    finally:
        stop.set()
        reader.join()
    new = [f"b{i}" for i in range(3)]
    assert not failures, failures
    assert before == [old]
    assert all(names in (old, new) for names in seen), [n for n in seen if n not in (old, new)]
    assert _names() == new

    target = _target()
    await target.begin()
    await target.write(pa.RecordBatch.from_pylist(_rows(2, "c")), _rows(2, "c"))
    await target.abort()
    assert _names() == new
    conn = _connect(autocommit=True)
    try:
        found = conn.cursor().execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (_SCHEMA, build_table_name(_TABLE)),
        )
        assert found.fetchone() is None
    finally:
        conn.close()


async def test_a_store_that_is_not_transactional_declares_no_atomic_swap():
    assert not _target(transactional=False).caps.atomic_swap

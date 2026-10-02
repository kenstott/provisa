# Copyright (c) 2026 Kenneth Stott
# Canary: baa373f4-1165-41ce-9cbe-9872fa615f5a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in Databricks is filled from Parquet streamed to a Unity Catalog volume
and replaced in one Delta commit, the replica's own table kept. Live warehouse; one throwaway
schema, dropped afterwards."""

from __future__ import annotations

import datetime
import os
from decimal import Decimal

import pyarrow as pa
import pytest

pytest.importorskip("databricks.sql", reason="databricks-sql-connector required")

from provisa.federation.replica_address import replica_schema  # noqa: E402
from provisa.federation.replica_target import build_table_name  # noqa: E402
from provisa.federation.replica_target_warehouse import (  # noqa: E402
    DATABRICKS_BUILD_VOLUME,
    DatabricksStoreTarget,
)
from tests.integration.databricks_warehouse import ensure_warehouse_running  # noqa: E402

_ENV = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not all(os.environ.get(k) for k in _ENV), reason="Databricks warehouse creds not set"
    ),
]

_CATALOG = "workspace"
_SCHEMA = replica_schema(f"swaptest{os.getpid()}")
_TABLE = "src__public__orders"
_REPLICA = f"`{_CATALOG}`.`{_SCHEMA}`.`{_TABLE}`"
_COLUMNS = [
    ("id", "integer"),
    ("name", "text"),
    ("amount", "numeric"),
    ("placed", "timestamp"),
    ("day", "date"),
    ("ok", "boolean"),
    ("doc", "json"),
]


def _connect():
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs

    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        **databricks_tls_kwargs(),
    )


def _rows(n: int, tag: str) -> list[dict]:
    return [
        {
            "id": i,
            "name": f"{tag}{i}",
            "amount": Decimal("1.5") + i,
            "placed": datetime.datetime(2026, 1, 2, 3, 4, 5),
            "day": datetime.date(2026, 1, 2),
            "ok": i % 2 == 0,
            "doc": {"k": [i, tag]},
        }
        for i in range(n)
    ]


@pytest.fixture
def warehouse():
    ensure_warehouse_running()
    conn = _connect()
    try:
        yield conn
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS `{_CATALOG}`.`{_SCHEMA}` CASCADE")
        cur.close()
        conn.close()


def _query(conn, sql: str) -> list:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]
    finally:
        cur.close()


def _target() -> DatabricksStoreTarget:
    return DatabricksStoreTarget(
        _connect,
        catalog=_CATALOG,
        schema=_SCHEMA,
        table=_TABLE,
        columns=_COLUMNS,
        pk_columns=["id"],
    )


async def _build(rows: list[dict]) -> None:
    target = _target()
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(pa.RecordBatch.from_pylist([{"id": r["id"]} for r in part]), part)
    await target.swap()


def _staged(conn) -> list:
    root = f"/Volumes/{_CATALOG}/{_SCHEMA}/{DATABRICKS_BUILD_VOLUME}"
    found = []
    for entry in _query(conn, f"LIST '{root}'"):
        if str(entry[1]).rstrip("/") == build_table_name(_TABLE):
            for attempt in _query(conn, f"LIST '{str(entry[0]).rstrip('/')}'"):
                found += _query(conn, f"LIST '{str(attempt[0]).rstrip('/')}'")
    return found


async def test_databricks_replaces_the_replica_in_one_commit_and_keeps_its_table(warehouse):
    await _build(_rows(5, "a"))
    got = _query(
        warehouse,
        f"SELECT `id`, `name`, `amount`, CAST(`placed` AS STRING), `day`, `ok`, `doc` "
        f"FROM {_REPLICA} ORDER BY 1",
    )
    assert len(got) == 5
    assert got[1][:3] == (1, "a1", Decimal("2.500000000"))
    assert got[1][3].startswith("2026-01-02 03:04:05")
    assert got[1][4:] == (datetime.date(2026, 1, 2), False, '{"k": [1, "a"]}')
    assert _staged(warehouse) == []

    created = _query(warehouse, f"DESCRIBE HISTORY {_REPLICA}")
    await _build(_rows(3, "b"))
    assert [r[0] for r in _query(warehouse, f"SELECT `name` FROM {_REPLICA} ORDER BY 1")] == [
        "b0",
        "b1",
        "b2",
    ]
    # The replica's own table stood through the rebuild: one more commit on the same history.
    assert len(_query(warehouse, f"DESCRIBE HISTORY {_REPLICA}")) == len(created) + 1
    assert _staged(warehouse) == []

    # An abandoned build leaves the replica as it was and nothing staged.
    target = _target()
    await target.begin()
    await target.write(pa.RecordBatch.from_pylist([{"id": 9}]), _rows(1, "c"))
    await target.abort()
    assert len(_query(warehouse, f"SELECT 1 FROM {_REPLICA}")) == 3
    assert _staged(warehouse) == []

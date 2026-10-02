# Copyright (c) 2026 Kenneth Stott
# Canary: 6d8c3414-921d-4844-8643-7cc43876efa2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915: a replica in Snowflake is filled by Arrow ingest into a build table and replaced in
one statement, the replica's own table (its key and grants) kept. Live account; one throwaway
database, dropped afterwards."""

from __future__ import annotations

import datetime
import os
from decimal import Decimal
from urllib.parse import quote

import pyarrow as pa
import pytest

from provisa.federation.replica_address import replica_schema
from provisa.federation.replica_target import build_table_name
from provisa.federation.replica_target_warehouse import SnowflakeStoreTarget, snowflake_adbc_connect

_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not all(os.environ.get(k) for k in _ENV), reason="Snowflake account creds not set"
    ),
]

_DB = f"PROVISA_REPLICA_TARGET_{os.getpid()}"
_SCHEMA = replica_schema("swaptest")
_TABLE = "src__public__orders"
_COLUMNS = [
    ("id", "integer"),
    ("name", "text"),
    ("amount", "numeric"),
    ("placed", "timestamp"),
    ("day", "date"),
    ("ok", "boolean"),
    ("doc", "json"),
]


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
def account():
    import snowflake.connector as sf

    conn = sf.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    conn.cursor().execute(f"CREATE DATABASE {_DB}")
    try:
        yield conn
    finally:
        conn.cursor().execute(f"DROP DATABASE IF EXISTS {_DB}")
        conn.close()


def _target() -> SnowflakeStoreTarget:
    url = (
        f"snowflake://{quote(os.environ['SNOWFLAKE_USER'], safe='')}:"
        f"{quote(os.environ['SNOWFLAKE_PASSWORD'], safe='')}@{os.environ['SNOWFLAKE_ACCOUNT']}"
        f"/{_DB}?warehouse={os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')}"
    )
    return SnowflakeStoreTarget(
        lambda: snowflake_adbc_connect(url, _DB),
        database=_DB,
        schema=_SCHEMA,
        table=_TABLE,
        columns=_COLUMNS,
        pk_columns=["id"],
    )


def _query(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


async def _build(rows: list[dict]) -> None:
    target = _target()
    await target.begin()
    for start in range(0, len(rows), 2):
        part = rows[start : start + 2]
        await target.write(pa.RecordBatch.from_pylist([{"id": r["id"]} for r in part]), part)
    await target.swap()


async def test_snowflake_replaces_the_replica_in_one_statement_and_keeps_its_table(account):
    replica = f'"{_DB}"."{_SCHEMA}"."{_TABLE}"'

    await _build(_rows(5, "a"))
    got = _query(
        account,
        f'SELECT "id", "name", "amount", "placed", "day", "ok", "doc":k[1]::string '
        f"FROM {replica} ORDER BY 1",
    )
    assert got[1] == (
        1,
        "a1",
        Decimal("2.500000000"),
        datetime.datetime(2026, 1, 2, 3, 4, 5),
        datetime.date(2026, 1, 2),
        False,
        "a",
    )
    assert len(got) == 5

    account.cursor().execute(f"GRANT SELECT ON TABLE {replica} TO ROLE PUBLIC")
    await _build(_rows(3, "b"))
    assert [r[0] for r in _query(account, f'SELECT "name" FROM {replica} ORDER BY 1')] == [
        "b0",
        "b1",
        "b2",
    ]
    # The replica's own table stood through the rebuild: its key and its grant are still on it.
    keys = _query(account, f"SHOW PRIMARY KEYS IN TABLE {replica}")
    assert [k[4] for k in keys] == ["id"]
    cur = account.cursor()
    cur.execute(f"SHOW GRANTS ON TABLE {replica}")
    names = [d[0] for d in cur.description]
    grants = {(g[names.index("privilege")], g[names.index("grantee_name")]) for g in cur.fetchall()}
    assert ("SELECT", "PUBLIC") in grants

    # An abandoned build leaves the replica as it was and no build table behind.
    target = _target()
    await target.begin()
    await target.write(pa.RecordBatch.from_pylist([{"id": 9}]), _rows(1, "c"))
    await target.abort()
    assert len(_query(account, f"SELECT 1 FROM {replica}")) == 3
    tables = _query(account, f'SHOW TABLES IN SCHEMA "{_DB}"."{_SCHEMA}"')
    assert build_table_name(_TABLE) not in {t[1] for t in tables}

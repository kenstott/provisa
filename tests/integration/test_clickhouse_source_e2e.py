# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: ClickHouse as a first-class NAMED SOURCE (REQ-986), reachable on ANY engine.

Registers ClickHouse through the REAL pool path (``SourcePool.add`` → registry ``create_driver`` →
``configure`` → ``connect`` over clickhouse-connect HTTP → ``execute``) and reads a live table — the
same client family the ClickHouse federation engine uses. Requires a reachable ClickHouse server
(CLICKHOUSE_HOST[/PORT/USER/PASSWORD]); skipped otherwise (CI-safe). ClickHouse-native Arrow reads for
the embedded (chdb) engine are covered in tests/unit/test_native_arrow_transport.py.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("clickhouse_connect", reason="clickhouse-connect required")

_HOST = os.environ.get("CLICKHOUSE_HOST")
pytestmark.append(
    pytest.mark.skipif(not _HOST, reason="ClickHouse server not set (CLICKHOUSE_HOST)")
)

from provisa.executor.pool import SourcePool  # noqa: E402

_SID = "ch_src_e2e"
_TABLE = "provisa_src_e2e_widgets"


@pytest.fixture
def seeded():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=_HOST or "localhost",
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    client.command(f"DROP TABLE IF EXISTS {_TABLE}")
    client.command(f"CREATE TABLE {_TABLE} (id Int64, name String) ENGINE = MergeTree ORDER BY id")
    client.command(f"INSERT INTO {_TABLE} VALUES (1,'a'),(2,'b'),(3,'c')")
    try:
        yield _TABLE
    finally:
        client.command(f"DROP TABLE IF EXISTS {_TABLE}")
        client.close()


@pytest.mark.asyncio
async def test_clickhouse_named_source_reads_through_source_pool(seeded):
    pool = SourcePool()
    await pool.add(
        source_id=_SID,
        source_type="clickhouse",
        host=_HOST or "localhost",
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        database="default",
        user=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    assert pool.has(_SID)
    assert pool.dialect_for(_SID) == "clickhouse"
    driver = pool.get(_SID)
    result = await driver.execute(f"SELECT id, name FROM {seeded} ORDER BY id")
    assert result.column_names == ["id", "name"]
    assert result.rows == [(1, "a"), (2, "b"), (3, "c")]
    await driver.close()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])

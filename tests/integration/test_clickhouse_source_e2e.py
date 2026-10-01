# Copyright (c) 2026 Kenneth Stott
# Canary: 0175312f-2c22-497a-aac5-d94e44e4d0cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: ClickHouse as a first-class NAMED SOURCE (REQ-986), reachable on ANY engine.

Registers ClickHouse through the REAL pool path (``SourcePool.add`` → registry ``create_driver`` →
``configure`` → ``connect`` over clickhouse-connect HTTP → ``execute``) and reads a live table — the
same client family the ClickHouse federation engine uses. The ``requires_clickhouse`` marker makes
``tests/conftest.py`` provision a ClickHouse server (docker-compose.test.yml) per-test and export
CLICKHOUSE_HOST/PORT, so this runs on every isolated run — no external creds. ClickHouse-native
Arrow reads for the embedded (chdb) engine are covered in tests/unit/test_native_arrow_transport.py.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_clickhouse]

pytest.importorskip("clickhouse_connect", reason="clickhouse-connect required")

_HOST = os.environ.get("CLICKHOUSE_HOST")

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


def test_concurrent_requests_on_one_worker_each_get_their_own_session(seeded):
    """REQ-1882: every request runs on its own thread and a connection is used by one request at a
    time. The driver held ONE clickhouse-connect client — one ClickHouse session — for the whole
    worker, so a second in-flight request failed with "Attempt to execute concurrent queries
    within the same session". Eight requests in flight at once on one driver must all succeed."""
    import asyncio
    import threading

    from provisa.core.connection_loop import connection_loop

    pool = SourcePool()
    asyncio.run(
        pool.add(
            source_id=_SID,
            source_type="clickhouse",
            host=_HOST or "localhost",
            port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
            database="default",
            user=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            max_size=4,
        )
    )
    n = 8
    start = threading.Barrier(n)
    results: list = [None] * n
    errors: list[BaseException] = []

    def _request(i: int) -> None:
        try:
            start.wait(timeout=30)
            with connection_loop() as cl:
                # sleep() keeps each statement in flight long enough to overlap the others
                results[i] = cl.run(
                    pool.execute(_SID, f"SELECT count() AS n, sleep(0.5) AS s FROM {seeded}")
                )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_request, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    asyncio.run(pool.close_all())
    assert not errors, errors[:2]
    assert [r.rows[0][0] for r in results] == [3] * n


if __name__ == "__main__":
    pytest.main([__file__, "-q"])

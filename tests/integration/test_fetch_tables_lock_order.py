# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2b6e41-7c3a-4f85-b0e9-2a6c1f8d4e37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of the registered tables beside a run of schema.sql on the same org schema: no deadlock.

schema.sql runs as one transaction and its ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` blocks take
an ACCESS EXCLUSIVE lock on ``registered_tables`` first and on ``table_columns`` after. A second
runtime of an org runs it (init_schema) while the first one's schema-rebuild lifecycle wiring reads
the tables with ``fetch_tables``. Its columns statement named ``table_columns`` before a subquery on
``registered_tables``, so it held ``table_columns`` while it waited for ``registered_tables``: the
two orders crossed, and PostgreSQL killed the read (bundle CI run 42084411583, retail).

The read is paused just before its columns statement, the DDL transaction takes
``registered_tables`` the way schema.sql does, the read goes on and waits, and only then does the
DDL ask for ``table_columns``. Every lock is PostgreSQL's own; only the pause is the test's.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import psycopg
import pytest
from psycopg import sql

from provisa.api.admin.db_queries import fetch_tables
from provisa.core.environments import org_schema

if TYPE_CHECKING:
    from provisa.core.database import Connection

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_STEP_TIMEOUT = 30.0


class _PausedBeforeColumns:
    """The read's connection, held before the statement that reads ``table_columns`` until the DDL
    transaction holds ``registered_tables``. Records the read's backend pid for the wait check."""

    def __init__(self, conn: Any, at_columns: threading.Event, ddl_holds: threading.Event) -> None:
        self._conn = conn
        self._at_columns = at_columns
        self._ddl_holds = ddl_holds
        self.pid: int | None = None

    async def fetch(self, sql: str, *args: Any) -> list:
        if "FROM table_columns" in sql or "JOIN table_columns" in sql:
            self.pid = (await self._conn.fetch("SELECT pg_backend_pid() AS pid"))[0]["pid"]
            self._at_columns.set()
            assert self._ddl_holds.wait(_STEP_TIMEOUT), "the DDL never took registered_tables"
        return await self._conn.fetch(sql, *args)


@pytest.fixture
async def org(docker_postgres):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"lockorder{uuid.uuid4().hex[:8]}"
    password = os.environ.get("PG_PASSWORD", "provisa")
    host, port = docker_postgres["host"], docker_postgres["port"]
    engine = create_engine_from_url(
        f"postgresql+psycopg://provisa:{password}@{host}:{port}/provisa", pool_size=2
    )
    db = Database(engine, name="org", search_path=org_schema(org_id))
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(db, schema_sql, org_id=org_id)
    conninfo = f"host={host} port={port} dbname=provisa user=provisa password={password}"
    try:
        yield db, org_schema(org_id), conninfo
    finally:
        with psycopg.connect(conninfo, autocommit=True) as drop:
            for name in (org_schema(org_id), org_schema(org_id, None, "_mv_cache")):
                drop.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name))
                )
        await db.close()


def _wait_until_blocked(monitor: psycopg.Connection, pid: int) -> None:
    deadline = time.monotonic() + _STEP_TIMEOUT
    while time.monotonic() < deadline:
        row = monitor.execute(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid = %s", (pid,)
        ).fetchone()
        if row is not None and row[0] == "Lock":
            return
    raise AssertionError(f"the read (pid {pid}) never waited on a lock")


async def test_read_beside_schema_sql_ddl_does_not_deadlock(org):
    db, schema, conninfo = org
    at_columns, ddl_holds = threading.Event(), threading.Event()
    outcome: dict[str, Any] = {}
    paused: list[_PausedBeforeColumns] = []

    async def read() -> None:
        async with db.acquire() as conn:
            proxy = _PausedBeforeColumns(conn, at_columns, ddl_holds)
            paused.append(proxy)
            outcome["tables"] = await fetch_tables(cast("Connection", proxy), catalog=False)

    def reader() -> None:
        try:
            asyncio.run(read())
        except BaseException as exc:  # handed to the test thread, which raises it
            outcome["error"] = exc

    thread = threading.Thread(target=reader, name="fetch-tables-reader")
    thread.start()
    with (
        psycopg.connect(conninfo) as ddl,
        psycopg.connect(conninfo, autocommit=True) as monitor,
    ):
        ddl.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        assert at_columns.wait(_STEP_TIMEOUT), "the read never reached its columns statement"
        # schema.sql's order: registered_tables, then table_columns, in one transaction.
        ddl.execute("ALTER TABLE registered_tables ADD COLUMN IF NOT EXISTS alias TEXT")
        ddl_holds.set()
        pid = paused[0].pid
        assert pid is not None
        _wait_until_blocked(monitor, pid)
        ddl.execute("ALTER TABLE table_columns ADD COLUMN IF NOT EXISTS alias TEXT")
        ddl.commit()
    thread.join(_STEP_TIMEOUT)
    assert not thread.is_alive(), "the read never finished"
    if "error" in outcome:
        raise outcome["error"]
    assert outcome["tables"] == []

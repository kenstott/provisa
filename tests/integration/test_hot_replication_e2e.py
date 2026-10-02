# Copyright (c) 2026 Kenneth Stott
# Canary: cb2dc676-b1c7-4985-b4e7-1625b52b6bb0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-826): a busy table is replicated, then read from its replica by EVERY process of the
deployment, with no restart; when its traffic falls away it is read live again.

Two real Provisa server processes share one org: one control plane, one Redis (the Hot counts),
one engine store. A table set to Hot-3 is read through both until the statements that read it —
counted across the two — pass 3 in the interval. One process (the scheduler holder) promotes it
and requests its build; a process builds the replica; the replica-state stamp tells the other,
which republishes its routes. From then on both read the replica: the upstream table is renamed
away, so a read that reached the source would fail with "does not exist".

Then the upstream is put back and the reads stop. The table falls below half its threshold, is
demoted, and both processes read the source again — they see a row added after the demotion.

Runs on the DuckDB engine (the store is one DuckDB file both processes open in turn) and on the
test stack's Trino.
"""

# Requirements: REQ-826, REQ-1912, REQ-1914, REQ-1915, REQ-1920

from __future__ import annotations

import asyncio
import os
import tempfile
import time
from pathlib import Path

import httpx
import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ORG = "hot_repl_e2e"
_CONFIG = "tests/fixtures/hot_replication_config.yaml"
_SCHEMA = "hot_repl_e2e"
_ROWS = [(1, "one"), (2, "two"), (3, "three")]
_ROLE = "org_admin"


async def _pg():
    import asyncpg

    return await asyncpg.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
        database=os.environ.get("PG_DATABASE", "provisa"),
        timeout=15,
    )


def _upstream(*statements: str) -> None:
    async def _go() -> None:
        conn = await _pg()
        try:
            for statement in statements:
                await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(_go())


def _seed() -> None:
    _upstream(
        f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE",
        f"CREATE SCHEMA {_SCHEMA}",
        *(
            statement
            for table in ("busy_items", "quiet_items")
            for statement in (
                f"CREATE TABLE {_SCHEMA}.{table} (id int PRIMARY KEY, name text)",
                f"INSERT INTO {_SCHEMA}.{table} VALUES "
                + ", ".join(f"({i}, '{n}')" for i, n in _ROWS),
            )
        ),
    )


@pytest.fixture(scope="module", params=["duckdb", "trino"])
def deployment(request):
    """Two server processes of one deployment: same org, control plane, Redis and store."""
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    _seed()
    org = f"{_ORG}_{request.param}"
    store_dir = tempfile.TemporaryDirectory()
    kwargs: dict = {"engine": request.param, "config": _CONFIG, "control_plane": "postgres"}
    if request.param == "duckdb":
        kwargs["materialize_store_url"] = f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}"
    a = IsolatedServer(org, **kwargs)
    b = IsolatedServer(org, **kwargs)
    a.start()
    try:
        b.start()
        try:
            yield a, b
        finally:
            b.stop_process()
    finally:
        a.stop_process()
        store_dir.cleanup()
        _upstream(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        asyncio.run(drop_org_schema(org))


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql", json={"sql": sql, "role": _ROLE}, timeout=120.0
    )


def _ids(resp: httpx.Response) -> list[int]:
    body = resp.json()
    rows = body.get("data") or body.get("rows") or []
    if isinstance(rows, dict):
        rows = next(iter(rows.values()))
    return sorted(int(r["id"]) for r in rows)


def _read(server, table: str = "busy_items") -> list[int]:
    resp = _sql(server, f"SELECT id, name FROM {table} ORDER BY id")
    assert resp.status_code == 200, _explain(server, resp)
    return _ids(resp)


def _explain(server, resp: httpx.Response) -> str:
    return (
        f"{resp.status_code} {resp.text}\n--- server log ---\n{server.dump_stderr_debug()[-5000:]}"
    )


def _admin(server, query: str) -> dict:
    resp = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120.0,
    )
    assert resp.status_code == 200, _explain(server, resp)
    body = resp.json()
    assert not body.get("errors"), body
    return body["data"]


def _kept(server) -> dict[str, str]:
    """``{table: kind}`` of the tables this process says are replicated for being busy."""
    return {
        row["tableName"]: row["kind"]
        for row in _admin(server, "{ hotTables { tableName kind } }")["hotTables"]
        if row["kind"].startswith("replica")
    }


def _summary(server, table: str) -> dict:
    tables = _admin(server, "{ tables { tableName refreshPolicySummary { text serving } } }")
    return next(t for t in tables["tables"] if t["tableName"] == table)["refreshPolicySummary"]


def _direct_hint(server, field: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": f"# @provisa route=direct\n{{ {field} {{ id name }} }}"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120.0,
    )


def _where_it_stands(*servers) -> str:
    """What the deployment holds, for a timeout's message: the Hot counts in Redis, the replica
    state in the control plane, what each process says of the table, and each process's log."""
    import redis

    out: list[str] = []
    r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    counts = {k: r.get(k) for k in r.scan_iter("provisa:replica_hot:*")}
    out.append(f"hot counts in Redis: {counts}")

    async def _state() -> list:
        conn = await _pg()
        try:
            schemas = await conn.fetch(
                "SELECT table_schema FROM information_schema.tables "
                "WHERE table_name = 'replica_state' AND table_schema LIKE $1",
                f"%{_ORG}%",
            )
            rows: list = []
            for schema in schemas:
                name = schema["table_schema"]
                rows += [
                    (name, dict(row))
                    for row in await conn.fetch(
                        f"SELECT table_name, promoted, build_state, requested_reason, "
                        f'completed_at, built_store, last_error, waiting_on FROM "{name}".replica_state'
                    )
                ]
                rows += [
                    (name, dict(row))
                    for row in await conn.fetch(f'SELECT kind, stamp FROM "{name}".config_stamp')
                ]
            return rows
        finally:
            await conn.close()

    out.append(f"replica state: {asyncio.run(_state())}")
    for server in servers:
        out.append(f"{server.base_url} says: {_summary(server, 'busy_items')} kept={_kept(server)}")
        out.append(f"--- log of {server.base_url} ---\n{server.dump_stderr_debug()[-2500:]}")
    return "\n".join(out)


def _until(what: str, check, *, servers, keep_busy=None, timeout: float = 150.0) -> None:
    """Poll ``check()`` until it is true. ``keep_busy()`` runs between polls — the reads that
    keep the table past its threshold while the deployment catches up."""
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting until {what}\n{_where_it_stands(*servers)}")
        if keep_busy is not None:
            keep_busy()
        time.sleep(0.5)


def test_a_busy_table_is_replicated_for_every_process_then_read_live_again(deployment):
    a, b = deployment
    pids = (a._proc.pid, b._proc.pid)

    # -- live: below its threshold the table is read from the source, by both ---------------------
    assert _read(a) == [1, 2, 3]
    assert _summary(b, "busy_items")["serving"] == "live"
    assert "3 governed statements per 4s" in _summary(b, "busy_items")["text"]
    assert _kept(a) == {} and _kept(b) == {}

    # -- busy: statements through BOTH processes count as one table's traffic ---------------------
    def _traffic() -> None:
        assert _read(a) == [1, 2, 3]
        assert _read(b) == [1, 2, 3]

    _until(
        "both processes serve busy_items from its replica",
        lambda: _kept(a).get("busy_items") == "replica" and _kept(b).get("busy_items") == "replica",
        keep_busy=_traffic,
        servers=(a, b),
    )
    assert _summary(a, "busy_items")["serving"] == "cache"
    assert _summary(b, "busy_items")["serving"] == "cache"
    # the table left at Default never reached the global threshold
    assert "quiet_items" not in _kept(a)
    assert _summary(a, "quiet_items")["serving"] == "live"

    # -- from the replica: the upstream is gone, and both still answer ---------------------------
    _upstream(f"ALTER TABLE {_SCHEMA}.busy_items RENAME TO busy_items_gone")
    try:
        for server in (a, b):
            assert _read(server) == [1, 2, 3], "a read reached the upstream"
            refused = _direct_hint(server, "busyItems")
            assert refused.status_code == 403, _explain(server, refused)
            assert refused.json()["code"] == "query.operator_floor", refused.text
            assert "replicate" in refused.text
            _traffic()  # keep it busy while it is being proven
    finally:
        _upstream(f"ALTER TABLE {_SCHEMA}.busy_items_gone RENAME TO busy_items")

    # -- quiet: no statement reads it; below half its threshold it is demoted --------------------
    _until(
        "both processes read busy_items live again",
        lambda: "busy_items" not in _kept(a) and "busy_items" not in _kept(b),
        servers=(a, b),
    )
    assert _summary(a, "busy_items")["serving"] == "live"
    assert _summary(b, "busy_items")["serving"] == "live"
    _upstream(f"INSERT INTO {_SCHEMA}.busy_items VALUES (4, 'four')")
    for server in (a, b):
        assert _read(server) == [1, 2, 3, 4], "the demoted table was not read live"
    allowed = _direct_hint(b, "busyItems")
    assert allowed.status_code == 200 and not allowed.json().get("errors"), _explain(b, allowed)

    # -- and none of it took a restart ------------------------------------------------------------
    assert a._proc.poll() is None and b._proc.poll() is None
    assert (a._proc.pid, b._proc.pid) == pids
    for server in (a, b):
        log = server.dump_stderr_debug()
        assert "Hot promotion evaluation failed" not in log, log[-3000:]
        assert "could not size the table" not in log, log[-3000:]

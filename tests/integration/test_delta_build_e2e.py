# Copyright (c) 2026 Kenneth Stott
# Canary: 9e46ea9a-355d-4f03-bfee-6d62e92fc2c2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E (REQ-874): a replicated SQL-source table with a delta is refreshed incrementally.

A real DuckDB-engine server over a real source Postgres. A table with a watermark column and a
delta declaration (upsert on the PK, tombstone deletes) is replicated and floored, so every read is
served from its replica. After the first whole build, each source change -- an insert, an update,
and a tombstone delete -- reaches a read, and the build that carries it is a DELTA that copied only
the changed rows (``method == "delta"``, ``rowsCopied == 1``), never the whole table.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.test_pg_engine_landing_never_writes_source_e2e import _ROLE, _SourceAndEngine

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        # org_admin is the reserved administrative role (REQ-1349): every org has it and a
        # config file may not declare it.
        "roles": [],
        "sources": [],
        "tables": [],
    }


@pytest.fixture
def databases():
    pg = _SourceAndEngine()
    pg.start()
    # A delta source table: a monotonic watermark (updated_at), a primary key, a tombstone flag.
    pg.update_source(
        "CREATE TABLE inc_orders (id INTEGER PRIMARY KEY, amount INTEGER, "
        "updated_at INTEGER, is_deleted BOOLEAN)"
    )
    pg.update_source("INSERT INTO inc_orders VALUES (1, 10, 1, false), (2, 20, 2, false)")
    try:
        yield pg
    finally:
        pg.stop()


def _server(pg: _SourceAndEngine, workdir: str):
    from tests.integration.isolated_server import IsolatedServer

    path = Path(workdir) / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    store = Path(workdir) / "materialize.duckdb"
    srv = IsolatedServer(
        "delta_build_e2e",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{store}",
    )
    return srv, store


def _admin(srv, mutation: str) -> dict:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert r.status_code == 200, r.text
    assert "errors" not in r.json(), r.text
    (result,) = r.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _poke(srv) -> None:
    """Hit the data path to request the floored table's next build (the read itself is served from
    the replica by other surfaces; here it only drives the build runner)."""
    httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": 'SELECT "id" FROM "inc_orders"', "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _store_ids(store: Path):
    """The id -> amount map the replica holds in the store, or None until the replica table exists."""
    import duckdb

    try:
        con = duckdb.connect(str(store), read_only=True)
    except duckdb.IOException:
        return None
    try:
        hit = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE schema_name LIKE '%\\_replicas' ESCAPE '\\' "
            "AND table_name LIKE '%inc\\_orders' ESCAPE '\\'"
        ).fetchall()
        if not hit:
            return None
        schema, table = hit[0]
        rows = con.execute(f'SELECT "id", "amount" FROM "{schema}"."{table}"').fetchall()
        return {int(r[0]): int(r[1]) for r in rows}
    finally:
        con.close()


def _builds_since(srv, after_id: int) -> list[dict]:
    """The inc_orders builds that ended after build-log row ``after_id``, oldest first, read from
    ``ops.replica_build_log`` (REQ-1915): one row a build, so every build is seen, not only the
    latest one."""
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={
            "sql": 'SELECT id, outcome, method, rows_copied FROM "ops"."replica_build_log" '
            f"WHERE source_id = 'src' AND table_name = 'inc_orders' AND id > {after_id} "
            "ORDER BY id",
            "role": _ROLE,
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert r.status_code == 200, r.text
    return r.json()["data"]["sql"]


def _last_build_id(srv) -> int:
    builds = _builds_since(srv, 0)
    return max((int(b["id"]) for b in builds), default=0)


def _wait(srv, store, want: dict[int, int], after_id: int, *, seconds: float = 120) -> list[dict]:
    """Poll the store until the replica holds exactly ``want``, then the build log until the build
    that carried the change has ended; return every build that ended after ``after_id``.

    The table's 1-second TTL has the runner rebuild it about every second on its own, so the
    table's LATEST build is usually an empty delta that ran after the one that carried the change
    (and the store is written before that build's record is): only the log shows which build
    carried it."""
    import duckdb

    deadline = time.monotonic() + seconds
    while True:
        try:
            got = _store_ids(store)
        except duckdb.IOException:
            got = None
        if got == want:
            break
        builds = _builds_since(srv, after_id)
        assert time.monotonic() < deadline, (
            f"timed out: replica holds {got}, want {want}; builds since: {builds}"
        )
        _poke(srv)
        time.sleep(1)
    while True:
        builds = _builds_since(srv, after_id)
        if any(b["rows_copied"] for b in builds):
            return builds
        assert time.monotonic() < deadline, (
            f"timed out waiting for the build that carried {want} to be logged: {builds}"
        )
        time.sleep(1)


def _assert_one_delta_row(builds: list[dict]) -> None:
    """Every build since the change was a delta, none failed, and exactly one of them read a row:
    the changed one -- never the whole table."""
    assert all(b["outcome"] == "completed" for b in builds), builds
    assert all(b["method"] == "delta" for b in builds), builds
    carried = [b for b in builds if b["rows_copied"]]
    assert len(carried) == 1, builds
    assert int(carried[0]["rows_copied"]) == 1, builds  # only the delta row, not the whole table


def test_a_delta_table_applies_insert_update_and_tombstone_incrementally(databases):
    pg = databases
    with tempfile.TemporaryDirectory() as workdir:
        srv, store = _server(pg, workdir)
        try:
            srv.start()
            _admin(
                srv,
                'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
                f'port: {pg.source_port}, database: "shop", username: "provisa", '
                'password: "provisa"})',
            )
            _admin(
                srv,
                'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
                'tableName: "inc_orders", watermarkColumn: "updated_at", '
                'delta: {apply: "upsert", deletes: "tombstone", tombstoneColumn: "is_deleted"}, '
                "columns: ["
                '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
                '{name: "amount", visibleTo: ["org_admin"], dataType: "integer"}, '
                '{name: "updated_at", visibleTo: ["org_admin"], dataType: "integer"}, '
                '{name: "is_deleted", visibleTo: ["org_admin"], dataType: "boolean"}]})',
            )
            # REQ-1921: a table registered through the admin starts as a draft; it is released into
            # service before it is read.
            # Floor the table so every read is served from its replica, with a short landing TTL so
            # the build runner re-refreshes quickly (REQ-1907).
            _admin(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 1)')
            _admin(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')

            # First build is a whole copy (no cursor yet); the replica serves the two seed rows.
            _wait(srv, store, {1: 10, 2: 20}, 0)

            # INSERT: a delta carries only the new row.
            mark = _last_build_id(srv)
            pg.update_source("INSERT INTO inc_orders VALUES (3, 30, 3, false)")
            _assert_one_delta_row(_wait(srv, store, {1: 10, 2: 20, 3: 30}, mark))

            # UPDATE: a delta carries only the changed row.
            mark = _last_build_id(srv)
            pg.update_source("UPDATE inc_orders SET amount = 111, updated_at = 4 WHERE id = 1")
            _assert_one_delta_row(_wait(srv, store, {1: 111, 2: 20, 3: 30}, mark))

            # TOMBSTONE: the flagged row deletes its key from the replica, via one delta row.
            mark = _last_build_id(srv)
            pg.update_source("UPDATE inc_orders SET is_deleted = true, updated_at = 5 WHERE id = 2")
            _assert_one_delta_row(_wait(srv, store, {1: 111, 3: 30}, mark))
        finally:
            srv.stop_process()

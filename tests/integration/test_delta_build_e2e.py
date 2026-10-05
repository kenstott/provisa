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
        "roles": [
            {
                "id": _ROLE,
                "capabilities": [
                    "source_registration",
                    "table_registration",
                    "query_development",
                    "access_config",
                ],
                "domain_access": ["*"],
            }
        ],
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
        control_plane="sqlite",
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


def _last_build(srv) -> dict | None:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ replicaBuilds { builds { tableName state method rowsCopied completedAt "
            "deltaSkipped lastError lastErrorCode } } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )
    assert r.status_code == 200, r.text
    builds = [
        b for b in r.json()["data"]["replicaBuilds"]["builds"] if b["tableName"] == "inc_orders"
    ]
    return builds[0] if builds else None


def _wait(srv, store, want: dict[int, int], *, seconds: float = 120) -> dict:
    """Poke the build runner and poll the store until the replica holds exactly ``want``; return the
    last build record. Raises on timeout."""
    import duckdb

    deadline = time.monotonic() + seconds
    while True:
        # The store is read BEFORE the next poke: a poke after the build that produced ``want``
        # would start another (an empty delta once the cursor has caught up), and the record read
        # below is the table's latest build, so it would report that one's 0 rows instead.
        try:
            got = _store_ids(store)
        except duckdb.IOException:
            got = None
        if got == want:
            return _last_build(srv) or {}
        assert time.monotonic() < deadline, (
            f"timed out: replica holds {got}, want {want}; last build: {_last_build(srv)}"
        )
        _poke(srv)
        time.sleep(2)


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
            # Floor the table so every read is served from its replica, with a short landing TTL so
            # the build runner re-refreshes quickly (REQ-1907).
            _admin(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 1)')
            _admin(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')

            # First build is a whole copy (no cursor yet); the replica serves the two seed rows.
            _wait(srv, store, {1: 10, 2: 20})

            # INSERT: a delta carries only the new row.
            pg.update_source("INSERT INTO inc_orders VALUES (3, 30, 3, false)")
            b = _wait(srv, store, {1: 10, 2: 20, 3: 30})
            assert b.get("method") == "delta", b
            assert b.get("rowsCopied") == 1, b  # only the delta row was read, not the whole table

            # UPDATE: a delta carries only the changed row.
            pg.update_source("UPDATE inc_orders SET amount = 111, updated_at = 4 WHERE id = 1")
            b = _wait(srv, store, {1: 111, 2: 20, 3: 30})
            assert b.get("method") == "delta", b
            assert b.get("rowsCopied") == 1, b

            # TOMBSTONE: the flagged row deletes its key from the replica, via one delta row.
            pg.update_source("UPDATE inc_orders SET is_deleted = true, updated_at = 5 WHERE id = 2")
            b = _wait(srv, store, {1: 111, 3: 30})
            assert b.get("method") == "delta", b
            assert b.get("rowsCopied") == 1, b
        finally:
            srv.stop_process()

# Copyright (c) 2026 Kenneth Stott
# Canary: 73de2470-5667-42ea-8d58-e72d87c971aa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Integration: a files source's file_glob table is ONE logical table over its matched files (REQ-788).

A real server (DuckDB engine, SQLite control plane) whose files source reads through the bundled
Calcite file adapter (the current pinned bundle, fetched by the normal resolver). The model declares
a files source over a directory of three CSVs and two tables whose file_glob matches all three:
- ``orders_live`` is read live through the adapter — the merge happens in the adapter;
- ``orders_rep`` is replicated (replicate=0) and served from its replica, landed the generic way
  from the adapter's connection.
Both return the union of the three files as one table over /data/sql. Everything here is the
``test`` instance. The refusal-by-name and the source-file column need the new adapter and are a
separate e2e."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ROLE = "org_admin"


def _config(data_dir: str) -> dict:
    v = {"visible_to": [_ROLE]}
    cols = [
        {"name": "id", "data_type": "integer", "is_primary_key": True, **v},
        {"name": "amount", "data_type": "integer", **v},
    ]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "sales", "description": "Sales"}],
        # org_admin is the reserved administrative role (REQ-1349): every org has it and a
        # config file may not declare it.
        "roles": [],
        "sources": [{"id": "files", "type": "files", "path": f"{data_dir}/orders"}],
        "tables": [
            {
                "source_id": "files",
                "domain_id": "sales",
                "schema": "main",
                "table": "orders_live",
                "file_glob": "*.csv",
                "columns": cols,
            },
            {
                "source_id": "files",
                "domain_id": "sales",
                "schema": "main",
                "table": "orders_rep",
                "file_glob": "*.csv",
                "replicate": 0,
                "cache_ttl": 3600,  # REQ-1907: a landing table needs its own TTL
                "columns": cols,
            },
        ],
        "relationships": [],
        "rls_rules": [],
        "functions": [],
        "webhooks": [],
    }


@pytest.fixture
def served():
    from tests.integration.isolated_server import IsolatedServer

    workdir = tempfile.TemporaryDirectory()
    work = Path(workdir.name)
    data = work / "orders"
    data.mkdir()
    (data / "a.csv").write_text("id,amount\n1,10\n2,20\n")
    (data / "b.csv").write_text("id,amount\n3,30\n")
    (data / "c.csv").write_text("id,amount\n4,40\n5,50\n")
    (work / "config.yaml").write_text(yaml.safe_dump(_config(str(work))))
    store = work / "materialize.duckdb"
    server = IsolatedServer(
        "file_glob_merge_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{store}",
    )
    server.start()
    try:
        yield server, store
    finally:
        server.stop_process()
        workdir.cleanup()


_last_err: dict = {}


def _ids(srv, table: str) -> list[int] | None:
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": f'SELECT "id" FROM "{table}" ORDER BY "id"', "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    if r.status_code != 200:
        _last_err[table] = f"{r.status_code} {r.text}"
        return None
    rows = r.json()["data"]["sql"]  # {"data": {"sql": [ {col: val}, ... ]}, "columns": [...]}
    if not rows:
        _last_err[table] = f"200 but no data: {r.text[:300]}"
        return None
    return sorted(int(row["id"]) for row in rows)  # adapter types CSV columns as text


def _builds(srv) -> list[dict]:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ replicaBuilds { builds { sourceId tableName state rowsCopied "
            "completedAt lastError } } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert r.status_code == 200, r.text
    assert "errors" not in r.json(), r.text
    return r.json()["data"]["replicaBuilds"]["builds"]


def _replica_ids(store: Path) -> list[int] | None:
    """The ids landed for orders_rep in the store's replicas schema, or None until it exists."""
    import duckdb

    try:
        con = duckdb.connect(str(store), read_only=True)
    except duckdb.IOException:  # the server holds the file for this instant
        return None
    try:
        hit = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE schema_name LIKE '%\\_replicas' ESCAPE '\\' AND table_name LIKE '%orders\\_rep' "
            "ESCAPE '\\'"
        ).fetchall()
        if not hit:
            return None
        schema, table = hit[0]
        rows = con.execute(f'SELECT "id" FROM "{schema}"."{table}"').fetchall()
        return sorted(int(r[0]) for r in rows)
    finally:
        con.close()


def _wait(fn, *, seconds: float, what: str):
    import duckdb

    deadline = time.monotonic() + seconds
    while True:
        try:
            got = fn()
        except duckdb.IOException:
            got = None
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(1)


def test_a_files_glob_table_merges_its_files_live_and_from_replica(served):
    srv, store = served
    expected = [1, 2, 3, 4, 5]
    # Served live through the adapter: the merge of the three CSVs happens in the file adapter.
    deadline = time.monotonic() + 30
    live = None
    while time.monotonic() < deadline:
        live = _ids(srv, "orders_live")
        if live == expected:
            break
        time.sleep(2)
    assert live == expected, f"live glob read: {live} / {_last_err.get('orders_live')}"

    # Served from its replica (replicate=0 → Always): the replicator reads the merged glob table
    # from the adapter's connection and lands the union; the build is recorded and the copy sits at
    # the resolver's address in the store.
    def built():
        rep = [b for b in _builds(srv) if b["tableName"] == "orders_rep"]
        return rep[0] if rep and rep[0]["completedAt"] else None

    rep_build = _wait(built, seconds=120, what="the orders_rep replica build")
    assert rep_build["lastError"] is None, rep_build
    assert rep_build["rowsCopied"] == 5, rep_build
    landed = _wait(lambda: _replica_ids(store), seconds=30, what="the replica rows in the store")
    assert landed == expected, f"replica rows: {landed}"

# Copyright (c) 2026 Kenneth Stott
# Canary: b7ad3010-57b8-48ba-8699-6868f571a4c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E (REQ-788): the file adapter's new-adapter surface that engine-v0.106.0 enables.

A real DuckDB-engine server over a bundled Calcite file adapter (the pinned pgwire-file bundle,
fetched by the normal resolver). Two things the earlier glob-merge e2e deferred to "the new adapter":

1. A files-glob table whose matched files do NOT share a column set is refused BY NAME -- the config
   load and the admin mutation both name the offending file and its missing/extra columns
   (schema.file_columns_differ), never silently null-filling. Exercised through the admin registerTable
   mutation so the refusal comes back as a MutationResult.
2. An optional source-file column carries each row's own file path. Declared via source_file_column,
   it is read live through the adapter AND served from the replica (replicate=0), with the same value
   -- each row's path resolves to the file it came from.

Everything here is the ``test`` instance.
"""

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
_SRC_COL = "src_file"


def _config(workdir: str) -> dict:
    v = {"visible_to": [_ROLE]}
    # The declared columns are the files' data columns plus the synthetic source-file column the
    # adapter exposes when source_file_column is set.
    cols = [
        {"name": "id", "data_type": "integer", "is_primary_key": True, **v},
        {"name": "amount", "data_type": "integer", **v},
        {"name": _SRC_COL, "data_type": "varchar", **v},
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
        "sources": [{"id": "files", "type": "files", "path": f"{workdir}/data"}],
        "tables": [
            {
                "source_id": "files",
                "domain_id": "sales",
                "schema": "main",
                "table": "docs_live",
                "file_glob": "ok/*.csv",
                "source_file_column": _SRC_COL,
                "columns": cols,
            },
            {
                "source_id": "files",
                "domain_id": "sales",
                "schema": "main",
                "table": "docs_rep",
                "file_glob": "ok/*.csv",
                "source_file_column": _SRC_COL,
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
    data = work / "data"
    (data / "ok").mkdir(parents=True)
    (data / "bad").mkdir(parents=True)
    # Matched files for the source-file column table: a.csv has ids 1,2; b.csv has id 3.
    (data / "ok" / "a.csv").write_text("id,amount\n1,10\n2,20\n")
    (data / "ok" / "b.csv").write_text("id,amount\n3,30\n")
    # Mismatched files for the refusal: x.csv has (id,amount), y.csv has (id,name) -- different set.
    (data / "bad" / "x.csv").write_text("id,amount\n1,10\n")
    (data / "bad" / "y.csv").write_text("id,name\n2,foo\n")
    (work / "config.yaml").write_text(yaml.safe_dump(_config(str(work))))
    store = work / "materialize.duckdb"
    server = IsolatedServer(
        "file_glob_new_adapter_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{store}",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        workdir.cleanup()


def _sql(srv, sql: str):
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    return r


def _rows_by_id(srv, table: str) -> dict[int, str] | None:
    """{id -> source-file value} for ``table``, or None until the adapter serves it."""
    r = _sql(srv, f'SELECT "id", "{_SRC_COL}" FROM "{table}" ORDER BY "id"')
    if r.status_code != 200:
        return None
    rows = r.json()["data"]["sql"]
    if not rows:
        return None
    return {int(row["id"]): str(row[_SRC_COL]) for row in rows}


def _admin(srv, mutation: str) -> dict:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message code } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert r.status_code == 200, r.text
    assert "errors" not in r.json(), r.text
    (result,) = r.json()["data"].values()
    return result


def _builds(srv) -> list[dict]:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ replicaBuilds { builds { tableName state rowsCopied completedAt lastError } } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert r.status_code == 200, r.text
    return r.json()["data"]["replicaBuilds"]["builds"]


def _wait(fn, *, seconds: float, what: str):
    deadline = time.monotonic() + seconds
    while True:
        got = fn()
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(1)


def test_a_glob_table_of_mismatched_files_is_refused_by_name(served):
    srv = served
    # bad/x.csv is (id,amount); bad/y.csv is (id,name) -- a different column set. Registering a glob
    # over both is refused, naming the offending file and what differs (REQ-788).
    result = _admin(
        srv,
        'registerTable(input: {sourceId: "files", domainId: "sales", schemaName: "main", '
        'tableName: "bad_glob", fileGlob: "bad/*.csv", '
        'columns: [{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}]})',
    )
    assert result["success"] is False, result
    assert result["code"] == "schema.file_columns_differ", result
    msg = result["message"]
    assert "y.csv" in msg, msg  # the offending file is named
    assert "amount" in msg and "name" in msg, msg  # missing amount / extra name
    # The refusal left nothing registered.
    r = _sql(srv, 'SELECT "id" FROM "bad_glob"')
    assert (
        r.status_code != 200
        or "errors" in r.text.lower()
        or not r.json().get("data", {}).get("sql")
    ), f"bad_glob should not be queryable: {r.text[:300]}"


def test_source_file_column_carries_each_rows_path_live_and_from_replica(served):
    srv = served
    # a.csv -> ids 1,2 ; b.csv -> id 3. Each row's source-file column resolves to its own file.
    live = _wait(
        lambda: _rows_by_id(srv, "docs_live"), seconds=45, what="live glob read of docs_live"
    )
    assert set(live) == {1, 2, 3}, live
    assert "a.csv" in live[1] and "a.csv" in live[2], live
    assert "b.csv" in live[3], live

    # Served from the replica (replicate=0 -> Always): the replicator lands the merged glob through
    # the adapter, carrying the source-file column, and the floored read is served from it.
    def built():
        rep = [b for b in _builds(srv) if b["tableName"] == "docs_rep"]
        return rep[0] if rep and rep[0]["completedAt"] else None

    rep_build = _wait(built, seconds=120, what="the docs_rep replica build")
    assert rep_build["lastError"] is None, rep_build

    rep = _wait(lambda: _rows_by_id(srv, "docs_rep"), seconds=45, what="replica read of docs_rep")
    assert rep == live, f"replica source-file column must match live: {rep} vs {live}"

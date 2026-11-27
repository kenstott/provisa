# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f3d1e-5a8c-4f27-9d61-2e7c4a9b1f05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a restart copies no rows into a replica that already matches its table
(REQ-1632, REQ-2300).

A real server (DuckDB engine, its own store file, a control plane of its own) over an HTTP API
the test serves. The model declares one OpenAPI table, ``list_pets``, whose whole collection is
replicated by the build runner (REQ-1915). Once its replica is built, the server is restarted on
the same control plane and store: boot keeps the matching replica as it stands -- its rows, its
build record -- and the API is not asked for the collection again.
"""

# Requirements: REQ-1632, REQ-2300

from __future__ import annotations

import json
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb")

_ROLE = "org_admin"
_N = 500
_PETS = [{"id": i, "name": f"pet {i}"} for i in range(_N)]
# How long the restarted server is watched for a copy it should not make. The boot seed this
# test guards against asked for its build within seconds of the boot job.
_WATCH_SECONDS = 45.0

_PET = {
    "type": "object",
    "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
}
_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Pets", "version": "1.0.0"},
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {"schema": {"type": "array", "items": _PET}}
                        },
                    }
                },
            }
        },
    },
}


class _Api:
    """The remote API: every request path it is asked, and the pets."""

    def __init__(self) -> None:
        asked: list[str] = []

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's handler name
                asked.append(self.path)
                if self.path != "/pets":
                    self.send_response(400)
                    self.end_headers()
                    return
                body = json.dumps(_PETS).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 - http.server's name
                pass

        self.asked = asked
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def collections_read(self) -> int:
        return sum(1 for path in self.asked if path == "/pets")

    def close(self) -> None:
        self._server.shutdown()


def _config(spec_path: str, base_url: str) -> dict:
    visible = {"visible_to": [_ROLE]}
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "pets", "description": "Pets"}],
        "roles": [],
        "sources": [
            {
                "id": "api",
                "type": "openapi",
                "path": spec_path,
                "base_url": base_url,
                # An hour: no refresh falls inside the test, so any copy after the restart is
                # the boot's own.
                "cache_ttl": 3600,
            }
        ],
        "tables": [
            {
                "source_id": "api",
                "domain_id": "pets",
                "schema": "default",
                "table": "list_pets",
                "columns": [
                    {"name": "id", "data_type": "integer", **visible},
                    {"name": "name", "data_type": "varchar", **visible},
                ],
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

    api = _Api()
    workdir = tempfile.TemporaryDirectory()
    work = Path(workdir.name)
    (work / "spec.json").write_text(json.dumps(_SPEC))
    (work / "config.yaml").write_text(yaml.safe_dump(_config(str(work / "spec.json"), api.url)))
    store = work / "materialize.duckdb"
    server = IsolatedServer(
        "boot_keeps_replicas",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{store}",
    )
    server.start()
    try:
        yield server, api, store
    finally:
        server.stop_process()
        workdir.cleanup()
        api.close()


def _pets_build(srv) -> dict | None:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ replicaBuilds { builds { tableName state requestedReason rowsCopied "
            "completedAt lastError } } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    builds = response.json()["data"]["replicaBuilds"]["builds"]
    return next((b for b in builds if b["tableName"] == "list_pets"), None)


def _replica_rows(store: Path) -> int | None:
    """The row count of the list_pets replica in the store, or None while it is absent. The
    broker holds the store file only for one operation, so a read-only open between requests
    does not contend with the server."""
    con = duckdb.connect(str(store), read_only=True)
    try:
        found = con.execute(
            "SELECT schema_name FROM duckdb_tables() WHERE table_name = 'api__default__list_pets'"
        ).fetchone()
        if found is None:
            return None
        return con.execute(
            f'SELECT COUNT(*) FROM "{found[0]}"."api__default__list_pets"'
        ).fetchone()[0]
    finally:
        con.close()


def _wait(condition, *, seconds: float, what: str):
    deadline = time.monotonic() + seconds
    while True:
        try:
            got = condition()
        except duckdb.IOException:  # the server holds the file for this instant
            got = None
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


def test_a_restart_copies_no_rows_into_a_matching_replica(served):
    srv, api, store = served

    def settled():
        pets = _pets_build(srv)
        return pets if pets and pets["completedAt"] and pets["state"] == "idle" else None

    first = _wait(settled, seconds=180, what="the first build of the collection")
    assert first["lastError"] is None, first
    assert first["rowsCopied"] == _N
    assert _wait(lambda: _replica_rows(store), seconds=30, what="the replica") == _N
    reads_before = api.collections_read()
    assert reads_before >= 1

    srv.restart()

    # Watch the restarted server: its boot reconcile finds the replica matching and keeps it,
    # and nothing asks for a copy, so the collection is never read again and the build record
    # is the first build's.
    deadline = time.monotonic() + _WATCH_SECONDS
    while time.monotonic() < deadline:
        assert api.collections_read() == reads_before, (
            f"the restart read the collection again: {api.asked[reads_before:]}"
        )
        pets = _pets_build(srv)
        assert pets is not None
        assert pets["completedAt"] == first["completedAt"], pets
        assert pets["state"] == "idle", pets
        time.sleep(1.0)

    # The replica was left as it stood: every row the first build copied is still there.
    assert _wait(lambda: _replica_rows(store), seconds=30, what="the kept replica") == _N

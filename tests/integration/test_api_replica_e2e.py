# Copyright (c) 2026 Kenneth Stott
# Canary: 410b3f13-54fd-41ed-a84a-2b5bcbc6f350
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: the whole collection of an API table is replicated by the build runner
(REQ-1915).

A real server (DuckDB engine, its own store file, a SQLite control plane) over an HTTP API the
test serves. The model declares two tables of an OpenAPI source: ``list_pets``, which has no
parameter, and ``get_pet_by_id``, which is a function of its path parameter.

- With no statement reading it, the collection is copied to the resolver's address in the
  store by a build the model's convergence asked for, through the spool file, and the admin
  build query shows how.
- The parameterized table is never built: no build is recorded for it and the API is never
  called for it without its argument.
"""

# Requirements: REQ-1915, REQ-1919, REQ-1865

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
_N = 3000
_PETS = [{"id": i, "name": f"pet {i}", "tags": ["a", "b"]} for i in range(_N)]

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
        "/pets/{petId}": {
            "get": {
                "operationId": "getPetById",
                "parameters": [
                    {"name": "petId", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {"application/json": {"schema": _PET}},
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
                if self.path == "/pets":
                    body = json.dumps(_PETS).encode()
                elif self.path.startswith("/pets/") and self.path[6:].isdigit():
                    body = json.dumps(_PETS[int(self.path[6:])]).encode()
                else:
                    self.send_response(400)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()  # no declared length: the body is streamed to its end
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 - http.server's name
                pass

        self.asked = asked
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

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
        "roles": [
            {
                "id": _ROLE,
                "capabilities": ["query_development", "observability"],
                "domain_access": ["*"],
            }
        ],
        "sources": [
            {
                "id": "api",
                "type": "openapi",
                "path": spec_path,
                "base_url": base_url,
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
            {
                "source_id": "api",
                "domain_id": "pets",
                "schema": "default",
                "table": "get_pet_by_id",
                "columns": [
                    {"name": "id", "data_type": "integer", **visible},
                    {"name": "name", "data_type": "varchar", **visible},
                    {
                        "name": "petId",
                        "data_type": "integer",
                        "native_filter_type": "path_param",
                        **visible,
                    },
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
        "api_replica_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{store}",
    )
    server.start()
    try:
        yield server, api, store
    finally:
        server.stop_process()
        workdir.cleanup()
        api.close()


def _builds(srv) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "{ replicaBuilds { builds { sourceId tableName state requestedReason method "
            "loadKind rowsCopied completedAt lastError } convergenceError } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]["replicaBuilds"]


def _replicas(store: Path) -> dict[str, int]:
    """Every table in the store's replicas schema with its row count. The broker holds the
    store file only for the length of one operation, so a read-only open between requests
    does not contend with the server."""
    con = duckdb.connect(str(store), read_only=True)
    try:
        tables = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE schema_name LIKE '%\\_replicas' ESCAPE '\\'"
        ).fetchall()
        return {
            table: con.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            for schema, table in tables
        }
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


def test_an_api_tables_collection_is_replicated_with_no_read_and_a_function_is_not(served):
    srv, api, store = served

    def built():
        (pets,) = [b for b in _builds(srv)["builds"] if b["tableName"] == "list_pets"] or [None]
        return pets if pets and pets["completedAt"] else None

    pets = _wait(built, seconds=60, what="the build of the collection")
    assert pets["lastError"] is None, pets
    assert (pets["state"], pets["requestedReason"]) == ("idle", "model")
    assert (pets["method"], pets["loadKind"]) == ("stream_batches", "bulk_stream")
    assert pets["rowsCopied"] == _N

    # The copy is at the resolver's address in the store: source, schema and table joined.
    replicas = _wait(lambda: _replicas(store), seconds=30, what="the replica in the store")
    assert replicas["api__default__list_pets"] == _N
    con = duckdb.connect(str(store), read_only=True)
    try:
        schema = con.execute(
            "SELECT schema_name FROM duckdb_tables() WHERE table_name = 'api__default__list_pets'"
        ).fetchone()[0]
        assert con.execute(
            f'SELECT id, name FROM "{schema}"."api__default__list_pets" ORDER BY id LIMIT 2'
        ).fetchall() == [(0, "pet 0"), (1, "pet 1")]
    finally:
        con.close()

    # A table that is a function of its argument has no whole to copy: nothing asked for a
    # build of it, and the API was never called for it without its argument.
    state = _builds(srv)
    assert state["convergenceError"] is None, state
    assert [b for b in state["builds"] if b["tableName"] == "get_pet_by_id"] == []
    assert replicas.get("api__default__get_pet_by_id", 0) == 0
    assert set(api.asked) == {"/pets"}, api.asked

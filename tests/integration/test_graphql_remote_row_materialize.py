# Copyright (c) 2026 Kenneth Stott
# Canary: ae117ef8-e8e5-42c0-a786-9164cb35ba2c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a remote GraphQL table replicated row by row fetches the rows a statement names,
by their key, in one request (REQ-1865).

The remote is the test's own: a GraphQL server in this process, Hasura-shaped -- ``albums`` takes
``where: {id: {_in: [...]}}``, ``tracks`` takes no filter -- that keeps every request it is sent.
A real isolated server (DuckDB engine, which reaches no remote GraphQL source in place; its own
Postgres control plane) is given the remote over the admin API, and the tables are registered
there as a steward registers them.

A statement naming two albums by key is answered with exactly those rows, fetched in one request
asking for those keys; the answer is the one the same rows give when the whole table is landed;
a second read within the table's TTL sends nothing. A table whose remote has no field fetching
its rows by key is refused row_materialize by name when it is registered."""

# Requirements: REQ-1865

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import yaml
from graphql import build_schema, graphql_sync

from tests.helpers import registered_id, release_mutation

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_ALBUMS = {1: "Let It Be", 2: "Abbey Road", 3: "Revolver", 4: "Help!"}
_TRACKS = {1: "Two of Us", 2: "Come Together"}

_SCHEMA = build_schema(
    """
    input Int_comparison_exp { _eq: Int, _in: [Int!] }
    input String_comparison_exp { _eq: String }
    input albums_bool_exp { id: Int_comparison_exp, title: String_comparison_exp }
    type albums { id: Int! title: String }
    type tracks { id: Int! name: String }
    type Query {
      albums(where: albums_bool_exp, limit: Int, offset: Int): [albums!]!
      tracks(limit: Int, offset: Int): [tracks!]!
    }
    """
)


def _albums(_root, _info, where=None, limit=None, offset=None):
    keys = ((where or {}).get("id") or {}).get("_in")
    rows = [{"id": i, "title": t} for i, t in _ALBUMS.items() if keys is None or i in keys]
    return rows[offset or 0 :][:limit]


def _tracks(_root, _info, limit=None, offset=None):
    return [{"id": i, "name": n} for i, n in _TRACKS.items()][offset or 0 :][:limit]


class _Remote:
    """The remote GraphQL server; ``reads`` holds every request that read ``albums``."""

    def __init__(self) -> None:
        self.reads: list[dict] = []
        remote = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if "__schema" not in body["query"] and "albums" in body["query"]:
                    remote.reads.append(body)
                result = graphql_sync(
                    _SCHEMA,
                    body["query"],
                    root_value={"albums": _albums, "tracks": _tracks},
                    variable_values=body.get("variables"),
                    field_resolver=_resolve,
                )
                answer: dict = {"data": result.data}
                if result.errors:
                    answer["errors"] = [{"message": e.message} for e in result.errors]
                payload = json.dumps(answer).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args) -> None:  # noqa: A002 -- the base's name
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/graphql"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()


def _resolve(source, info, **args):
    """A root field is resolved by its function; any other field is read off its row."""
    value = source.get(info.field_name) if isinstance(source, dict) else None
    return value(source, info, **args) if callable(value) else value


@pytest.fixture(scope="module")
def remote():
    served = _Remote()
    yield served
    served.close()


@pytest.fixture(scope="module")
def server(remote, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    config = {
        "auth": {"provider": "none"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "music", "description": "Music"}],
        "roles": [],
        "sources": [],
        "tables": [],
    }
    path = tmp_path_factory.mktemp("gql_row_materialize") / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "gql_row_materialize", engine="duckdb", config=str(path), control_plane="own"
    )
    srv.start(timeout=240)
    try:
        added = httpx.post(
            f"{srv.base_url}/admin/sources/graphql-remote",
            json={
                "source_id": "hasura",
                "url": remote.url,
                "namespace": "",
                "domain_id": "music",
                "auth": None,
                "cache_ttl": 300,
            },
            headers=_HEADERS,
            timeout=60,
        )
        assert added.status_code == 200, added.text
        yield srv
    finally:
        srv.stop_process()


def _register(srv, source_id: str, table: str, *, row_materialize: bool, name: str) -> dict:
    flag = f"rowMaterialize: {'true' if row_materialize else 'false'}, "
    mutation = (
        f'mutation {{ registerTable(input: {{ sourceId: "{source_id}", domainId: "music", '
        f'schemaName: "graphql", tableName: "{table}", {flag}cacheTtl: 300, columns: ['
        f'{{ name: "id", visibleTo: ["{_ROLE}"], isPrimaryKey: true }}, '
        f'{{ name: "{name}", visibleTo: ["{_ROLE}"] }}] }}) {{ success message code }} }}'
    )
    resp = httpx.post(
        f"{srv.base_url}/admin/graphql", json={"query": mutation}, headers=_HEADERS, timeout=60
    )
    assert resp.status_code == 200, resp.text
    assert "errors" not in resp.json(), resp.text
    return resp.json()["data"]["registerTable"]


def _release(srv, registered: dict) -> None:
    """Put a registered table in service, as a domain owner does after registering it."""
    resp = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": release_mutation(registered_id(registered["message"]))},
        headers=_HEADERS,
        timeout=60,
    )
    assert resp.status_code == 200 and "errors" not in resp.json(), resp.text
    assert resp.json()["data"]["setTableDraft"]["success"], resp.text


def _sql(srv, sql: str) -> list[dict]:
    resp = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers=_HEADERS,
        timeout=srv.request_timeout + 30,
    )
    assert resp.status_code == 200, f"{resp.text}\n{srv.dump_stderr_debug()[-6000:]}"
    return resp.json()["data"]["sql"]


def test_the_rows_named_by_key_are_fetched_in_one_request_and_kept_for_the_ttl(server, remote):
    registered = _register(server, "hasura", "albums", row_materialize=True, name="title")
    assert registered["success"], registered
    _release(server, registered)
    remote.reads.clear()  # registration offers the table to the remote once

    sql = "SELECT id, title FROM albums WHERE id IN (1, 3) ORDER BY id"
    first = _sql(server, sql)
    assert first == [{"id": 1, "title": "Let It Be"}, {"id": 3, "title": "Revolver"}]
    (fetch,) = remote.reads  # one request, for exactly the keys named
    assert sorted(fetch["variables"]["where"]["id"]["_in"]) == [1, 3]

    assert _sql(server, sql) == first
    assert len(remote.reads) == 1, "a read within the table's TTL asked the remote again"

    # The same rows, from the whole table landed: the table updated to be read whole.
    whole = _register(server, "hasura", "albums", row_materialize=False, name="title")
    assert whole["success"], whole
    _release(server, whole)  # an update puts the table back in draft
    remote.reads.clear()
    assert _sql(server, sql) == first
    # The release's landing reconcile and the read may each land the table: every request is
    # for the whole table, none for keys.
    assert remote.reads, "the whole table was never read from the remote"
    assert all("where" not in (r.get("variables") or {}) for r in remote.reads), remote.reads


def test_a_table_its_remote_cannot_fetch_by_key_is_refused_by_name(server):
    refused = _register(server, "hasura", "tracks", row_materialize=True, name="name")
    assert refused["success"] is False, refused
    assert refused["code"] == "schema.row_materialize_no_key_fetch", refused
    assert refused["message"].startswith("tracks: row_materialize needs row fetch by primary keys")

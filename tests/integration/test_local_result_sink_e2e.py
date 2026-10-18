# Copyright (c) 2026 Kenneth Stott
# Canary: eb692f46-cefa-49d6-a105-81d95937f43d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1191, REQ-1192, REQ-1193, REQ-1195: with no object store, a large result lands in the org's
folder and is served to the user who asked for it alone.

The regression: since GraphQL moved onto the one pipeline (c4fa112ca), a result the redirect took
on a deployment with no object store answered 502 (``run_materialize`` had no local tier). A real
server (DuckDB engine, SQLite control plane, simple auth with two users) over a SQLite source,
redirect on with a threshold of 100 rows and no object store named. A GraphQL query over the
threshold, and a small one that asks for the redirect itself, each land a file in the org's
folder; the user who asked downloads it from /data/redirect-file/<name>, and the other user is
refused."""

# Requirements: REQ-1191, REQ-1192, REQ-1193, REQ-1195

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import bcrypt
import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "local_result_sink_e2e"
_ROWS = 300
_THRESHOLD = 100
_PASSWORD = "local result sink"
_ROLE = "reader"


def _seed(db: Path) -> None:
    con = sqlite3.connect(db)
    try:
        con.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        con.executemany(
            "INSERT INTO items VALUES (?, ?)", [(i, f"item {i}") for i in range(1, _ROWS + 1)]
        )
        con.commit()
    finally:
        con.close()


def _user(name: str) -> dict:
    hashed = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt()).decode()
    return {"username": name, "password_hash": hashed, "roles": [_ROLE]}


def start_server(work: Path):
    """The server, its data directory ``work / 'data'``, over ``work / 'items.sqlite'``."""
    from tests.integration.isolated_server import IsolatedServer

    db = work / "items.sqlite"
    _seed(db)
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg = {
        "naming": base["naming"],
        "relationships": [],
        "roles": [
            {
                "id": _ROLE,
                "description": "Test role.",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["store"],
            }
        ],
        "sources": [{"id": "items-sqlite", "type": "sqlite", "path": str(db)}],
        "domains": [{"id": "store", "description": "local result sink e2e"}],
        "tables": [
            {
                "source_id": "items-sqlite",
                "domain_id": "store",
                "schema": "default",
                "table": "items",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ROLE]},
                    {"name": "name", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            }
        ],
    }
    cfg_path = work / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(cfg_path),
        control_plane="own",
        auth={
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "local-result-sink-e2e-signing-secret-0123456789",
            "default_role": _ROLE,
            "simple": {"users": [_user("ada"), _user("ben")]},
        },
        env={
            "PROVISA_DATA_DIR": str(work / "data"),
            "PROVISA_REDIRECT_ENABLED": "true",
            "PROVISA_REDIRECT_THRESHOLD": str(_THRESHOLD),
            "PROVISA_REDIRECT_FORMAT": "csv",
            # No object store: no endpoint and no credentials, whatever the runner's env holds.
            "PROVISA_REDIRECT_ENDPOINT": "",
            "PROVISA_REDIRECT_ACCESS_KEY": "",
            "PROVISA_REDIRECT_SECRET_KEY": "",
        },
    )
    srv.start(timeout=300)
    srv.work = work  # type: ignore[attr-defined]
    return srv


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = start_server(tmp_path_factory.mktemp("local_result_sink"))
    try:
        yield srv
    finally:
        srv.stop_process()


def login(server, user: str) -> dict[str, str]:
    resp = httpx.post(
        f"{server.base_url}/auth/login",
        json={"username": user, "password": _PASSWORD},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def items_field(server, headers: dict[str, str]) -> str:
    """The GraphQL query field the items table is served as, under this deployment's naming."""
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": "{ __schema { queryType { fields { name } } } }"},
        headers=headers,
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    names = [f["name"] for f in resp.json()["data"]["__schema"]["queryType"]["fields"]]
    found = [n for n in names if n.lower().endswith("items")]
    assert len(found) == 1, names
    return found[0]


def graphql(server, headers: dict[str, str], query: str) -> dict:
    resp = httpx.post(
        f"{server.base_url}/data/graphql", json={"query": query}, headers=headers, timeout=120
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def landed(server, handle: dict) -> Path:
    """The file a delivery handle names, in the org's folder; it must be there."""
    url = handle["redirect_url"]
    assert url.startswith("/data/redirect-file/"), handle
    path = server.work / "data" / "orgs" / _ORG / "results" / url.rsplit("/", 1)[1]
    assert path.is_file(), path
    return path


def assert_served_to_owner_only(server, handle: dict, rows: int) -> None:
    url = f"{server.base_url}{handle['redirect_url']}"
    owner = httpx.get(url, headers=login(server, "ada"), timeout=60)
    assert owner.status_code == 200, owner.text
    assert owner.headers["content-type"].startswith("text/csv")
    body = list(csv.reader(owner.text.splitlines()))
    assert body[0] == ["id", "name"]
    assert len(body) - 1 == rows == handle["row_count"]
    assert owner.content == landed(server, handle).read_bytes()

    other = httpx.get(url, headers=login(server, "ben"), timeout=60)
    assert other.status_code == 403, other.text
    assert httpx.get(url, timeout=60).status_code == 401


def test_a_result_over_the_threshold_lands_in_the_org_folder(server):
    headers = login(server, "ada")
    field = items_field(server, headers)
    body = graphql(server, headers, f"{{ {field} {{ id name }} }}")
    handle = body["redirect"]
    assert handle["sink"] == "local", handle
    assert handle["content_type"] == "text/csv"
    assert handle["expires_in"] > 0
    landed(server, handle)
    assert_served_to_owner_only(server, handle, _ROWS)


def test_a_forced_redirect_under_the_threshold_lands_in_the_org_folder(server):
    headers = login(server, "ada")
    field = items_field(server, headers)
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": f"{{ {field}(limit: 5) {{ id name }} }}"},
        headers={**headers, "X-Provisa-Redirect": "true", "X-Provisa-Redirect-Format": "csv"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    handle = resp.json()["redirect"]
    assert handle["sink"] == "local", handle
    assert_served_to_owner_only(server, handle, 5)


def test_under_the_threshold_without_asking_answers_inline(server):
    headers = login(server, "ada")
    field = items_field(server, headers)
    body = graphql(server, headers, f"{{ {field}(limit: 5) {{ id name }} }}")
    assert "redirect" not in body
    assert len(body["data"][field]) == 5

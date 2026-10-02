# Copyright (c) 2026 Kenneth Stott
# Canary: 8cc89fc6-5369-4b96-a38d-ec60dd5bcdca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source with registered tables is not deleted, on each control plane (REQ-1918).

A served instance on a PostgreSQL control plane and one on a SQLite control plane are each asked
to delete a source of their config that has registered tables. Before this rule the source went
and took its tables with it — and, on PostgreSQL, everything that referred to those tables. Now
the request is refused naming the tables, and the source and its tables are still there.
Everything here lands on the TEST instance: dedicated server processes on their own ports with
their own control planes.
"""

# Requirements: REQ-1918, REQ-1919

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import httpx
import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration]

_PG_ORG = "source_delete_pg"


@pytest.fixture(scope="module", params=["postgres", "sqlite"])
def served(request):
    if request.param == "postgres":
        srv = IsolatedServer(_PG_ORG, config="tests/fixtures/sample_config.yaml")
        srv.start()
        try:
            yield srv
        finally:
            srv.stop_process()
            asyncio.run(drop_org_schema(_PG_ORG))
        return
    pytest.importorskip("duckdb")
    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        "source_delete_sqlite",
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        store_dir.cleanup()


def _gql(srv: IsolatedServer, query: str) -> dict:
    resp = httpx.post(
        f"{srv.base_url}/admin/graphql", json={"query": query}, timeout=srv.request_timeout + 30
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _tables_by_source(srv: IsolatedServer) -> dict[str, set[int]]:
    by_source: dict[str, set[int]] = {}
    for table in _gql(srv, "{ tables { id sourceId } }")["tables"]:
        by_source.setdefault(table["sourceId"], set()).add(table["id"])
    return by_source


def test_a_source_with_registered_tables_is_refused_naming_them_and_nothing_is_removed(served):
    before = _tables_by_source(served)
    registered = {
        s["id"]
        for s in _gql(served, "{ sources { id } }")["sources"]
        if not s["id"].startswith("_")
    }
    source_id = sorted(s for s in registered if before.get(s) and not s.startswith("provisa-"))[0]

    answer = _gql(
        served,
        f'mutation {{ deleteSource(id: "{source_id}") {{ success code message params }} }}',
    )["deleteSource"]
    assert (answer["success"], answer["code"]) == (False, "schema.source_has_dependents"), answer
    named = {d["id"] for d in answer["params"]["dependents"] if d["kind"] == "table"}
    assert named == before[source_id], answer

    assert source_id in {s["id"] for s in _gql(served, "{ sources { id } }")["sources"]}
    assert _tables_by_source(served) == before


def test_a_source_that_is_not_there_is_not_found(served):
    answer = _gql(served, 'mutation { deleteSource(id: "no-such-source-1918") { success code } }')[
        "deleteSource"
    ]
    assert (answer["success"], answer["code"]) == (False, "schema.source_not_found")

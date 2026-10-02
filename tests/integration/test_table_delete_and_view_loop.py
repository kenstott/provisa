# Copyright (c) 2026 Kenneth Stott
# Canary: 6e7e85c3-096a-4d82-9c94-f11951235318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view loop is refused at save, and a table something reads is not deleted, on each control plane (REQ-1918).

Run through served instances on a PostgreSQL and on a SQLite control plane. Before this rule a
view could be re-saved to read a view that reads it — both views were then unqueryable — and a
table a view read could be deleted from under it. Everything here lands on the TEST instance:
dedicated server processes on their own ports with their own control planes.
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

_PG_ORG = "table_delete_pg"


@pytest.fixture(scope="module", params=["postgres", "sqlite"])
def served(request):
    """The server and a domain of its config."""
    if request.param == "postgres":
        srv = IsolatedServer(_PG_ORG, config="tests/fixtures/sample_config.yaml")
        srv.start()
        try:
            yield srv, "sales-analytics"
        finally:
            srv.stop_process()
            asyncio.run(drop_org_schema(_PG_ORG))
        return
    pytest.importorskip("duckdb")
    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        "table_delete_sqlite",
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    srv.start()
    try:
        yield srv, "shelter"
    finally:
        srv.stop_process()
        store_dir.cleanup()


def _gql(srv: IsolatedServer, query: str) -> dict:
    resp = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "org_admin"},
        timeout=srv.request_timeout + 60,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _save_view(srv: IsolatedServer, domain: str, name: str, sql: str) -> dict:
    return _gql(
        srv,
        f"""mutation {{ registerTable(input: {{
            sourceId: "__derived__", domainId: "{domain}", schemaName: "views",
            tableName: "{name}", alias: "{name}", viewSql: "{sql}", materialize: false,
            columns: [{{ name: "id", visibleTo: ["org_admin"] }}]
        }}) {{ success message code params }} }}""",
    )["registerTable"]


def _views(srv: IsolatedServer) -> dict[str, dict]:
    listed = _gql(srv, "{ tables { id tableName viewSql } }")["tables"]
    return {t["tableName"]: t for t in listed if t["viewSql"]}


def _delete(srv: IsolatedServer, table_id: int) -> dict:
    return _gql(
        srv, f"mutation {{ deleteTable(id: {table_id}) {{ success code message params }} }}"
    )["deleteTable"]


def test_a_view_cannot_be_saved_to_read_a_view_that_reads_it(served):
    srv, domain = served
    assert _save_view(srv, domain, "loop_a", "SELECT 1 AS id")["success"] is True
    assert _save_view(srv, domain, "loop_b", "SELECT id FROM loop_a")["success"] is True

    refused = _save_view(srv, domain, "loop_a", "SELECT id FROM loop_b")
    assert (refused["success"], refused["code"]) == (False, "schema.view_reads_itself"), refused
    assert refused["params"]["loop"] == ["loop_a", "loop_b", "loop_a"]
    assert "loop_a -> loop_b -> loop_a" in refused["message"]

    stored = _views(srv)
    assert stored["loop_a"]["viewSql"] == "SELECT 1 AS id"
    assert stored["loop_b"]["viewSql"] == "SELECT id FROM loop_a"


def test_a_view_another_view_reads_is_deleted_only_after_its_reader(served):
    srv, domain = served
    assert _save_view(srv, domain, "base_view", "SELECT 1 AS id")["success"] is True
    assert _save_view(srv, domain, "reader_view", "SELECT id FROM base_view")["success"] is True
    views = _views(srv)
    base, reader = views["base_view"]["id"], views["reader_view"]["id"]

    refused = _delete(srv, base)
    assert (refused["success"], refused["code"]) == (False, "schema.table_has_dependents"), refused
    assert refused["params"]["name"] == "base_view"
    assert {
        "kind": "table",
        "id": reader,
        "name": "reader_view",
        "via": ["registered_tables.view_sql"],
    } in refused["params"]["dependents"]
    assert "base_view" in _views(srv)

    assert _delete(srv, reader)["success"] is True
    gone = _delete(srv, base)
    assert (gone["success"], gone["code"]) == (True, "schema.table_deleted"), gone
    assert not {"base_view", "reader_view"} & set(_views(srv))
    assert _delete(srv, base)["code"] == "schema.table_not_found"

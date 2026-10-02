# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e1c64-5d27-4a80-9e4f-7a1c6b8d2f05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a materialized view over a row-level replicated table is refused.

A real isolated server (DuckDB engine — no connector for neo4j, so the table declared
``row_materialize`` is reached by row-level replication; SQLite control plane) over a real Neo4j.
A row-level table holds only the rows requests have fetched, so a view built over it would hold
whatever happened to be cached. Saving such a view through the admin API is refused with an error
naming the view, the input and the reason; a config that declares one fails the load the same
way; a view that reads no such input is saved and refreshed as before."""

from __future__ import annotations

import os

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_neo4j]

_ROLE = "org_admin"
_TABLE = "rl_click"
_REPO = os.path.join(os.path.dirname(__file__), "..", "..")
_VIEW_SQL = f"SELECT region, count(*) AS n FROM rowlevel.{_TABLE} GROUP BY region"


def _config(views: list[dict], view_tables: list[dict] | None = None) -> dict:
    with open(os.path.join(_REPO, "tests/fixtures/sample_config.yaml")) as f:
        base = yaml.safe_load(f)
    return {
        "naming": base["naming"],
        "roles": base["roles"],
        "relationships": [],
        "sources": [
            {
                "id": "rl-neo4j",
                "type": "neo4j",
                "host": "localhost",
                "port": int(os.environ["NEO4J_HTTP_PORT"]),
                "database": "neo4j",
            }
        ],
        "domains": [{"id": "rowlevel", "description": "view input refusal e2e"}],
        "tables": [
            {
                "source_id": "rl-neo4j",
                "domain_id": "rowlevel",
                "schema": "neo4j",
                "table": _TABLE,
                "row_materialize": True,
                "cache_ttl": 300,
                "query_template": (
                    "MATCH (c:RlClick) RETURN c.click_id AS click_id, c.region AS region "
                    "ORDER BY c.click_id"
                ),
                "columns": [
                    {
                        "name": "click_id",
                        "data_type": "integer",
                        "is_primary_key": True,
                        "visible_to": [_ROLE],
                    },
                    {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            },
            *(view_tables or []),
        ],
        "views": views,
    }


def _server(tmp_path_factory, org: str, views: list[dict], view_tables: list[dict] | None = None):
    from tests.integration.isolated_server import IsolatedServer

    path = tmp_path_factory.mktemp(org) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(views, view_tables)))
    return IsolatedServer(org, engine="duckdb", config=str(path), control_plane="sqlite")


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = _server(tmp_path_factory, "view_input_refusal", [])
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(server, query: str) -> dict:
    resp = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _register_view(server, name: str, sql: str, columns: list[str]) -> dict:
    cols = ", ".join(f'{{ name: "{c}", visibleTo: ["{_ROLE}"] }}' for c in columns)
    out = _admin(
        server,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "__derived__",
                domainId: "rowlevel",
                schemaName: "views",
                tableName: "{name}",
                alias: "{name}",
                viewSql: "{sql}",
                materialize: true,
                columns: [{cols}]
            }}) {{ success message }}
        }}
        """,
    )
    return out


def _views(server) -> dict[str, dict]:
    listed = _admin(server, "query { mvList { id status lastError } }")
    return {v["id"]: v for v in listed["data"]["mvList"]}


def test_saving_a_view_over_a_row_level_table_is_refused_naming_it(server):
    out = _register_view(server, "clicks_by_region", _VIEW_SQL, ["region", "n"])
    text = str(out)
    assert not (out.get("data") or {}).get("registerTable", {}).get("success"), out
    assert "view-clicks_by_region" in text, out
    assert f"rowlevel.{_TABLE}" in text and "row-level replicated table" in text, out
    assert "only the rows requests have fetched" in text, out
    assert "view-clicks_by_region" not in _views(server)


def test_a_view_that_reads_no_such_input_is_saved_and_refreshed(server):
    out = _register_view(server, "constants", "SELECT 1 AS id, 10 AS amount", ["id", "amount"])
    assert out["data"]["registerTable"]["success"], out
    assert "view-constants" in _views(server)

    refreshed = _admin(server, 'mutation { refreshMv(mvId: "view-constants") { success message } }')
    assert refreshed["data"]["refreshMv"]["success"], refreshed
    view = _views(server)["view-constants"]
    assert view["lastError"] is None, view
    assert view["status"] == "fresh", view


def test_a_config_declaring_such_a_view_fails_the_load_naming_it(tmp_path_factory):
    srv = _server(
        tmp_path_factory,
        "view_input_refusal_cfg",
        [
            {
                "id": "clicks-by-region",
                "sql": _VIEW_SQL,
                "materialize": True,
                "domain_id": "rowlevel",
                "source_id": "rl-neo4j",
                "columns": [
                    {"name": "region", "visible_to": [_ROLE]},
                    {"name": "n", "visible_to": [_ROLE]},
                ],
            }
        ],
    )
    try:
        with pytest.raises(RuntimeError):
            srv.start(timeout=240)
        log = srv.dump_stderr_debug()
    finally:
        srv.stop_process()
    assert "materialized view 'view-clicks-by-region' cannot be built" in log, log[-3000:]
    # The load reads the view's SQL with its tables resolved to where they live.
    assert f".{_TABLE}' is a row-level replicated table" in log, log[-3000:]
    assert "only the rows requests have fetched" in log, log[-3000:]


def test_a_config_declaring_such_a_view_as_a_table_entry_fails_the_load_too(tmp_path_factory):
    """The other spelling of a config view — a table entry with ``view_sql`` and
    ``materialize: true`` — is refused the same way: a load does not accept in one spelling
    what it refuses in another."""
    srv = _server(
        tmp_path_factory,
        "view_input_refusal_tbl",
        [],
        [
            {
                "source_id": "__derived__",
                "domain_id": "rowlevel",
                "schema": "views",
                "table": "clicks_by_region_t",
                "view_sql": _VIEW_SQL,
                "materialize": True,
                "columns": [
                    {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
                    {"name": "n", "data_type": "bigint", "visible_to": [_ROLE]},
                ],
            }
        ],
    )
    try:
        with pytest.raises(RuntimeError):
            srv.start(timeout=240)
        log = srv.dump_stderr_debug()
    finally:
        srv.stop_process()
    assert "materialized view 'view-clicks_by_region_t' cannot be built" in log, log[-3000:]
    assert f"{_TABLE}' is a row-level replicated table" in log, log[-3000:]
    assert "only the rows requests have fetched" in log, log[-3000:]

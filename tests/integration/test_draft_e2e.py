# Copyright (c) 2026 Kenneth Stott
# Canary: 51d92067-dfd7-4fbb-abb9-c34c41cc93c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a table registered through the admin starts as draft, out of service until its
domain's owners release it (REQ-1921), on a real pg-engine server over a real source.

* Freshly registered it is a draft: the role here is org_admin, whose schema offers it marked as
  a draft and whose GraphQL, REST and SQL reads of it answer its rows (A DRAFT IS SERVED TO
  ORG_ADMIN).
* Released, it is read; replicated, its replica is built.
* Set as draft again, it is a draft again and keeps the replica it has: it may hold its data.
* A draft the engine reaches only through a replica is landed by its steward's preview — the
  ordinary build, asked for by that read — and the preview is answered from it. Released, it is
  read from the copy already landed.
"""

# Requirements: REQ-1921

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import psycopg
import pytest
import yaml

from tests.helpers import registered_id
from tests.integration.test_pg_engine_landing_never_writes_source_e2e import (
    _ROLE,
    _ROWS,
    _SourceAndEngine,
)
from tests.integration.test_replica_convergence_e2e import _builds, _config

pytestmark = [pytest.mark.integration]

_NAME = "draft_e2e"
_REPLICA = f'"org_{_NAME}_replicas"."src__public__orders"'
_ID_AMOUNT = [(i, amount) for i, amount, _note in _ROWS]
# Many of this server's reloads (the config's interval is half a second): an undeclared
# replica is retired by the first of them.
_KEPT_S = 20


@pytest.fixture
def server():
    from tests.integration.isolated_server import IsolatedServer

    pg = _SourceAndEngine()
    pg.start()
    with tempfile.TemporaryDirectory() as workdir:
        path = Path(workdir) / "config.yaml"
        path.write_text(yaml.safe_dump(_config()))
        control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
        srv = IsolatedServer(
            _NAME,
            engine="pg",
            config=str(path),
            control_plane="postgres",
            env={
                "PLATFORM_DATABASE_URL": control_plane,
                "TENANT_DATABASE_URL": control_plane,
                "PROVISA_MATERIALIZE_URL": pg.url(pg.engine_port, "provisa"),
                "PROVISA_REDIS_EMBEDDED": "1",
            },
        )
        try:
            srv.start()
            yield srv, pg
        finally:
            srv.stop_process()
            pg.stop()


def _mutate(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _graphql(srv, query: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _sql(srv, sql: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _read(srv) -> list[tuple]:
    response = _graphql(srv, "{ orders { id amount } }")
    assert response.status_code == 200, response.text
    return sorted((r["id"], r["amount"]) for r in response.json()["data"]["orders"])


def _rest(srv) -> httpx.Response:
    return httpx.get(
        f"{srv.base_url}/data/rest/shop/orders",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _a_draft_read_by_org_admin(srv) -> None:
    """REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN: the role here is org_admin, whose schema offers
    the draft marked as one, and whose GraphQL, REST and SQL reads naming it answer its rows, read
    as a served table is (admin/table_read)."""
    assert _read(srv) == _ID_AMOUNT
    rest = _rest(srv)
    assert rest.status_code == 200, rest.text
    assert sorted((r["id"], r["amount"]) for r in rest.json()["data"]) == _ID_AMOUNT, rest.text
    sql = _sql(srv, "SELECT id FROM orders ORDER BY id")
    assert sql.status_code == 200, sql.text
    assert [r["id"] for r in sql.json()["data"]["sql"]] == [i for i, _ in _ID_AMOUNT], sql.text
    schema = _graphql(
        srv, "{ __schema { queryType { fields { name } } types { name description } } }"
    )
    assert schema.status_code == 200, schema.text
    offered = {f["name"] for f in schema.json()["data"]["__schema"]["queryType"]["fields"]}
    assert "orders" in offered
    marked = [
        t["name"]
        for t in schema.json()["data"]["__schema"]["types"]
        if (t["description"] or "").startswith("Draft: not released.")
    ]
    assert marked, schema.text


def _ways_to_reach(srv, table_id: int) -> dict:
    """The admin's ways to reach the table, as the Your data step reads them."""
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": "query($id: Int!, $role: String!) { tableAccess(tableId: $id, role: $role) "
            "{ graphql sql draft } }",
            "variables": {"id": table_id, "role": _ROLE},
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]["tableAccess"]


def _replica_rows(pg: _SourceAndEngine) -> int | None:
    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        if conn.execute("SELECT to_regclass(%s)", (_REPLICA,)).fetchone()[0] is None:
            return None
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {_REPLICA}").fetchone()[0]
        except psycopg.errors.UndefinedTable:
            return None  # dropped between the two statements: the drop being waited for


def _wait(condition, *, seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


def test_a_table_registered_through_the_admin_is_out_of_service_until_released(server):
    srv, pg = server
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    table_id = registered_id(registered["message"])
    _a_draft_read_by_org_admin(srv)
    # Its ways to reach it, as org_admin is served it: marked as a draft's.
    ways = _ways_to_reach(srv, table_id)
    assert ways["draft"] is True, ways
    assert "orders(limit: 10)" in ways["graphql"], ways
    assert "orders" in ways["sql"], ways

    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: false)")
    assert _read(srv) == _ID_AMOUNT
    assert _ways_to_reach(srv, table_id)["draft"] is False

    _mutate(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
    _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
    _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=90, what="the replica")
    assert _read(srv) == _ID_AMOUNT

    # Set as draft again: a draft again, and its replica stays — through the reloads in which
    # an undeclared replica is retired and, its wait over, dropped.
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: true)")
    _a_draft_read_by_org_admin(srv)
    time.sleep(_KEPT_S)
    (build,) = _builds(srv)["builds"]
    assert build["state"] != "retired", build
    assert _replica_rows(pg) == len(_ROWS)
    _a_draft_read_by_org_admin(srv)


def _preview(srv, table_id: int, body: dict) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/admin/tables/{table_id}/preview",
        json=body,
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def _previewed(srv, table_id: int, body: dict) -> dict:
    """The preview's rows, once its copy has landed: a preview that outlasts its time while the
    build runs is answered as building, by code, and the next one reads the landed copy."""
    deadline = time.monotonic() + 120
    while True:
        response = _preview(srv, table_id, body)
        if response.status_code == 200:
            return response.json()
        assert response.status_code == 504, response.text
        assert response.json()["code"] == "data.replica_building", response.text
        assert time.monotonic() < deadline, "the draft's replica was never landed"
        time.sleep(0.5)


@pytest.fixture(params=["pg", "duckdb"])
def landing_server(request):
    """A server on each engine, over a real PostgreSQL source whose tables the operator's
    setting puts on their replica."""
    from tests.integration.isolated_server import IsolatedServer

    engine = request.param
    pg = _SourceAndEngine()
    pg.start()
    with tempfile.TemporaryDirectory() as workdir:
        path = Path(workdir) / "config.yaml"
        path.write_text(yaml.safe_dump({**_config(), "federation_engine": engine}))
        control_plane = pg.url(pg.engine_port, "provisa", "+psycopg")
        env = {
            "PLATFORM_DATABASE_URL": control_plane,
            "TENANT_DATABASE_URL": control_plane,
            "PROVISA_REDIS_EMBEDDED": "1",
        }
        kwargs: dict = {}
        if engine == "pg":
            env["PROVISA_MATERIALIZE_URL"] = pg.url(pg.engine_port, "provisa")
        else:
            kwargs["materialize_store_url"] = f"duckdb:///{Path(workdir) / 'materialize.duckdb'}"
        srv = IsolatedServer(
            f"{_NAME}_land_{engine}",
            engine=engine,
            config=str(path),
            control_plane="postgres",
            env=env,
            **kwargs,
        )
        try:
            srv.start()
            yield srv, pg
        finally:
            srv.stop_process()
            pg.stop()


def test_a_draft_reached_through_a_replica_is_landed_by_its_stewards_preview(landing_server):
    srv, pg = landing_server
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    # Every table of the source is served from its replica, never read in place.
    _mutate(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
    _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    table_id = registered_id(registered["message"])
    _a_draft_read_by_org_admin(srv)

    # Its steward's preview: the first page, then a filter, a sort and a second page of it --
    # each a statement the server writes, answered from the replica that read landed.
    first = _previewed(srv, table_id, {"sorts": [{"col": "id", "dir": "asc"}]})
    assert first["draft"] is True
    assert [(r["id"], r["amount"]) for r in first["rows"]] == _ID_AMOUNT

    low, high = _ID_AMOUNT[0][0], _ID_AMOUNT[-1][0]
    filtered = _previewed(
        srv,
        table_id,
        {"filters": [{"col": "id", "kind": "number", "op": "eq", "a": str(high)}]},
    )
    assert [r["id"] for r in filtered["rows"]] == [high]
    paged = _previewed(
        srv,
        table_id,
        {"sorts": [{"col": "id", "dir": "desc"}], "page": 1, "pageSize": len(_ID_AMOUNT) - 1},
    )
    assert [r["id"] for r in paged["rows"]] == [low]

    unknown = _preview(
        srv, table_id, {"filters": [{"col": "nope", "kind": "text", "op": "contains", "a": "x"}]}
    )
    assert unknown.status_code == 400, unknown.text
    assert unknown.json()["code"] == "data.viewer_column_unknown", unknown.text

    # Landed -- by the ordinary build, on the ordinary record -- and still a draft.
    (build,) = _builds(srv)["builds"]
    assert (build["sourceId"], build["tableName"]) == ("src", "orders")
    assert build["rowsCopied"] == len(_ROWS) and build["lastError"] is None
    _a_draft_read_by_org_admin(srv)

    # Released: read by anyone the model serves it to, from the copy already landed.
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: false)")
    assert _read(srv) == _ID_AMOUNT


def test_a_views_inputs_are_in_service_for_its_readers(server):
    """A view is answered from the tables its SQL names. While one of them is out of service
    the view is refused naming that table, on SQL and GraphQL alike, exactly as a statement
    naming the table is; the view stays, and is answered again once the table is released."""
    srv, pg = server
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    table_id = registered_id(registered["message"])
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: false)")
    view = _mutate(
        srv,
        'registerTable(input: {sourceId: "__derived__", domainId: "shop", schemaName: "views", '
        'tableName: "order_amounts", alias: "order_amounts", '
        'viewSql: "SELECT id, amount FROM orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"]}, {name: "amount", visibleTo: ["org_admin"]}]})',
    )
    _mutate(srv, f"setTableDraft(tableId: {registered_id(view['message'])}, draft: false)")

    def through_the_view() -> httpx.Response:
        return _sql(srv, "SELECT id, amount FROM order_amounts ORDER BY id")

    answered = through_the_view()
    assert answered.status_code == 200, answered.text

    # Its input goes out of service: the view names it, and is refused naming it.
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: true)")
    _a_draft_read_by_org_admin(srv)
    refused = through_the_view()
    assert refused.status_code != 200, refused.text
    assert "'orders' is a draft" in refused.text, refused.text
    # The root field follows the deployment's GraphQL convention (apollo_graphql: camelCase).
    graphql = _graphql(srv, "{ orderAmounts { id amount } }")
    assert graphql.status_code != 200 or "errors" in graphql.json(), graphql.text
    assert "'orders' is a draft" in graphql.text, graphql.text

    # Released: the same statement over the same view is answered again.
    _mutate(srv, f"setTableDraft(tableId: {table_id}, draft: false)")
    again = through_the_view()
    assert again.status_code == 200, again.text
    assert again.json() == answered.json()


def _view_builds(srv) -> dict[str, dict]:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "query { mvList { id status lastError } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return {mv["id"]: mv for mv in response.json()["data"]["mvList"]}


def _draft_view_ids(srv) -> list[int]:
    """The ids the draft view answers the role here (org_admin) by SQL."""
    read = _sql(srv, "SELECT id FROM order_amounts ORDER BY id")
    assert read.status_code == 200, read.text
    return [r["id"] for r in read.json()["data"]["sql"]]


def test_a_materialized_draft_view_is_built_by_its_stewards_preview_and_kept(server):
    """A draft view is offered to no reader without table registration in its domain, and is
    built on no schedule. Its steward's preview
    answers from the view's inputs at once and starts its build; the build lands while the view
    is still a draft, is kept, and the view is read by its readers once released."""
    srv, pg = server
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    _mutate(srv, f"setTableDraft(tableId: {registered_id(registered['message'])}, draft: false)")
    view = _mutate(
        srv,
        'registerTable(input: {sourceId: "__derived__", domainId: "shop", schemaName: "views", '
        'tableName: "order_amounts", alias: "order_amounts", materialize: true, '
        'viewSql: "SELECT id, amount FROM orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"]}, {name: "amount", visibleTo: ["org_admin"]}]})',
    )
    view_id = registered_id(view["message"])

    # Registered, a draft: its build is known and nothing has built it.
    time.sleep(_KEPT_S / 2)
    assert _view_builds(srv)["view-order_amounts"]["status"] != "fresh"
    # REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN: the role here holds table registration in the
    # view's domain, so its statement naming the draft view reads it, from the view's inputs.
    assert _draft_view_ids(srv) == [i for i, _ in _ID_AMOUNT]

    # Its steward's preview: answered from its inputs, and its build started.
    previewed = _previewed(srv, view_id, {"sorts": [{"col": "id", "dir": "asc"}]})
    assert [(r["id"], r["amount"]) for r in previewed["rows"]] == _ID_AMOUNT
    _wait(
        lambda: _view_builds(srv)["view-order_amounts"]["status"] == "fresh",
        seconds=90,
        what="the build the preview started",
    )
    assert _view_builds(srv)["view-order_amounts"]["lastError"] is None

    # Still a draft, and kept through the reloads and sweeps that follow.
    time.sleep(_KEPT_S)
    assert "view-order_amounts" in _view_builds(srv)
    assert _draft_view_ids(srv) == [i for i, _ in _ID_AMOUNT]

    # Released: read by its readers.
    _mutate(srv, f"setTableDraft(tableId: {view_id}, draft: false)")
    read = _sql(srv, "SELECT id, amount FROM order_amounts ORDER BY id")
    assert read.status_code == 200, read.text

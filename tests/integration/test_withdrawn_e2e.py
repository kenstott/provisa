# Copyright (c) 2026 Kenneth Stott
# Canary: 51d92067-dfd7-4fbb-abb9-c34c41cc93c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a registered table is live; withdrawn takes it out of production; put back
returns it (REQ-1921, amended 2026-11-16), on a real pg-engine server over a real source.

* Registered through the admin, a table is live at once: read over GraphQL and through a real
  PostgreSQL driver on pgwire. There is no draft.
* Withdrawn, it serves no role: the pgwire driver's statement naming it is refused, the GraphQL
  schema offers it no more, the admin's lists and the graph schema leave it out (and show it,
  marked, when asked), its replica is retired, and its steward's preview reads it in place.
* Put back, it is read again, through the same driver; replicated, its replica is built again.
* A withdrawn source takes its tables out the same way, and is put back by one action.
* A view whose input is withdrawn is refused naming it; put back, the view is answered again.
* A move between regions passes through withdrawn: put back names a region (none here: this
  server declares no regions), is refused without one, and is refused for a table or source
  that is not withdrawn.
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

_NAME = "withdrawn_e2e"
_REPLICA = f'"org_{_NAME}_replicas"."src__public__orders"'
_ID_AMOUNT = [(i, amount) for i, amount, _note in _ROWS]


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
            enable_pgwire=True,
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


def _admin(srv, document: str, variables: dict | None = None) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": document, "variables": variables or {}},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    return response.json()["data"]


def _mutate(srv, mutation: str) -> dict:
    (result,) = _admin(srv, "mutation { " + mutation + " { success message code } }").values()
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


def _pgwire(srv, sql: str) -> list[tuple]:
    """``sql`` through a real PostgreSQL driver on the server's pgwire listener, as the role."""
    with psycopg.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        dbname="provisa",
        user=_ROLE,
        password="provisa",
        autocommit=True,
    ) as conn:
        return [tuple(r) for r in conn.execute(sql).fetchall()]  # pyright: ignore[reportArgumentType]


_ORDERS_SQL = "SELECT id, amount FROM orders ORDER BY id"


def _read(srv) -> list[tuple]:
    response = _graphql(srv, "{ orders { id amount } }")
    assert response.status_code == 200, response.text
    return sorted((r["id"], r["amount"]) for r in response.json()["data"]["orders"])


def _offered(srv) -> set[str]:
    schema = _graphql(srv, "{ __schema { queryType { fields { name } } } }")
    assert schema.status_code == 200, schema.text
    return {f["name"] for f in schema.json()["data"]["__schema"]["queryType"]["fields"]}


def _listed(srv, *, include_withdrawn: bool) -> dict[str, dict]:
    tables = _admin(
        srv,
        "query($w: Boolean!) { tables(includeWithdrawn: $w) "
        "{ id tableName withdrawn sourceWithdrawn } }",
        {"w": include_withdrawn},
    )["tables"]
    return {t["tableName"]: t for t in tables}


def _graph_labels(srv, *, include_withdrawn: bool) -> dict[str, bool]:
    response = httpx.get(
        f"{srv.base_url}/data/graph-schema",
        params={"include_withdrawn": str(include_withdrawn).lower()},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    assert response.status_code == 200, response.text
    return {n["table_label"]: n["withdrawn"] for n in response.json()["node_labels"]}


def _replica_rows(pg: _SourceAndEngine) -> int | None:
    with psycopg.connect(pg.url(pg.engine_port, "provisa"), autocommit=True) as conn:
        if conn.execute("SELECT to_regclass(%s)", (_REPLICA,)).fetchone()[0] is None:  # pyright: ignore[reportOptionalSubscript]
            return None
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {_REPLICA}").fetchone()[0]  # pyright: ignore[reportOptionalSubscript]
        except psycopg.errors.UndefinedTable:
            return None  # dropped between the two statements: the drop being waited for


def _wait(condition, *, seconds: float, what: str) -> None:
    deadline = time.monotonic() + seconds
    while not condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.25)


def _source(srv, pg: _SourceAndEngine) -> None:
    _mutate(
        srv,
        'createSource(input: {id: "src", type: "postgresql", host: "127.0.0.1", '
        f'port: {pg.source_port}, database: "shop", username: "provisa", password: "provisa"}})',
    )


def _register_orders(srv) -> int:
    registered = _mutate(
        srv,
        'registerTable(input: {sourceId: "src", domainId: "shop", schemaName: "public", '
        'tableName: "orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
        '{name: "amount", visibleTo: ["org_admin"], dataType: "double"}]})',
    )
    return registered_id(registered["message"])


def _refused_by_pgwire(srv) -> str:
    with pytest.raises(psycopg.Error) as refused:
        _pgwire(srv, _ORDERS_SQL)
    return str(refused.value)


def _preview(srv, table_id: int, body: dict) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/admin/tables/{table_id}/preview",
        json=body,
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 10,
    )


def test_a_registered_table_is_live_withdrawn_refused_by_a_real_driver_and_put_back(server):
    srv, pg = server
    _source(srv, pg)
    table_id = _register_orders(srv)

    # Live on registration: read at once, over GraphQL and through the pgwire driver.
    assert _read(srv) == _ID_AMOUNT
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT
    listed = _listed(srv, include_withdrawn=False)["orders"]
    assert listed["withdrawn"] is False and "draft" not in listed

    # Withdrawn: no role is served it, on any front door.
    assert _mutate(srv, f"withdrawTable(tableId: {table_id})")["code"] == "schema.table_withdrawn"
    _refused_by_pgwire(srv)
    assert "orders" not in _offered(srv)
    sql = _sql(srv, _ORDERS_SQL)
    assert sql.status_code != 200, sql.text
    # Left out of the admin's lists and the graph schema, shown marked when a steward asks.
    assert "orders" not in _listed(srv, include_withdrawn=False)
    shown = _listed(srv, include_withdrawn=True)["orders"]
    assert (shown["withdrawn"], shown["sourceWithdrawn"]) == (True, False)
    assert "Orders" not in _graph_labels(srv, include_withdrawn=False)
    assert _graph_labels(srv, include_withdrawn=True)["Orders"] is True
    # Its steward previews it, read in place.
    previewed = _preview(srv, table_id, {"sorts": [{"col": "id", "dir": "asc"}]})
    assert previewed.status_code == 200, previewed.text
    assert [(r["id"], r["amount"]) for r in previewed.json()["rows"]] == _ID_AMOUNT

    # Put back: read again through the same driver.
    assert (
        _mutate(srv, f"putBackTable(tableId: {table_id}, region: null)")["code"]
        == "schema.table_put_back"
    )
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT
    assert _read(srv) == _ID_AMOUNT
    assert "Orders" in _graph_labels(srv, include_withdrawn=False)


def test_a_withdrawn_table_keeps_no_replica_and_put_back_is_replicated_again(server):
    srv, pg = server
    _source(srv, pg)
    table_id = _register_orders(srv)
    _mutate(srv, 'updateSourceCache(sourceId: "src", cacheEnabled: true, cacheTtl: 3600)')
    _mutate(srv, 'updateSourceReplicate(sourceId: "src", replicate: 0)')
    _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=90, what="the replica")

    _mutate(srv, f"withdrawTable(tableId: {table_id})")
    _wait(
        lambda: [b["state"] for b in _builds(srv)["builds"]] in ([], ["retired"]),
        seconds=60,
        what="the withdrawn table's replica being retired",
    )
    _wait(lambda: _replica_rows(pg) is None, seconds=90, what="the replica's drop")
    # Its source is floored to its replica (no live attach), and a withdrawn table keeps no
    # replica: its steward's preview is refused saying why, and lands nothing.
    previewed = _preview(srv, table_id, {"sorts": [{"col": "id", "dir": "asc"}]})
    assert previewed.status_code == 409, previewed.text
    assert previewed.json()["code"] == "schema.withdrawn_table_not_previewable", previewed.text
    assert _replica_rows(pg) is None

    _mutate(srv, f"putBackTable(tableId: {table_id}, region: null)")
    _wait(lambda: _replica_rows(pg) == len(_ROWS), seconds=90, what="the replica built again")
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT


def test_a_withdrawn_source_takes_its_tables_out_and_put_back_returns_them(server):
    srv, pg = server
    _source(srv, pg)
    _register_orders(srv)
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT

    withdrawn = _mutate(srv, 'withdrawSource(sourceId: "src")')
    assert withdrawn["code"] == "schema.source_withdrawn"
    _refused_by_pgwire(srv)
    assert "orders" not in _offered(srv)
    shown = _listed(srv, include_withdrawn=True)["orders"]
    assert (shown["withdrawn"], shown["sourceWithdrawn"]) == (False, True)
    sources = _admin(srv, "{ sources { id withdrawn } }")["sources"]
    assert {s["id"]: s["withdrawn"] for s in sources}["src"] is True

    assert (
        _mutate(srv, 'putBackSource(sourceId: "src", region: null)')["code"]
        == "schema.source_put_back"
    )
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT
    assert _read(srv) == _ID_AMOUNT


def test_a_view_over_a_withdrawn_table_is_refused_and_answered_once_it_is_put_back(server):
    """A view is answered from the tables its SQL names. While one of them is withdrawn the view
    is refused; the view stays, and is answered again once the table is put back."""
    srv, pg = server
    _source(srv, pg)
    table_id = _register_orders(srv)
    _mutate(
        srv,
        'registerTable(input: {sourceId: "__derived__", domainId: "shop", schemaName: "views", '
        'tableName: "order_amounts", alias: "order_amounts", '
        'viewSql: "SELECT id, amount FROM orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"]}, {name: "amount", visibleTo: ["org_admin"]}]})',
    )

    def through_the_view() -> httpx.Response:
        return _sql(srv, "SELECT id, amount FROM order_amounts ORDER BY id")

    answered = through_the_view()
    assert answered.status_code == 200, answered.text

    _mutate(srv, f"withdrawTable(tableId: {table_id})")
    refused = through_the_view()
    assert refused.status_code == 409, refused.text
    assert refused.json()["code"] == "data.table_is_withdrawn", refused.text
    assert refused.json()["params"] == {"table": "orders"}, refused.text

    _mutate(srv, f"putBackTable(tableId: {table_id}, region: null)")
    again = through_the_view()
    assert again.status_code == 200, again.text
    assert again.json() == answered.json()


def _refused(srv, mutation: str) -> dict:
    (result,) = _admin(
        srv, "mutation { " + mutation + " { success message code params } }"
    ).values()
    assert not result["success"], result
    return result


def test_a_move_through_withdrawn_puts_back_naming_a_region_and_nothing_else(server):
    """Put back names a region: refused without one, refused for what is not withdrawn (that is a
    region change, by setTableRegion), and with one it returns the table to production there."""
    srv, pg = server
    _source(srv, pg)
    table_id = _register_orders(srv)

    live = _refused(srv, f"putBackTable(tableId: {table_id}, region: null)")
    assert (live["code"], live["params"]) == ("schema.table_not_withdrawn", {"table": "orders"})
    live_source = _refused(srv, 'putBackSource(sourceId: "src", region: null)')
    assert live_source["code"] == "schema.source_not_withdrawn"

    _mutate(srv, f"withdrawTable(tableId: {table_id})")
    unnamed = _refused(srv, f"putBackTable(tableId: {table_id})")
    assert (unnamed["code"], unnamed["params"]) == (
        "schema.put_back_needs_region",
        {"object": f"table {table_id}"},
    )
    _refused_by_pgwire(srv)  # still withdrawn

    back = _mutate(srv, f"putBackTable(tableId: {table_id}, region: null)")
    assert back["code"] == "schema.table_put_back"
    assert _pgwire(srv, _ORDERS_SQL) == _ID_AMOUNT
    regions = _admin(srv, "{ tables { tableName region withdrawn } }")["tables"]
    assert {t["tableName"]: (t["region"], t["withdrawn"]) for t in regions}["orders"] == (
        None,
        False,
    )


def _view_builds(srv) -> dict[str, dict]:
    return {
        mv["id"]: mv for mv in _admin(srv, "query { mvList { id status lastError } }")["mvList"]
    }


def test_a_materialized_view_withdrawn_is_offered_to_no_reader_and_put_back_is_built(server):
    """A materialized view registered is live and built; withdrawn, it is offered to no reader
    and its build is kept unscheduled; put back, it is read by its readers."""
    srv, pg = server
    _source(srv, pg)
    _register_orders(srv)
    view = _mutate(
        srv,
        'registerTable(input: {sourceId: "__derived__", domainId: "shop", schemaName: "views", '
        'tableName: "order_amounts", alias: "order_amounts", materialize: true, '
        'viewSql: "SELECT id, amount FROM orders", columns: ['
        '{name: "id", visibleTo: ["org_admin"]}, {name: "amount", visibleTo: ["org_admin"]}]})',
    )
    view_id = registered_id(view["message"])
    _wait(
        lambda: _view_builds(srv)["view-order_amounts"]["status"] == "fresh",
        seconds=90,
        what="the build of the view registered live",
    )

    _mutate(srv, f"withdrawTable(tableId: {view_id})")
    refused = _sql(srv, "SELECT id FROM order_amounts ORDER BY id")
    assert refused.status_code != 200, refused.text
    assert "view-order_amounts" in _view_builds(srv)  # kept, unscheduled

    _mutate(srv, f"putBackTable(tableId: {view_id}, region: null)")
    read = _sql(srv, "SELECT id, amount FROM order_amounts ORDER BY id")
    assert read.status_code == 200, read.text
    assert [(r["id"], r["amount"]) for r in read.json()["data"]["sql"]] == _ID_AMOUNT

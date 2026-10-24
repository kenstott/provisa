# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: SQL and GraphQL mutations on a code table, routed to its source's commands (REQ-2100).

A Python source keeps its orders in its own process: a @table reads them, and three commands
write them, each bound to an operation by its input type -- ``add_orders(rows: list[Order])``,
``set_orders(keys, changes)`` and ``remove_orders(keys: list[int])``. A real server (DuckDB
engine, its own control plane) registers the table with a primary key and write grants, and every
write is read back through the table. An UPDATE or a DELETE first reads the keys it touches, an
ordinary governed read: its WHERE may join another table and its SET may be computed per row, and
a role's row rule narrows the keys. A table with no primary key, and one with no bound command, is
refused by name."""

from __future__ import annotations

import io
import json
import zipfile

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field, write_grants_field

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ADMIN = "org_admin"
_CLERK = "clerk"
_CODE = {
    "shop/shop.py": '''
from dataclasses import dataclass

_ORDERS = {1: ("open", 10.0, "eu"), 2: ("open", 20.0, "us"), 3: ("shipped", 30.0, "eu")}


@dataclass
class Order:
    id: int
    status: str
    total: float
    region: str


@dataclass
class OrderChanges:
    status: str | None = None
    total: float | None = None
    region: str | None = None


@dataclass
class Region:
    code: str
    zone: str


@dataclass
class Event:
    id: int
    what: str


def orders() -> list[Order]:
    """@table"""
    return [Order(i, s, t, r) for i, (s, t, r) in sorted(_ORDERS.items())]


def regions() -> list[Region]:
    """@table"""
    return [Region("eu", "emea"), Region("us", "amer")]


def events() -> list[Event]:
    """@table"""
    return [Event(1, "opened")]


def add_orders(rows: list[Order]) -> int:
    """@command"""
    for r in rows:
        _ORDERS[r["id"]] = (r["status"], r["total"], r["region"])
    return len(rows)


def set_orders(keys: list[int], changes: OrderChanges) -> int:
    """@command"""
    for k in keys:
        status, total, region = _ORDERS[k]
        _ORDERS[k] = (
            changes.get("status") or status,
            changes.get("total") if changes.get("total") is not None else total,
            changes.get("region") or region,
        )
    return len(keys)


def remove_orders(keys: list[int]) -> int:
    """@command
    @delete orders
    """
    for k in keys:
        _ORDERS.pop(k, None)
    return len(keys)


def remove_events(keys: list[int]) -> int:
    """A command that fits a delete on orders too: the tag on remove_orders tells them apart.

    @command
    """
    return len(keys)
''',
}


def _archive(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return buf.getvalue()


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [
                {"domain_id": "*", "role_id": _ADMIN},
                {"domain_id": "*", "role_id": _CLERK},
            ],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "code table mutations e2e"}],
        "roles": [
            {
                "id": _CLERK,
                "description": "A clerk.",
                "capabilities": ["query_development", "write"],
                "domain_access": ["*"],
            }
        ],
        "sources": [],
        "tables": [],
    }


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("code_table_mutations")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_table_mutations",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps"))},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": f"mutation {{ {mutation} {{ success message }} }}"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _column(name: str, data_type: str, *, key: bool = False) -> str:
    pk = ", isPrimaryKey: true" if key else ""
    return f'{{name: "{name}", visibleTo: ["{_ADMIN}", "{_CLERK}"], dataType: "{data_type}"{pk}}}'


def _sql(srv, sql: str, role: str = _ADMIN) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )


def _graphql(srv, query: str, role: str = _ADMIN) -> dict:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, body
    return body["data"]


def _ok(srv, response: httpx.Response) -> None:
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-12000:]}"


def _orders(srv) -> dict[int, tuple]:
    response = _sql(srv, "SELECT id, status, total, region FROM orders ORDER BY id")
    assert response.status_code == 200, f"{response.text}\n{srv.dump_stderr_debug()[-8000:]}"
    return {
        int(r["id"]): (r["status"], float(r["total"]), r["region"])
        for r in response.json()["data"]["sql"]
    }


@pytest.fixture(scope="module")
def shop(server):
    """The source, its tables registered and released, orders writable by both roles, and a row
    rule that shows the clerk only orders under 25."""
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "shop.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "shop-src", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    tables = {
        "orders": [
            _column("id", "bigint", key=True),
            _column("status", "text"),
            _column("total", "double"),
            _column("region", "text"),
        ],
        "regions": [_column("code", "text", key=True), _column("zone", "text")],
        "events": [_column("id", "bigint"), _column("what", "text")],
    }
    ids = {}
    for table, columns in tables.items():
        registered = _admin(
            server,
            f'registerTable(input: {{sourceId: "shop-src", domainId: "shop", '
            f'schemaName: "default", tableName: "{table}", cacheTtl: 300, columns: [{", ".join(columns)}]}})',
        )
        ids[table] = registered_id(registered["message"])
        _admin(server, release_field(ids[table]))
    _admin(
        server,
        write_grants_field(ids["orders"], [_ADMIN, _CLERK], ["id", "status", "total", "region"]),
    )
    _admin(server, write_grants_field(ids["regions"], [_ADMIN], ["code", "zone"]))
    _admin(server, write_grants_field(ids["events"], [_ADMIN], ["id", "what"]))
    _admin(
        server,
        f'upsertRlsRule(input: {{tableId: "orders", roleId: "{_CLERK}", filterExpr: "total < 25", '
        'description: "A clerk sees the small orders."})',
    )
    return ids


def test_an_insert_passes_its_typed_rows_to_the_insert_command(server, shop):
    assert _orders(server) == {
        1: ("open", 10.0, "eu"),
        2: ("open", 20.0, "us"),
        3: ("shipped", 30.0, "eu"),
    }
    response = _sql(
        server,
        "INSERT INTO orders (id, status, total, region) VALUES (4, 'open', 40, 'us'), "
        "(5, 'open', 5, 'eu')",
    )
    _ok(server, response)
    rows = _orders(server)
    assert rows[4] == ("open", 40.0, "us") and rows[5] == ("open", 5.0, "eu")


def test_an_update_by_a_where_that_joins_another_table_calls_the_command_with_its_keys(
    server, shop
):
    response = _sql(
        server,
        "UPDATE orders SET status = 'held' FROM regions AS r "
        "WHERE r.code = orders.region AND r.zone = 'emea' AND orders.status = 'open'",
    )
    _ok(server, response)
    rows = _orders(server)
    held = sorted(k for k, (status, _t, _r) in rows.items() if status == "held")
    assert held == sorted(k for k, (_s, _t, region) in rows.items() if region == "eu" and k != 3)


def test_a_computed_set_gives_each_key_its_own_changes(server, shop):
    before = _orders(server)
    response = _sql(server, "UPDATE orders SET total = total * 2 WHERE id IN (1, 2)")
    _ok(server, response)
    after = _orders(server)
    assert after[1][1] == before[1][1] * 2 and after[2][1] == before[2][1] * 2
    assert after[3] == before[3]


def test_a_delete_calls_the_command_with_the_keys_its_where_reads(server, shop):
    response = _sql(server, "DELETE FROM orders WHERE id = 3")
    _ok(server, response)
    assert 3 not in _orders(server)


def test_a_row_rule_narrows_the_keys_an_update_touches(server, shop):
    before = _orders(server)
    response = _sql(server, "UPDATE orders SET region = 'apac' WHERE id > 0", role=_CLERK)
    _ok(server, response)
    after = _orders(server)
    for key, (_status, total, region) in before.items():
        if total < 25:
            assert after[key][2] == "apac", key
        else:
            assert after[key][2] == region, key  # outside the clerk's rows: untouched


def test_a_table_with_no_primary_key_or_no_bound_command_is_refused_by_name(server, shop):
    no_key = _sql(server, "DELETE FROM events WHERE id = 1")
    assert no_key.status_code >= 400 and "primary key" in no_key.text, no_key.text
    unbound = _sql(server, "INSERT INTO regions (code, zone) VALUES ('ap', 'apac')")
    assert unbound.status_code >= 400 and "no command" in unbound.text, unbound.text


def test_a_graphql_mutation_goes_through_the_same_pipeline(server, shop):
    inserted = _graphql(
        server,
        'mutation { insertOrders(input: {id: 9, status: "open", total: 9, region: "us"}) '
        "{ affected_rows } }",
    )
    assert inserted["insertOrders"]["affected_rows"] == 1
    updated = _graphql(
        server,
        'mutation { updateOrders(set: {status: "gql"}, where: {id: {eq: 9}}) { affected_rows } }',
    )
    assert updated["updateOrders"]["affected_rows"] == 1
    assert _orders(server)[9][0] == "gql"

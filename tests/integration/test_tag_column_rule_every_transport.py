# Copyright (c) 2026 Kenneth Stott
# Canary: 4c9e2b71-8d35-4a06-b7f1-3e6a9c5d2f80
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column rule keyed on a tag hides or masks the column on every transport (REQ-2278).

One real server, one table of four orders whose ``region`` column carries the tag ``restricted``.
The column's own grant serves it to every role; two column rules on the tag, saved through the
admin API, decide the rest without naming the column:

* ``org_admin`` -- no rule: the regions in clear;
* ``rule_masked`` -- a mask rule with no mask: every region completely masked (``****``);
* ``rule_hidden`` -- a hide rule: the role reads the table without the column.

Taking the tag off the column releases it at once, and putting it back brings it under the rules
again. Everything here lands on the TEST instance: a dedicated server on its own ports and
database."""

# Requirements: REQ-2278

from __future__ import annotations

import json
import os

import pytest
import sqlalchemy as sa

from tests.integration.test_row_filter_every_transport import _http, _ok
from tests.integration.worker_boot_harness import WorkerBoot, _config

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_NEW_ROLES = ["rule_masked", "rule_hidden"]
_REGIONS = {"east", "west"}
_MASKED = {"****"}


def _sql_http(boot, role: str) -> list[dict]:
    out = _ok(*_http(boot, role, "POST", "/data/sql", {"sql": "SELECT * FROM sales.orders"}))
    return out["data"]["sql"]


def _rest(boot, role: str) -> list[dict]:
    return _ok(*_http(boot, role, "GET", "/data/rest/sales/orders"))["data"]


def _pgwire(boot, role: str) -> list[dict]:
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=role,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
        row_factory=dict_row,
    ) as conn:
        return list(conn.execute("SELECT * FROM sales.orders").fetchall())


def _flight(boot, role: str) -> list[dict]:
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = fl.Ticket(
            json.dumps({"query": "SELECT * FROM sales.orders", "role": role}).encode()
        )
        return client.do_get(ticket).read_all().to_pylist()
    finally:
        client.close()


def _graphql(boot, role: str) -> list[dict]:
    """The orders with their region where the role's schema has the field; without it, by id."""
    status, body = _http(
        boot, role, "POST", "/data/graphql", {"query": "{ s__orders { id region } }"}
    )
    out = json.loads(body)
    if status == 200 and "errors" not in out:
        return out["data"]["s__orders"]
    assert any("region" in e.get("message", "") for e in out.get("errors", [])), body
    out = _ok(*_http(boot, role, "POST", "/data/graphql", {"query": "{ s__orders { id } }"}))
    return out["data"]["s__orders"]


_TRANSPORTS = {
    "sql_http": _sql_http,
    "rest": _rest,
    "pgwire": _pgwire,
    "flight": _flight,
    "graphql": _graphql,
}


def _regions(rows: list[dict]) -> set | None:
    """The region values the rows carry; None when the column is not there at all."""
    assert len(rows) == 4, rows
    if not all("region" in r for r in rows):
        assert not any("region" in r for r in rows), rows
        return None
    return {r["region"] for r in rows}


def _save_rule(boot, role: str, action: str) -> None:
    body = {"tag_id": "restricted", "role_id": role, "action": action}
    _ok(*_http(boot, "org_admin", "PUT", "/admin/column-rules", body))


@pytest.fixture(scope="module")
def server():
    base = _config(_PG_HOST, _PG_PORT, "unused")
    orders = base["tables"][0]
    roles = ["org_admin"] + [r["id"] for r in base["roles"]] + _NEW_ROLES
    for column in orders["columns"]:
        column["visible_to"] = roles
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "tables": [orders],
            "roles": base["roles"]
            + [
                {
                    "id": r,
                    "description": "Test role.",
                    "capabilities": ["query_development"],
                    "domain_access": ["*"],
                }
                for r in _NEW_ROLES
            ],
            "tags": [{"id": "restricted", "applies_to": ["column"]}],
            "tag_assignments": [
                {
                    "tag_id": "restricted",
                    "object_type": "column",
                    "table_ref": "sales-pg.public.orders",
                    "column_name": "region",
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        own = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
        with own.connect() as conn:
            conn.execute(sa.text("INSERT INTO public.orders VALUES (3, 'east'), (4, 'west')"))
        own.dispose()
        boot.start()
        boot.wait_all_ready(timeout=300)
        _save_rule(boot, "rule_masked", "mask")
        _save_rule(boot, "rule_hidden", "hide")
        yield boot
    finally:
        boot.cleanup()


def _admin(boot, query: str) -> dict:
    out = _ok(*_http(boot, "org_admin", "POST", "/admin/graphql", {"query": query}))
    assert "errors" not in out, out
    return out["data"]


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_the_rule_on_the_tag_hides_or_masks_the_column(server, transport):
    read = _TRANSPORTS[transport]
    assert _regions(read(server, "org_admin")) == _REGIONS
    assert _regions(read(server, "rule_masked")) == _MASKED
    assert _regions(read(server, "rule_hidden")) is None


def test_the_rules_are_listed_by_their_tag(server):
    out = _ok(*_http(server, "org_admin", "GET", "/admin/column-rules?tag=restricted"))
    assert {(r["roleId"], r["action"]) for r in out["rules"]} == {
        ("rule_masked", "mask"),
        ("rule_hidden", "hide"),
    }


def test_taking_the_tag_off_releases_the_column_and_putting_it_back_restores_the_rule(server):
    table_id = next(
        t["id"]
        for t in _admin(server, "{ tables { id tableName } }")["tables"]
        if t["tableName"] == "orders"
    )
    assignment = (
        f'{{tagId: "restricted", objectType: "column", tableId: {table_id}, columnName: "region"}}'
    )
    try:
        removed = _admin(
            server, f"mutation {{ unassignTag(input: {assignment}) {{ success message }} }}"
        )["unassignTag"]
        assert removed["success"], removed["message"]
        assert _regions(_sql_http(server, "rule_masked")) == _REGIONS
        assert _regions(_sql_http(server, "rule_hidden")) == _REGIONS
    finally:
        restored = _admin(
            server, f"mutation {{ assignTag(input: {assignment}) {{ success message }} }}"
        )["assignTag"]
    assert restored["success"], restored["message"]
    assert _regions(_sql_http(server, "rule_masked")) == _MASKED
    assert _regions(_sql_http(server, "rule_hidden")) is None


def test_a_tag_that_carries_a_column_rule_is_refused_deletion(server):
    refused = _admin(server, 'mutation { deleteTag(id: "restricted") { success code params } }')[
        "deleteTag"
    ]
    assert (refused["success"], refused["code"]) == (False, "schema.tag_has_dependents")
    assert {d["kind"] for d in refused["params"]["dependents"]} == {"column_rule"}

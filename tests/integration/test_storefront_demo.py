# Copyright (c) 2026 Kenneth Stott
# Canary: 4a7f8ce6-0d2a-4799-9d5d-32d8bfa06519
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``storefront`` works as shipped.

Its own docker-compose.yml brings up its three stores on ports of this test's own, under a compose
project of its own: the order database (Postgres, db/init.sql), the product catalog (MongoDB,
db/catalog.js) and the shipping service (REST, shipping/). A real Provisa server (DuckDB engine,
its own control plane) loads the demo's config.yaml as is, every store read live.

The tour's example, its JSON:API call and its statement each answer rows as the role the tour
runs them as (its role steps); the customer reads only customer 42's orders, the analyst every
order with each email masked; JSON:API ids are primary keys (a line's is its composite key) and
each order links what it includes.
"""

# Requirements: REQ-2010

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "storefront"
_ADMIN = "org_admin"
#: The demo's signed-in customer (config.yaml's row filters) and their orders, as db/init.sql
#: places them: order n is customer 1 + (n * 37) % 400's.
_CUSTOMER = 42
_CUSTOMER_ORDERS = sorted(n for n in range(1, 3001) if 1 + (n * 37) % 400 == _CUSTOMER)


class StorefrontStack:
    """The demo's compose stack, on leased ports."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.mongo_port, self.shipping_port = lease_ports(3)
        self.project = f"storefront-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {
            "STOREFRONT_PG_PORT": str(self.pg_port),
            **store_namespaces(_DEMO),
            "STOREFRONT_MONGO_PORT": str(self.mongo_port),
            "STOREFRONT_SHIPPING_PORT": str(self.shipping_port),
        }

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                self.project,
                "-f",
                str(_DEMO / "docker-compose.yml"),
                *args,
            ],
            env={**os.environ, **self.env},
            capture_output=True,
            text=True,
            check=check,
            timeout=900,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--build", "--wait")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the storefront stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = StorefrontStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("storefront_demo")
    srv = IsolatedServer(
        "storefront_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=600)
    try:
        await_tables_serving(
            srv,
            _ADMIN,
            [
                f'"shop"."{t}"'
                for t in ("customers", "orders", "order_items", "products", "shipments")
            ],
            timeout=600,
        )
        yield srv
    finally:
        srv.stop_process()


def _headers(role: str) -> dict[str, str]:
    return {"X-Provisa-Role": role}


def query(server, sql: str, role: str = _ADMIN) -> list[dict[str, Any]]:
    resp = httpx.post(
        f"{server.base_url}/data/sql", json={"sql": sql}, headers=_headers(role), timeout=300
    )
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def jsonapi(server, path: str, role: str) -> dict[str, Any]:
    resp = httpx.get(
        f"{server.base_url}{path}",
        headers={**_headers(role), "Accept": "application/vnd.api+json"},
        timeout=300,
    )
    assert resp.status_code == 200, f"{path}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()


def _tour_with_roles() -> list[tuple[dict[str, Any], str]]:
    """Each step of tour.yaml with the role it runs as: the role step before it ("all" is every
    role, the demo's org_admin), org_admin before any."""
    role = _ADMIN
    out = []
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        if "role" in step:
            role = _ADMIN if step["role"] == "all" else step["role"]
        out.append((step, role))
    return out


_TOUR = _tour_with_roles()


def _ids(case: tuple[dict[str, Any], str]) -> str:
    return f"{case[0]['key']}-as-{case[1]}"


@pytest.mark.parametrize("case", [c for c in _TOUR if "sql" in c[0]], ids=_ids)
def test_each_tour_statement_answers_rows_as_its_role(server, case):
    step, role = case
    assert query(server, step["sql"], role), step["key"]


@pytest.mark.parametrize("case", [c for c in _TOUR if "example" in c[0]], ids=_ids)
def test_each_tour_example_answers_rows_as_its_role(server, case):
    step, role = case
    text = (_DEMO / "queries" / f"{step['example']}.graphql").read_text()
    resp = httpx.post(
        f"{server.base_url}/data/graphql", json={"query": text}, headers=_headers(role), timeout=300
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, step["key"]


@pytest.mark.parametrize("case", [c for c in _TOUR if "jsonapi" in c[0]], ids=_ids)
def test_each_tour_jsonapi_call_answers_rows_as_its_role(server, case):
    step, role = case
    assert jsonapi(server, step["jsonapi"], role)["data"], step["key"]


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": path.read_text()},
        headers=_headers(_ADMIN),
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


def test_the_customer_reads_only_their_own_orders(server):
    rows = query(server, 'SELECT id, customer_id FROM "shop"."orders" ORDER BY id', "customer")
    assert [r["id"] for r in rows] == _CUSTOMER_ORDERS
    assert {r["customer_id"] for r in rows} == {_CUSTOMER}
    [me] = query(server, 'SELECT id FROM "shop"."customers"', "customer")
    assert me["id"] == _CUSTOMER
    lines = query(server, 'SELECT DISTINCT order_id FROM "shop"."order_items"', "customer")
    assert sorted(r["order_id"] for r in lines) == _CUSTOMER_ORDERS


def test_the_customer_reads_their_own_orders_in_the_apps_query(server):
    text = (_DEMO / "queries" / "1-order-history.graphql").read_text()
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": text},
        headers=_headers("customer"),
        timeout=300,
    )
    body = resp.json()
    assert not body.get("errors"), body
    [orders] = body["data"].values()
    assert sorted(o["id"] for o in orders) == _CUSTOMER_ORDERS


def test_the_analyst_reads_every_order_and_no_email(server):
    [row] = query(server, 'SELECT COUNT(*) AS n FROM "shop"."orders"', "shop_analyst")
    assert row["n"] == 3000
    emails = [
        r["email"] for r in query(server, 'SELECT email FROM "shop"."customers"', "shop_analyst")
    ]
    assert len(emails) > 0
    assert all(e.startswith("***@") for e in emails), emails[:5]
    [admin] = query(server, f'SELECT email FROM "shop"."customers" WHERE id = {_CUSTOMER}')
    assert admin["email"] == f"customer{_CUSTOMER}@example.com"


def test_jsonapi_ids_are_keys_and_each_order_links_what_it_includes(server):
    body = jsonapi(server, "/data/jsonapi/shop/orders?include=order_items,shipment", "customer")
    assert sorted(int(r["id"]) for r in body["data"]) == _CUSTOMER_ORDERS
    included = {(i["type"], i["id"]) for i in body["included"]}
    assert len(included) == len(body["included"]), "an included resource appears twice"
    linked = set()
    for order in body["data"]:
        lines = order["relationships"]["order_items"]["data"]
        # A line's id is its composite key, order then line.
        assert lines and all(li["id"].split(":")[0] == order["id"] for li in lines), order
        linked |= {(li["type"], li["id"]) for li in lines}
        shipment = order["relationships"]["shipment"]["data"]
        if shipment is not None:
            assert shipment["id"] == order["id"], order
            linked.add((shipment["type"], shipment["id"]))
    assert linked == included


def test_the_shipping_service_is_read_over_rest(server):
    rows = query(server, 'SELECT order_id, carrier, tracking FROM "shop"."shipments"')
    assert rows
    assert all(r["carrier"] and r["tracking"] for r in rows), rows[:3]
    own = query(server, 'SELECT order_id FROM "shop"."shipments"', "customer")
    assert own and {r["order_id"] for r in own} <= set(_CUSTOMER_ORDERS)

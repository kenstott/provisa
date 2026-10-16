# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2063: the named demo ``datavault`` works as shipped, its vault refreshed by change signals.

Its own docker-compose.yml brings the order system and the CRM up on ports of this test's own,
under a compose project of its own. A real Provisa server (DuckDB engine, its own control plane)
loads the demo's config.yaml as is, and the demo's setup.yaml is run against it as the bundle build
runs it (scripts/run_demo_setup.py): each scripted change sent as a new row through a command. The
vault's views follow through the sources' change signals, nothing refreshing them by hand: the
satellites keep every version, a resend with nothing changed is no version, and the point-in-time
view answers what was known about Alfreds Futterkiste, from which source, and when. As the analyst,
the satellites are masked and limited to European customers.
"""

# Requirements: REQ-2063

from __future__ import annotations

import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "datavault"
_ROLE = "org_admin"
# How long the views may take to follow the changes: each source table's probe runs on its 10 s
# cache TTL, and each view after it recomputes as the change ripples on.
_FOLLOW_S = 180
_EUROPE = {
    "Austria", "Belgium", "Denmark", "Finland", "France", "Germany", "Ireland", "Italy", "Norway",
    "Poland", "Portugal", "Spain", "Sweden", "Switzerland", "UK",
}  # fmt: skip


class VaultStack:
    """The demo's two databases, on leased ports."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.orders_port, self.crm_port = lease_ports(2)
        self.project = f"datavault-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {
            "DATAVAULT_ORDERS_PORT": str(self.orders_port),
            "DATAVAULT_CRM_PORT": str(self.crm_port),
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
            self._compose("up", "-d", "--wait")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the data vault stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = VaultStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("datavault_demo")
    srv = IsolatedServer(
        "datavault_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=300
    )


def query(server, sql: str, role: str = _ROLE) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _versions(server) -> dict[str, int]:
    """How many versions the satellites hold of Alfreds Futterkiste, Chai and Chang."""
    rows = query(
        server,
        'SELECT \'orders\' AS k, COUNT(*) AS n FROM "vault"."view_sat_customer_orders" '
        "WHERE customer_hk = MD5('ALFKI') "
        'UNION ALL SELECT \'crm\' AS k, COUNT(*) AS n FROM "vault"."view_sat_customer_crm" '
        "WHERE customer_hk = MD5('ALFKI') "
        'UNION ALL SELECT \'products\' AS k, COUNT(*) AS n FROM "vault"."view_sat_product" '
        "WHERE product_hk IN (MD5('1'), MD5('2'))",
    )
    return {r["k"]: r["n"] for r in rows}


def follow(server, done: Callable[[dict], bool]) -> dict:
    deadline = time.monotonic() + _FOLLOW_S
    while True:
        state = _versions(server)
        if done(state):
            return state
        assert time.monotonic() < deadline, f"the vault did not follow the changes: {state}"
        time.sleep(2)


@pytest.fixture(scope="module")
def changed(server):
    """The vault before the scripted changes, then after setup.yaml has sent them all."""
    before = follow(server, lambda v: v == {"orders": 1, "crm": 1, "products": 2})
    subprocess.run(
        [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
        + ["--url", server.base_url],
        check=True,
        timeout=600,
    )
    after = follow(server, lambda v: v == {"orders": 2, "crm": 2, "products": 4})
    return server, before, after


def test_the_hubs_and_links_hold_every_business_key_once(changed):
    server, *_ = changed
    [row] = query(
        server,
        'SELECT (SELECT COUNT(*) FROM "vault"."view_hub_customer") AS customers, '
        '(SELECT COUNT(*) FROM "vault"."view_hub_product") AS products, '
        '(SELECT COUNT(*) FROM "vault"."view_hub_order") AS orders, '
        '(SELECT COUNT(*) FROM "vault"."view_link_customer_order") AS placed, '
        '(SELECT COUNT(*) FROM "vault"."view_link_order_product") AS contains',
    )
    assert row == {"customers": 91, "products": 77, "orders": 830, "placed": 830, "contains": 2155}


def test_each_real_change_is_a_version_and_a_resend_is_none(changed):
    _server, before, after = changed
    assert before == {"orders": 1, "crm": 1, "products": 2}
    # The CRM's resend on 1 October changed nothing, so the CRM satellite gains one version, not two.
    assert after == {"orders": 2, "crm": 2, "products": 4}


def test_the_point_in_time_view_answers_what_was_known_and_when(changed):
    server, *_ = changed
    rows = query(
        server,
        "SELECT CAST(as_of AS VARCHAR) AS as_of, orders_source, orders_address, crm_source, "
        'crm_address FROM "vault"."view_customer_as_known" '
        "WHERE customer_hk = MD5('ALFKI') ORDER BY as_of",
    )
    old, new = "Obere Str. 57", "Kurfürstendamm 21"
    assert [(r["as_of"][:10], r["orders_address"], r["crm_address"]) for r in rows] == [
        ("1996-07-01", old, old),
        ("1997-09-01", new, old),
        ("1997-11-15", new, new),
    ]
    assert {(r["orders_source"], r["crm_source"]) for r in rows} == {("orders", "crm")}


def test_the_mart_is_the_customer_as_it_stands_now(changed):
    server, *_ = changed
    [row] = query(
        server,
        'SELECT address, postal_code, contact_email FROM "vault"."view_dim_customer" '
        "WHERE customer_hk = MD5('ALFKI')",
    )
    assert row == {
        "address": "Kurfürstendamm 21",
        "postal_code": "10719",
        "contact_email": "maria.anders@alfreds.example",
    }


def test_an_analyst_reads_european_satellites_with_personal_data_masked(changed):
    server, *_ = changed
    rows = query(
        server,
        'SELECT country, contact_name, contact_email, phone FROM "vault"."view_sat_customer_crm"',
        role="analyst_eu",
    )
    assert rows and {r["country"] for r in rows} <= _EUROPE
    for r in rows:
        assert r["contact_name"] == "withheld"
        assert r["contact_email"].startswith("***@")
        assert not any(ch.isdigit() for ch in r["phone"] or "")


def test_an_analyst_reads_the_views_built_on_the_satellites_governed_alike(changed):
    server, *_ = changed
    for view in ("view_customer_as_known", "view_dim_customer"):
        rows = query(
            server,
            f'SELECT country, contact_name, contact_email FROM "vault"."{view}"',
            "analyst_eu",
        )
        assert rows and {r["country"] for r in rows} <= _EUROPE, view
        assert {r["contact_name"] for r in rows} == {"withheld"}, view


def test_an_analyst_reads_no_personal_data_from_the_sources_either(changed):
    # Through a view a reader sees only what it sees of the view's inputs, so the analyst holds
    # the sources; their personal data is masked there as in the satellites.
    server, *_ = changed
    rows = query(
        server, 'SELECT contact_name, contact_email, phone FROM "crm"."account"', "analyst_eu"
    )
    assert rows
    for r in rows:
        assert r["contact_name"] == "withheld"
        assert r["contact_email"].startswith("***@")
        assert not any(ch.isdigit() for ch in r["phone"] or "")


def test_an_analyst_reads_the_hubs_in_full(changed):
    server, *_ = changed
    [row] = query(
        server, 'SELECT COUNT(*) AS n FROM "vault"."view_hub_customer"', role="analyst_eu"
    )
    assert row["n"] == 91


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(changed, path):
    server, *_ = changed
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize(
    "step", [s for s in _TOUR if {"sql", "api", "lineage"} & set(s)], ids=lambda s: s["key"]
)
def test_each_tour_statement_answers(changed, step):
    server, *_ = changed
    if "sql" in step:
        assert query(server, step["sql"]), step["key"]
        return
    if "lineage" in step:
        resp = _post(server, "/admin/lineage/graph", {"sql": step["lineage"]})
        assert resp.status_code == 200, resp.text
        assert resp.json()["nodes"], step["key"]
        return
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]

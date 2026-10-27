# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``agentdata`` works as shipped: what its tour says the MCP explorer's agent is
served, as each role, is what the one pipeline serves that role.

Its own docker-compose.yml brings the order database up on a port of this test's own, under a
compose project of its own. A real Provisa server (DuckDB engine, its own control plane) loads the
demo's config.yaml as is. The agent's run_sql tool runs a statement through the same pipeline as
/data/sql (provisa/api/mcp/tools.py run_sql), so each statement here is one the agent could send,
sent as the role the tour picks; the explain is the one the explorer opens on a run_sql call."""

# Requirements: REQ-2172

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "agentdata"
# The support agent's question, as an agent writes it: last week of September's customers, and how
# to reach them.
_RECENT = (
    'SELECT DISTINCT c.name, c.email, c.region FROM "shop"."customers" AS c '
    'JOIN "shop"."orders" AS o ON o.customer_id = c.id '
    "WHERE o.placed_at >= TIMESTAMP '2026-09-24' ORDER BY c.name"
)


class OrdersStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"agentdata-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"AGENTDATA_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

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
                f"the database did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = OrdersStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("agentdata_demo")
    srv = IsolatedServer(
        "agentdata_demo",
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


def _sql(server, role: str, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=300,
    )


def _rows(server, role: str, sql: str) -> list[dict[str, Any]]:
    resp = _sql(server, role, sql)
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def test_the_support_agent_reads_the_west_s_recent_customers_emails_masked(server):
    rows = _rows(server, "support_west", _RECENT)
    assert rows, "the West has customers who ordered that week"
    assert {r["region"] for r in rows} == {"West"}, rows
    assert all(r["email"].startswith("***@") for r in rows), rows


def test_the_explain_of_that_call_shows_the_filter_and_mask_governance_added(server):
    resp = httpx.post(
        f"{server.base_url}/data/sql/explain",
        json={"sql": _RECENT, "analyze": False},
        headers={"X-Provisa-Role": "support_west"},
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    ran = resp.json()["sql"]
    assert "'West'" in ran, ran
    assert "'***'" in ran, ran
    assert "'West'" not in _RECENT and "***" not in _RECENT


def test_the_support_agent_reads_no_prices(server):
    resp = _sql(
        server,
        "support_west",
        'SELECT i.order_id, i.quantity * i.unit_price AS total FROM "shop"."order_items" AS i',
    )
    assert resp.status_code == 403, resp.text


def test_the_analyst_reads_every_region_and_no_email(server):
    rows = _rows(
        server,
        "shop_analyst",
        'SELECT DISTINCT c.region FROM "shop"."customers" AS c '
        'JOIN "shop"."orders" AS o ON o.customer_id = c.id '
        "WHERE o.placed_at >= TIMESTAMP '2026-09-24'",
    )
    assert {r["region"] for r in rows} == {"North", "South", "East", "West", "Central"}, rows
    assert _sql(server, "shop_analyst", _RECENT).status_code == 403


def test_the_analyst_s_example_reads_what_each_customer_s_orders_came_to(server):
    query = (_DEMO / "queries" / "1-revenue-by-customer.graphql").read_text()
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "shop_analyst"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]["s__viewRevenueByCustomer"]
    assert len(rows) == 20, rows
    revenue = [float(r["revenue"]) for r in rows]
    assert revenue == sorted(revenue, reverse=True), revenue

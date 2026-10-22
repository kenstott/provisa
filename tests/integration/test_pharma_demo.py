# Copyright (c) 2026 Kenneth Stott
# Canary: d6d5bdad-63f6-4684-a308-c9589204a56c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2038: the named demo ``pharma`` works as shipped.

Its own docker-compose.yml brings the pharmacy's Postgres up on a port of this test's own, under a
compose project of its own (db/init.sql holds the formulary and generates the stock). A real
Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is: the
formulary read live, and the three govdata health tables landed from AskAmerica as TTL -1
replicas on their first read, the recalls and the utilization only as far as their landing
filters reach. The answer's view is read live and built, every risk it names has products, and
the demo's example queries, its tour's statements and its REST call answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2038, REQ-2031, REQ-2062

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

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "pharma"
_ROLE = "org_admin"
#: What db/init.sql holds: the formulary, and its stock at each of three sites.
_PRODUCTS = 36
_STOCK = 108
_VIEW = '"pharma"."view_product_risk"'


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the pharma demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class PharmaStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"pharma-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"PHARMA_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
            timeout=600,
        )

    def up(self) -> None:
        try:
            self._compose("up", "-d", "--wait", "postgres")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the pharma stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = PharmaStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("pharma_demo")
    srv = IsolatedServer(
        "pharma_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "ASKAMERICA_API_KEY": _api_key(),
            "PROVISA_DATA_DIR": str(home / "provisa"),
        },
    )
    # The govdata server mounts its schemas from object storage before it accepts a connection,
    # which takes minutes on a cold cache.
    srv.start(timeout=900)
    try:
        # Each govdata table answers before the first read: the server may still be mounting
        # schemas past what one statement waits, and each replica lands on its first read,
        # longer than one request lasts.
        await_tables_serving(
            srv,
            _ROLE,
            [
                '"pharma"."drug_shortages"',
                '"pharma"."drug_recalls"',
                '"pharma"."medicaid_utilization"',
            ],
            timeout=1800,
        )
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=900
    )


def query(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_the_formulary_is_read_live(server):
    [row] = query(
        server,
        'SELECT (SELECT COUNT(*) FROM "pharma"."products") AS products, '
        '(SELECT COUNT(*) FROM "pharma"."stock") AS stock',
    )
    assert (row["products"], row["stock"]) == (_PRODUCTS, _STOCK)


@pytest.mark.parametrize(
    ("table", "at_least"),
    [("drug_shortages", 1000), ("drug_recalls", 1000), ("medicaid_utilization", _PRODUCTS)],
)
def test_each_govdata_table_is_landed_with_rows(server, table, at_least):
    [row] = query(server, f'SELECT COUNT(*) AS n FROM "pharma"."{table}"')
    assert row["n"] >= at_least, table


def test_the_recalls_land_only_the_ongoing(server):
    rows = query(server, 'SELECT DISTINCT status FROM "pharma"."drug_recalls"')
    assert [r["status"] for r in rows] == ["Ongoing"]


def test_the_utilization_lands_only_2023_and_the_formulary(server):
    rows = query(
        server,
        'SELECT DISTINCT u."year" AS y, u.product_ndc9 FROM "pharma"."medicaid_utilization" AS u',
    )
    formulary = {
        r["product_ndc9"] for r in query(server, 'SELECT product_ndc9 FROM "pharma"."products"')
    }
    assert {r["y"] for r in rows} == {"2023"}
    assert {r["product_ndc9"] for r in rows} == formulary


def test_every_product_has_its_medicaid_prescriptions(server):
    rows = query(server, f"SELECT product_ndc, prescriptions_2023 FROM {_VIEW}")
    assert len(rows) == _PRODUCTS
    missing = [r["product_ndc"] for r in rows if not r["prescriptions_2023"]]
    assert missing == [], (
        f"products of the formulary with no 2023 Medicaid prescriptions: {missing}"
    )


def test_every_risk_has_products(server):
    """The formulary was drawn so each answer the view gives is real: products in a current
    shortage, products whose maker has an ongoing Class I or II recall, both, and neither."""
    rows = query(server, f"SELECT risk, COUNT(*) AS n FROM {_VIEW} GROUP BY risk")
    assert {r["risk"] for r in rows} == {"shortage and recall", "shortage", "recall", "none"}, rows


def test_the_answer_is_the_same_live_and_built(server):
    sql = (
        f"SELECT product_ndc, risk, open_recalls, days_of_supply FROM {_VIEW} ORDER BY product_ndc"
    )
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {"query": 'mutation { refreshMv(mvId: "view-view_product_risk") { success message } }'},
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live
    assert len(live) == _PRODUCTS


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "sql" in s], ids=lambda s: s["key"])
def test_each_tour_statement_answers_rows(server, step):
    assert query(server, step["sql"]), step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(server, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    assert resp.json()["data"], step["key"]

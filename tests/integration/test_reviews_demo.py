# Copyright (c) 2026 Kenneth Stott
# Canary: 21355bf5-7c17-4fd7-b69a-c9f7df69862b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2030: the named demo ``reviews`` works as shipped.

Its own docker-compose.yml brings the shop's three stores up -- MongoDB, Elasticsearch and Neo4j --
on ports of this test's own, under a compose project of its own; its seeder fills them. A real
Provisa server (DuckDB engine) loads the demo's config.yaml as is and reads the stores live. Then
each table is read, the search, the pipeline and the Cypher table are checked against what the
seeder reports it wrote, and the demo's example queries and every statement and call of its tour
are run.

Everything here is the ``test`` instance: the stack and an isolated server of the test's own."""

# Requirements: REQ-2030

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "reviews"
_ROLE = "org_admin"


class ReviewsStack:
    """The demo's compose stack, on leased ports, its seeder's marker under ``home``."""

    def __init__(self, home: Path) -> None:
        from tests.port_lease import lease_ports

        self.home = home
        self.mongo_port, self.es_port, self.http_port, self.bolt_port = lease_ports(4)
        self.project = f"reviews-demo-{os.getpid()}"
        (home / "data").mkdir(parents=True, exist_ok=True)

    @property
    def env(self) -> dict[str, str]:
        return {
            "PROVISA_NAMED_DEMO_DIR": str(self.home),
            "REVIEWS_MONGO_PORT": str(self.mongo_port),
            **store_namespaces(_DEMO),
            "REVIEWS_ES_PORT": str(self.es_port),
            "REVIEWS_NEO4J_HTTP_PORT": str(self.http_port),
            "REVIEWS_NEO4J_BOLT_PORT": str(self.bolt_port),
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
        """Bring the stores up and fill them: the seeder's exit is the end of it."""
        try:
            self._compose("up", "-d", "--build", "--wait", "mongodb", "elasticsearch", "neo4j")
            self._compose("up", "--build", "--exit-code-from", "seeder", "seeder")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the reviews stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def written(self) -> dict[str, int]:
        """How many of each the seeder wrote, as it recorded them."""
        return json.loads((self.home / "data" / ".seeded").read_text())

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    demo = ReviewsStack(tmp_path_factory.mktemp("reviews"))
    try:
        demo.up()
        yield demo
    finally:
        demo.down()


@pytest.fixture(scope="module")
def server(stack):
    from tests.integration.isolated_server import IsolatedServer

    srv = IsolatedServer(
        "reviews_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        env={**stack.env, "PROVISA_DATA_DIR": str(stack.home / "provisa")},
    )
    srv.start(timeout=300)
    try:
        yield srv
    except BaseException:
        print(srv.dump_stderr_debug()[-6000:])
        raise
    finally:
        srv.stop_process()


def _post(server: Any, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=300
    )


def query(server: Any, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _count(server: Any, table: str) -> int:
    return query(server, f'SELECT COUNT(*) AS n FROM "store"."{table}"')[0]["n"]


def test_each_store_s_table_holds_what_the_seeder_wrote(stack, server):
    written = stack.written()
    assert {
        "products": _count(server, "products"),
        "reviews": _count(server, "reviews"),
        "customers": _count(server, "customers"),
        "purchases": _count(server, "purchases"),
    } == {k: written[k] for k in ("products", "reviews", "customers", "purchases")}


def test_a_product_s_nested_specs_are_its_columns(server):
    rows = query(
        server,
        'SELECT COUNT(*) AS n FROM "store"."products" '
        "WHERE category = 'power bank' AND battery_hours IS NULL AND weight_g > 0",
    )
    assert rows[0]["n"] > 0


def test_the_search_finds_every_review_that_says_the_battery_dies(stack, server):
    rows = query(
        server,
        'SELECT review_id, score, text, highlight_text FROM "store"."reviews_matching" '
        "WHERE _nf_phrase = 'battery dies'",
    )
    assert len(rows) == stack.written()["battery_dies"]
    for row in rows:
        assert "battery dies" in row["text"].lower(), row
        assert "<em>" in json.dumps(row["highlight_text"]), row
        assert row["score"] > 0, row


def test_the_search_without_its_phrase_is_refused_by_name(server):
    resp = _post(server, "/data/sql", {"sql": 'SELECT review_id FROM "store"."reviews_matching"'})
    assert resp.status_code != 200, resp.text
    assert "phrase" in resp.text, resp.text


def test_the_pipeline_rates_every_rated_product(stack, server):
    rows = query(
        server,
        "SELECT SUM(ratings) AS ratings, MIN(avg_stars) AS low, MAX(avg_stars) AS high "
        'FROM "store"."ratings_by_product"',
    )
    assert rows[0]["ratings"] == stack.written()["ratings"]
    assert 1 <= rows[0]["low"] < rows[0]["high"] <= 5


def test_buyers_of_a_weak_battery_most_often_also_bought_a_power_bank(server):
    rows = query(
        server,
        'SELECT o.category, SUM(t.customers) AS customers FROM "store"."bought_together" AS t '
        'JOIN "store"."products" AS o ON o.product_id = t.also_bought_id '
        "WHERE t.product_id = 6 GROUP BY o.category ORDER BY customers DESC",
    )
    assert rows[0]["category"] == "power bank", rows


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, f"{resp.text}\n{server.dump_stderr_debug()[-4000:]}"
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize(
    "step",
    [s for s in _TOUR if {"sql", "cypher", "api"} & set(s)],
    ids=lambda s: s["key"],
)
def test_each_tour_statement_answers_rows(server, step):
    if "sql" in step:
        assert query(server, step["sql"]), step["key"]
        return
    if "cypher" in step:
        resp = _post(server, "/data/cypher", {"query": step["cypher"]})
        assert resp.status_code == 200, resp.text
        assert resp.json()["rows"], step["key"]
        return
    method, path = step["api"].split(" ", 1)
    resp = httpx.request(
        method, f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300
    )
    assert resp.status_code == 200, resp.text
    assert resp.json(), step["key"]


def test_a_purchase_s_date_is_a_date(stack, server):
    """#267: the Cypher table's bought_on is typed date, compared as one."""
    rows = query(
        server,
        'SELECT COUNT(*) AS n, MIN(bought_on) AS first FROM "store"."purchases" '
        "WHERE bought_on >= DATE '2026-01-01'",
    )
    assert rows == [{"n": stack.written()["purchases"], "first": "2026-01-01"}]

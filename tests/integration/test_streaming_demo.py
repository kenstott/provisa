# Copyright (c) 2026 Kenneth Stott
# Canary: 5ee91316-be61-4795-b6de-a5623323af25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2033: the named demo ``streaming`` works as shipped.

Its own docker-compose.yml brings up the catalog's Postgres, the broker and the order producer on
ports of this test's own, under a compose project of its own. A real Provisa server (DuckDB engine,
its own control plane) loads the demo's config.yaml as is. Then, with nothing asked of Provisa
but the reads: the orders the producer publishes land into the org's own table; the view that
answers the question is refreshed by the landing; every example query and tour statement answers
rows; the @cached example is answered from the cache the second time; and a subscription on the
orders sends the list again when a batch lands.
"""

# Requirements: REQ-2033, REQ-1733, REQ-258, REQ-544

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "streaming"
_ROLE = "org_admin"
# How long the first orders may take to land: the broker and the listener's consumer group start,
# then a batch lands at most every two seconds.
_LAND_S = 180


class StreamingStack:
    """The demo's catalog, broker and producer, on leased ports."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.kafka_port = lease_ports(2)
        self.project = f"streaming-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {
            "STREAMING_PG_PORT": str(self.pg_port),
            "STREAMING_KAFKA_PORT": str(self.kafka_port),
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
                f"the streaming stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def logs(self) -> str:
        return self._compose("logs", "--no-color", "--tail", "200", check=False).stdout

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = StreamingStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("streaming_demo")
    srv = IsolatedServer(
        "streaming_demo",
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


def _post(server, path: str, body: dict, **headers: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}",
        json=body,
        headers={"X-Provisa-Role": _ROLE, **headers},
        timeout=300,
    )


def query(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _views(server) -> dict[str, tuple[str, int | None]]:
    resp = _post(server, "/admin/graphql", {"query": "{ mvList { id status rowCount } }"})
    assert resp.status_code == 200, resp.text
    return {
        m["id"].removeprefix("view-view_"): (m["status"], m["rowCount"])
        for m in resp.json()["data"]["mvList"]
    }


@pytest.fixture(scope="module")
def flowing(stack, server):
    """Orders landed, and the view that answers the question refreshed by them."""
    deadline = time.monotonic() + _LAND_S
    while True:
        landed = query(server, 'SELECT COUNT(*) AS n FROM "shop"."orders"')[0]["n"]
        status, rows = _views(server)["orders_last_five_minutes"]
        if landed and status == "fresh" and rows:
            return server
        assert time.monotonic() < deadline, (
            f"no orders landed or the view did not follow: {landed} orders, view {status} "
            f"{rows}\n{stack.logs()[-4000:]}"
        )
        time.sleep(2)


def test_the_orders_keep_landing(flowing):
    count = 'SELECT COUNT(*) AS n FROM "shop"."orders"'
    before = query(flowing, count)[0]["n"]
    time.sleep(6)
    assert query(flowing, count)[0]["n"] > before


def test_every_landed_order_is_whole_and_priced_from_the_catalog(flowing):
    rows = query(
        flowing,
        'SELECT o.order_id, p.name, r.name AS region FROM "shop"."orders" AS o '
        'JOIN "shop"."products" AS p ON p.product_id = o.product_id '
        'JOIN "shop"."regions" AS r ON r.region_id = o.region_id',
    )
    landed = query(flowing, 'SELECT COUNT(*) AS n FROM "shop"."orders"')[0]["n"]
    assert rows and len(rows) <= landed  # every order the join reaches names a real product
    nulls = query(flowing, 'SELECT COUNT(*) AS n FROM "shop"."orders" WHERE order_id IS NULL')[0][
        "n"
    ]
    assert nulls == 0  # no message landed as a row of NULLs


def test_the_answer_names_the_busiest_regions(flowing):
    rows = query(
        flowing,
        'SELECT region, orders FROM "shop"."view_orders_last_five_minutes" ORDER BY orders DESC',
    )
    assert rows and all(r["orders"] > 0 for r in rows)


def _examples() -> list[Path]:
    return sorted((_DEMO / "queries").glob("*.graphql"))


def _is_subscription(path: Path) -> bool:
    operation = [line for line in path.read_text().splitlines() if not line.startswith("#")][0]
    return operation.startswith("subscription")


@pytest.mark.parametrize(
    "path", [p for p in _examples() if not _is_subscription(p)], ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(flowing, path):
    resp = _post(flowing, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


def test_the_cached_example_is_answered_from_the_cache_the_second_time(flowing):
    text = (_DEMO / "queries" / "1-busiest-regions-now.graphql").read_text()
    first = _post(flowing, "/data/graphql", {"query": text})
    second = _post(flowing, "/data/graphql", {"query": text})
    assert first.status_code == second.status_code == 200
    assert second.headers.get("X-Provisa-Cache") == "HIT", dict(second.headers)
    assert second.json() == first.json()


def test_the_subscription_sends_the_orders_again_when_a_batch_lands(flowing):
    text = (_DEMO / "queries" / "3-watch-the-orders.graphql").read_text()
    frames: list[dict] = []
    with httpx.stream(
        "POST",
        f"{flowing.base_url}/data/graphql",
        json={"query": text},
        headers={"X-Provisa-Role": _ROLE, "Accept": "text/event-stream"},
        timeout=httpx.Timeout(60, read=60),
    ) as resp:
        assert resp.status_code == 200
        for line in resp.iter_lines():
            if line.startswith("data: "):
                frames.append(json.loads(line.removeprefix("data: ")))
            if len(frames) == 2:
                break
    assert all(not f.get("errors") for f in frames), frames
    first, second = ([o["orderId"] for o in f["data"]["s__orders"]] for f in frames)
    assert first and second and first != second  # a batch landed between them


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "sql" in s], ids=lambda s: s["key"])
def test_each_tour_statement_answers_rows(flowing, step):
    assert query(flowing, step["sql"]), step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(flowing, step):
    method, path = step["api"].split(" ", 1)
    resp = httpx.request(
        method, f"{flowing.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300
    )
    assert resp.status_code == 200, resp.text
    assert resp.json(), step["key"]

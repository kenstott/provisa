# Copyright (c) 2026 Kenneth Stott
# Canary: 3587d753-deca-42d1-8778-68190c25a35b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2051, REQ-2055: the named demo ``quality`` works as shipped, driven by landing alone.

Its own docker-compose.yml brings the chain's Postgres up on a port of this test's own, under a
compose project of its own: the feed's two batches delivered, bronze empty (the seeder, which lands
them, is not run). A real Provisa server (DuckDB engine, its own control plane) loads the demo's
config.yaml as is. Then each batch is landed into bronze by the database's own sales.land_batch --
nothing is asked of Provisa -- and the views over bronze follow: bronze's probe posts the landing,
dq_results is recomputed, then silver, then gold, each read as fresh. The clean batch reaches
gold; the bad one shows failed in dq_results and never reaches silver or gold.
"""

# Requirements: REQ-2051, REQ-2055, REQ-1634

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "quality"
_ROLE = "org_admin"
# How long the views may take to follow a landing: bronze's probe runs on its 10 s cache TTL, and
# each view after it recomputes as the change ripples on.
_FOLLOW_S = 120


class QualityStack:
    """The demo's database, on a leased port; its seeder is not run -- the test lands the batches."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"quality-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"QUALITY_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
            self._compose("up", "-d", "--wait", "postgres")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the quality stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def land(self, batch: int) -> int:
        """Land a batch into bronze as the feed's loader does: in the database, not through Provisa."""
        done = self._compose(
            "exec", "-T", "postgres", "psql", "-U", "provisa", "-d", "sales", "-tAc",
            f"SELECT sales.land_batch({batch})",
        )  # fmt: skip
        return int(done.stdout.strip())

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = QualityStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("quality_demo")
    srv = IsolatedServer(
        "quality_demo",
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


def _post(server, path: str, body: dict) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": _ROLE}, timeout=300
    )


def query(server, sql: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql})
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def views(server) -> dict[str, tuple[str, int | None]]:
    """Each view's status and row count, as the MV registry a read asks has them."""
    resp = _post(server, "/admin/graphql", {"query": "{ mvList { id status rowCount } }"})
    assert resp.status_code == 200, resp.text
    return {
        m["id"].removeprefix("view-view_"): (m["status"], m["rowCount"])
        for m in resp.json()["data"]["mvList"]
    }


def follow(server, done: Callable[[dict], bool]) -> dict:
    deadline = time.monotonic() + _FOLLOW_S
    while True:
        state = views(server)
        if done(state):
            return state
        assert time.monotonic() < deadline, f"the views did not follow the landing: {state}"
        time.sleep(2)


@pytest.fixture(scope="module")
def landed(stack, server):
    """Both batches landed, one after the other, the views having followed each."""
    assert stack.land(1) == 123
    after_clean = follow(
        server, lambda v: v["silver"] == ("fresh", 120) and v["gold"] == ("fresh", 5)
    )
    assert stack.land(2) == 123
    after_bad = follow(server, lambda v: v["dq_results"] == ("fresh", 8))
    return server, after_clean, after_bad


def test_landing_the_clean_batch_alone_builds_its_checks_silver_and_gold(landed):
    _server, after_clean, _after_bad = landed
    assert after_clean == {
        "dq_results": ("fresh", 4),
        "silver": ("fresh", 120),  # 123 lines: three sales sent twice, each kept once
        "gold": ("fresh", 5),
    }


def test_the_bad_batch_is_failed_and_never_reaches_silver_or_gold(landed):
    server, _after_clean, after_bad = landed
    assert after_bad["silver"] == ("fresh", 120) and after_bad["gold"] == ("fresh", 5)
    failed = query(
        server,
        'SELECT check_name, failing_rows FROM "sales"."view_dq_results" '
        "WHERE batch_id = 2 AND outcome = 'fail' ORDER BY check_name",
    )
    assert failed == [
        {"check_name": "price_is_a_number", "failing_rows": 2},
        {"check_name": "quantity_positive", "failing_rows": 2},
        {"check_name": "store_known", "failing_rows": 3},
    ]
    batches = query(server, 'SELECT DISTINCT batch_id FROM "sales"."view_silver"')
    assert batches == [{"batch_id": 1}]


def test_silver_cleans_what_bronze_holds_raw(landed):
    server, *_ = landed
    raw = {r["product"] for r in query(server, 'SELECT DISTINCT product FROM "sales"."bronze"')}
    clean = {
        r["product"] for r in query(server, 'SELECT DISTINCT product FROM "sales"."view_silver"')
    }
    assert {"COFFEE", "cofee", "muffins", "sandwhich"} <= raw
    assert clean == {"coffee", "juice", "muffin", "sandwich", "tea"}


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(landed, path):
    server, *_ = landed
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "sql" in s], ids=lambda s: s["key"])
def test_each_tour_statement_answers_rows(landed, step):
    server, *_ = landed
    assert query(server, step["sql"]), step["key"]


def test_the_data_product_holds_gold_silver_and_the_checks(server):
    resp = _post(
        server,
        "/admin/graphql",
        {"query": "{ dataProducts { id domainId } tables { tableName productId } }"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()["data"]
    assert [p["id"] for p in data["dataProducts"] if p["domainId"] == "sales"] == ["daily_sales"]
    members = sorted(t["tableName"] for t in data["tables"] if t["productId"] == "daily_sales")
    assert members == ["view_dq_results", "view_gold", "view_silver"]

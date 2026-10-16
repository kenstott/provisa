# Copyright (c) 2026 Kenneth Stott
# Canary: faa347c7-516d-4b90-a120-c89d4db6d4b1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2067: the named demo ``provisioning`` works as shipped.

Its own docker-compose.yml brings up the warehouse database, the broker and the feeds on ports of
this test's own, under a compose project of its own. A real Provisa server (DuckDB engine, its own
control plane, Arrow Flight) loads the demo's config.yaml as is. Then: the feeds land; the dataset
is built from them; the scheduled triggers write a notice for each team and the notices table's
live Kafka output publishes it; the demo's consumer (consumer.py, run here against this server)
pulls the dataset over Arrow Flight as of each notice's time and records the pull; each team reads
its own governed view; a read of the close outside its window is refused naming when it opens.

Every load window is a time of day, so what a read answers depends on the clock: each test asserts
what the clock says at that moment -- rows inside a window, the refusal outside it -- never
skipping.
"""

# Requirements: REQ-2067, REQ-2066, REQ-1163, REQ-565

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "provisioning"
_ROLE = "org_admin"
#: A shorter month of orders than a local start's: what the feed publishes before its live orders.
_HISTORY = 20000
_DATASET = '"provisioning"."view_order_margins"'
_CLOSE = '"provisioning"."view_close_by_category"'
#: The first notice of each team comes within one cron period (five minutes) and two minutes.
_NOTICE_S = 9 * 60


def _utc() -> datetime:
    return datetime.now(timezone.utc)


def _dataset_open(at: datetime) -> bool:
    """order_margins' window, 00:30-00:00 UTC: closed from midnight to half past."""
    return not (at.hour == 0 and at.minute < 30)


def _close_open(at: datetime) -> bool:
    """close_by_category's window, 02:00-02:15 UTC."""
    return at.hour == 2 and at.minute < 15


def _notices_due(at: datetime) -> bool:
    """The triggers run 01:00-23:59 UTC (``*/5 1-23``)."""
    return at.hour >= 1


class ProvisioningStack:
    """The demo's warehouse database, broker and feeds, on leased ports."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        self.pg_port, self.kafka_port = lease_ports(2)
        self.project = f"provisioning-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {
            "PROVISIONING_PG_PORT": str(self.pg_port),
            "PROVISIONING_KAFKA_PORT": str(self.kafka_port),
            "PROVISIONING_ORDERS_HISTORY": str(_HISTORY),
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
        # The consumer is run against this test's own server below, not as the stack's service.
        try:
            self._compose("up", "-d", "--build", "--wait", "postgres", "kafka", "producer")
        except subprocess.CalledProcessError as exc:
            logs = self._compose("logs", "--no-color", check=False).stdout
            raise AssertionError(
                f"the provisioning stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def logs(self) -> str:
        return self._compose("logs", "--no-color", "--tail", "200", check=False).stdout

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = ProvisioningStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("provisioning_demo")
    srv = IsolatedServer(
        "provisioning_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
        await_flight=True,
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


def _refused_outside_window(resp: httpx.Response, view: str) -> None:
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["code"] == "data.view_outside_load_window", body
    assert body["params"]["view"] == view and body["params"]["opens_at"], body


def _views(server) -> dict[str, tuple[str, int | None]]:
    resp = _post(server, "/admin/graphql", {"query": "{ mvList { id status rowCount } }"})
    assert resp.status_code == 200, resp.text
    return {
        m["id"].removeprefix("view-view_"): (m["status"], m["rowCount"])
        for m in resp.json()["data"]["mvList"]
    }


@pytest.fixture(scope="module")
def landed(stack, server):
    """The month of orders landed, and the dataset built from it."""
    deadline = time.monotonic() + 600
    while True:
        orders = query(server, 'SELECT COUNT(*) AS n FROM "provisioning"."orders"')[0]["n"]
        status, rows = _views(server)["order_margins"]
        if orders >= _HISTORY and status == "fresh" and rows:
            return server
        assert time.monotonic() < deadline, (
            f"the feeds did not land or the dataset did not follow: {orders} orders, dataset "
            f"{status} {rows}\n{stack.logs()[-4000:]}"
        )
        time.sleep(5)


@pytest.fixture(scope="module")
def consumer(stack, landed):
    """The demo's consumer, run against this server, for both teams."""
    env = {
        **os.environ,
        "FEEDS_BOOTSTRAP": f"127.0.0.1:{stack.kafka_port}",
        "PROVISA_FLIGHT": f"grpc://127.0.0.1:{landed.flight_port}",
        "CONSUMERS": "finance,data_science",
        "PULLS_DSN": f"postgresql://consumer:consumer@127.0.0.1:{stack.pg_port}/provisioning",
    }
    proc = subprocess.Popen(
        [sys.executable, str(_DEMO / "consumer.py")],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield proc
    finally:
        proc.terminate()
        proc.wait(timeout=30)


@pytest.fixture(scope="module")
def pulled(landed, consumer):
    """A recorded pull for each team, once their notices have come (01:00-23:59 UTC)."""
    if not _notices_due(_utc()):
        return None
    deadline = time.monotonic() + _NOTICE_S
    while True:
        rows = query(
            landed,
            'SELECT consumer, rows, rows_per_sec, refused, version_time FROM "provisioning"."pulls"',
        )
        teams = {r["consumer"] for r in rows}
        if {"finance", "data_science"} <= teams:
            return rows
        if consumer.poll() is not None:
            raise AssertionError(f"the consumer exited: {consumer.stdout.read()[-4000:]}")
        assert time.monotonic() < deadline, f"no pull of each team was recorded: {rows}"
        time.sleep(10)


def test_the_feeds_land_into_the_warehouse(landed):
    [orders] = query(landed, 'SELECT COUNT(*) AS n FROM "provisioning"."orders"')
    [prices] = query(landed, 'SELECT COUNT(*) AS n FROM "provisioning"."prices"')
    [stock] = query(landed, 'SELECT COUNT(*) AS n FROM "provisioning"."inventory"')
    assert orders["n"] >= _HISTORY
    assert prices["n"] == stock["n"] == 13  # one row a product: the latest price and count


def test_each_notice_is_published_and_each_team_pulls_as_of_it(landed, pulled):
    if pulled is None:  # 00:00-00:59 UTC: no trigger runs, so no notice is due
        assert not _notices_due(_utc())
        return
    by_team = {}
    for row in pulled:
        by_team.setdefault(row["consumer"], []).append(row)
    for team, rows in by_team.items():
        done = [r for r in rows if r["refused"] is None]
        assert done, f"{team}'s pulls were all refused: {rows}"
        assert all(r["rows"] > 0 and r["rows_per_sec"] > 0 for r in done), rows
    # Data science sees no service orders (its row rule): fewer rows of the same dataset.
    assert max(r["rows"] for r in by_team["data_science"]) < max(
        r["rows"] for r in by_team["finance"]
    )


def test_a_pull_as_of_a_notice_reads_that_version(landed, pulled):
    """Pulling again as of an earlier notice reads the rows that were current then, though the
    feed has landed more orders since."""
    if pulled is None:
        assert not _notices_due(_utc())
        return
    import json

    import pyarrow.flight as flight

    [first] = sorted(
        (r for r in pulled if r["consumer"] == "finance"), key=lambda r: r["version_time"]
    )[:1]
    ticket = {
        "query": f"SELECT * FROM {_DATASET}",
        "role": "finance",
        "as_of": first["version_time"],
    }
    reader = flight.connect(f"grpc://127.0.0.1:{landed.flight_port}").do_get(
        flight.Ticket(json.dumps(ticket).encode())
    )
    assert reader.read_all().num_rows == first["rows"]


def test_each_team_reads_its_own_governed_view_of_the_dataset(landed):
    sql = f"SELECT category, margin FROM {_DATASET} ORDER BY ordered_at DESC LIMIT 200"
    if not _dataset_open(_utc()):
        _refused_outside_window(
            _post(landed, "/data/sql", {"sql": sql}, "finance"), "view_order_margins"
        )
        return
    finance = query(landed, sql, "finance")
    science = query(landed, sql, "data_science")
    assert any(r["margin"] for r in finance)
    assert all(float(r["margin"]) == 0 for r in science)  # masked
    assert all(r["category"] != "services" for r in science)  # its row rule


def test_the_close_answers_only_inside_its_window(landed):
    sql = f"SELECT category, revenue, stock_value FROM {_CLOSE}"
    if _close_open(_utc()):
        assert query(landed, sql, "finance")
    else:
        _refused_outside_window(
            _post(landed, "/data/sql", {"sql": sql}, "finance"), "view_close_by_category"
        )


def test_the_warehouses_own_refresh_builds_the_close_at_any_time(landed):
    """A refresh is Provisa's own work: it runs outside the close's window."""
    deadline = time.monotonic() + 300
    while _views(landed)["close_by_category"][0] != "fresh":
        assert time.monotonic() < deadline, _views(landed)["close_by_category"]
        time.sleep(5)


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers(landed, pulled, path):
    resp = _post(landed, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    if "viewOrderMargins" in path.read_text() and not _dataset_open(_utc()):
        assert body.get("errors"), body
        return
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    if "__pulls" in path.read_text() and pulled is None:
        return  # no pull is recorded before the first notice of the day (01:00 UTC)
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


def _statements_as_run() -> list[tuple[dict, str]]:
    role, out = _ROLE, []
    for step in _TOUR:
        if "role" in step:
            role = _ROLE if step["role"] == "all" else step["role"]
        if "sql" in step:
            out.append((step, role))
    return out


@pytest.mark.parametrize(
    ("step", "role"),
    _statements_as_run(),
    ids=lambda v: v["key"] if isinstance(v, dict) else v,
)
def test_each_tour_statement_answers_what_the_clock_says(landed, pulled, step, role):
    sql, now = step["sql"], _utc()
    if "view_close_by_category" in sql and not _close_open(now):
        _refused_outside_window(
            _post(landed, "/data/sql", {"sql": sql}, role), "view_close_by_category"
        )
        return
    if "view_order_margins" in sql and not _dataset_open(now):
        _refused_outside_window(
            _post(landed, "/data/sql", {"sql": sql}, role), "view_order_margins"
        )
        return
    rows = query(landed, sql, role)
    if ('"notices"' in sql or '"pulls"' in sql) and pulled is None:
        return  # no notice is due before 01:00 UTC
    assert rows, step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers(landed, pulled, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{landed.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    if pulled is not None:
        assert resp.json()["data"], step["key"]

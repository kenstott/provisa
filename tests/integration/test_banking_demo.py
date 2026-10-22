# Copyright (c) 2026 Kenneth Stott
# Canary: 25617e90-62b9-43d1-882d-52e9bbc9bd41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2107: the named demo ``banking`` works as shipped -- end-of-day reporting without copying
the ledger.

Its own docker-compose.yml brings the ledger up on a port of this test's own. Two real Provisa
nodes load the demo's config.yaml as is over one model store, as start-ui-install.sh starts them:
the ledger node headless, and the warehouse node a visitor uses. The demo's setup.yaml is run
against the warehouse node as the launcher runs it: thirty business days loaded one at a time, each
day's totals built sealed at its close. Then each reading step of tour.yaml is run as the tour runs
it -- in order, as the reader and under the time travel the steps before it set -- and its answer
checked: each day recorded at its own close, the two restatements recorded at the close of the day
they arrived, the version reported at a close kept beside them, no posting in the warehouse's
store, the drill-down under the ledger's row rules and masks, the same figure over GraphQL, SQL
and a pgwire session that sets provisa.as_recorded."""

# Requirements: REQ-2107, REQ-2108, REQ-2109, REQ-2110, REQ-2111, REQ-2120

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces
from tests.integration.polly_bakeoff_questions import QUESTIONS
from tests.integration.test_region_read_in_place_e2e import _MASTER_KEY, _Stack

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "banking"
_STEPS = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]
_ADMIN = "org_admin"
_REGIONS = ("ledger", "warehouse")
# The closes the tour reads at, in UTC: 18:00 London in October before the clocks change.
_OCT_1_CLOSE = "2026-10-01T17:00:00"
_OCT_2_CLOSE = "2026-10-02T17:00:00"
_LAST_CLOSE = "2026-10-12T17:00:00"
# db/init.sql's two restatements: the payment reversed, and the trade booked late.
_REVERSED = Decimal("1850000.00")
_LATE_TRADE = Decimal("640000.00")
_CORPORATE = (
    "SELECT e.business_date, e.branch, e.currency, e.net_movement, e.recorded_at "
    'FROM "banking"."view_eod_totals" AS e WHERE e.product = \'corporate\''
)


class _Stores(_Stack):
    """The shared model store, a Redis, and each of the demo's regions' own store."""

    def start(self) -> None:
        super().start()
        with psycopg.connect(self.url(self.engine_port, "provisa"), autocommit=True) as conn:
            for region in _REGIONS:
                conn.execute(f"CREATE DATABASE {region}_store")


class _Ledger:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)

    @property
    def env(self) -> dict[str, str]:
        return {"BANKING_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", "-p", f"banking-itest-{self.port}"]
            + ["-f", str(_DEMO / "docker-compose.yml"), *args],
            env={**os.environ, **self.env},
            capture_output=True,
            text=True,
            check=check,
            timeout=900,
        )

    def up(self) -> None:
        started = self._compose("up", "-d", "--wait", check=False)
        assert started.returncode == 0, f"{started.stdout}\n{started.stderr}"

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def nodes(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    ledger, stores = _Ledger(), _Stores()
    started = []
    try:
        # Inside the try: a stack that fails to come up is still taken down.
        ledger.up()
        stores.start()
        home = tmp_path_factory.mktemp("banking_demo")
        demo = home / "banking"
        shutil.copytree(_DEMO, demo)
        (demo / "data").mkdir(exist_ok=True)
        control_plane = stores.url(stores.engine_port, "provisa", "+psycopg")
        servers = {}
        for region in _REGIONS:  # the ledger node first, headless; then the visitor's
            srv = IsolatedServer(
                "banking_demo",
                engine="duckdb",
                config=str(demo / "config.yaml"),
                control_plane="postgres",
                enable_pgwire=region == "warehouse",
                env={
                    "PLATFORM_DATABASE_URL": control_plane,
                    "TENANT_DATABASE_URL": control_plane,
                    "PROVISA_REGION": region,
                    "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
                    "PROVISA_NAMED_DEMO_DIR": str(demo),
                    "PROVISA_DEMO_LEDGER_ADDRESS": "http://127.0.0.1:1",
                    "PROVISA_DEMO_WAREHOUSE_ADDRESS": "http://127.0.0.1:2",
                    # Each region's own PostgreSQL store, never one shared by both.
                    "PROVISA_DEMO_LEDGER_PG_URL": stores.url(
                        stores.engine_port, "ledger_store", "+psycopg"
                    ),
                    "PROVISA_DEMO_WAREHOUSE_PG_URL": stores.url(
                        stores.engine_port, "warehouse_store", "+psycopg"
                    ),
                    "REDIS_URL": f"redis://127.0.0.1:{stores.redis_port}/0",
                    **ledger.env,
                },
            )
            srv.start(timeout=600)
            started.append(srv)
            servers[region] = srv
        # The demo's setup, as start-ui-install.sh runs it against the visitor's node.
        warehouse = servers["warehouse"]
        subprocess.run(
            [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(demo)]
            + ["--url", warehouse.base_url]
            + ["--flight", f"grpc://127.0.0.1:{warehouse.flight_port}"],
            env={**os.environ, **ledger.env},
            check=True,
            timeout=1800,
        )
        yield {"stores": stores, **servers}
    finally:
        for srv in started:
            srv.stop_process()
        stores.stop()
        ledger.down()


def _sql(srv, sql: str, role: str, travel: dict[str, str] | None = None) -> list[dict[str, Any]]:
    headers = {"X-Provisa-Role": role}
    if travel and travel.get("as_of"):
        headers["X-Provisa-As-Of"] = travel["as_of"]
    if travel and travel.get("as_recorded"):
        headers["X-Provisa-As-Recorded"] = travel["as_recorded"]
    resp = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers=headers,
        timeout=srv.request_timeout + 10,
    )
    assert resp.status_code == 200, (
        f"{sql} as {role}: HTTP {resp.status_code}: {resp.text}\n"
        f"--- the node's log ---\n{srv.dump_stderr_debug()[-12000:]}"
    )
    return resp.json()["data"]["sql"]


@pytest.fixture(scope="module")
def answers(nodes) -> dict[str, list[dict[str, Any]]]:
    """Every sql step of the tour, in order, as its reader and under the time travel the steps
    before it set, as the tour runner runs it on the explorer (a role picked on another page is
    the header's, not the explorer's)."""
    role, travel = _ADMIN, {}
    out: dict[str, list[dict[str, Any]]] = {}
    page = None
    for step in _STEPS:
        page = step.get("route", page)
        if "role" in step and page == "/sql":
            role = _ADMIN if step["role"] == "all" else step["role"]
        if "as_recorded" in step:
            travel["as_recorded"] = step["as_recorded"]
        if "sql" in step:
            out[step["key"]] = _sql(nodes["warehouse"], step["sql"], role, dict(travel))
    return out


def _total(rows: list[dict[str, Any]], day: str, branch: str, currency: str) -> Decimal | None:
    found = [
        r
        for r in rows
        if str(r["business_date"]) == day and r["branch"] == branch and r["currency"] == currency
    ]
    assert len(found) <= 1, found
    return Decimal(str(found[0]["net_movement"])) if found else None


def test_every_tour_statement_answers_rows(answers):
    empty = [key for key, rows in answers.items() if not rows]
    assert empty == [], empty


def test_each_business_day_was_first_recorded_at_its_own_close(answers):
    rows = answers["bankingCloses"]
    assert len(rows) == 30, rows
    for row in rows:
        close = f"{row['business_date']}T17:00:00"
        assert str(row["first_recorded"]).replace(" ", "T").startswith(close), row


def test_the_restated_days_were_recorded_again_when_their_restatements_arrived(answers):
    last = {str(r["business_date"]): str(r["last_recorded"]) for r in answers["bankingCloses"]}
    assert last["2026-10-01"].replace(" ", "T").startswith("2026-10-06T17:00:00"), last
    assert last["2026-10-02"].replace(" ", "T").startswith("2026-10-07T17:00:00"), last


def test_as_reported_at_a_close_keeps_the_payment_reversed_later(answers):
    reported = _total(answers["bankingReported"], "2026-10-01", "London", "GBP")
    restated = _total(answers["bankingRestated"], "2026-10-01", "London", "GBP")
    assert reported is not None and restated is not None
    assert restated - reported == _REVERSED


def test_a_day_not_yet_closed_has_no_figures_as_reported(answers):
    assert {str(r["business_date"]) for r in answers["bankingReported"]} == {"2026-10-01"}


def test_the_late_trade_restates_its_own_trade_date(nodes):
    # Its own day only: the corporate totals of every day run past the server's default row limit.
    oct_2 = f"{_CORPORATE} AND e.business_date = DATE '2026-10-02'"
    at_its_close = _total(
        _sql(nodes["warehouse"], oct_2, "analyst", {"as_recorded": _OCT_2_CLOSE}),
        "2026-10-02",
        "Edinburgh",
        "USD",
    )
    now = _total(
        _sql(nodes["warehouse"], oct_2, "analyst", {"as_recorded": _LAST_CLOSE}),
        "2026-10-02",
        "Edinburgh",
        "USD",
    )
    assert at_its_close is not None and now is not None
    assert now - at_its_close == _LATE_TRADE


def test_the_drill_down_reads_under_the_ledgers_rules(answers, nodes):
    rows = answers["bankingPostings"]
    assert {r["counterparty"] for r in rows} == {"withheld"}, rows
    [recalled] = [r for r in rows if r["transaction_id"] == "T20261001-LON-PAY-7781"]
    assert recalled["status"] == "reversed" and Decimal(str(recalled["amount"])) == 0
    staff = _sql(
        nodes["warehouse"],
        'SELECT p.transaction_id FROM "banking"."view_ledger_postings" AS p '
        "WHERE p.product = 'staff'",
        "ops_analyst",
    )
    assert staff == []
    seen_by_admin = _sql(
        nodes["warehouse"],
        'SELECT COUNT(*) AS n FROM "banking"."view_ledger_postings" AS p '
        "WHERE p.product = 'staff'",
        _ADMIN,
    )
    assert seen_by_admin[0]["n"] > 0


def test_the_analyst_reads_the_totals_and_no_posting(nodes):
    refused = httpx.post(
        f"{nodes['warehouse'].base_url}/data/sql",
        json={"sql": 'SELECT p.counterparty FROM "banking"."view_ledger_postings" AS p'},
        headers={"X-Provisa-Role": "analyst"},
        timeout=nodes["warehouse"].request_timeout + 10,
    )
    assert refused.status_code in (400, 403), refused.text


def test_no_posting_is_kept_in_the_warehouse(nodes):
    stores = nodes["stores"]
    with psycopg.connect(stores.url(stores.engine_port, "warehouse_store")) as conn:
        held = conn.execute(
            "SELECT table_schema, table_name FROM information_schema.columns "
            "WHERE column_name IN ('transaction_id', 'counterparty', 'account_id')"
        ).fetchall()
    assert held == [], held


def test_the_dashboard_and_bi_give_the_same_figure(answers, nodes):
    query = (_DEMO / "queries" / "1-eod-dashboard.graphql").read_text()
    resp = httpx.post(
        f"{nodes['warehouse'].base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "ops_analyst"},
        timeout=nodes["warehouse"].request_timeout + 10,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [totals] = body["data"].values()
    assert any(t["postings"] for t in totals), totals
    corporate_gbp = [t for t in totals if t["product"] == "corporate" and t["currency"] == "GBP"]
    assert len(corporate_gbp) == 1, "; ".join(
        f"{t['product']} {t['currency']} {t['netMovement']} ({len(t['postings'])} postings)"
        for t in totals
    )
    [dashboard] = corporate_gbp
    day = [r for r in answers["bankingDay"] if r["product"] == "corporate"]
    bi = _total(day, "2026-10-01", "London", "GBP")
    assert Decimal(str(dashboard["netMovement"])) == bi


def test_a_pgwire_session_reads_as_recorded_at_a_close(answers, nodes):
    """What a BI tool connected over pgwire sees with SET provisa.as_recorded."""
    srv = nodes["warehouse"]
    with psycopg.connect(
        host="127.0.0.1",
        port=srv.pgwire_port,
        dbname="provisa",
        user="analyst",
        password="provisa",
        autocommit=True,
    ) as conn:
        conn.execute(f"SET provisa.as_recorded = '{_OCT_1_CLOSE}'")
        # Its own day only: the corporate totals of every day run past the default row limit.
        cursor = conn.execute(f"{_CORPORATE} AND e.business_date = DATE '2026-10-01'")
        names = [c.name for c in cursor.description]
        rows = [dict(zip(names, r)) for r in cursor.fetchall()]
    over_pgwire = _total(rows, "2026-10-01", "London", "GBP")
    assert over_pgwire == _total(answers["bankingReported"], "2026-10-01", "London", "GBP")


def test_the_agents_question_has_an_answer_in_the_totals(nodes):
    for question in QUESTIONS["banking"]:
        assert _sql(nodes["warehouse"], question.answer_sql, _ADMIN), question.text

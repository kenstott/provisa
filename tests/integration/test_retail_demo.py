# Copyright (c) 2026 Kenneth Stott
# Canary: 4da9fdde-7bd7-4882-95f4-c16870a95af6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2035: the named demo ``retail`` works as shipped -- one group, two regions, one definition.

Its own docker-compose.yml brings up the group's three databases on a port of this test's own. Two
real Provisa nodes load the demo's config.yaml as is over one model store, as start-ui-install.sh
starts them: the eu node lands the EU tables and builds the EU views into eu's store; the us node,
the one a visitor uses, reads them in place. Every tour statement answers rows as the reader its
tour names; a group analyst reads EU customers pseudonymised; the record names eu as the region
that answered; no EU customer row is kept in us's stores; the review queue's sender is revealed
only to a reviewer acting with region__eu."""

# Requirements: REQ-2035, REQ-1921, REQ-1922, REQ-2057, REQ-2059

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import httpx
import psycopg
from psycopg import sql
import pytest
import yaml

from tests.integration.test_region_read_in_place_e2e import _MASTER_KEY, _Stack

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "retail"
_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]
_ADMIN = "org_admin"
_QUEUE = (
    'SELECT message_id, sender_email, score FROM "retail"."view_eu_mail_review" WHERE score >= 70'
)


class _Databases:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", "-p", f"retail-itest-{self.port}"]
            + ["-f", str(_DEMO / "docker-compose.yml"), *args],
            env={**os.environ, "RETAIL_PG_PORT": str(self.port)},
            capture_output=True,
            text=True,
            check=check,
        )

    def up(self) -> None:
        started = self._compose("up", "-d", "--wait", "postgres", check=False)
        assert started.returncode == 0, f"{started.stdout}\n{started.stderr}"

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def nodes(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    databases, stack = _Databases(), _Stack()
    started = []
    try:
        # Inside the try: a stack that fails to come up is still taken down.
        databases.up()
        stack.start()
        home = tmp_path_factory.mktemp("retail_demo")
        demo = home / "retail"
        shutil.copytree(_DEMO, demo)
        (demo / "data").mkdir(exist_ok=True)
        control_plane = stack.url(stack.engine_port, "provisa", "+psycopg")
        servers = {}
        for region in ("eu", "us"):  # eu first: it lands and builds what us reads
            # One org in two regions: each node serves the same org, as its own region.
            srv = IsolatedServer(
                "retail_demo",
                engine="duckdb",
                config=str(demo / "config.yaml"),
                control_plane="postgres",
                env={
                    "PLATFORM_DATABASE_URL": control_plane,
                    "TENANT_DATABASE_URL": control_plane,
                    "PROVISA_REGION": region,
                    "PROVISA_ENCRYPTION_KEY": _MASTER_KEY,
                    "PROVISA_NAMED_DEMO_DIR": str(demo),
                    "PROVISA_DEMO_EU_ADDRESS": "http://127.0.0.1:1",
                    "PROVISA_DEMO_US_ADDRESS": "http://127.0.0.1:2",
                    # REQ-2068: each region's own PostgreSQL store, never one shared by both.
                    "PROVISA_DEMO_EU_PG_URL": stack.url(stack.engine_port, "eu_store", "+psycopg"),
                    "PROVISA_DEMO_US_PG_URL": stack.url(stack.engine_port, "us_store", "+psycopg"),
                    "REDIS_URL": f"redis://127.0.0.1:{stack.redis_port}/0",
                    "RETAIL_PG_PORT": str(databases.port),
                },
            )
            srv.start(timeout=600)
            started.append(srv)
            servers[region] = srv
        yield {"stack": stack, **servers}
    finally:
        for srv in started:
            srv.stop_process()
        stack.stop()
        databases.down()


def _sql(srv, sql: str, role: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 10,
    )


def _rows(srv, sql: str, role: str, deadline_s: float = 300) -> list[dict]:
    """The statement's rows on ``srv`` as ``role`` -- waited for while eu is still landing or
    building what it reads (a read of a copy not yet built is refused, naming eu)."""
    deadline, last = time.monotonic() + deadline_s, None
    while time.monotonic() < deadline:
        resp = _sql(srv, sql, role)
        last = (resp.status_code, resp.text[:600])
        if resp.status_code == 200 and resp.json()["data"]["sql"]:
            return resp.json()["data"]["sql"]
        time.sleep(2)
    raise AssertionError(f"{sql} as {role}: {last}\n--- the node's log ---\n{_log_tail(srv)}")


def _log_tail(srv) -> str:
    """The end of ``srv``'s own log: what it said while it answered, for a failure to show."""
    return srv.dump_stderr_debug()[-12000:]


def test_every_tour_statement_answers_rows_as_its_reader(nodes):
    role = _ADMIN
    for step in _TOUR:
        if "role" in step:
            role = _ADMIN if step["role"] == "all" else step["role"]
        if "sql" in step:
            assert _rows(nodes["us"], step["sql"], role), step["key"]


def test_a_group_analyst_reads_eu_customers_pseudonymised(nodes):
    rows = _rows(
        nodes["us"],
        # The role's row limit (100) cuts an unfiltered read to the US rows, which come first.
        'SELECT subsidiary, customer FROM "retail"."view_group_customers" WHERE subsidiary = \'eu\'',
        "group_analyst",
    )
    eu = [r["customer"] for r in rows]
    assert eu and all(re.fullmatch(r"[0-9a-f]{32}", c) for c in eu), (
        f"{rows}\n--- the node's log ---\n{_log_tail(nodes['us'])}"
    )
    refused = _sql(nodes["us"], 'SELECT email FROM "retail"."eu_customers"', "group_analyst")
    assert refused.status_code in (400, 403), refused.text


def test_the_record_names_eu_as_the_region_that_answered(nodes):
    _rows(nodes["us"], 'SELECT customer FROM "retail"."view_group_customers"', "group_analyst")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        rows = _rows(
            nodes["us"],
            'SELECT region FROM "ops"."queries" WHERE domain_id = \'retail\'',
            _ADMIN,
        )
        if any(r["region"] == "eu" for r in rows):
            return
        time.sleep(2)
    raise AssertionError("no statement of the demo's is recorded as answered by eu")


def test_no_eu_customer_row_is_kept_in_us(nodes):
    _rows(nodes["us"], 'SELECT customer FROM "retail"."view_group_customers"', "group_analyst")
    stack = nodes["stack"]
    with psycopg.connect(stack.url(stack.engine_port, "us_store")) as conn:
        tables = conn.execute(
            "SELECT c.table_schema, c.table_name FROM information_schema.columns c "
            "WHERE c.column_name = 'customer_id'"
        ).fetchall()
        for schema, table in tables:
            (eu_rows,) = conn.execute(
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE customer_id < 200000").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            ).fetchone()
            assert eu_rows == 0, f"{schema}.{table} holds EU customers in us"


def test_the_sender_is_revealed_only_to_a_reviewer_in_the_eu(nodes):
    withheld = _rows(nodes["us"], _QUEUE, "reviewer")
    assert {r["sender_email"] for r in withheld} == {"withheld"}
    revealed = _rows(nodes["us"], _QUEUE, "reviewer,region__eu")
    assert all(r["sender_email"].endswith("@eu.retailgroup.example") for r in revealed)

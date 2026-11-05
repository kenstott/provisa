# Copyright (c) 2026 Kenneth Stott
# Canary: 22f1a009-6363-4330-951e-eaac2d66d127
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2040: the named demo ``audit`` works as shipped.

Its own docker-compose.yml brings the audit firm's Postgres up on a port of this test's own, under
a compose project of its own. A real Provisa server (DuckDB engine, its own control plane) loads
the demo's config.yaml as is: the firm's database read live, and the govdata tables landed from
AskAmerica as TTL -1 replicas on their first read, each through its landing filter (the
Clearinghouse's 2023 award lines of Vermont, New Hampshire and Maine beside USAspending's
obligations; USAspending's fiscal 2023 obligations in the clients' nine counties). The readiness
view is read live and built; each engagement team reads its own clients alone; and the demo's
example queries, its tour's statements, its REST call and its Polly questions' grading queries
answer rows.

Credentials
-----------
govdata is a real account (``ASKAMERICA_API_KEY``, a secret of the warehouse lane), so this runs
there. A missing key fails the module by name.
"""

# Requirements: REQ-2040, REQ-2031, REQ-2062

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest

import yaml

from provisa.core.demo_bundles import store_namespaces
from tests.integration import demo_capture
from tests.integration.polly_bakeoff_questions import QUESTIONS
from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "audit"
_ROLE = "org_admin"
#: The govdata tables, by their alias in the demo.
_GOVDATA = ("filed_awards", "county_obligations")
#: What db/init.sql generates: the firm's clients, their schedule lines and award records.
_CLIENTS, _LINES, _NOTICES = 9, 31, 17
#: The states and counties the clients are in, each govdata table's landing filter.
_STATES = {"VT", "NH", "ME"}
_COUNTIES = {"50007", "50023", "50025", "33011", "33013", "33007", "23005", "23019", "23003"}
#: What the auditor finds on each line of the schedules, and the awards they leave out.
_FINDINGS = {"matched": 7, "timing": 4, "missing": 4, "passed through": 16, "unrecorded": 3}
#: Each engagement team's role and the state of its clients.
_TEAMS = {"vermont_team": "VT", "new_hampshire_team": "NH", "maine_team": "ME"}


def _api_key() -> str:
    key = os.environ.get("ASKAMERICA_API_KEY")
    if not key:
        raise RuntimeError(
            "ASKAMERICA_API_KEY is not set: the audit demo's govdata tables need a real key "
            "(a secret of the warehouse lane)"
        )
    return key


class AuditStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"audit-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"AUDIT_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
                f"the audit stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = AuditStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("audit_demo")
    srv = IsolatedServer(
        "audit_demo",
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
        # Each replica lands on its first read, longer than one request lasts.
        await_tables_serving(srv, _ROLE, [f'"audit"."{table}"' for table in _GOVDATA], timeout=1800)
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=900
    )


def query(server, sql: str, role: str = _ROLE) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_the_firm_is_read_live(server):
    [row] = query(
        server,
        'SELECT (SELECT COUNT(*) FROM "audit"."clients") AS clients, '
        '(SELECT COUNT(*) FROM "audit"."grant_schedule") AS lines, '
        '(SELECT COUNT(*) FROM "audit"."award_notices") AS notices',
    )
    assert (row["clients"], row["lines"], row["notices"]) == (_CLIENTS, _LINES, _NOTICES)


def test_the_filed_awards_land_matched_and_unmatched_lines(server):
    """The Clearinghouse's 2023 award lines of the three states land beside USAspending's
    obligations, the lines with no USAspending match among them."""
    rows = query(
        server,
        "SELECT auditee_state AS state, matched_usaspending AS matched, COUNT(*) AS lines, "
        'MIN(audit_year) AS lo, MAX(audit_year) AS hi FROM "audit"."filed_awards" '
        "GROUP BY auditee_state, matched_usaspending",
    )
    assert {r["state"] for r in rows} == _STATES, rows
    assert {r["matched"] for r in rows} == {True, False}, rows
    assert {(r["lo"], r["hi"]) for r in rows} == {(2023, 2023)}, rows


def test_the_county_obligations_land_the_clients_counties_alone(server):
    rows = query(
        server,
        'SELECT DISTINCT county_fips, year FROM "audit"."county_obligations"',
    )
    assert {r["county_fips"] for r in rows} == _COUNTIES, rows
    assert {r["year"] for r in rows} == {2023}, rows


def test_every_schedule_line_and_unrecorded_award_has_its_finding(server):
    rows = query(
        server,
        'SELECT finding, COUNT(*) AS n FROM "audit"."view_readiness" GROUP BY finding',
    )
    assert {r["finding"]: r["n"] for r in rows} == _FINDINGS


def test_the_answer_is_the_same_live_and_built(server):
    sql = (
        'SELECT client_name, award_number, finding, county_obligated FROM "audit"."view_readiness" '
        "ORDER BY client_name, award_number"
    )
    live = query(server, sql)
    resp = _post(
        server,
        "/admin/graphql",
        {"query": 'mutation { refreshMv(mvId: "view-view_readiness") { success message } }'},
    )
    assert resp.json()["data"]["refreshMv"]["success"], resp.text
    assert query(server, sql) == live


@pytest.mark.parametrize(("role", "state"), sorted(_TEAMS.items()))
def test_each_engagement_team_reads_only_its_own_clients(server, role, state):
    for table in ("clients", "view_readiness"):
        rows = query(server, f'SELECT DISTINCT state FROM "audit"."{table}"', role)
        assert [r["state"] for r in rows] == [state], (table, rows)
    [row] = query(
        server, 'SELECT COUNT(DISTINCT client_id) AS n FROM "audit"."grant_schedule"', role
    )
    assert row["n"] == 3


def test_the_partner_reads_every_client(server):
    [row] = query(
        server, 'SELECT COUNT(DISTINCT client_name) AS n FROM "audit"."view_readiness"', "partner"
    )
    assert row["n"] == _CLIENTS


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    demo_capture.keep("audit", f"gql-{path.stem}.json", resp.text)
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


def _statements_as_run() -> list[tuple[dict, str]]:
    """Each statement of the tour with the role it runs as: a role step picks the role the
    statements from it on run as (``all`` is every role, as org_admin)."""
    role, out = _ROLE, []
    for step in _TOUR:
        if "role" in step:
            role = _ROLE if step["role"] == "all" else step["role"]
        if "sql" in step:
            out.append((step, role))
    return out


@pytest.mark.parametrize(
    ("step", "role"), _statements_as_run(), ids=lambda v: v["key"] if isinstance(v, dict) else v
)
def test_each_tour_statement_answers_rows(server, step, role):
    resp = _post(server, "/data/sql", {"sql": step["sql"]}, role)
    assert resp.status_code == 200, f"{step['key']}: HTTP {resp.status_code}: {resp.text}"
    demo_capture.keep("audit", f"sql-{step['key']}.json", resp.text)
    assert resp.json()["data"]["sql"], step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(server, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    demo_capture.keep("audit", "rest.json", resp.text)
    assert resp.json()["data"], step["key"]


@pytest.mark.parametrize("question", QUESTIONS["audit"], ids=lambda q: q.text[:40])
def test_each_polly_question_has_right_answers(server, question):
    """Each question the tour asks Polly is graded over rows the demo has."""
    assert query(server, question.answer_sql), question.text


def test_the_pages_beside_the_answers_are_served(server):
    """The schema, graph, glossary and REST documentation the demo's pages read beside its
    answers each answer (kept for its card with PROVISA_DEMO_CAPTURE_DIR set)."""
    demo_capture.keep_served("audit", server.base_url, _ROLE)

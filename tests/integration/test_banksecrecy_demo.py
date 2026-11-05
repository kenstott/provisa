# Copyright (c) 2026 Kenneth Stott
# Canary: 3f7d1b92-8e4a-4c06-a5d9-6b2e0c81f437
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2201: the named demo ``banksecrecy`` works as shipped.

Its own docker-compose.yml brings the bank's database up on a port of this test's own, under a
compose project of its own, and a real Provisa server (DuckDB engine, its own control plane) loads
the demo's config.yaml as is. Asked by concept, the cash that needs a currency transaction report
comes back aggregated per person per business day -- across accounts and branches, cash brought by
someone else counted toward the person it is on behalf of, cash in and cash out apart -- with
account numbers and tax ids masked for a teller and in full for compliance; the people depositing
just under $10,000 again and again are compliance's alone; and the regulation's terms reach the
bank's tables. The scans of the regulation are the bundle build's (setup.yaml, an AI model and
eCFR's network): this test reads what the demo's own data answers.
"""

# Requirements: REQ-2201

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration, pytest.mark.requires_vector]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "banksecrecy"
_ADMIN = "org_admin"
_TELLER = "teller"
_COMPLIANCE = "compliance_officer"
_CTR = (
    "SELECT person, tax_id, CAST(business_day AS VARCHAR) AS business_day, direction, "
    "multiple_transactions, branches, account_numbers, conducted_by_another, cash_total "
    'FROM "banksecrecy"."view_ctr_required" ORDER BY business_day'
)
# Who needs a report, on which business day, cash in or out, from how many transactions at how
# many branches, and the total (db/init.sql).
_REPORTS = [
    ("Harold Finch", "2026-10-05", "deposit", 1, 1, 12500),
    ("Greenleaf Landscaping LLC", "2026-10-07", "deposit", 2, 2, 11600),
    ("Dana Whitmore", "2026-10-09", "deposit", 2, 2, 11500),
    ("Corner Market Inc", "2026-10-14", "withdrawal", 3, 2, 12000),
    ("Marcus Reed", "2026-10-20", "deposit", 2, 2, 18700),
]


class BankStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"banksecrecy-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"BANKSECRECY_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

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
                f"the bank's stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = BankStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("banksecrecy_demo")
    srv = IsolatedServer(
        "banksecrecy_demo",
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


def _post(server, path: str, body: dict, role: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=300
    )


def query(server, sql: str, role: str) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _reports(rows: list[dict[str, Any]]) -> list[tuple]:
    return [
        (
            r["person"],
            r["business_day"][:10],
            r["direction"],
            r["multiple_transactions"],
            r["branches"],
            float(r["cash_total"]),
        )
        for r in rows
    ]


def test_the_cash_needing_a_report_is_aggregated_per_person_per_business_day(server):
    rows = query(server, _CTR, _COMPLIANCE)
    assert _reports(rows) == [(*r[:5], float(r[5])) for r in _REPORTS]
    # Dana Whitmore's total holds $4,500 brought to the window by someone else on her behalf.
    dana = next(r for r in rows if r["person"] == "Dana Whitmore")
    assert dana["conducted_by_another"] == 1
    # Compliance reads what the report needs in full.
    harold = rows[0]
    assert harold["tax_id"] == "412-55-1093"
    assert harold["account_numbers"] == "4417-1001-7919"


def test_a_teller_sees_the_same_reports_with_account_numbers_and_tax_ids_masked(server):
    rows = query(server, _CTR, _TELLER)
    assert _reports(rows) == [(*r[:5], float(r[5])) for r in _REPORTS]
    for r in rows:
        assert "4417-" not in r["account_numbers"], r
        assert r["account_numbers"].startswith("XXXX-XXXX-"), r
        assert r["tax_id"].startswith("***"), r


def test_who_is_structuring_is_compliance_s_alone(server):
    sql = (
        "SELECT person, deposits_under_threshold, branches, business_days, cash_total "
        'FROM "banksecrecy"."view_structuring" ORDER BY cash_total DESC'
    )
    rows = query(server, sql, _COMPLIANCE)
    assert [(r["person"], r["deposits_under_threshold"], r["branches"]) for r in rows] == [
        ("Victor Hale", 4, 3),
        ("Marcus Reed", 3, 2),
        ("Elena Petrova", 3, 2),
    ]
    refused = _post(server, "/data/sql", {"sql": sql}, _TELLER)
    assert refused.status_code >= 400 or refused.json().get("errors"), refused.text


def test_the_regulation_s_terms_reach_the_bank_s_tables(server):
    resp = httpx.get(
        f"{server.base_url}/admin/glossary/terms",
        headers={"X-Provisa-Role": _ADMIN},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    grounding = {t["name"]: t["grounding"] for t in resp.json()}
    for term in (
        "transaction in currency",
        "financial institution",
        "person",
        "business day",
        "multiple transactions",
        "on behalf of",
        "currency transaction report",
        "structuring",
    ):
        assert grounding.get(term) == "grounded", (term, grounding.get(term))


def test_who_stood_at_the_window_and_whose_cash_it_is_are_two_fields(server):
    # Both relationships reach customers from one table: each is its own GraphQL field, so the
    # server serves both (it refuses to start when two relationships would share a field).
    # Paul Greer brought $4,500 to the window on Dana Whitmore's behalf (transaction 1005); her
    # own $7,000 she brought herself (1004).
    gql = (
        "{ b__cashTransactions(where: { id: { in: [1004, 1005] } }, order_by: [{ id: asc }]) "
        "{ id conductedBy { name } onBehalfOf { name } } }"
    )
    resp = _post(server, "/data/graphql", {"query": gql}, _COMPLIANCE)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    assert [
        (r["id"], r["conductedBy"]["name"], r["onBehalfOf"]["name"])
        for r in body["data"]["b__cashTransactions"]
    ] == [(1004, "Dana Whitmore", "Dana Whitmore"), (1005, "Paul Greer", "Dana Whitmore")]


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()}, _COMPLIANCE)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert len(rows) == len(_REPORTS), path.stem


def _statement_roles() -> list[tuple[str, str, str]]:
    """Each tour statement with the role the tour runs it as (the SQL explorer's run-as, kept
    from the step that picked it)."""
    role = _ADMIN
    out = []
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        role = step.get("role", role)
        if "sql" in step:
            out.append((step["key"], step["sql"], role))
    return out


@pytest.mark.parametrize(
    "key,sql,role", _statement_roles(), ids=lambda v: v if " " not in v else ""
)
def test_each_tour_statement_answers_as_its_role(server, key, sql, role):
    assert query(server, sql, role), key

# Copyright (c) 2026 Kenneth Stott
# Canary: 64c924f8-24ca-414e-9fc8-45f79c588b0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2056: the named demo ``insider`` works as shipped.

Its own docker-compose.yml brings the bank's Postgres up on a port of this test's own, under a
compose project of its own, its generated data loaded. A real Provisa server (DuckDB engine, its own
control plane) loads the demo's config.yaml as is, and scripts/run_demo_setup.py runs its setup.yaml
against it, as the bundle build does: match_deals matches every message to the watch list (the
demo's local sentence-transformers model -- the vector extra) and the review queue is built. Then
the ranking, the governance of a first-level reviewer, the example queries and the tour's
statements are read, each statement as the role the tour runs it as.
"""

# Requirements: REQ-2056

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.requires_vector]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "insider"
# The seeded cases (generate.py): the paraphrased leak by a trader not on deal Falcon, and the
# Falcon deal team's mail to the deal's counsel.
_LEAK, _EXPECTED = "M0111", "M0112"


class InsiderStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"insider-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"INSIDER_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
                f"the insider stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = InsiderStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("insider_demo")
    srv = IsolatedServer(
        "insider_demo",
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


@pytest.fixture(scope="module")
def set_up(server):
    done = subprocess.run(
        [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
        + ["--url", server.base_url],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
    return server


def _post(server, path: str, body: dict, role: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=300
    )


def query(server, sql: str, role: str = "org_admin") -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _queue(server, role: str = "org_admin") -> list[dict[str, Any]]:
    return query(
        server,
        'SELECT message_id, deal_id, flagged, reviewer, score FROM "bank"."view_review_queue" '
        "ORDER BY score DESC",
        role,
    )


def test_the_paraphrased_leak_outranks_the_deal_team_s_expected_mail(set_up):
    deals = [row for row in _queue(set_up) if row["deal_id"] is not None]
    rank = {(row["message_id"], row["deal_id"]): i for i, row in enumerate(deals)}
    assert deals[0]["message_id"] == _LEAK  # the top deal case
    assert rank[(_LEAK, "D-01")] < rank[(_EXPECTED, "D-01")]


def test_the_leak_scores_by_meaning_alone(set_up):
    [match] = query(
        set_up,
        'SELECT signals, sender_on_team FROM "bank"."deal_match" '
        f"WHERE message_id = '{_LEAK}' AND deal_id = 'D-01'",
    )
    assert match == {"signals": "meaning", "sender_on_team": 0}


def test_customer_data_mailed_out_after_reading_customer_records_ranks_first(set_up):
    top = _queue(set_up)[0]
    assert top["flagged"] == "customer data" and top["deal_id"] is None


@pytest.mark.parametrize("reviewer", ["reviewer_ana", "reviewer_ben"])
def test_a_first_level_reviewer_reads_only_their_own_cases_the_deal_withheld(set_up, reviewer):
    rows = _queue(set_up, reviewer)
    assert rows and {row["reviewer"] for row in rows} == {reviewer}
    assert {row["deal_id"] for row in rows} == {"withheld"}
    everyone = [row for row in _queue(set_up) if row["reviewer"] == reviewer]
    assert len(rows) == len(everyone)


@pytest.mark.parametrize("column", ["legal_name", "code_name", "description"])
def test_a_first_level_reviewer_reads_no_row_of_the_watch_list(set_up, column):
    resp = _post(
        set_up, "/data/sql", {"sql": f'SELECT {column} FROM "bank"."deal"'}, "reviewer_ben"
    )
    assert resp.status_code == 403 and "V003" in resp.text, resp.text


def test_a_first_level_reviewer_reads_the_flagged_entity_of_their_cases(set_up):
    flagged = {row["flagged"] for row in _queue(set_up, "reviewer_ben")}
    assert "Halcyon Biosciences, Inc." in flagged  # the leak is reviewer_ben's case


def test_a_deal_team_reads_only_its_own_deal(set_up):
    rows = query(set_up, 'SELECT deal_id, code_name FROM "bank"."deal"', "deal_team_falcon")
    assert rows == [{"deal_id": "D-01", "code_name": "Falcon"}]


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(set_up, path):
    resp = _post(set_up, "/data/graphql", {"query": path.read_text()}, "org_admin")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


def _tour_statements() -> list[tuple[str, str, str]]:
    """(key, role, sql) of each tour statement, as the role the tour runs it as."""
    role, out = "org_admin", []
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        if "role" in step:
            role = "org_admin" if step["role"] == "all" else step["role"]
        if "sql" in step:
            out.append((step["key"], role, step["sql"]))
    return out


def test_each_tour_statement_answers_rows_as_its_role(set_up):
    # In the tour's order: the ops record steps read what the steps before them did.
    for key, role, sql in _tour_statements():
        assert query(set_up, sql, role), key

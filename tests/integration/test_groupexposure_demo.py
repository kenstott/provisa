# Copyright (c) 2026 Kenneth Stott
# Canary: 8688603c-1b1f-4e5a-9702-c7850fcfd7df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2140: the named demo ``groupexposure`` works as shipped.

Its own docker-compose.yml brings the bank's Postgres up on a port of this test's own, under a
compose project of its own (db/init.sql writes the counterparties, facilities and capital). A
real Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is: the
bank read live, and the two Wikidata tables landed from Wikidata's public SPARQL query service as
TTL -1 replicas on their first read, as the bundle build lands them. The bank and its borrowers are
fictional; each borrower's owner is a real company the bank records by LEI. Then:
- every owner is found on Wikidata by its LEI, with one group at the top of its chain;
- no borrower alone is over the large-exposure limit, while the Veolia and Vinci groups are;
- the joint venture, owned by two groups, counts in both;
- the demo's example queries, its tour's statements and its REST call answer rows.

With PROVISA_DEMO_CAPTURE_DIR set, each answer is kept for the demo's card.

Wikidata needs no credential, only the internet; this runs in the warehouse lane, the lane that
reaches public services and keeps the demos' captures.
"""

# Requirements: REQ-2140, REQ-2138

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
from tests.integration.serving_wait import await_tables_serving

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "groupexposure"
_NAME = "groupexposure"
_ROLE = "org_admin"
#: What db/init.sql writes.
_BORROWERS = 37
_FACILITIES = 75
_GROUPS = '"groupexposure"."view_group_exposure"'
_ALONE = '"groupexposure"."view_borrower_exposure"'
#: The groups the tour says are over the limit, by their Wikidata entities.
_OVER = {"http://www.wikidata.org/entity/Q1632461", "http://www.wikidata.org/entity/Q1475312"}
#: The joint venture, owned through Veolia Česká republika and by Energetický a průmyslový holding.
_TWO_GROUPS = "B-002"


class GroupExposureStack:
    """The demo's compose stack, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"groupexposure-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"GROUPEXPOSURE_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

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
                f"the groupexposure stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = GroupExposureStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("groupexposure_demo")
    srv = IsolatedServer(
        "groupexposure_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=600)
    try:
        # Each Wikidata replica lands on its first read, from the public query service.
        await_tables_serving(
            srv,
            _ROLE,
            ['"groupexposure"."owner_company"', '"groupexposure"."owner_group"'],
            timeout=900,
        )
        yield srv
    finally:
        srv.stop_process()


def _post(server, path: str, body: dict, role: str = _ROLE) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}{path}", json=body, headers={"X-Provisa-Role": role}, timeout=600
    )


def query(server, sql: str, role: str = _ROLE) -> list[dict[str, Any]]:
    resp = _post(server, "/data/sql", {"sql": sql}, role)
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def test_the_bank_is_read_live(server):
    [row] = query(
        server,
        "SELECT COUNT(*) AS facilities, COUNT(DISTINCT borrower_id) AS borrowers "
        'FROM "groupexposure"."facilities"',
    )
    assert (row["facilities"], row["borrowers"]) == (_FACILITIES, _BORROWERS)


def test_every_owner_is_found_on_wikidata_with_one_group(server):
    rows = query(
        server,
        'SELECT o.owner_lei FROM "groupexposure"."owners" AS o '
        'LEFT JOIN "groupexposure"."owner_company" AS w ON w.lei = o.owner_lei '
        "WHERE w.entity IS NULL",
    )
    assert rows == []
    groups = query(
        server, 'SELECT lei, COUNT(*) AS n FROM "groupexposure"."owner_group" GROUP BY lei'
    )
    owners = query(server, 'SELECT DISTINCT owner_lei FROM "groupexposure"."owners"')
    assert {r["lei"] for r in groups} == {r["owner_lei"] for r in owners}
    assert [r for r in groups if r["n"] != 1] == []


def test_no_borrower_alone_is_over_the_limit(server):
    rows = query(server, f"SELECT name, pct_of_tier1 FROM {_ALONE} WHERE pct_of_tier1 >= 25")
    assert rows == []


def test_the_veolia_and_vinci_groups_are_over_it(server):
    rows = query(server, f"SELECT group_entity, borrowers FROM {_GROUPS} WHERE over_limit")
    assert {r["group_entity"] for r in rows} == _OVER
    assert all(r["borrowers"] == 6 for r in rows), rows


def test_the_joint_venture_counts_in_both_groups(server):
    rows = query(
        server,
        "SELECT o.borrower_id, COUNT(DISTINCT g.group_entity) AS groups "
        'FROM "groupexposure"."owners" AS o '
        'JOIN "groupexposure"."owner_group" AS g ON g.lei = o.owner_lei '
        "GROUP BY o.borrower_id HAVING COUNT(DISTINCT g.group_entity) > 1",
    )
    assert rows == [{"borrower_id": _TWO_GROUPS, "groups": 2}]


@pytest.mark.parametrize(
    "path", sorted((_DEMO / "queries").glob("*.graphql")), ids=lambda p: p.stem
)
def test_each_example_query_answers_rows(server, path):
    resp = _post(server, "/data/graphql", {"query": path.read_text()})
    assert resp.status_code == 200, resp.text
    demo_capture.keep(_NAME, f"gql-{path.stem}.json", resp.text)
    body = resp.json()
    assert not body.get("errors"), body
    [rows] = body["data"].values()
    assert rows, path.stem


_TOUR = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "sql" in s], ids=lambda s: s["key"])
def test_each_tour_statement_answers_rows(server, step):
    resp = _post(server, "/data/sql", {"sql": step["sql"]})
    assert resp.status_code == 200, f"{step['key']}: HTTP {resp.status_code}: {resp.text}"
    demo_capture.keep(_NAME, f"sql-{step['key']}.json", resp.text)
    assert resp.json()["data"]["sql"], step["key"]


@pytest.mark.parametrize("step", [s for s in _TOUR if "api" in s], ids=lambda s: s["key"])
def test_each_tour_rest_call_answers_rows(server, step):
    _method, path = step["api"].split(" ", 1)
    resp = httpx.get(f"{server.base_url}{path}", headers={"X-Provisa-Role": _ROLE}, timeout=300)
    assert resp.status_code == 200, resp.text
    demo_capture.keep(_NAME, "rest.json", resp.text)
    assert resp.json()["data"], step["key"]


def test_the_pages_beside_the_answers_are_served(server):
    """The schema, graph, glossary and REST documentation the demo's pages read beside its
    answers each answer (kept for its card with PROVISA_DEMO_CAPTURE_DIR set)."""
    demo_capture.keep_served(_NAME, server.base_url, _ROLE)

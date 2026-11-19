# Copyright (c) 2026 Kenneth Stott
# Canary: 5b41dfaf-7460-467c-9adc-8aa216e8e219
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: the named demo ``affirmation`` works as shipped.

Its own docker-compose.yml brings up the one Postgres holding the five systems' databases on a port
of this test's own, under a compose project of its own. A real Provisa server (DuckDB engine, its
own control plane) loads the demo's config.yaml as is, and scripts/run_demo_setup.py runs its
setup.yaml against it, as the bundle build does: every allocation is matched by the Python code
table (trade_status, stored) and the affirmation of each that matched is logged (affirm_matched).
Then the break queue, the affirmation log, the cutoffs and penalties, the example queries, the
tour's statements and Polly's grading queries are read.
"""

# Requirements: REQ-2006

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces
from tests.integration.polly_bakeoff_questions import QUESTIONS

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "affirmation"


def _generate():
    spec = importlib.util.spec_from_file_location("affirmation_generate", _DEMO / "generate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_GEN = _generate()
_ALLOCATIONS = {ref for _, ref, _, _ in _GEN.allocations_of()}


class AffirmationStack:
    """The demo's database server, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.pg_port,) = lease_ports(1)
        self.project = f"affirmation-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"AFFIRMATION_PG_PORT": str(self.pg_port), **store_namespaces(_DEMO)}

    def _compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", "-p", self.project, "-f", str(_DEMO / "docker-compose.yml")]
            + list(args),
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
                f"the affirmation stack did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = AffirmationStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("affirmation_demo")
    srv = IsolatedServer(
        "affirmation_demo",
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


def test_the_matcher_names_each_seeded_break_and_matches_every_other_allocation(set_up):
    rows = query(set_up, 'SELECT trade_ref, status, break_type FROM "postrade"."view_trade_status"')
    breaks = {r["trade_ref"]: r["break_type"] for r in rows if r["status"] == "break"}
    matched = {r["trade_ref"] for r in rows if r["status"] == "matched"}
    assert breaks == _GEN.BREAKS
    assert matched == _ALLOCATIONS - set(_GEN.BREAKS)
    assert len(rows) == len(_ALLOCATIONS)


def test_the_command_affirmed_exactly_the_matched_allocations_at_the_desk_s_time(set_up):
    rows = query(
        set_up, 'SELECT trade_ref, affirmed_at, affirmed_by FROM "postrade"."affirmation_log"'
    )
    assert {r["trade_ref"] for r in rows} == _ALLOCATIONS - set(_GEN.BREAKS)
    assert {r["affirmed_by"] for r in rows} == {"affirm_matched"}
    assert {str(r["affirmed_at"])[:19].replace("T", " ") for r in rows} == {"2026-11-18 21:15:00"}


def test_each_break_shows_the_time_to_its_market_s_cutoff(set_up):
    rows = query(
        set_up, 'SELECT trade_ref, market, minutes_to_cutoff FROM "postrade"."view_break_queue"'
    )
    # The desk is at 21:15 UTC: the EU markets cut off at 22:00 UTC, the US at 02:00 UTC next day.
    wanted = {"US": 285, "DE": 45, "FR": 45, "NL": 45}
    assert {r["trade_ref"] for r in rows} == set(_GEN.BREAKS)
    assert all(r["minutes_to_cutoff"] == wanted[r["market"]] for r in rows), rows


def test_each_break_shows_the_penalty_a_failed_settlement_would_accrue_a_day(set_up):
    rows = query(
        set_up,
        "SELECT q.trade_ref, q.gross_amount, q.penalty_per_day, r.penalty_bp_per_day "
        'FROM "postrade"."view_break_queue" AS q '
        'JOIN "postrade"."settlement_rules" AS r ON r.market = q.market',
    )
    for r in rows:
        expected = round(float(r["gross_amount"]) * float(r["penalty_bp_per_day"]) / 10000, 2)
        assert abs(float(r["penalty_per_day"]) - expected) < 0.011, r
    top = max(rows, key=lambda r: float(r["penalty_per_day"]))
    assert top["trade_ref"] == "A-1002-1"  # the largest failing amount, in the US


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


def test_each_tour_statement_answers_rows_as_its_role(set_up):
    role = "org_admin"
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        if "role" in step:
            role = "org_admin" if step["role"] == "all" else step["role"]
        if "sql" in step:
            assert query(set_up, step["sql"], role), step["key"]


@pytest.mark.parametrize("question", QUESTIONS["affirmation"], ids=lambda q: q.text[:40])
def test_each_polly_question_has_a_right_answer(set_up, question):
    assert query(set_up, question.answer_sql), question.text


def test_the_settlements_desk_reads_the_break_queue(set_up):
    rows = query(set_up, 'SELECT trade_ref FROM "postrade"."view_break_queue"', "settlements")
    assert {r["trade_ref"] for r in rows} == set(_GEN.BREAKS)

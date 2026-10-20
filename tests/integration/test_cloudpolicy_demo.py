# Copyright (c) 2026 Kenneth Stott
# Canary: 2f9092b0-ff8c-44f3-bba1-7ff69208a965
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``cloudpolicy`` works as shipped: its inventory and incidents land from the rows
it declares, its policy is read from the HTML its Python package ships, and its tour's statements
answer what its words say.

Its own docker-compose.yml brings the review store up on a port of this test's own. A real
Provisa server (DuckDB engine, its own control plane) loads the demo's config.yaml as is: the
cloud inventory and the ServiceNow incidents land from replica_rows/ and neither is contacted
(REQ-2089), and the policy reader is the org's upload from code/ (REQ-2101). The demo's setup
embeds every clause into the store. Then the daily review is checked against generate.py's rules
disk by disk, the daily trigger's statement is run as the trigger runs it, and each statement of
tour.yaml is run as the role the steps before it picked: the payments team's row rules hold on
every view built over the inventory and the incidents."""

# Requirements: REQ-2102, REQ-2089, REQ-2101

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "cloudpolicy"


def _generator():
    spec = importlib.util.spec_from_file_location("cloudpolicy_generate", _DEMO / "generate.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ReviewStore:
    """The demo's review store, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"cloudpolicy-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"CLOUDPOLICY_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

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
                f"the review store did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = ReviewStore()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("cloudpolicy_demo")
    srv = IsolatedServer(
        "cloudpolicy_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
            # The pinned CPython and uv the policy reader runs on, fetched once for the module.
            "PROVISA_RUNTIME_DEPS_CACHE": str(home / "runtime-deps"),
        },
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def set_up(server):
    done = subprocess.run(
        [
            sys.executable,
            str(_REPO / "scripts" / "run_demo_setup.py"),
            str(_DEMO),
            "--url",
            server.base_url,
        ],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert done.returncode == 0, f"{done.stdout}\n{done.stderr}"
    return server


def _sql(server, sql: str, role: str = "org_admin") -> list[dict[str, Any]]:
    resp = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=900,
    )
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


_STEPS = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.fixture(scope="module")
def answers(set_up) -> dict[str, list[dict[str, Any]]]:
    """Every statement of the tour, in order, as the role the steps before it picked."""
    role = "org_admin"
    out: dict[str, list[dict[str, Any]]] = {}
    for step in _STEPS:
        role = step.get("role", role)
        if "sql" in step:
            out[step["key"]] = _sql(set_up, step["sql"], role)
    return out


def test_the_declared_inventory_and_incidents_land(server):
    counts = {
        table: _sql(server, f'SELECT COUNT(*) AS n FROM "cloudpolicy"."{table}"')[0]["n"]
        for table in ("compute_resources", "storage_resources", "incident")
    }
    assert counts == {"compute_resources": 111, "storage_resources": 215, "incident": 32}


def test_the_policy_is_read_from_its_html_by_the_package(server):
    rows = _sql(
        server, 'SELECT section, heading FROM "cloudpolicy"."policy_sections" ORDER BY section'
    )
    assert [r["section"] for r in rows] == ["4.2", "4.3", "4.4", "5.1", "5.2", "6.1", "7.1"]
    assert rows[0]["heading"] == "Disk encryption with customer-managed keys"


def test_a_search_by_meaning_finds_the_disk_encryption_control(answers):
    assert answers["cloudpolicySearch"][0]["section"] == "4.2", answers["cloudpolicySearch"]


def test_the_daily_review_classifies_each_disk_by_the_policy_s_rules(set_up):
    gen = _generator()
    _vms, disks, incidents = gen.build(__import__("random").Random(2102))
    expected = {
        d.resource_id: gen.status_on(gen.REVIEW_DAY, d, incidents)
        for d in disks
        if d.vm is not None
    }
    rows = _sql(
        set_up, 'SELECT resource_id, status, incident_number FROM "cloudpolicy"."view_daily_review"'
    )
    assert {r["resource_id"]: (r["status"], r["incident_number"]) for r in rows} == expected
    violations = _sql(set_up, 'SELECT resource_name FROM "cloudpolicy"."view_cmek_violations"')
    assert len(violations) == sum(1 for s, _ in expected.values() if s != "compliant") == 30


def test_the_daily_trigger_writes_the_day_s_review(set_up):
    from provisa.scheduler.templating import substitute_date_tokens

    config = yaml.safe_load((_DEMO / "config.yaml").read_text())
    (trigger,) = config["scheduled_triggers"]
    listed = httpx.post(
        f"{set_up.base_url}/admin/graphql",
        json={"query": "{ scheduledTasks { id cronExpression kind role nextRunAt } }"},
        headers={"X-Provisa-Role": "org_admin"},
        timeout=120,
    )
    assert listed.status_code == 200 and "errors" not in listed.json(), listed.text
    (task,) = [t for t in listed.json()["data"]["scheduledTasks"] if t["id"] == "daily-review"]
    assert task["cronExpression"] == trigger["cron"] and task["kind"] == "sql", task
    # The statement the trigger runs each morning, as the scheduler renders it, through the same
    # governed pipeline, as its role.
    now = datetime.now(UTC)
    _sql(set_up, substitute_date_tokens(trigger["sql"], now), trigger["role"])
    day = now.date().isoformat()
    kept = _sql(
        set_up,
        f'SELECT status, COUNT(*) AS n FROM "cloudpolicy"."review_history" '
        f"WHERE review_date = DATE '{day}' GROUP BY status",
    )
    assert sum(r["n"] for r in kept) == 201, kept
    assert {r["status"]: r["n"] for r in kept}["compliant"] == 171, kept


def test_fourteen_days_of_history_lead_to_the_review_day(answers):
    trend = answers["cloudpolicyTrend"]
    assert len(trend) >= 14, trend
    # Disks fixed during the fortnight: fewer violations at its end than at its start.
    assert trend[0]["violations"] > trend[13]["violations"] >= 29, trend


def test_incidents_citing_the_wrong_control_and_unchecked_controls(set_up):
    wrong = _sql(
        set_up,
        'SELECT cited, finding FROM "cloudpolicy"."view_incident_controls" '
        "WHERE finding <> 'cites its control' ORDER BY cited",
    )
    assert [(r["cited"], r["finding"]) for r in wrong] == [
        ("4.1", "cites a section the policy does not have"),
        ("5.1", "cites the wrong control"),
        ("6.1", "cites the wrong control"),
    ]
    unchecked = _sql(
        set_up, 'SELECT section FROM "cloudpolicy"."view_unchecked_controls" ORDER BY section'
    )
    assert [r["section"] for r in unchecked] == ["4.4", "5.2", "7.1"]


def test_the_payments_team_reads_its_own_rows_only(answers):
    every = answers["cloudpolicyOpen"]
    own = answers["cloudpolicyAppTeamRows"]
    assert len(every) == 30 and {r["application"] for r in every} == {
        "payments",
        "checkout",
        "analytics",
        "identity",
    }
    assert 0 < len(own) < len(every)
    assert {r["application"] for r in own} == {"payments"}
    assert own == [r for r in every if r["application"] == "payments"]


def test_each_polly_question_s_grading_query_answers(set_up):
    from tests.integration.polly_bakeoff_questions import QUESTIONS

    for question in QUESTIONS["cloudpolicy"]:
        assert _sql(set_up, question.answer_sql), question.text


def test_the_example_query_answers_the_open_review(set_up):
    query = (_DEMO / "queries" / "1-open-review.graphql").read_text()
    resp = httpx.post(
        f"{set_up.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "auditor"},
        timeout=300,
    )
    assert resp.status_code == 200 and "errors" not in resp.json(), resp.text
    assert len(resp.json()["data"]["c__viewDailyReview"]) == 30

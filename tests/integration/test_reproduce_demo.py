# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``reproduce`` works as shipped, its tour's own statements answering what its
words say.

Its own docker-compose.yml brings the trading book up on a port of this test's own, under a
compose project of its own. A real Provisa server (DuckDB engine, its own control plane, its
OpenTelemetry SDK on so each refresh's lineage is recorded) loads the demo's config.yaml as is, and
the demo's setup.yaml is run against it as the bundle build runs it: the report filed, a rate
corrected, the report built again. Then each step of tour.yaml that reads is run as the tour runs
it -- in order, each under the time travel the steps before it set, the as-recorded time taken
from the refreshes step's answer as the tour takes it (REQ-2083) -- and its answer checked."""

# Requirements: REQ-862, REQ-1163, REQ-1484, REQ-2016, REQ-2083

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "reproduce"
_ROLE = "org_admin"
# The rates desk: 1,000,000 USD at 1.00 and 500,000 EUR at 1.08 as filed, at 1.10 once corrected.
_FILED, _CORRECTED = 1_540_000, 1_550_000
# How long the refresh records may take to land: the span exporter's batch delay, then the read.
_LANDED_S = 120


class BookStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"reproduce-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"REPRODUCE_PG_PORT": str(self.port)}

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
            raise AssertionError(f"the book did not come up: {exc.stderr}\n{logs[-4000:]}") from exc

    def down(self) -> None:
        self._compose("down", "-v", "--remove-orphans", check=False)


@pytest.fixture(scope="module")
def stack():
    built = BookStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("reproduce_demo")
    srv = IsolatedServer(
        "reproduce_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
            # Each refresh's lineage is a recorded span, landed where the native engine reads it.
            "OTEL_SDK_DISABLED": "false",
            "PROVISA_TELEMETRY_DIR": str(home / "telemetry"),
        },
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


def _sql(server, sql: str, travel: dict[str, str] | None = None) -> list[dict[str, Any]]:
    headers = {"X-Provisa-Role": _ROLE}
    if travel and travel.get("as_of"):
        headers["X-Provisa-As-Of"] = travel["as_of"]
    if travel and travel.get("as_recorded"):
        headers["X-Provisa-As-Recorded"] = travel["as_recorded"]
    resp = httpx.post(
        f"{server.base_url}/data/sql", json={"sql": sql}, headers=headers, timeout=300
    )
    assert resp.status_code == 200, f"{sql}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


def _admin(server, query: str) -> dict:
    resp = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    return body["data"]


_STEPS = {s["key"]: s for s in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]}


@pytest.fixture(scope="module")
def answers(server) -> dict[str, list[dict[str, Any]]]:
    """setup.yaml run as the bundle build runs it; then every reading step of the tour, in order,
    under the time travel the steps before it set, as the tour runner runs it."""
    subprocess.run(
        [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
        + ["--url", server.base_url],
        check=True,
        timeout=600,
    )
    refreshes = _STEPS["reproduceRefreshes"]["sql"]
    deadline = time.monotonic() + _LANDED_S
    while len(_sql(server, refreshes)) < 2:
        assert time.monotonic() < deadline, "the two refreshes' records never landed"
        time.sleep(2)
    travel: dict[str, str] = {}
    out: dict[str, list[dict[str, Any]]] = {}
    for key, step in _STEPS.items():
        if "as_of" in step:
            travel["as_of"] = step["as_of"]
        if "as_recorded_from" in step:
            taken = step["as_recorded_from"]
            travel["as_recorded"] = str(out[taken["step"]][taken["row"]][taken["column"]])
        if "sql" in step:
            out[key] = _sql(server, step["sql"], dict(travel))
    return out


def _rates(rows: list[dict[str, Any]]) -> float:
    [rates] = [r for r in rows if r["desk"] == "rates"]
    return float(rates["exposure"])


def test_each_refresh_records_what_it_was_built_from(answers):
    first, second = answers["reproduceRefreshes"][:2]
    # One definition, built twice: once as filed, once after the correction.
    assert first["definition_version"] == second["definition_version"]
    assert first["input_version_kind"] and second["input_version_kind"]


def test_the_report_now_carries_the_correction(answers):
    assert _rates(answers["reproduceNow"]) == _CORRECTED


def test_as_of_30_september_as_recorded_now_is_the_corrected_figure(answers):
    assert _rates(answers["reproduceAsOfNow"]) == _CORRECTED


def test_as_recorded_when_filed_is_the_figure_the_regulator_received(answers):
    assert _rates(answers["reproduceFiled"]) == _FILED


def test_the_audit_log_names_each_read_and_the_times_it_read_as_of(answers):
    recorded = [r for r in answers["reproduceAudit"] if r["as_recorded"]]
    assert recorded, answers["reproduceAudit"]
    assert all(r["as_of"] and r["role_id"] == _ROLE for r in recorded)


def test_the_reported_figure_traces_to_the_books_columns(server):
    resp = httpx.post(
        f"{server.base_url}/admin/lineage/graph",
        json={"sql": _STEPS["reproduceLineage"]["lineage"], "dialect": "postgres"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    sources = {e["source"] for e in resp.json()["edges"]}
    assert any(s.endswith("positions.notional") for s in sources), sources
    assert any(s.endswith("positions.fx_rate") for s in sources), sources


def test_renaming_the_rate_names_the_report_it_would_break(server):
    tables = _admin(server, "query { tables { id tableName } }")["tables"]
    [positions] = [t for t in tables if t["tableName"] == "positions"]
    found = _admin(
        server,
        f'query {{ columnDependents(tableId: "{positions["id"]}", renamed: ["fx_rate"], removed: []) '
        "{ columnName dependents { kind name breaksOn } } }",
    )["columnDependents"]
    names = {d["name"] for c in found for d in c["dependents"]}
    assert any("exposure" in n for n in names), found

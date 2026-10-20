# Copyright (c) 2026 Kenneth Stott
# Canary: 4ecd7358-9ba1-4232-9eb1-53a9cf64a2e8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``churn`` works as shipped: its tour's statements answer what its words say, and
its notebook trains its model.

Its own docker-compose.yml brings the customer database up on a port of this test's own, under a
compose project of its own. A real Provisa server (DuckDB engine, its own control plane) loads the
demo's config.yaml as is, and the demo's setup.yaml is run against it as the bundle build runs it:
the features the model is trained on, built through the governed pipeline. Then each statement of
tour.yaml is run as the tour runs it, as the role the steps before it picked, and the demo's
notebook (notebook.ipynb, REQ-2084) is run under this Python as a desktop Jupyter runs it -- the
same provisa_client code, reading over Arrow Flight as the analyst."""

# Requirements: REQ-2016, REQ-2071, REQ-2084, REQ-2085

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from provisa.core.demo_bundles import store_namespaces
import yaml

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "churn"
_CUSTOMERS = 1500


class BookStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"churn-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"CHURN_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

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
                f"the database did not come up: {exc.stderr}\n{logs[-4000:]}"
            ) from exc

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

    home = tmp_path_factory.mktemp("churn_demo")
    srv = IsolatedServer(
        "churn_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={**stack.env, "PROVISA_DATA_DIR": str(home / "provisa")},
    )
    srv.start(timeout=400)
    try:
        subprocess.run(
            [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(_DEMO)]
            + ["--url", srv.base_url, "--flight", f"grpc://127.0.0.1:{srv.flight_port}"],
            check=True,
            timeout=600,
        )
        yield srv
    finally:
        srv.stop_process()


_STEPS = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


@pytest.fixture(scope="module")
def answers(server) -> dict[str, list[dict[str, Any]]]:
    """Every statement of the tour, in order, as the role the steps before it picked."""
    role = "org_admin"
    out: dict[str, list[dict[str, Any]]] = {}
    for step in _STEPS:
        role = step.get("role", role)
        if "sql" in step:
            resp = httpx.post(
                f"{server.base_url}/data/sql",
                json={"sql": step["sql"]},
                headers={"X-Provisa-Role": role},
                timeout=300,
            )
            assert resp.status_code == 200, f"{step['key']}: {resp.text}"
            out[step["key"]] = resp.json()["data"]["sql"]
    return out


def _masked(email: str) -> bool:
    return email.startswith("***@")


def test_the_analyst_reads_the_features_with_email_addresses_masked(answers):
    rows = answers["churnFeatures"]
    assert len(rows) == 20
    assert all(_masked(r["email"]) for r in rows), rows
    # Most support tickets first: those who opened the most have mostly cancelled.
    assert [r["tickets"] for r in rows] == sorted((r["tickets"] for r in rows), reverse=True)
    assert sum(r["churned"] for r in rows) > len(rows) / 2, rows


def test_the_example_query_answers_the_same_features_masked(server):
    query = (_DEMO / "queries" / "1-churn-features.graphql").read_text()
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "analyst"},
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]["c__viewChurnFeatures"]
    assert len(rows) == 20 and all(_masked(r["email"]) for r in rows), rows


def _notebook() -> str:
    """The notebook's code cells as one script, a magic line (``%pip``) left out: what a desktop
    Jupyter with provisa-client installed runs."""
    from provisa.core.demo import demo_notebook

    return "\n\n".join(
        "\n".join(line for line in cell.splitlines() if not line.lstrip().startswith("%"))
        for cell in demo_notebook(_DEMO)
    )


def test_the_notebook_trains_a_model_on_the_masked_features_over_flight(server):
    report = (
        "\nimport json\n"
        "print(json.dumps({'rows': len(df), 'auc': auc, "
        "'emails': sorted(set(df['email'].str.split('@').str[0]))}))"
    )
    run = subprocess.run(
        [sys.executable, "-c", _notebook() + report],
        env={
            **os.environ,
            "PROVISA_URL": server.base_url,
            "PROVISA_FLIGHT_PORT": str(server.flight_port),
        },
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert run.returncode == 0, run.stderr[-4000:]
    result = json.loads(run.stdout.strip().splitlines()[-1])
    assert result["rows"] == _CUSTOMERS
    assert result["emails"] == ["***"], "every email address arrives masked"
    assert result["auc"] > 0.75, result

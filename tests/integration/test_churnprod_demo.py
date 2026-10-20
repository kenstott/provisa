# Copyright (c) 2026 Kenneth Stott
# Canary: 4a99c89e-d071-427a-8682-0a0338e1a72c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The named demo ``churnprod`` works as shipped: its model's package is a source, its function a
table, and its tour's statements answer what its words say.

Its own docker-compose.yml brings the customer database up on a port of this test's own, under a
compose project of its own. A real Provisa server (DuckDB engine, its own control plane) loads the
demo's config.yaml as is; the archives in code/ become its uploads as the schema is built
(REQ-2101), and each code source installs its package from them, the Python one from the wheels
its uv.lock pins. Then each statement of tour.yaml is run as the tour runs it, as the role the
steps before it picked: the join feeds each customer the role sees to the model (REQ-2099), the
analysts' email mask applies to what the code returns, and the EU account team's row rule on
churn_risk keeps only its region's scores."""

# Requirements: REQ-2097, REQ-2099, REQ-2101

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from provisa.core.demo_bundles import store_namespaces

pytestmark = [pytest.mark.integration]

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "churnprod"
# The churn demo's customers who have not cancelled, by where they are billed (db/init.sql, seeded).
_CURRENT = 1114
_CURRENT_EU = 363


class ChurnStack:
    """The demo's database, on a leased port."""

    def __init__(self) -> None:
        from tests.port_lease import lease_ports

        (self.port,) = lease_ports(1)
        self.project = f"churnprod-demo-{os.getpid()}"

    @property
    def env(self) -> dict[str, str]:
        return {"CHURNPROD_PG_PORT": str(self.port), **store_namespaces(_DEMO)}

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
    built = ChurnStack()
    built.up()
    try:
        yield built
    finally:
        built.down()


@pytest.fixture(scope="module")
def server(stack, tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("churnprod_demo")
    srv = IsolatedServer(
        "churnprod_demo",
        engine="duckdb",
        config=str(_DEMO / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{home / 'store.duckdb'}",
        env={
            **stack.env,
            "PROVISA_DATA_DIR": str(home / "provisa"),
            # The pinned Node, CPython and uv, fetched once for the module.
            "PROVISA_RUNTIME_DEPS_CACHE": str(home / "runtime-deps"),
        },
    )
    srv.start(timeout=400)
    try:
        yield srv
    finally:
        srv.stop_process()


_STEPS = yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]


def _sql(server, sql: str, role: str) -> httpx.Response:
    # A first read of a code source installs its package: its dependencies from wheels.
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=900,
    )


@pytest.fixture(scope="module")
def answers(server) -> dict[str, list[dict[str, Any]]]:
    """Every statement of the tour, in order, as the role the steps before it picked."""
    role = "org_admin"
    out: dict[str, list[dict[str, Any]]] = {}
    for step in _STEPS:
        role = step.get("role", role)
        if "sql" in step:
            resp = _sql(server, step["sql"], role)
            assert resp.status_code == 200, f"{step['key']}: {resp.text}"
            out[step["key"]] = resp.json()["data"]["sql"]
    return out


def _masked(email: str) -> bool:
    return email.startswith("***@")


def test_discovery_offers_the_function_s_columns_from_its_types(server):
    query = (
        '{ availableColumnsMetadata(sourceId: "churn-model", schemaName: "default", '
        'tableName: "churn_risk") { name dataType nativeFilterType nativeFilterRequired } }'
    )
    resp = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "org_admin"},
        timeout=900,
    )
    assert resp.status_code == 200 and "errors" not in resp.json(), resp.text
    found = {c["name"]: c for c in resp.json()["data"]["availableColumnsMetadata"]}
    assert {
        n: found[n]["dataType"] for n in ("customer_id", "score", "reasons", "computed_at")
    } == {
        "customer_id": "text",
        "score": "double",
        "reasons": "json",
        "computed_at": "timestamp",
    }
    # Each argument a parameter, required where the function gives no default.
    assert found["_nf_customer_id"]["nativeFilterRequired"] is True
    assert found["_nf_tenure_months"]["dataType"] == "bigint"
    assert found["_nf_as_of"]["nativeFilterRequired"] is False
    # The registered table declares the same columns.
    config = yaml.safe_load((_DEMO / "config.yaml").read_text())
    (table,) = [t for t in config["tables"] if t["table"] == "churn_risk"]
    assert {c["name"]: c["data_type"] for c in table["columns"]} == {
        n: c["dataType"] for n, c in found.items()
    }


def test_a_read_without_a_customer_is_refused_before_the_code_runs(server):
    refused = _sql(server, 'SELECT customer_id, score FROM "churn"."churn_risk"', "org_admin")
    assert refused.status_code != 200, refused.text
    assert "customer_id" in refused.text, refused.text


def test_the_join_scores_every_current_customer(server, answers):
    rows = answers["churnprodAtRisk"]
    assert len(rows) == 20
    scores = [r["score"] for r in rows]
    assert scores == sorted(scores, reverse=True) and scores[0] > 0.9, rows
    # org_admin reads the addresses as they are.
    assert not any(_masked(r["email"]) for r in rows), rows
    assert all(r["reasons"] for r in rows), rows
    every = _sql(server, 'SELECT COUNT(*) AS n FROM "churn"."view_at_risk"', "org_admin")
    assert every.status_code == 200, every.text
    assert every.json()["data"]["sql"] == [{"n": _CURRENT}]


def test_the_analyst_reads_the_same_scores_with_emails_masked(answers):
    masked = answers["churnprodMasked"]
    assert all(_masked(r["email"]) for r in masked), masked
    as_admin = {r["customer_id"]: r["score"] for r in answers["churnprodAtRisk"]}
    assert {r["customer_id"]: r["score"] for r in masked} == as_admin


def test_the_eu_team_reads_its_region_s_scores_only(server, answers):
    assert answers["churnprodRegionalRows"] == [
        {
            "region": "EU",
            "customers": _CURRENT_EU,
            "average_score": answers["churnprodRegionalRows"][0]["average_score"],
        }
    ]
    # The row rule is on churn_risk itself: read alone, it keeps only EU customers' scores, and
    # its email mask holds on the list.
    listed = _sql(
        server,
        'SELECT customer_id, email, region FROM "churn"."view_at_risk" ORDER BY score DESC',
        "analyst_eu",
    )
    assert listed.status_code == 200, listed.text
    rows = listed.json()["data"]["sql"]
    assert len(rows) == _CURRENT_EU
    assert {r["region"] for r in rows} == {"EU"}
    assert all(_masked(r["email"]) for r in rows), rows[:3]


def test_the_node_twin_scores_each_customer_as_the_python_model(answers):
    rows = answers["churnprodNode"]
    assert len(rows) == 20
    assert all(r["python_score"] == r["node_score"] for r in rows), rows
    # The EU account team picked before it: its row rule holds on both.
    assert len({r["customer_id"] for r in rows}) == 20


def test_the_example_query_answers_the_at_risk_list_masked(server):
    query = (_DEMO / "queries" / "1-at-risk.graphql").read_text()
    resp = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": "analyst"},
        timeout=300,
    )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"]["c__viewAtRisk"]
    assert len(rows) == 20 and all(_masked(r["email"]) for r in rows), rows

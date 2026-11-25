# Copyright (c) 2026 Kenneth Stott
# Canary: e4a71c08-5d2b-4f93-86e1-b0c9d3f27a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checkout's demo makes a Spark source with no Docker (REQ-2225, amended 2026-11-24).

The smoke check's isolated instance of the default demo, run from this checkout, offers the Spark
connector run natively. A source is created from it by name alone, with an annotated script and no
cluster, so it runs on the demo's local test cluster (a native one-node Spark). Provisa builds the
connector's client from connectors/spark, starts its code host itself and pairs with it. The
script's table is registered and one row is read through Provisa; after a restart of the backend
the host is started again from its state and the row is read again. The script's two-table @query,
registered, is offered as a code resolver (the Relationships page's isCodeResolver).

Lands on the TEST instance only: the smoke instance's PROVISA_HOME, data directory, control plane
and ports are this run's own."""

# Requirements: REQ-2225

from __future__ import annotations

import json
import textwrap
from typing import Any

import httpx
import pytest

from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_DOMAIN = "spark_native"
_SOURCE = "spark_jobs"
#: The first create builds the client, installs it, downloads and starts the local test cluster.
_CREATE_TIMEOUT_S = 1800

_SCRIPT = textwrap.dedent(
    '''
    from dataclasses import dataclass

    from provisa_services import TableRef
    from provisa_spark import dataframe, session


    @dataclass
    class Number:
        id: int
        label: str


    @dataclass
    class Match:
        left_id: int
        right_id: int


    def numbers() -> list[Number]:
        """Three numbers, computed by the cluster.

        @table
        """
        frame = session().sql("SELECT id, concat('n', id) AS label FROM range(3) ORDER BY id")
        return [Number(**row.asDict()) for row in frame.collect()]


    def same_label(left: TableRef[Number], right: TableRef[Number]) -> list[Match]:
        """Which rows of two tables carry the same label, matched on the cluster.

        @query
        """
        a = dataframe(left).alias("a")
        b = dataframe(right).alias("b")
        joined = a.join(b, a.label == b.label).selectExpr("a.id AS left_id", "b.id AS right_id")
        return [Match(**row.asDict()) for row in joined.collect()]
    '''
)


@pytest.fixture(scope="module")
def instance():
    from tests.smoke import test_main_is_startable as smoke

    yield from smoke.instance.__wrapped__()


def _log_tail(inst: Instance) -> str:
    logs = sorted(inst.log_dir.glob("backend*.log"), key=lambda p: p.stat().st_mtime)
    return logs[-1].read_text()[-6000:]


def _gql(inst: Instance, query: str, timeout: float = 300) -> dict[str, Any]:
    r = httpx.post(
        f"{inst.url}/admin/graphql", json={"query": query}, headers=_HEADERS, timeout=timeout
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "errors" not in body, f"{body}\n{_log_tail(inst)}"
    (result,) = body["data"].values()
    return result


def _mutation(inst: Instance, mutation: str, timeout: float = 300) -> None:
    result = _gql(inst, "mutation { " + mutation + " { success message } }", timeout)
    assert result["success"], f"{result['message']}\n{_log_tail(inst)}"


def _first_number(inst: Instance) -> dict:
    r = httpx.post(
        f"{inst.url}/data/sql",
        json={"sql": f'SELECT id, label FROM "{_DOMAIN}"."numbers" ORDER BY id LIMIT 1'},
        headers=_HEADERS,
        timeout=900,
    )
    assert r.status_code == 200, f"{r.text[:2000]}\n{_log_tail(inst)}"
    body = r.json()
    assert "errors" not in body, f"{body}\n{_log_tail(inst)}"
    (row,) = body["data"]["sql"]
    return {"id": int(row["id"]), "label": row["label"]}


def _is_code_resolver(f: dict) -> bool:
    """The Relationships page's test (relationship-types.ts isCodeResolver)."""
    return (
        f.get("implKind") == "code_command"
        and f.get("kind") == "query"
        and len(f["arguments"]) == 2
        and all(a.get("argKind") == "table_ref" for a in f["arguments"])
        and bool(f.get("outputColumns"))
    )


@pytest.fixture(scope="module")
def spark_source(instance: Instance) -> Instance:
    """The Spark source, made from the offered connector by name, its table registered."""
    offered = httpx.get(
        f"{instance.url}/admin/orgs/default/connectors", headers=_HEADERS, timeout=120
    )
    assert offered.status_code == 200, f"{offered.text}\n{_log_tail(instance)}"
    (spark,) = [c for c in offered.json()["connectors"] if c["name"] == "spark"]
    assert spark["native"] is True and spark["image"] == ""

    _mutation(instance, f'createDomain(input: {{id: "{_DOMAIN}"}})')
    joining = {"connector_join": {"connector": "spark"}, "settings": {"script": _SCRIPT}}
    _mutation(
        instance,
        f'createSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
        timeout=_CREATE_TIMEOUT_S,
    )
    columns = ", ".join(
        f'{{name: "{name}", visibleTo: ["{_ROLE}"], dataType: "{kind}"}}'
        for name, kind in (("id", "bigint"), ("label", "text"))
    )
    _mutation(
        instance,
        f'registerTable(input: {{sourceId: "{_SOURCE}", domainId: "{_DOMAIN}", '
        f'schemaName: "default", tableName: "numbers", cacheTtl: 300, columns: [{columns}]}})',
        timeout=900,
    )
    return instance


def test_a_checkout_demo_reads_a_spark_table_through_a_native_host(spark_source: Instance):
    assert _first_number(spark_source) == {"id": 0, "label": "n0"}


def test_a_restart_starts_the_native_host_again(spark_source: Instance):
    spark_source.restart_backend()
    assert _first_number(spark_source) == {"id": 0, "label": "n0"}


def test_its_two_table_query_is_offered_as_a_code_resolver(spark_source: Instance):
    saved = httpx.post(
        f"{spark_source.url}/admin/actions/functions",
        json={
            "name": "same_label",
            "sourceId": _SOURCE,
            "functionName": "same_label",
            "domainId": _DOMAIN,
            "visibleTo": [_ROLE],
        },
        headers=_HEADERS,
        timeout=600,
    )
    assert saved.status_code == 200, f"{saved.text}\n{_log_tail(spark_source)}"
    listed = httpx.get(f"{spark_source.url}/admin/actions", headers=_HEADERS, timeout=120)
    assert listed.status_code == 200, listed.text
    (found,) = [f for f in listed.json()["functions"] if f["name"] == "same_label"]
    assert _is_code_resolver(found), found

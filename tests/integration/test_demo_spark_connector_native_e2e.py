# Copyright (c) 2026 Kenneth Stott
# Canary: e4a71c08-5d2b-4f93-86e1-b0c9d3f27a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A checkout's demo makes a Spark source with no Docker (REQ-2225, amended 2026-11-24 and
2026-11-25).

The smoke check's isolated instance of the default demo, run from this checkout, offers the Spark
connector run natively. A source is created from it by name alone, with no script (a Spark source
has none) and no cluster, so it runs on the demo's local test cluster (a native one-node Spark).
Provisa builds the connector's client from connectors/spark, starts its code host itself and pairs
with it; the host's @on-start is given the local test cluster. The host answers; after a restart of
the backend the host is started again from its state (the health poll dials it) and answers again.

Lands on the TEST instance only: the smoke instance's PROVISA_HOME, data directory, control plane
and ports are this run's own."""

# Requirements: REQ-2225

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import pytest

from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_SOURCE = "spark_jobs"
#: The first create builds the client, installs it, downloads and starts the local test cluster.
_CREATE_TIMEOUT_S = 1800
#: The health poll runs every 30 seconds (codesource.health_poll_interval) and dials the host,
#: which starts a native one again.
_RESTARTED_WITHIN_S = 180


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


def _host(inst: Instance) -> dict:
    r = httpx.get(
        f"{inst.url}/admin/orgs/default/code-sources/{_SOURCE}/host", headers=_HEADERS, timeout=120
    )
    assert r.status_code == 200, f"{r.text}\n{_log_tail(inst)}"
    return r.json()


@pytest.fixture(scope="module")
def spark_source(instance: Instance) -> Instance:
    """The Spark source, a built-in source type (REQ-2225, amended 2026-11-24), joined to the
    native host its form names, with no script."""
    offered = httpx.get(
        f"{instance.url}/admin/orgs/default/connectors", headers=_HEADERS, timeout=120
    )
    assert offered.status_code == 200, f"{offered.text}\n{_log_tail(instance)}"
    assert [c for c in offered.json()["connectors"] if c["name"] == "spark"] == []
    builtin = httpx.get(
        f"{instance.url}/admin/orgs/default/builtin-sources/spark", headers=_HEADERS, timeout=120
    )
    assert builtin.status_code == 200, f"{builtin.text}\n{_log_tail(instance)}"
    spark = builtin.json()["host"]
    assert spark["native"] is True and spark["image"] == ""
    assert [r["name"] for r in spark["requires"]] == ["cluster"]

    joining = {"connector_join": {"connector": "spark"}, "settings": {}}
    _mutation(
        instance,
        f'createSource(input: {{id: "{_SOURCE}", type: "spark", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(joining))}}})',
        timeout=_CREATE_TIMEOUT_S,
    )
    return instance


def test_a_checkout_demo_joins_a_spark_source_to_a_native_host(spark_source: Instance):
    found = _host(spark_source)
    assert found["mode"] == "built", found
    assert found["answering"] is True, f"{found}\n{_log_tail(spark_source)}"
    assert found["host"].startswith("127.0.0.1:"), found


def test_a_restart_starts_the_native_host_again(spark_source: Instance):
    spark_source.restart_backend()
    deadline = time.monotonic() + _RESTARTED_WITHIN_S
    found = _host(spark_source)
    while not found.get("answering") and time.monotonic() < deadline:
        time.sleep(5)
        found = _host(spark_source)
    assert found.get("answering") is True, f"{found}\n{_log_tail(spark_source)}"

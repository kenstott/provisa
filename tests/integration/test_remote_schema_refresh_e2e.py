# Copyright (c) 2026 Kenneth Stott
# Canary: ac2717a6-d0d5-44dc-ac91-326845d1b55e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A remote source's schema is read again on demand, and the rules on its tables stand
(REQ-311, REQ-321, REQ-329).

The ``test`` instance: the smoke check's isolated instance of the default demo, with its shelter
GraphQL API (``graphql-demo``) and pet store OpenAPI (``petstore-api``), and a gRPC remote
source this test registers against demo/grpc_remote_server (proto-based), started on a leased
port. For each source a row rule is set for the demo's restricted role on one of its registered
tables, the source is refreshed through its admin route, and afterwards the table is still read
and the restricted role still reads only the rows its rule allows. The gRPC source keeps the
import paths it was registered with.
"""

# Requirements: REQ-311, REQ-321, REQ-329

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from tests.port_lease import lease_ports
from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import ADMIN, REPO, RESTRICTED, Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated]

_TIMEOUT = 120.0
_PROTO_DIR = REPO / "demo" / "grpc_remote_server"


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    yield from smoke.instance.__wrapped__()


@pytest.fixture(scope="module")
def grpc_remote(tmp_path_factory) -> Iterator[str]:
    """demo/grpc_remote_server on a leased port; its address."""
    (port,) = lease_ports(1)
    log = tmp_path_factory.mktemp("grpc-remote") / "server.log"
    with log.open("w") as out:
        proc = subprocess.Popen(
            [sys.executable, "-m", "demo.grpc_remote_server.server"],
            cwd=REPO,
            env={**os.environ, "DEMO_GRPC_REMOTE_PORT": str(port)},
            stdout=out,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        import socket

        deadline = time.monotonic() + 60
        while True:
            assert proc.poll() is None, log.read_text()
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                break
            except OSError:
                assert time.monotonic() < deadline, log.read_text()
                time.sleep(0.5)
        yield f"localhost:{port}"
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


def _admin_gql(inst: Instance, document: str, variables: dict | None = None) -> dict:
    resp = httpx.post(
        f"{inst.url}/admin/graphql",
        json={"query": document, "variables": variables or {}},
        headers={"X-Provisa-Role": ADMIN},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body["errors"]
    return body["data"]


def _rows(inst: Instance, sql: str, role: str) -> list[dict]:
    resp = httpx.post(
        f"{inst.url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["data"]["sql"]


def _rule(inst: Instance, table: str, filter_expr: str) -> None:
    result = _admin_gql(
        inst,
        "mutation($r: RLSRuleInput!) { upsertRlsRule(input: $r) { success message } }",
        {
            "r": {
                "tableId": table,
                "roleId": RESTRICTED,
                "filterExpr": filter_expr,
                "description": "The refresh test's rule: it must outlive a refresh.",
                "creating": True,
            }
        },
    )["upsertRlsRule"]
    assert result["success"], result["message"]


def _refresh(inst: Instance, path: str) -> dict:
    resp = httpx.post(f"{inst.url}{path}", headers={"X-Provisa-Role": ADMIN}, timeout=_TIMEOUT)
    assert resp.status_code == 200, resp.text
    return resp.json()


def _rule_outlives_refresh(
    inst: Instance, table: str, sql: str, column: str, refresh_path: str
) -> None:
    """Set a rule on ``table`` keeping the restricted role to one value of ``column``, refresh the
    source, and check the table is read and the rule holds both before and after."""
    every = {r[column] for r in _rows(inst, sql, ADMIN)}
    assert len(every) > 1, every  # a rule that keeps one value must narrow something
    kept = sorted(every)[0]
    _rule(inst, table, f"{column} = '{kept}'")
    assert {r[column] for r in _rows(inst, sql, RESTRICTED)} == {kept}

    _refresh(inst, refresh_path)

    assert {r[column] for r in _rows(inst, sql, ADMIN)} == every
    assert {r[column] for r in _rows(inst, sql, RESTRICTED)} == {kept}


def test_a_graphql_remote_refresh_keeps_its_tables_and_their_rules(instance):
    """REQ-311."""
    _rule_outlives_refresh(
        instance,
        "employees",
        "SELECT id, department FROM graphql.employees",
        "department",
        "/admin/sources/graphql-remote/graphql-demo/refresh",
    )


def test_an_openapi_refresh_keeps_its_tables_and_their_rules(instance):
    """REQ-321."""
    _rule_outlives_refresh(
        instance,
        "inventory",  # its virtual name: the alias it is registered under
        "SELECT status, count FROM default.get_inventory",
        "status",
        "/admin/openapi/refresh/petstore-api",
    )


def test_a_grpc_remote_refresh_keeps_its_tables_their_rules_and_its_import_paths(
    instance, grpc_remote
):
    """REQ-329."""
    source_id, namespace = "catalog-grpc", "catalog"
    registered = httpx.post(
        f"{instance.url}/admin/grpc-remote/register",
        json={
            "source_id": source_id,
            "proto_path": str(_PROTO_DIR / "animal_catalog.proto"),
            "server_address": grpc_remote,
            "namespace": namespace,
            "domain_id": "shelter",
            "import_paths": [str(_PROTO_DIR)],
        },
        headers={"X-Provisa-Role": ADMIN},
        timeout=_TIMEOUT,
    )
    assert registered.status_code == 200, registered.text

    offered = _admin_gql(
        instance,
        "query($s: String!, $n: String!) { availableTables(sourceId: $s, schemaName: $n) "
        "{ name } }",
        {"s": source_id, "n": "grpc_remote"},
    )["availableTables"]
    matching = [t["name"] for t in offered if "listbreeds" in t["name"].lower().replace("_", "")]
    assert len(matching) == 1, offered
    table = matching[0]
    added = _admin_gql(
        instance,
        "mutation($t: TableInput!) { registerTable(input: $t) { success message } }",
        {
            "t": {
                "sourceId": source_id,
                "domainId": "shelter",
                "schemaName": "grpc_remote",
                "tableName": table,
                "columns": [
                    {"name": "name", "visibleTo": [ADMIN, RESTRICTED]},
                    {"name": "species", "visibleTo": [ADMIN, RESTRICTED]},
                ],
            }
        },
    )["registerTable"]
    assert added["success"], added["message"]

    _rule_outlives_refresh(
        instance,
        table,
        f"SELECT name, species FROM grpc_remote.{namespace}__animal_catalog__list_breeds",
        "species",
        f"/admin/grpc-remote/refresh/{source_id}",
    )

    listed = httpx.get(
        f"{instance.url}/admin/grpc-remote/list",
        headers={"X-Provisa-Role": ADMIN},
        timeout=_TIMEOUT,
    )
    assert listed.status_code == 200, listed.text
    (source,) = [s for s in listed.json() if s["source_id"] == source_id]
    assert source["import_paths"] == [str(_PROTO_DIR)], source
    assert Path(source["proto_path"]) == _PROTO_DIR / "animal_catalog.proto", source

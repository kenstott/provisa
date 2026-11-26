# Copyright (c) 2026 Kenneth Stott
# Canary: e3c1a7d2-48b6-4f9e-a05c-7d2b91f6c384
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a dev-mode code source read by Trino through its pgwire sidecar (REQ-2267 part 3).

The ``test`` instance: an isolated server on Trino. A Python code source is saved in dev mode (its
code in the server's worker pool) and its table declares a landing TTL of 0, so a read joining it
to a Postgres table is the engine's: the schema build names the source to Trino's code-host
connector, whose catalog dials the source's sidecar where the Trino container reaches it, in plain
PostgreSQL. The join is answered with the function called on the read's parameter, and the plan
reads the code table from the source's own catalog, not from a landed copy. The source edited, its
sidecar stops, Trino's catalog of it is dropped and made again, and the next read answers from the
new code on the same port.

Reachability: Trino runs in a container and the sidecar on the test host, as the govdata Trino
test's adapter does (tests/integration/test_govdata_trino_source_e2e.py): on Linux both the bind
and the engine host are the default bridge's gateway; on Docker Desktop the sidecar stays on
loopback and Trino reaches it as ``host.docker.internal``."""

# Requirements: REQ-2267

from __future__ import annotations

import json
import subprocess
import sys

import httpx
import pytest

from tests.integration.test_code_source_attach_trino import _CODE, _JOIN, _ROLE, _config
from tests.integration.test_code_sources_e2e import (
    _admin,
    _archive,
    _column,
    _parameter,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_ORG = "codesidecar_trino"
_SOURCE = "regions-src"


def _container_reach() -> dict[str, str]:
    """Where the sidecar listens and the name the Trino container reaches it by."""
    from provisa.federation import pgwire_replica as pr

    if sys.platform == "darwin":
        return {pr.ENGINE_HOST_ENV: "host.docker.internal"}
    gateway = subprocess.run(
        [
            "docker",
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{(index .IPAM.Config 0).Gateway}}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {pr.BIND_HOST_ENV: gateway, pr.ENGINE_HOST_ENV: gateway}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    import asyncio

    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("codesidecar_trino")
    path = work / "config.yaml"
    _config(path)
    srv = IsolatedServer(_ORG, engine="trino", config=str(path), env=_container_reach())
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _upload(srv, files: dict[str, str]) -> str:
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "regions.zip"},
        content=_archive(files),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    return upload.json()["content_hash"]


def _mapping(upload: str) -> str:
    return json.dumps(
        json.dumps({"runtime": "python", "code": {"kind": "upload", "upload": upload}})
    )


@pytest.fixture(scope="module")
def dev_source(server):
    _admin(
        server,
        f'createSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {_mapping(_upload(server, _CODE))}}})',
    )
    columns = [
        _column("region", "text"),
        _column("label", "text"),
        _column("asked", "text"),
        _parameter("tag", "text", required=False),
    ]
    _admin(
        server,
        f'registerTable(input: {{sourceId: "{_SOURCE}", domainId: "fx", schemaName: "default", '
        f'tableName: "regions", cacheTtl: 0, columns: [{", ".join(columns)}]}})',
    )
    _admin(
        server,
        'createRelationship(input: {id: "orders-regions", sourceTableId: "orders", '
        'targetTableId: "regions", sourceColumn: "region", targetColumn: "region", '
        'cardinality: "many-to-one", graphqlAlias: "regionDetail"})',
    )
    return _SOURCE


def test_trino_joins_a_dev_sources_function_read_through_its_sidecar(server, dev_source):
    rows = _rows(_sql(server, _JOIN))
    assert rows, "no eu-west order was joined to its region"
    assert {(r["label"], r["asked"]) for r in rows} == {("EU West", "joined")}


def test_the_join_reads_the_code_table_from_the_sources_catalog_not_a_landed_copy(
    server, dev_source
):
    plan = httpx.post(
        f"{server.base_url}/data/sql/explain",
        json={"sql": _JOIN},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 120,
    )
    assert plan.status_code == 200, plan.text
    text = json.dumps(plan.json())
    assert "regions_src" in text.replace("-", "_"), text[:2000]


def test_an_edited_source_is_read_from_its_new_code_through_trino(server, dev_source):
    changed = {name: text.replace('"EU West"', '"Europe West"') for name, text in _CODE.items()}
    _admin(
        server,
        f'updateSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {_mapping(_upload(server, changed))}}})',
    )
    rows = _rows(_sql(server, _JOIN))
    assert rows and {r["label"] for r in rows} == {"Europe West"}

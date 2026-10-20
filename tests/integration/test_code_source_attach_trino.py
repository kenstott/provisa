# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a built code host attached by Trino (REQ-2100).

The ``test`` instance: an isolated server on Trino. A Python code source's image is built
through the API, loaded and run on the stack's compose network, where both Trino and this host
reach it, and connected, which switches the source to built mode. Its table declares no landing
TTL, so a read joining it to a Postgres table is the engine's, Trino attaching the host over
pgwire with Provisa's identity and verifying the host by its pinned certificate: the join is
answered, the host's function called once with the read's parameter, and the plan reads the code
table from the host's catalog, not from a landed copy. A read of the code table alone is answered
DIRECT from the host."""

# Requirements: REQ-2100

from __future__ import annotations

import json
import platform
import re
import subprocess
import time

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import (
    _admin,
    _archive,
    _column,
    _parameter,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"
_ORG = "codehost_trino"
_SOURCE = "regions-src"
_ARCH = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[
    platform.machine().lower()
]

_CODE = {
    "regions/regions.py": '''
from dataclasses import dataclass

_LABELS = {"us-east": "US East", "us-west": "US West", "eu-west": "EU West", "ap-south": "AP South"}


@dataclass
class Region:
    region: str
    label: str
    asked: str


def regions(tag: str = "none") -> list[Region]:
    """The regions orders are placed in.

    @table
    """
    return [Region(r, label, tag) for r, label in sorted(_LABELS.items())]
''',
}


def _config(path) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "federation_engine": "trino",
                "auth": {
                    "provider": "none",
                    "assignments_source": "provisa",
                    "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
                },
                "naming": {"domain_prefix": False, "rules": []},
                "cache": {"enabled": False},
                "domains": [{"id": "fx", "description": "code host attach"}],
                "roles": [],
                "sources": [
                    {
                        "id": "sales-pg",
                        "type": "postgresql",
                        "host": "localhost",
                        "port": "${env:PG_PORT:-5432}",
                        "database": "provisa",
                        "username": "provisa",
                        "password": "${env:PG_PASSWORD}",
                    }
                ],
                "tables": [
                    {
                        "source_id": "sales-pg",
                        "domain_id": "fx",
                        "schema": "public",
                        "table": "orders",
                        "columns": [
                            {"name": "id", "data_type": "integer", "visible_to": [_ROLE]},
                            {"name": "region", "data_type": "varchar", "visible_to": [_ROLE]},
                        ],
                    }
                ],
            }
        )
    )


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    import asyncio

    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    work = tmp_path_factory.mktemp("codehost_trino")
    path = work / "config.yaml"
    _config(path)
    srv = IsolatedServer(
        _ORG,
        engine="trino",
        config=str(path),
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(work / "runtime-deps")},
    )
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _api(srv, method: str, path: str, **kw) -> httpx.Response:
    return httpx.request(
        method,
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-sources/{_SOURCE}{path}",
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 900,
        **kw,
    )


@pytest.fixture(scope="module")
def code_source(server):
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "regions.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "{_SOURCE}", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    columns = [
        _column("region", "text"),
        _column("label", "text"),
        _column("asked", "text"),
        _parameter("tag", "text", required=False),
    ]
    # No landing TTL of its own: once built, a read across sources attaches the host.
    registered = _admin(
        server,
        f'registerTable(input: {{sourceId: "{_SOURCE}", domainId: "fx", schemaName: "default", '
        f'tableName: "regions", columns: [{", ".join(columns)}]}})',
    )
    _admin(server, release_field(registered_id(registered["message"])))


def _docker(*argv: str) -> str:
    return subprocess.run(["docker", *argv], capture_output=True, text=True, check=True).stdout


def _trino_network() -> str:
    """The compose network the stack's Trino is on: where the host is run, so both reach it."""
    from tests.itest_stack import ITEST_PROJECT

    (container,) = _docker(
        "ps",
        "--filter",
        f"label=com.docker.compose.project={ITEST_PROJECT}",
        "--filter",
        "label=com.docker.compose.service=trino",
        "--format",
        "{{.Names}}",
    ).split()
    networks = json.loads(
        _docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", container)
    )
    return next(iter(networks))


@pytest.fixture(scope="module")
def built(server, code_source):
    """The source's image built, run on Trino's network and connected: its address."""
    started = _api(server, "POST", "/builds", json={"arch": _ARCH, "download": True})
    assert started.status_code == 200, started.text
    build = started.json()
    deadline = time.monotonic() + 1500
    while build["status"] == "building":
        assert time.monotonic() < deadline, "the build did not finish"
        time.sleep(5)
        build = _api(server, "GET", f"/builds/{build['id']}").json()
    assert build["status"] == "done", build["error"]
    archive = _api(server, "GET", f"/builds/{build['id']}/archive")
    assert archive.status_code == 200, archive.text
    loaded = subprocess.run(
        ["docker", "load"], input=archive.content, capture_output=True, check=True
    )
    assert build["image_ref"] in loaded.stdout.decode(), loaded.stdout
    network = _trino_network()
    name = f"provisa-codehost-trino-{int(time.time())}"
    _docker("run", "-d", "--name", name, "--network", network, build["image_ref"])
    try:
        pairing = None
        deadline = time.monotonic() + 120
        while pairing is None and time.monotonic() < deadline:
            logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True).stderr
            found = re.search(r"pairing code: (\S+)", logs)
            if found and "remote host listening" in logs:
                pairing = found.group(1)
            else:
                time.sleep(1)
        assert pairing is not None, subprocess.run(
            ["docker", "logs", name], capture_output=True, text=True
        )
        # Its address on the network, which Trino's container and this (Linux) host both route to.
        address = _docker(
            "inspect",
            "--format",
            f'{{{{(index .NetworkSettings.Networks "{network}").IPAddress}}}}',
            name,
        ).strip()
        connected = _api(
            server,
            "POST",
            "/connect",
            json={"host": address, "port": 7443, "pairing_code": pairing},
        )
        assert connected.status_code == 200, connected.text
        assert connected.json()["mode"] == "built"
        yield name
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        subprocess.run(["docker", "rmi", "-f", build["image_ref"]], capture_output=True)


_JOIN = (
    "SELECT o.id, r.label, r.asked FROM orders o JOIN regions r ON r.region = o.region "
    "WHERE r._nf_tag = 'joined' AND o.region = 'eu-west' ORDER BY o.id"
)


def test_trino_attaches_the_host_and_joins_its_function_to_postgres(server, built):
    rows = _rows(_sql(server, _JOIN))
    assert rows, "no eu-west order was joined to its region"
    assert {(r["label"], r["asked"]) for r in rows} == {("EU West", "joined")}
    eu = _rows(_sql(server, "SELECT id FROM orders WHERE region = 'eu-west' ORDER BY id"))
    assert [r["id"] for r in rows] == [r["id"] for r in eu]


def test_the_join_reads_the_code_table_from_the_hosts_catalog_not_a_landed_copy(server, built):
    plan = _sql(server, f"EXPLAIN {_JOIN}")
    assert plan.status_code == 200, plan.text
    text = json.dumps(plan.json())
    assert "regions" in text
    # An attached read scans the source's own catalog; a landed one, the engine's store.
    assert "regions_src" in text.replace("-", "_"), text[:2000]


def test_a_read_of_the_code_table_alone_is_answered_from_the_host(server, built):
    rows = _rows(_sql(server, "SELECT label FROM regions WHERE region >= 'eu' ORDER BY label"))
    assert [r["label"] for r in rows] == ["EU West", "US East", "US West"]

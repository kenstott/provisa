# Copyright (c) 2026 Kenneth Stott
# Canary: b8d06c80-c06b-4db8-b1e2-9657c23dc4f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a sandboxed worker uses Provisa's services through the module always present in it
(REQ-2106): provisa_services in Python, @provisa/services in Node.

The ``test`` instance, as the code sources e2e runs it. The source declares a setting; its table's
function reads it, caches it, reads it back from the cache, writes a log line and a metric point,
and returns what it read -- through the egress proxy, with nothing on the allow-list."""

# Requirements: REQ-2106

from __future__ import annotations

import json

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import (
    _ROLE,
    _admin,
    _archive,
    _column,
    _config,
    _parameter,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_PYTHON = {
    "uses/uses.py": '''
from dataclasses import dataclass

import provisa_services as ps


@dataclass
class Used:
    name: str
    setting: str
    cached: str


@dataclass
class UsedInputs:
    name: str


def services_read(inputs: UsedInputs) -> list[Used]:
    """What the services answer, read through the module.

    @table
    """
    name = inputs.name
    region = ps.settings.get("region")
    ps.cache.set(f"region-{name}", region, ttl=60)
    cached = ps.cache.get(f"region-{name}")
    ps.log("read the region", level="info", name=name)
    ps.metric("reads", 1, source_name=name)
    return [Used(name, region, cached)]
''',
}

_NODE = {
    "uses/package.json": json.dumps({"name": "uses", "type": "module", "main": "index.js"}),
    "uses/index.js": """
import { settings, cache, log, metric } from "@provisa/services";

export async function services_read({ name }) {
  const region = await settings.get("region");
  await cache.set(`region-${name}`, region, 60);
  const cached = await cache.get(`region-${name}`);
  await log("read the region", "info", { name });
  await metric("reads", 1, "counter", { source_name: name });
  return [{ name, setting: region, cached }];
}
""",
    "uses/index.d.ts": """
export interface Used { name: string; setting: string; cached: string; }
export interface UsedInputs { name: string; }
/**
 * What the services answer, read through the module.
 * @table
 */
export declare function services_read(inputs: UsedInputs): Promise<Used[]>;
""",
}


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node"])
def server(tmp_path_factory, runtime_cache, request):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp(f"code_services_{request.param}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_services",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(runtime_cache)},
    )
    srv.language = request.param
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def uses(server):
    language = server.language
    source_id = f"uses-{language}"
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "uses.zip"},
        content=_archive(_PYTHON if language == "python" else _NODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python" if language == "python" else "node",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
        "settings": {"region": "eu-west"},
    }
    _admin(
        server,
        f'createSource(input: {{id: "{source_id}", type: "{language}", host: "", port: 0, '
        f'database: "", cacheTtl: 0, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    registered = _admin(
        server,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", '
        f'schemaName: "default", tableName: "services_read", cacheTtl: 0, '
        f"columns: [{_column('name', 'text')}, {_column('setting', 'text')}, "
        f"{_column('cached', 'text')}, {_parameter('name', 'text', required=True)}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    return source_id


def test_the_code_reads_its_setting_and_its_cache_through_the_module(server, uses):
    (row,) = _rows(_sql(server, "SELECT setting, cached FROM services_read WHERE _nf_name = 'a'"))
    assert row == {"setting": "eu-west", "cached": "eu-west"}

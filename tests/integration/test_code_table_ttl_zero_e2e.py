# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a code table with a cache_ttl of 0 -- its replica stale the moment it lands -- is answered
by the build its read waited for, never left waiting out its deadline (REQ-2100, REQ-2003)."""

from __future__ import annotations

import json

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_table_mutations_e2e import _ADMIN, _archive, _config

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_CODE = {
    "shop/shop.py": '''
from dataclasses import dataclass


@dataclass
class Order:
    id: int
    status: str
    total: float
    region: str


def orders() -> list[Order]:
    """@table"""
    return [Order(1, "open", 10.0, "eu"), Order(2, "open", 20.0, "us"), Order(3, "x", 1.0, "eu")]
''',
}


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("code_ttl_zero")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_ttl_zero",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={
            "PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps")),
        },
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": f"mutation {{ {mutation} {{ success message }} }}"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def test_a_code_table_with_a_landing_ttl_of_0_is_read_live(server):
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "shop.zip"},
        content=_archive(_CODE),
        headers={"X-Provisa-Role": _ADMIN, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        'createSource(input: {id: "shop-src", type: "python", host: "", port: 0, '
        f'database: "", mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    columns = ", ".join(
        f'{{name: "{n}", visibleTo: ["{_ADMIN}"], dataType: "{t}"}}'
        for n, t in (("id", "bigint"), ("status", "text"), ("total", "double"), ("region", "text"))
    )
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "shop-src", domainId: "shop", schemaName: "default", '
        f'tableName: "orders", cacheTtl: 0, columns: [{columns}]}})',
    )
    _admin(server, release_field(registered_id(registered["message"])))
    response = httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": "SELECT id, status FROM orders ORDER BY id"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    assert [r["id"] for r in response.json()["data"]["sql"]] == [1, 2, 3]

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
(REQ-2106): provisa_services in Python and Go, @provisa/services in Node.

The ``test`` instance, as the code sources e2e runs it. The source declares a setting; its table's
function reads it, caches it, reads it back from the cache, writes a log line, a metric point and
an audit event, and returns what it read -- through the egress proxy, with nothing on the allow-list."""

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
    ps.audit("read.region", name=name, region=region)
    return [Used(name, region, cached)]
''',
}

_NODE = {
    "uses/package.json": json.dumps({"name": "uses", "type": "module", "main": "index.js"}),
    "uses/index.js": """
import { settings, cache, log, metric, audit } from "@provisa/services";

export async function services_read({ name }) {
  const region = await settings.get("region");
  await cache.set(`region-${name}`, region, 60);
  const cached = await cache.get(`region-${name}`);
  await log("read the region", "info", { name });
  await metric("reads", 1, "counter", { source_name: name });
  await audit("read.region", { name, region });
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


_GO = {
    "uses/go.mod": "module example.com/uses\n\ngo 1.23\n",
    "uses/uses.go": """
package uses

import (
	"context"

	ps "provisa_services"
)

type Used struct {
	Name    string `json:"name"`
	Setting string `json:"setting"`
	Cached  string `json:"cached"`
}

type UsedInputs struct {
	Name string `json:"name"`
}

// What the services answer, read through the module.
//
// @table
func Services_read(ctx context.Context, in UsedInputs) ([]Used, error) {
	region, err := ps.Settings.Get(ctx, "region")
	if err != nil {
		return nil, err
	}
	if err := ps.Cache.Set(ctx, "region-"+in.Name, region, 60); err != nil {
		return nil, err
	}
	cached, err := ps.Cache.Get(ctx, "region-"+in.Name)
	if err != nil {
		return nil, err
	}
	if err := ps.Log(ctx, "read the region", "info", map[string]any{"name": in.Name}); err != nil {
		return nil, err
	}
	if err := ps.Metric(ctx, "reads", 1, "counter", map[string]any{"source_name": in.Name}); err != nil {
		return nil, err
	}
	if err := ps.Audit(ctx, "read.region", map[string]any{"name": in.Name, "region": region}); err != nil {
		return nil, err
	}
	return []Used{{in.Name, region.(string), cached.(string)}}, nil
}
""",
}

_CODE = {"python": _PYTHON, "node": _NODE, "go": _GO}
#: A table is named for its function, and a Go function that is exported starts with a capital.
_TABLE = {"python": "services_read", "node": "services_read", "go": "Services_read"}


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node", "go"])
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
        content=_archive(_CODE[language]),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": language,
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
        f'schemaName: "default", tableName: "{_TABLE[language]}", cacheTtl: 0, '
        f"columns: [{_column('name', 'text')}, {_column('setting', 'text')}, "
        f"{_column('cached', 'text')}, {_parameter('name', 'text', required=True)}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    return source_id


def test_the_code_reads_its_setting_and_its_cache_through_the_module(server, uses):
    (row,) = _rows(_sql(server, "SELECT setting, cached FROM services_read WHERE _nf_name = 'a'"))
    assert row == {"setting": "eu-west", "cached": "eu-west"}


def test_the_codes_audit_event_is_in_the_orgs_trail(server, uses):
    from sqlalchemy import text

    from provisa.codesource.service_ops import CODE_AUDIT
    from provisa.core.environments import org_schema

    _rows(_sql(server, "SELECT setting FROM services_read WHERE _nf_name = 'audited'"))
    trail = f'"{org_schema(server.org_id)}".admin_audit_log'
    with server.plane.engine().connect() as conn:
        written = conn.execute(
            text(f"SELECT actor_id, subject_id, detail FROM {trail} WHERE action = :action"),
            {"action": CODE_AUDIT},
        ).fetchall()
    (row,) = [w for w in written if w.detail["fields"].get("name") == "audited"]
    assert row.actor_id == f"source:{uses}" and row.subject_id == "read.region"
    assert row.detail["source"] == uses
    assert row.detail["fields"] == {"name": "audited", "region": "eu-west"}
    assert "trace_id" in row.detail

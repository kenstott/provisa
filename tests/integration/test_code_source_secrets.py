# Copyright (c) 2026 Kenneth Stott
# Canary: 8aea360e-fa43-4778-8b2a-7898ca1a8893
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a sandboxed worker reads its source's declared secret through Provisa's services
(REQ-2104).

The ``test`` instance: an isolated server on DuckDB with its own control plane, as the code
sources e2e runs it. For each language the code is an uploaded archive whose ``secret_read`` table
GETs ``$PROVISA_SERVICES/secrets/<name>`` through the egress proxy, with nothing on the egress
allow-list, and returns what came back. The source declares ``openrouter``; the org secret is
stored through the admin API. A declared secret is read, a rotated one is read anew on the next
call, and an undeclared one is refused by name."""

# Requirements: REQ-2104

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
    "reads/reads.py": '''
import os
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass
class Read:
    name: str
    status: int
    body: str


@dataclass
class ReadInputs:
    name: str


def secret_read(inputs: ReadInputs) -> list[Read]:
    """What the services answer for one secret.

    @table
    """
    name = inputs.name
    url = f"{os.environ['PROVISA_SERVICES']}/secrets/{name}"
    try:
        with urllib.request.urlopen(url, timeout=30) as answered:
            return [Read(name, answered.status, answered.read().decode())]
    except urllib.error.HTTPError as refused:
        return [Read(name, refused.code, refused.read().decode())]
''',
}

_NODE = {
    "reads/package.json": json.dumps({"name": "reads", "type": "module", "main": "index.js"}),
    "reads/index.js": """
export async function secret_read({ name }) {
  const answered = await fetch(`${process.env.PROVISA_SERVICES}/secrets/${name}`);
  return [{ name, status: answered.status, body: await answered.text() }];
}
""",
    "reads/index.d.ts": """
export interface Read { name: string; status: number; body: string; }
export interface ReadInputs { name: string; }
/**
 * What the services answer for one secret.
 * @table
 */
export declare function secret_read(inputs: ReadInputs): Promise<Read[]>;
""",
}


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node"])
def server(tmp_path_factory, runtime_cache, request):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp(f"code_secrets_{request.param}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_secrets",
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


def _store_secret(srv, name: str, value: str) -> None:
    stored = httpx.put(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/secrets/{name}",
        json={"value": value},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 30,
    )
    assert stored.status_code == 200, stored.text


@pytest.fixture(scope="module")
def reads(server):
    """The server's code source, declaring ``openrouter``, its table registered and released."""
    language = server.language
    source_id = f"reads-{language}"
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "reads.zip"},
        content=_archive(_PYTHON if language == "python" else _NODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python" if language == "python" else "node",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
        "secrets": ["openrouter"],
    }
    _admin(
        server,
        f'createSource(input: {{id: "{source_id}", type: "{language}", host: "", port: 0, '
        f'database: "", cacheTtl: 0, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    status_type = "bigint" if language == "python" else "double"
    registered = _admin(
        server,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", '
        f'schemaName: "default", tableName: "secret_read", cacheTtl: 0, '
        # A Python int is a bigint; a JavaScript number is a double.
        f"columns: [{_column('name', 'text')}, {_column('status', status_type)}, "
        f"{_column('body', 'text')}, {_parameter('name', 'text', required=True)}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    return source_id


def _read(srv, name: str) -> dict:
    (row,) = _rows(_sql(srv, f"SELECT status, body FROM secret_read WHERE _nf_name = '{name}'"))
    return row


def test_a_declared_secret_is_read_at_use_and_a_rotation_on_the_next_read(server, reads):
    _store_secret(server, "openrouter", "sk-integration-first")
    assert _read(server, "openrouter") == {"status": 200, "body": "sk-integration-first"}
    _store_secret(server, "openrouter", "sk-integration-rotated")
    assert _read(server, "openrouter") == {"status": 200, "body": "sk-integration-rotated"}


def test_an_undeclared_secret_is_refused_by_name(server, reads):
    _store_secret(server, "stripe", "sk-integration-undeclared")
    row = _read(server, "stripe")
    assert row["status"] == 403
    assert "'stripe'" in row["body"] and "sk-integration-undeclared" not in row["body"]

# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2f6d13-7a49-4c5e-b0d8-1e93c6a4f725
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a reader's rules on a view applying a column transform (REQ-2131, amended 2026-11-16).

The ``test`` instance: an isolated server on DuckDB with its own control plane, a Python code
source with a ``suppliers`` table and ``clean_address``, tagged @columns, which asks an
OpenAI-compatible endpoint served by this test once for each batch it is called with -- so the
endpoint counts the transform's calls. A materialized view ``clean_suppliers`` applies it.

The view is built once and stored. Each reader reads it through SQL over HTTP, pgwire and GraphQL:
east_reader's row rule on raw_address (no column of the view) filters the stored rows;
masked_reader sees raw_address masked, so street, city and country (computed from it) are masked
and id is not; id_masked sees id masked and the transform's outputs in clear; org_admin, with no
rule, reads the stored rows as they are. No read calls the transform."""

# Requirements: REQ-2131

from __future__ import annotations

import json
import time

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_source_llm import _MODEL, _Endpoint
from tests.integration.test_code_sources_e2e import _archive

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("view")]

psycopg2 = pytest.importorskip("psycopg2")

_ADMIN = "org_admin"
_EAST = "east_reader"
_MASKED = "masked_reader"
_ID_MASKED = "id_masked"
_READERS = (_ADMIN, _EAST, _MASKED, _ID_MASKED)
_ROWS = 70

_CODE = {
    "suppliers/suppliers.py": '''
import json
import os
import urllib.request
from dataclasses import dataclass


@dataclass
class Supplier:
    id: int
    raw_address: str


@dataclass
class Address:
    street: str
    city: str
    country: str


def suppliers() -> list[Supplier]:
    """@table"""
    return [Supplier(i, f"{i} Main St, springfield {i % 7}, us") for i in range(70)]


def clean_address(raw_address: list[str]) -> list[Address]:
    """Each raw address of the batch, as its parts.

    @columns
    """
    request = urllib.request.Request(
        f"{os.environ['PROVISA_SERVICES']}/llm/v1/chat/completions",
        data=json.dumps(
            {"model": "provisa:sql_generation", "messages": [{"role": "user", "content": "clean"}]}
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as answered:
        answered.read()
    out = []
    for raw in raw_address:
        street, city, country = (part.strip() for part in raw.split(","))
        out.append(Address(street, city.title(), country.upper()))
    return out
''',
}


def _config(endpoint_url: str) -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": r} for r in _READERS],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "fx", "description": "transform view reader rules e2e"}],
        "roles": [
            {
                "id": role,
                "description": f"The {role}.",
                "capabilities": ["query_development"],
                "domain_access": ["*"],
            }
            for role in (_EAST, _MASKED, _ID_MASKED)
        ],
        "sources": [],
        "tables": [],
        "ai_endpoints": [{"id": "test-endpoint", "style": "openai", "base_url": endpoint_url}],
        "ai_models": {"sql_generation": {"vendor": "test-endpoint", "model": _MODEL}},
    }


@pytest.fixture(scope="module")
def endpoint():
    served = _Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def server(tmp_path_factory, endpoint):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("transform_view_reader_rules")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config(endpoint.url)))
    srv = IsolatedServer(
        "transform_view_rules",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        enable_pgwire=True,
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path_factory.mktemp("runtime-deps"))},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _column(name: str, data_type: str, masks: str = "") -> str:
    readers = ", ".join(f'"{r}"' for r in _READERS)
    masked = f", masks: [{masks}]" if masks else ""
    return f'{{name: "{name}", visibleTo: [{readers}], dataType: "{data_type}"{masked}}}'


@pytest.fixture(scope="module")
def view(server):
    """The code source, its suppliers table (raw_address masked for masked_reader, id for
    id_masked, east_reader's row rule on raw_address), clean_address registered, and the view
    applying it built."""
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "suppliers.zip"},
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
        'createSource(input: {id: "suppliers-src", type: "python", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    # A column with a mask list masks every reader holding none of its entries (REQ-2059): the
    # last entry, with no "to" and no mask, leaves every other reader unmasked.
    id_mask = f'{{to: ["{_ID_MASKED}"], mask: {{type: "constant", value: 0}}}}, {{mask: null}}'
    address_mask = (
        f'{{to: ["{_MASKED}"], mask: {{type: "constant", value: "***"}}}}, {{mask: null}}'
    )
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "suppliers-src", domainId: "fx", schemaName: "default", '
        f'tableName: "suppliers", cacheTtl: 300, columns: [{_column("id", "bigint", id_mask)}, '
        f"{_column('raw_address', 'text', address_mask)}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    _admin(
        server,
        f'upsertRlsRule(input: {{tableId: "suppliers", roleId: "{_EAST}", '
        "filterExpr: \"raw_address LIKE '%springfield 3,%'\", "
        'description: "East sees the springfield 3 suppliers."})',
    )
    response = httpx.post(
        f"{server.base_url}/admin/actions/functions",
        json={
            "name": "clean_address",
            "sourceId": "suppliers-src",
            "functionName": "clean_address",
            "domainId": "fx",
            "visibleTo": list(_READERS),
        },
        headers={"X-Provisa-Role": _ADMIN},
        timeout=server.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    columns = ", ".join(
        _column(n, t)
        for n, t in (("id", "bigint"), ("street", "text"), ("city", "text"), ("country", "text"))
    )
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "__derived__", domainId: "fx", schemaName: "views", '
        'tableName: "clean_suppliers", alias: "clean_suppliers", '
        f"viewSql: {json.dumps('SELECT id, clean_address(raw_address) FROM suppliers')}, "
        f"materialize: true, mvRefreshInterval: 3600, columns: [{columns}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    response = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": 'mutation { refreshMv(mvId: "view-clean_suppliers") { success message } }'},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=server.request_timeout + 600,
    )
    (refreshed,) = response.json()["data"].values()
    assert refreshed["success"], f"{refreshed}\n{server.dump_stderr_debug()[-12000:]}"
    deadline = time.monotonic() + 300
    while True:
        rows = _http(server, _ADMIN)
        if rows is not None and len(rows) == _ROWS:
            return
        assert time.monotonic() < deadline, server.dump_stderr_debug()[-6000:]
        time.sleep(2)


_SQL = "SELECT id, street, city, country FROM clean_suppliers ORDER BY id"


def _http(srv, role: str) -> list[dict] | None:
    response = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": _SQL},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )
    if response.status_code != 200:
        return None
    return response.json()["data"]["sql"]


def _sql_over_http(srv, role: str) -> list[dict]:
    rows = _http(srv, role)
    assert rows is not None, f"{role}: {srv.dump_stderr_debug()[-4000:]}"
    return rows


def _pgwire(srv, role: str) -> list[dict]:
    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=role, password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute(_SQL)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _graphql(srv, role: str) -> list[dict]:
    response = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": "{ clean_suppliers(order_by: {id: asc}) { id street city country } }"},
        headers={"X-Provisa-Role": role},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert not body.get("errors"), body
    return body["data"]["clean_suppliers"]


_DOORS = {"sql_http": _sql_over_http, "pgwire": _pgwire, "graphql": _graphql}


def _parts(row: dict) -> tuple:
    return (row["street"], row["city"], row["country"])


def _clear(i: int) -> tuple:
    return (f"{i} Main St", f"Springfield {i % 7}", "US")


@pytest.mark.parametrize("door", list(_DOORS))
def test_a_reader_with_no_rule_reads_the_stored_rows_as_they_are(server, endpoint, door):
    before = len(endpoint.received)
    rows = _DOORS[door](server, _ADMIN)
    assert [int(r["id"]) for r in rows] == list(range(_ROWS))
    assert [_parts(r) for r in rows] == [_clear(i) for i in range(_ROWS)]
    assert len(endpoint.received) == before  # no transform is called on a read


@pytest.mark.parametrize("door", list(_DOORS))
def test_a_row_rule_on_an_input_that_is_no_view_column_filters_the_stored_rows(
    server, endpoint, door
):
    before = len(endpoint.received)
    rows = _DOORS[door](server, _EAST)
    expected = [i for i in range(_ROWS) if i % 7 == 3]
    assert [int(r["id"]) for r in rows] == expected
    assert [_parts(r) for r in rows] == [_clear(i) for i in expected]
    assert len(endpoint.received) == before


@pytest.mark.parametrize("door", list(_DOORS))
def test_a_masked_input_masks_the_outputs_computed_from_it_and_not_the_others(
    server, endpoint, door
):
    before = len(endpoint.received)
    rows = _DOORS[door](server, _MASKED)
    assert [int(r["id"]) for r in rows] == list(range(_ROWS))
    assert {_parts(r) for r in rows} == {("***", "***", "***")}
    assert len(endpoint.received) == before


@pytest.mark.parametrize("door", list(_DOORS))
def test_a_masked_plain_input_masks_its_own_output_only(server, endpoint, door):
    before = len(endpoint.received)
    rows = _DOORS[door](server, _ID_MASKED)
    assert {int(r["id"]) for r in rows} == {0}
    assert sorted(_parts(r) for r in rows) == sorted(_clear(i) for i in range(_ROWS))
    assert len(endpoint.received) == before

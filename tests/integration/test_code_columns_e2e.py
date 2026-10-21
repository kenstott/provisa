# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a code source's ``@columns`` function, a batch column transform (REQ-2131).

The ``test`` instance: an isolated server on DuckDB with its own control plane, its AI Models
pointing the sql_generation role at an OpenAI-compatible endpoint served by this test. For each
language the code is an uploaded archive with a ``suppliers`` table of 10,000 rows and
``clean_address``, tagged @columns, taking raw_address and returning street, city and country: it
asks the LLM service once for each batch and splits each address. The source runs two workers.

clean_address is registered as a command from what it declares, and applied to the supplier
rows in a statement and in a materialized view: it is called in batches, one LLM call a batch,
the batches across the source's workers, and each row's street, city and country are the values
returned for it. short_address answers one result too few for each batch, and a refresh of a view
applying it is refused naming it."""

# Requirements: REQ-2131

from __future__ import annotations

import json
import time

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_source_llm import _MODEL, _Endpoint
from tests.integration.test_code_sources_e2e import (
    _ROLE,
    _admin,
    _archive,
    _column,
    _config,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_ROWS = 10_000
_BATCH = 1000  # codesource.columns.BATCH

_PYTHON = {
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
    return [Supplier(i, f"{i} Main St, springfield {i % 7}, us") for i in range(10000)]


def _ask(batch: list[str]) -> None:
    request = urllib.request.Request(
        f"{os.environ['PROVISA_SERVICES']}/llm/v1/chat/completions",
        data=json.dumps(
            {
                "model": "provisa:sql_generation",
                "messages": [{"role": "user", "content": "clean: " + "; ".join(batch[:3])}],
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as answered:
        answered.read()


def clean_address(raw_address: list[str]) -> list[Address]:
    """Each raw address of the batch, as its parts.

    @columns
    """
    _ask(raw_address)
    out = []
    for raw in raw_address:
        street, city, country = (part.strip() for part in raw.split(","))
        out.append(Address(street, city.title(), country.upper()))
    return out


def short_address(raw_address: list[str]) -> list[Address]:
    """One result too few for each batch.

    @columns
    """
    return clean_address(raw_address)[:-1]
''',
}

_NODE = {
    "suppliers/package.json": json.dumps(
        {"name": "suppliers", "type": "module", "main": "index.js"}
    ),
    "suppliers/index.js": """
export function suppliers() {
  return Array.from({ length: 10000 }, (_, i) => (
    { id: i, raw_address: `${i} Main St, springfield ${i % 7}, us` }
  ));
}
async function ask(batch) {
  const answered = await fetch(`${process.env.PROVISA_SERVICES}/llm/v1/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "provisa:sql_generation",
      messages: [{ role: "user", content: "clean: " + batch.slice(0, 3).join("; ") }],
    }),
  });
  await answered.text();
}
const title = (s) => s.replace(/\\b\\w/g, (c) => c.toUpperCase());
export async function clean_address(raw_address) {
  await ask(raw_address);
  return raw_address.map((raw) => {
    const [street, city, country] = raw.split(",").map((p) => p.trim());
    return { street, city: title(city), country: country.toUpperCase() };
  });
}
export async function short_address(raw_address) {
  return (await clean_address(raw_address)).slice(0, -1);
}
""",
    "suppliers/index.d.ts": """
export interface Supplier { id: number; raw_address: string; }
export interface Address { street: string; city: string; country: string; }
/** @table */
export declare function suppliers(): Supplier[];
/**
 * Each raw address of the batch, as its parts.
 * @columns
 */
export declare function clean_address(raw_address: string[]): Promise<Address[]>;
/**
 * One result too few for each batch.
 * @columns
 */
export declare function short_address(raw_address: string[]): Promise<Address[]>;
""",
}


@pytest.fixture(scope="module")
def endpoint():
    served = _Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node"])
def server(tmp_path_factory, runtime_cache, endpoint, request):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp(f"code_columns_{request.param}")
    config = {
        **_config(),
        "ai_endpoints": [{"id": "test-endpoint", "style": "openai", "base_url": endpoint.url}],
        "ai_models": {"sql_generation": {"vendor": "test-endpoint", "model": _MODEL}},
    }
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "code_columns",
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


def _command(srv, source_id: str, function: str) -> None:
    """``function`` registered as a command, from what it declares (the form sends no columns)."""
    response = httpx.post(
        f"{srv.base_url}/admin/actions/functions",
        json={
            "name": function,
            "sourceId": source_id,
            "functionName": function,
            "domainId": "fx",
            "visibleTo": [_ROLE],
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert response.status_code == 200, response.text


def _view(srv, name: str, sql: str) -> None:
    registered = _admin(
        srv,
        f'registerTable(input: {{sourceId: "__derived__", domainId: "fx", schemaName: "views", '
        f'tableName: "{name}", alias: "{name}", viewSql: {json.dumps(sql)}, materialize: true, '
        f"mvRefreshInterval: 3600, columns: [{_column('id', 'bigint')}, "
        f"{_column('street', 'text')}, {_column('city', 'text')}, {_column('country', 'text')}]}})",
    )
    _admin(srv, release_field(registered_id(registered["message"])))


@pytest.fixture(scope="module")
def code_source(server):
    """The server's code source with two workers, its suppliers table, both column transforms
    registered, and a materialized view of each over the suppliers."""
    language = server.language
    source_id = f"suppliers-{language}"
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "suppliers.zip"},
        content=_archive(_PYTHON if language == "python" else _NODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python" if language == "python" else "node",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
        "workers": 2,
    }
    _admin(
        server,
        f'createSource(input: {{id: "{source_id}", type: "{language}", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    id_type = "bigint" if language == "python" else "double"
    registered = _admin(
        server,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", '
        f'schemaName: "default", tableName: "suppliers", cacheTtl: 300, '
        f"columns: [{_column('id', id_type)}, {_column('raw_address', 'text')}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    for function in ("clean_address", "short_address"):
        _command(server, source_id, function)
    _view(server, "clean_suppliers", "SELECT id, clean_address(raw_address) FROM suppliers")
    _view(server, "short_suppliers", "SELECT id, short_address(raw_address) FROM suppliers")
    return language


def _expected(i: int) -> dict:
    return {"street": f"{i} Main St", "city": f"Springfield {i % 7}", "country": "US"}


def test_each_row_holds_the_values_returned_for_it_one_llm_call_a_batch(
    server, code_source, endpoint
):
    before = len(endpoint.received)
    rows = _rows(_sql(server, "SELECT id, clean_address(raw_address) FROM suppliers ORDER BY id"))
    assert len(rows) == _ROWS
    for row in (rows[0], rows[4321], rows[-1]):
        i = int(row["id"])
        assert {k: row[k] for k in ("street", "city", "country")} == _expected(i)
    # Called in batches: one model call for each, metered against the source.
    assert len(endpoint.received) - before == _ROWS // _BATCH


def test_a_materialized_view_recomputes_them_when_it_is_refreshed(server, code_source):
    refreshed = _admin(server, 'refreshMv(mvId: "view-clean_suppliers")')
    assert refreshed["success"], refreshed
    deadline = time.monotonic() + 300
    while True:
        response = _sql(server, "SELECT id, street, city, country FROM clean_suppliers ORDER BY id")
        if response.status_code == 200 and len(response.json()["data"]["sql"]) == _ROWS:
            break
        assert time.monotonic() < deadline, f"{response.text}\n{server.dump_stderr_debug()[-6000:]}"
        time.sleep(2)
    rows = response.json()["data"]["sql"]
    assert {k: rows[17][k] for k in ("street", "city", "country")} == _expected(17)


def test_a_function_that_returns_fewer_results_than_rows_is_refused_naming_it(server, code_source):
    refused = _sql(server, "SELECT id, short_address(raw_address) FROM suppliers")
    assert refused.status_code != 200
    assert "short_address" in refused.text and "returned 999 results" in refused.text
    # The refresh of a view applying it is refused, naming it: refreshMv answers the build's error.
    response = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": 'mutation { refreshMv(mvId: "view-short_suppliers") { success message } }'},
        headers={"X-Provisa-Role": _ROLE},
        timeout=server.request_timeout + 300,
    )
    (result,) = response.json()["data"].values()
    assert not result["success"] and "short_address" in result["message"], result


def test_a_plain_view_applying_it_is_landed_and_a_filter_on_an_output_answers(server, code_source):
    """A view applying a column transform is landed though not declared materialized: built through
    the materialized views' build, read from its stored table, so a statement filtering on an
    output column answers from the values the function returned."""
    registered = _admin(
        server,
        'registerTable(input: {sourceId: "__derived__", domainId: "fx", schemaName: "views", '
        'tableName: "plain_suppliers", alias: "plain_suppliers", '
        f"viewSql: {json.dumps('SELECT id, clean_address(raw_address) FROM suppliers')}, "
        f"columns: [{_column('id', 'bigint')}, {_column('street', 'text')}, "
        f"{_column('city', 'text')}, {_column('country', 'text')}]}})",
    )
    _admin(server, release_field(registered_id(registered["message"])))
    views = httpx.post(
        f"{server.base_url}/admin/graphql",
        json={"query": "{ mvList { id } }"},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    ).json()["data"]["mvList"]
    assert "view-plain_suppliers" in [v["id"] for v in views]  # landed, as a materialized view is
    deadline = time.monotonic() + 300
    while True:
        response = _sql(
            server,
            "SELECT id, street, city FROM plain_suppliers WHERE city = 'Springfield 3' ORDER BY id",
        )
        if response.status_code == 200 and response.json()["data"]["sql"]:
            break
        assert time.monotonic() < deadline, f"{response.text}\n{server.dump_stderr_debug()[-6000:]}"
        time.sleep(2)
    rows = response.json()["data"]["sql"]
    assert [int(r["id"]) for r in rows] == [i for i in range(_ROWS) if i % 7 == 3]
    assert {k: rows[0][k] for k in ("street", "city")} == {
        "street": "3 Main St",
        "city": "Springfield 3",
    }

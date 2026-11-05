# Copyright (c) 2026 Kenneth Stott
# Canary: 2d312582-ac4e-4b3c-96af-d2f160822f3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a Node source and a Python source, registered as a steward registers them and queried
(REQ-1984, REQ-1993; part 5).

Everything here is the ``test`` instance: an isolated server on DuckDB with its own control
plane, and its own runtime cache (the pinned Node, CPython and uv are fetched into it once for the
module). For each language the code is an uploaded archive with no SDK import: a ``rates`` table
whose ``currency`` argument the function requires and whose ``day`` it defaults, and a
``currencies`` table with no argument. The source is created and its table registered through the
admin API, the code is installed and read when the source is saved, and the table is read over
SQL: each argument set is its own answer, a missing required argument is refused before the code
is run, and an argument left out takes the function's own default."""

# Requirements: REQ-1984, REQ-1993, REQ-1986, REQ-1980

from __future__ import annotations

import io
import json
import zipfile

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"

_PYTHON = {
    "rates/rates.py": '''
from dataclasses import dataclass
from datetime import date

_TABLE = {"USD": 1.0, "EUR": 0.9, "GBP": 0.8}


@dataclass
class Rate:
    code: str
    rate: float
    day: date


@dataclass
class RateInputs:
    currency: str
    day: date = date(2026, 1, 2)


def rates(inputs: RateInputs) -> list[Rate]:
    """Exchange rates against one currency.

    @table
    """
    base = _TABLE[inputs.currency]
    return [
        Rate(code, round(value / base, 4), inputs.day) for code, value in sorted(_TABLE.items())
    ]


def currencies() -> list[dict]:
    """@table"""
    return [{"code": code} for code in sorted(_TABLE)]
''',
}

_NODE = {
    "rates/package.json": json.dumps({"name": "rates", "type": "module", "main": "index.js"}),
    "rates/index.js": """
const TABLE = { EUR: 0.9, GBP: 0.8, USD: 1.0 };
export async function rates({ currency, day = new Date("2026-01-02T00:00:00Z") }) {
  const base = TABLE[currency];
  return Object.keys(TABLE).sort().map((code) => (
    { code, rate: Math.round((TABLE[code] / base) * 10000) / 10000, day }
  ));
}
export function currencies() {
  return Object.keys(TABLE).sort().map((code) => ({ code }));
}
""",
    "rates/index.d.ts": """
export interface Rate { code: string; rate: number; day: Date; }
export interface RateInputs { currency: string; day?: Date; }
/**
 * Exchange rates against one currency.
 * @table
 */
export declare function rates(inputs: RateInputs): Promise<Rate[]>;
/** @table */
export declare function currencies(): { code: string }[];
""",
}


def _archive(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return buf.getvalue()


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "fx", "description": "code sources e2e"}],
        "roles": [],
        "sources": [],
        "tables": [],
    }


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    """The pinned runtimes, fetched once for the module and shared by both servers."""
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node"])
def server(tmp_path_factory, runtime_cache, request):
    """An isolated server per language, each with one code source: the two register tables of
    the same names."""
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp(f"code_sources_{request.param}")
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(_config()))
    srv = IsolatedServer(
        "code_sources",
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


def _admin(srv, mutation: str) -> dict:
    response = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": "mutation { " + mutation + " { success message } }"},
        headers={"X-Provisa-Role": _ROLE},
        # Saving a code source fetches its runtime and installs and reads its code.
        timeout=srv.request_timeout + 600,
    )
    assert response.status_code == 200, response.text
    assert "errors" not in response.json(), response.text
    (result,) = response.json()["data"].values()
    assert result["success"], result["message"]
    return result


def _sql(srv, sql: str) -> httpx.Response:
    return httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )


def _rows(response: httpx.Response) -> list[dict]:
    assert response.status_code == 200, response.text
    return response.json()["data"]["sql"]


def _column(name: str, data_type: str) -> str:
    return f'{{name: "{name}", visibleTo: ["{_ROLE}"], dataType: "{data_type}"}}'


def _parameter(name: str, data_type: str, *, required: bool) -> str:
    return (
        f'{{name: "_nf_{name}", visibleTo: [], dataType: "{data_type}", '
        f'nativeFilterType: "query_arg", nativeFilterRequired: {str(required).lower()}}}'
    )


@pytest.fixture(scope="module")
def code_source(server):
    """The server's code source, created and its tables registered and released."""
    language = server.language
    source_id = f"fx-{language}"
    files = _PYTHON if language == "python" else _NODE
    upload = httpx.post(
        f"{server.base_url}/admin/orgs/{server.org_id}/code-uploads/archive",
        params={"filename": "rates.zip"},
        content=_archive(files),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=server.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python" if language == "python" else "node",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
    }
    _admin(
        server,
        f'createSource(input: {{id: "{source_id}", type: "{language}", host: "", port: 0, '
        f'database: "", cacheTtl: 300, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    for table, columns in (
        (
            "rates",
            [
                _column("code", "text"),
                _column("rate", "double"),
                _column("day", "date" if language == "python" else "timestamp"),
                _parameter("currency", "text", required=True),
                _parameter("day", "date" if language == "python" else "timestamp", required=False),
            ],
        ),
        ("currencies", [_column("code", "text")]),
    ):
        registered = _admin(
            server,
            f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", '
            f'schemaName: "default", tableName: "{table}", cacheTtl: 300, '
            f"columns: [{', '.join(columns)}]}})",
        )
        _admin(server, release_field(registered_id(registered["message"])))
    return language


def test_a_function_is_read_as_a_table_under_its_arguments(server, code_source):
    usd = _rows(
        _sql(server, "SELECT code, rate FROM rates WHERE _nf_currency = 'USD' ORDER BY code")
    )
    assert usd == [
        {"code": "EUR", "rate": 0.9},
        {"code": "GBP", "rate": 0.8},
        {"code": "USD", "rate": 1.0},
    ]
    # Another argument set is another answer, never the first one's landed rows.
    eur = _rows(
        _sql(server, "SELECT code, rate FROM rates WHERE _nf_currency = 'EUR' ORDER BY code")
    )
    assert [r["rate"] for r in eur] == [1.0, 0.8889, 1.1111]


def test_an_argument_left_out_takes_the_functions_own_default(server, code_source):
    rows = _rows(_sql(server, "SELECT DISTINCT day FROM rates WHERE _nf_currency = 'USD'"))
    assert [str(r["day"])[:10] for r in rows] == ["2026-01-02"]


def test_a_missing_required_argument_is_refused_before_the_code_runs(server, code_source):
    refused = _sql(server, "SELECT code FROM rates")
    assert refused.status_code != 200
    assert "currency" in refused.text


def test_a_function_with_no_arguments_reads_as_a_table(server, code_source):
    rows = _rows(_sql(server, "SELECT code FROM currencies ORDER BY code"))
    assert [r["code"] for r in rows] == ["EUR", "GBP", "USD"]

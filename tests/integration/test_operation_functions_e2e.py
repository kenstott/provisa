# Copyright (c) 2026 Kenneth Stott
# Canary: 5d6510ee-a93d-4598-92c3-076406fe1f9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a source operation that requires a parameter is a function, end to end (REQ-1986).

A real server (DuckDB engine, its own control plane, pgwire on) over an HTTP API the test serves,
which records every path it is asked. ``get_pet`` is the function of the operation ``getPetById``;
``get_pet_by_id`` is its parameterized table; ``list_pets`` is the plain list.

Called in FROM, the function is its table with the arguments assigned: the same rows and the same
calls to the source as the table form, the same refusals, and the same governed result for a role
with a row rule on the table. pg_proc and information_schema, read through a PostgreSQL driver,
list it with its argument names and types and the rows it returns.
"""

# Requirements: REQ-1986

from __future__ import annotations

import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import psycopg2
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ADMIN = "org_admin"
#: Reads only available pets of get_pet_by_id (a row rule on the table).
_READER = "pet_reader"
_ROLES = [_ADMIN, _READER]
#: Six pets; the odd ones are sold.
_PETS = [
    {"id": i, "name": f"pet {i}", "status": "sold" if i % 2 else "available"} for i in range(1, 7)
]
_PET = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "status": {"type": "string"},
    },
}
_OK = {"description": "ok"}
_SPEC = {
    "openapi": "3.0.0",
    "info": {"title": "Pets", "version": "1.0.0"},
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "responses": {
                    "200": {
                        **_OK,
                        "content": {
                            "application/json": {"schema": {"type": "array", "items": _PET}}
                        },
                    }
                },
            }
        },
        "/pets/{petId}": {
            "get": {
                "operationId": "getPetById",
                "parameters": [
                    {"name": "petId", "in": "path", "required": True, "schema": {"type": "integer"}}
                ],
                "responses": {"200": {**_OK, "content": {"application/json": {"schema": _PET}}}},
            }
        },
    },
}


class _Api:
    """The remote API: every by-id path it is asked. A pet asked by id answers with the name
    ``detail of pet <id>``, so a row is known to come from the by-id call."""

    def __init__(self) -> None:
        asked: list[str] = []

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's handler name
                asked.append(self.path)
                if self.path == "/pets":
                    body = json.dumps(_PETS).encode()
                elif self.path.startswith("/pets/") and self.path[6:].isdigit():
                    pet = dict(_PETS[int(self.path[6:]) - 1])
                    pet["name"] = f"detail of {pet['name']}"
                    body = json.dumps(pet).encode()
                else:
                    self.send_response(400)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):  # noqa: A002 - http.server's name
                pass

        self.asked = asked
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def by_id(self) -> list[str]:
        """The by-id calls received so far, sorted; and forgotten."""
        calls = sorted(p for p in self.asked if p != "/pets")
        self.asked.clear()
        return calls

    def close(self) -> None:
        self._server.shutdown()


def _config(spec_path: str, base_url: str) -> dict:
    visible = {"visible_to": _ROLES}
    columns = [
        {"name": "id", "data_type": "integer", **visible},
        {"name": "name", "data_type": "varchar", **visible},
        {"name": "status", "data_type": "varchar", **visible},
    ]
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": role} for role in _ROLES],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "pets", "description": "Pets"}],
        "roles": [
            {
                "id": _READER,
                "description": "Test role.",
                "capabilities": ["query_development", "command_execution", "full_results"],
                "domain_access": ["pets"],
            }
        ],
        "sources": [
            {
                "id": "api",
                "type": "openapi",
                "path": spec_path,
                "base_url": base_url,
                "cache_ttl": 3600,
            }
        ],
        "tables": [
            {
                "id": "list_pets",
                "source_id": "api",
                "domain_id": "pets",
                "schema": "default",
                "table": "list_pets",
                "columns": columns,
            },
            {
                "id": "get_pet_by_id",
                "source_id": "api",
                "domain_id": "pets",
                "schema": "default",
                "table": "get_pet_by_id",
                "columns": [
                    *columns,
                    {
                        "name": "_nf_petId",
                        "data_type": "integer",
                        "native_filter_type": "path_param",
                        "native_filter_required": True,
                        **visible,
                    },
                ],
            },
        ],
        # A join on the parameter is governed as any join: by the relationship that admits it.
        "relationships": [
            {
                "id": "list-pets-to-get-pet-by-id",
                "alias": "DETAIL",
                "cardinality": "many-to-one",
                "source_table_id": "list_pets",
                "source_column": "id",
                "target_table_id": "get_pet_by_id",
                "target_column": "_nf_petId",
            }
        ],
        "rls_rules": [
            {
                "description": "Test rule.",
                "table_id": "get_pet_by_id",
                "role_id": _READER,
                "filter": "status = 'available'",
            }
        ],
        "functions": [
            {
                "name": "get_pet",
                "description": "A pet by its id",
                "source_id": "api",
                "schema_name": "openapi",
                "function_name": "getPetById",
                "impl_kind": "source_operation",
                "kind": "query",
                "returns": "default.get_pet_by_id",
                "arguments": [{"name": "petId", "type": "integer"}],
                "domain_id": "pets",
                **visible,
            }
        ],
        "webhooks": [],
    }


@pytest.fixture(scope="module")
def served():
    from tests.integration.isolated_server import IsolatedServer

    api = _Api()
    workdir = tempfile.TemporaryDirectory()
    work = Path(workdir.name)
    (work / "spec.json").write_text(json.dumps(_SPEC))
    (work / "config.yaml").write_text(yaml.safe_dump(_config(str(work / "spec.json"), api.url)))
    server = IsolatedServer(
        "operation_functions_duckdb",
        engine="duckdb",
        enable_pgwire=True,
        config=str(work / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'materialize.duckdb'}",
    )
    server.start()
    try:
        yield server, api
    finally:
        server.stop_process()
        workdir.cleanup()
        api.close()


def _sql(server, sql: str, role: str = _ADMIN) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": role},
        timeout=300,
    )


def _rows(server, sql: str, role: str = _ADMIN) -> list[tuple]:
    response = _sql(server, sql, role)
    assert response.status_code == 200, response.text
    return sorted(tuple(row.values()) for row in response.json()["data"]["sql"])


_TABLE_FORM = 'SELECT id, name FROM pets.get_pet_by_id WHERE "_nf_petId" = {}'
_FUNCTION_FORM = "SELECT id, name FROM get_pet({})"


@pytest.mark.parametrize("pet", [2, 3])
def test_the_function_form_answers_the_table_forms_rows_with_the_same_call(served, pet):
    server, api = served
    api.by_id()
    as_table = _rows(server, _TABLE_FORM.format(pet))
    assert api.by_id() == [f"/pets/{pet}"]
    as_function = _rows(server, _FUNCTION_FORM.format(pet))
    assert api.by_id() == [f"/pets/{pet}"]
    assert as_function == as_table == [(pet, f"detail of pet {pet}")]


def test_an_argument_given_by_name_is_assigned_as_by_position(served):
    server, api = served
    api.by_id()
    assert _rows(server, 'SELECT id FROM get_pet("petId" => 4)') == [(4,)]
    assert api.by_id() == ["/pets/4"]


def test_an_argument_that_is_another_tables_column_calls_for_each_row_kept(served):
    server, api = served
    api.by_id()
    as_function = _rows(
        server,
        "SELECT l.id, g.name FROM pets.list_pets l, get_pet(l.id) g WHERE l.status = 'sold'",
    )
    assert api.by_id() == ["/pets/1", "/pets/3", "/pets/5"]
    as_table = _rows(
        server,
        "SELECT l.id, g.name FROM pets.list_pets l JOIN pets.get_pet_by_id g "
        "ON g.\"_nf_petId\" = l.id WHERE l.status = 'sold'",
    )
    assert api.by_id() == ["/pets/1", "/pets/3", "/pets/5"]
    assert as_function == as_table


@pytest.mark.parametrize(
    "function_form, table_form",
    [
        ("SELECT id FROM get_pet()", "SELECT id FROM pets.get_pet_by_id"),
        (
            "SELECT id FROM get_pet(2 + 3)",
            'SELECT id FROM pets.get_pet_by_id WHERE "_nf_petId" = 2 + 3',
        ),
        (
            "SELECT id FROM get_pet('two')",
            "SELECT id FROM pets.get_pet_by_id WHERE \"_nf_petId\" = 'two'",
        ),
    ],
)
def test_an_argument_that_assigns_nothing_is_the_table_forms_refusal(
    served, function_form, table_form
):
    server, api = served
    api.by_id()
    as_function = _sql(server, function_form)
    as_table = _sql(server, table_form)
    assert as_function.status_code == as_table.status_code, (as_function.text, as_table.text)
    assert as_function.status_code >= 400, as_function.text
    assert "code" in as_table.json(), as_table.text
    assert as_function.json().get("code") == as_table.json()["code"], (
        as_function.text,
        as_table.text,
    )
    assert api.by_id() == []


@pytest.mark.parametrize("pet, rows", [(1, []), (2, [(2, "detail of pet 2")])])
def test_a_role_with_a_row_rule_gets_the_same_governed_rows_through_both_forms(served, pet, rows):
    server, _ = served
    assert _rows(server, _TABLE_FORM.format(pet), _READER) == rows
    assert _rows(server, _FUNCTION_FORM.format(pet), _READER) == rows


# --- the catalogs a PostgreSQL client reads -----------------------------------------------------


def _read(server, sql: str) -> list[tuple]:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=server.pgwire_port,
        dbname="provisa",
        user=_ADMIN,
        password="provisa",
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def test_pg_proc_lists_the_function_with_its_arguments_and_the_rows_it_returns(served):
    server, _ = served
    [(names, types, retset, rettype)] = _read(
        server,
        "SELECT proargnames, proargtypes::text, proretset, prorettype FROM pg_catalog.pg_proc "
        "WHERE proname = 'get_pet'",
    )
    assert names == ["petId"]
    assert types == "23"  # integer
    assert retset is True
    [(reltype,)] = _read(
        server, "SELECT reltype FROM pg_catalog.pg_class WHERE relname = 'get_pet_by_id'"
    )
    assert rettype == reltype


def test_information_schema_lists_its_parameters_and_its_returned_table(served):
    server, _ = served
    [(data_type, udt_name)] = _read(
        server,
        "SELECT data_type, type_udt_name FROM information_schema.routines "
        "WHERE routine_name = 'get_pet'",
    )
    assert (data_type, udt_name) == ("USER-DEFINED", "get_pet_by_id")
    parameters = _read(
        server,
        "SELECT p.parameter_name, p.data_type FROM information_schema.parameters p "
        "JOIN information_schema.routines r ON r.specific_name = p.specific_name "
        "WHERE r.routine_name = 'get_pet' ORDER BY p.ordinal_position",
    )
    assert parameters == [("petId", "integer")]

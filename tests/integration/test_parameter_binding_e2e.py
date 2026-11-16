# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9a1f64-5b7c-4d20-8a16-9c4e7f2b1d05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a statement binds an OpenAPI table's path parameter, end to end (REQ-1986).

A real server (DuckDB engine, its own store file, a SQLite control plane) over an HTTP API the
test serves, which records every path it is asked. ``get_pet_by_id`` is a function of its path
parameter; ``list_pets`` is the plain list; the DETAIL relationship joins a listed pet to the
by-id table on the parameter itself, as the default demo's does.

Each test counts the calls the remote received for the statement and reads the rows answered.
"""

# Requirements: REQ-1986

from __future__ import annotations

import json
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ROLE = "org_admin"
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
    """The remote API: every request path it is asked, and the pets. A pet asked by id answers
    with the name ``detail of pet <id>``, so a row is known to come from the by-id call."""

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
        """The by-id calls received so far, sorted; and forgotten, so the next statement's are
        its own."""
        calls = sorted(p for p in self.asked if p != "/pets")
        self.asked.clear()
        return calls

    def close(self) -> None:
        self._server.shutdown()


def _config(spec_path: str, base_url: str) -> dict:
    visible = {"visible_to": [_ROLE]}
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
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "pets", "description": "Pets"}],
        "roles": [],
        "sources": [
            {
                "id": "api",
                "type": "openapi",
                "path": spec_path,
                "base_url": base_url,
                # The list lands for its source's TTL; the by-id table, a function of its
                # parameter with no TTL of its own, is called for every statement.
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
        "rls_rules": [],
        "functions": [],
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
        "parameter_binding_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'materialize.duckdb'}",
        # The cap, low enough to pass with a statement and to refuse with one.
        env={"PROVISA_PARAMETERS_MAX_CALLS_PER_STATEMENT": "4"},
    )
    server.start()
    try:
        yield server, api
    finally:
        server.stop_process()
        workdir.cleanup()
        api.close()


def _sql(server, sql: str) -> httpx.Response:
    return httpx.post(
        f"{server.base_url}/data/sql",
        json={"sql": sql},
        headers={"X-Provisa-Role": _ROLE},
        timeout=300,
    )


def _rows(server, sql: str) -> list[tuple]:
    response = _sql(server, sql)
    assert response.status_code == 200, response.text
    return sorted(tuple(row.values()) for row in response.json()["data"]["sql"])


def _graphql(server, query: str) -> dict:
    response = httpx.post(
        f"{server.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=300,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "errors" not in body, response.text
    return body["data"]


def test_one_value_is_one_call_and_its_column_repeats_the_value(served):
    server, api = served
    api.by_id()
    rows = _rows(
        server, 'SELECT id, name, "_nf_petId" FROM pets.get_pet_by_id WHERE "_nf_petId" = 2'
    )
    assert rows == [(2, "detail of pet 2", 2)]
    assert api.by_id() == ["/pets/2"]


def test_in_of_three_values_makes_three_calls_and_unions_their_rows(served):
    server, api = served
    api.by_id()
    rows = _rows(
        server,
        'SELECT id, name, "_nf_petId" FROM pets.get_pet_by_id WHERE "_nf_petId" IN (1, 3, 5)',
    )
    assert rows == [
        (1, "detail of pet 1", 1),
        (3, "detail of pet 3", 3),
        (5, "detail of pet 5", 5),
    ]
    assert api.by_id() == ["/pets/1", "/pets/3", "/pets/5"]


def test_a_join_narrowed_by_the_other_tables_filter_calls_only_for_the_rows_it_keeps(served):
    """Six pets are listed and three are sold: the join on the parameter calls the by-id
    operation for those three, never once per listed pet (which the cap of 4 would refuse)."""
    server, api = served
    api.by_id()
    rows = _rows(
        server,
        "SELECT l.id, g.name FROM pets.list_pets l JOIN pets.get_pet_by_id g "
        "ON g.\"_nf_petId\" = l.id WHERE l.status = 'sold'",
    )
    assert rows == [(1, "detail of pet 1"), (3, "detail of pet 3"), (5, "detail of pet 5")]
    assert api.by_id() == ["/pets/1", "/pets/3", "/pets/5"]


def test_a_correlated_lateral_join_on_the_parameter_calls_per_kept_row(served):
    server, api = served
    api.by_id()
    rows = _rows(
        server,
        "SELECT l.id, d.name FROM pets.list_pets l LEFT JOIN LATERAL "
        '(SELECT g.name FROM pets.get_pet_by_id g WHERE g."_nf_petId" = l.id) d ON TRUE '
        "WHERE l.status = 'available' AND l.id < 5",
    )
    assert rows == [(2, "detail of pet 2"), (4, "detail of pet 4")]
    assert api.by_id() == ["/pets/2", "/pets/4"]


def test_in_a_subquery_calls_for_each_value_it_lists(served):
    server, api = served
    api.by_id()
    rows = _rows(
        server,
        'SELECT name FROM pets.get_pet_by_id WHERE "_nf_petId" IN '
        "(SELECT id FROM pets.list_pets WHERE status = 'sold' AND id > 1)",
    )
    assert rows == [("detail of pet 3",), ("detail of pet 5",)]
    assert api.by_id() == ["/pets/3", "/pets/5"]


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT id FROM pets.get_pet_by_id WHERE "_nf_petId" IN (1, 2, 3, 4, 5)',
        # every listed pet, unnarrowed: six keys
        'SELECT g.id FROM pets.list_pets l JOIN pets.get_pet_by_id g ON g."_nf_petId" = l.id',
    ],
)
def test_a_statement_past_the_cap_is_refused_with_its_code_and_makes_no_call(served, sql):
    server, api = served
    api.by_id()
    response = _sql(server, sql)
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["code"] == "data.parameter_calls_over_cap", body
    assert body["params"]["cap"] == "4"
    assert body["params"]["setting"] == "parameters.max_calls_per_statement"
    assert body["params"]["table"] == "get_pet_by_id"
    assert api.by_id() == []


@pytest.mark.parametrize(
    "condition, code, form",
    [
        ('"_nf_petId" > 2', "data.parameter_by_equality_only", ">"),
        ('"_nf_petId" NOT IN (1, 2)', "data.parameter_by_equality_only", "NOT IN"),
        ('"_nf_petId" IS NULL', "data.parameter_by_equality_only", "IS NULL"),
        ('"_nf_petId" = 1 AND "_nf_petId" = 2', "data.parameter_conflict", None),
        ("\"_nf_petId\" = 1 OR name = 'x'", "data.parameter_unbound_on_branch", None),
    ],
)
def test_http_sql_answers_each_refusal_with_its_code_and_makes_no_call(
    served, condition, code, form
):
    server, api = served
    api.by_id()
    response = _sql(server, f"SELECT id FROM pets.get_pet_by_id WHERE {condition}")
    assert response.status_code == 400, response.text
    body = response.json()
    assert body["code"] == code, body
    assert body["params"]["parameter"] == "_nf_petId"
    if form is not None:
        assert body["params"]["form"] == form
    assert api.by_id() == []


# --- the compiled path: GraphQL following the DETAIL relationship -------------------------------


def _named(type_ref: dict) -> str:
    while type_ref.get("ofType"):
        type_ref = type_ref["ofType"]
    return type_ref["name"]


_TYPE_REF = "name kind ofType { name kind ofType { name kind ofType { name kind } } }"


def _detail_field(server) -> tuple[str, str]:
    """(the root field that lists pets, its field that follows DETAIL), read from the schema the
    role is served."""
    schema = _graphql(
        server,
        "{ __schema { queryType { fields { name type { %s } } } "
        "types { name fields { name type { %s } } } } }" % (_TYPE_REF, _TYPE_REF),
    )["__schema"]
    types = {t["name"]: t for t in schema["types"] if t.get("fields")}
    for root in schema["queryType"]["fields"]:
        listed = types.get(_named(root["type"]))
        if listed is None or "list" not in root["name"].lower():
            continue
        for field in listed["fields"]:
            target = types.get(_named(field["type"]))
            if target is not None and {"id", "name"} <= {f["name"] for f in target["fields"]}:
                return root["name"], field["name"]
    raise AssertionError(f"no relationship field from the pet list: {sorted(types)}")


def test_graphql_following_detail_answers_each_listed_pets_own_by_id_row(served):
    """What hydration once fed from the parent's keys: each listed pet's DETAIL is the row the
    by-id operation answers for that pet, one call per pet the query keeps."""
    server, api = served
    root, detail = _detail_field(server)
    api.by_id()
    data = _graphql(
        server,
        '{ %s(where: {status: {eq: "sold"}}) { id %s { id name } } }' % (root, detail),
    )
    answered = sorted((pet["id"], pet[detail]["id"], pet[detail]["name"]) for pet in data[root])
    assert answered == [
        (1, 1, "detail of pet 1"),
        (3, 3, "detail of pet 3"),
        (5, 5, "detail of pet 5"),
    ]
    assert api.by_id() == ["/pets/1", "/pets/3", "/pets/5"]

# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2a9f41-0c6e-4b85-b13f-5e8c2a7d6f90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The built-in OpenAPI code source (REQ-2267 part 2, v0): its @discover wraps parse_spec and the
REQ-1986 row-type grouping; a list-of-ids GET is its row type's table's loader (REQ-2100); its
handlers read through call_api and call commands as source_operation does.

Parity with the existing OpenAPI source, for the fixture spec: the same tables, columns and
parameters as Register Table offers, the same functions and commands, and the same rows. The code
runs in a real Python host process under Provisa's own Python; the API is a real HTTP server on
an ephemeral loopback port, which records each call."""

# Requirements: REQ-2267, REQ-1986, REQ-2100
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.discovery import declared_columns
from provisa.codesource.manifest import discovered_handlers
from provisa.openapi import code_source as oc

pytestmark = pytest.mark.unit

SPEC = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "openapi_code" / "petstore.json").read_text()
)

PETS = [
    {
        "id": i,
        "name": f"pet{i}",
        "status": "sold" if i % 2 else "available",
        "weight": i + 0.5,
        "vaccinated": bool(i % 2),
        "owner_id": 1 + i % 2,
        "tags": [f"t{i}"],
    }
    for i in range(1, 6)
]
OWNERS = [
    {"id": 1, "first-name": "Ada", "class": "gold", "address": {"city": "Leeds", "zip": "LS1"}},
    {"id": 2, "first-name": "Bo", "class": "silver", "address": {"city": "York", "zip": "YO1"}},
]
VISITS = {1: [{"visit_id": 10, "pet_id": 2, "on": "2026-01-02"}], 2: []}


class _Api(BaseHTTPRequestHandler):
    calls: list[tuple[str, str, dict, object]] = []

    def log_message(self, format, *args):  # noqa: A002 -- quiet; the base's own signature
        return

    def _send(self, status: int, body: object) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _record(self, body: object = None) -> tuple[str, dict]:
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        self.calls.append((self.command, url.path, query, body))
        return url.path, query

    def do_GET(self):  # noqa: N802 -- the handler's own name
        path, query = self._record()
        if path == "/pets":
            status = query.get("status", [None])[0]
            self._send(200, [p for p in PETS if status in (None, p["status"])])
        elif path == "/pets/lookup":
            ids = {int(i) for i in query["ids"]}
            self._send(200, [p for p in PETS if p["id"] in ids])
        elif path.startswith("/pets/"):
            pet_id = int(path.rsplit("/", 1)[1])
            self._send(200, next(p for p in PETS if p["id"] == pet_id))
        elif path == "/owners":
            self._send(200, OWNERS)
        elif path.startswith("/owners/") and path.endswith("/visits"):
            self._send(200, VISITS[int(path.split("/")[2])])
        else:
            self._send(404, {"error": path})

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self._record(body)
        self._send(201, body)

    def do_DELETE(self):  # noqa: N802
        path, _ = self._record()
        self._send(200, {"deleted": int(path.rsplit("/", 1)[1])})


@pytest.fixture
def api():
    _Api.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield SimpleNamespace(url=f"http://127.0.0.1:{server.server_address[1]}", calls=_Api.calls)
    server.shutdown()
    server.server_close()


@pytest.fixture
def code(tmp_path, api):
    directory = tmp_path / "code"
    content_hash = oc.write_code(directory, source_id="pets-src", spec=SPEC, base_url=api.url)
    return SimpleNamespace(directory=directory, hash=content_hash, api=api)


@pytest.fixture
def prepared(code, monkeypatch):
    manifest = oc.read_code(code.directory)
    command = oc.host_command(code.directory, manifest)
    ready = code_source.Prepared("pets-src", code.hash, manifest, command, "acme")
    source = SimpleNamespace(id="pets-src", type=SimpleNamespace(value="python"), mapping={})

    async def _registered(_state):
        return [source]

    async def _prepare(*_args):
        return ready

    async def _context(_state):
        return None

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _registered)
    monkeypatch.setattr(code_source, "prepare", _prepare)
    monkeypatch.setattr(code_source, "context_for", _context)
    yield ready
    w.pool.stop_source("pets-src")


def _table(manifest, name: str) -> dict:
    """The table as discovery registers it: its declared columns, parameters after them."""
    columns = [
        {**c, "column_name": c["name"]} for c in declared_columns(manifest, manifest.export(name))
    ]
    return {"table_name": name, "source_id": "pets-src", "columns": columns}


# -- what the existing OpenAPI source offers for the fixture spec ----------------------------------


def _existing_state(base_url: str = "http://127.0.0.1:1") -> SimpleNamespace:
    from provisa.api_source.models import ApiSource, ApiSourceType

    return SimpleNamespace(
        source_types={"pets-src": "openapi"},
        openapi_specs={"pets-src": {"spec": SPEC}},
        api_sources={
            "pets-src": ApiSource(id="pets-src", type=ApiSourceType.openapi, base_url=base_url)
        },
    )


def _existing_query(operation_id: str):
    from provisa.openapi.mapper import parse_spec

    return next(q for q in parse_spec(SPEC)[0] if q.operation_id == operation_id)


def _existing_columns(operation_id: str) -> list[dict]:
    """The columns Register Table offers for the operation's table (schema_query
    _introspect_available_columns): its response's, then one per parameter."""
    from provisa.api.admin.introspect import openapi_parameter_columns
    from provisa.openapi.register import _schema_to_columns

    q = _existing_query(operation_id)
    cols = _schema_to_columns(q.response_schema)
    existing = {c["name"] for c in cols}
    return cols + [c for c in openapi_parameter_columns(q) if c["name"] not in existing]


#: An OpenAPI column's registered type, and the IR type a code source's column of it has: a
#: Python int is a bigint and a float a double under the code-source contract (manifest.py).
_CODE_IR = {"string": "text", "integer": "bigint", "number": "double", "boolean": "boolean"}


def _ir(openapi_type: str) -> str:
    return _CODE_IR.get(openapi_type, "json")


async def _existing_rows(base_url: str, operation_id: str, table: str, given: dict) -> list[dict]:
    """The rows the existing source reads for the table registered on ``operation_id`` with the
    paging the spec suggests: register_openapi_endpoint's endpoint, read as handle_api_query
    reads it (its defaults under what the statement binds), through call_api and answer_rows."""
    from provisa.api_source.caller import answer_rows, call_api
    from provisa.api_source.models import ApiEndpoint
    from provisa.api_source.openapi_endpoint import (
        default_params_from_spec,
        endpoint_columns,
        openapi_operation,
    )

    q = _existing_query(operation_id)
    match = openapi_operation(SPEC, "pets-src", operation_id, q.pagination)
    endpoint = ApiEndpoint(
        source_id="pets-src",
        path=match.path,
        table_name=table,
        columns=endpoint_columns(match),  # pyright: ignore[reportArgumentType]
        default_params=default_params_from_spec(SPEC, match.path),
        pagination=q.pagination,
        response_root=match.rows_field,
    )
    answer = await call_api(endpoint, {**endpoint.default_params, **given}, base_url=base_url)
    return answer_rows(endpoint, answer)[0]


def _project(rows: list[dict], columns: list[str]) -> list[dict]:
    """The rows' values of ``columns``, a JSON column's value decoded: the existing source holds it
    as JSON text (api_source.flattener), a code source as JSON text or the value itself."""
    return [
        {c: json.loads(v) if c in _JSON_COLUMNS and isinstance(v, str) else v for c, v in row}
        for row in ([(c, r.get(c)) for c in columns] for r in rows)
    ]


_JSON_COLUMNS = frozenset({"tags", "address"})


# -- discovery --------------------------------------------------------------------------------------


def test_the_spec_is_grouped_by_row_type_as_req_1986_groups_it():
    plan = oc.plan_spec(SPEC)
    # Each plain list a table; a parameterised read of a listed type a query on its first plain
    # list; one whose type no plain list answers a table of its own.
    assert list(plan.tables) == ["listPets", "listOwners", "listOwnerVisits"]
    assert {n: q.table for n, q in plan.queries.items()} == {
        "findPetsByIds": "listPets",
        "getPetById": "listPets",
    }
    assert list(plan.commands) == ["createPet", "deletePet"]
    lookup = plan.tables["listPets"].lookup
    assert lookup is not None
    assert (lookup.operation.operation_id, lookup.parameter, lookup.key, lookup.batch) == (
        "findPetsByIds",
        "ids",
        "id",
        2,
    )
    assert plan.tables["listOwners"].lookup is None


def test_a_get_with_no_row_schema_is_a_reading_command_and_no_table():
    spec = json.loads(json.dumps(SPEC))
    spec["paths"]["/health"] = {
        "get": {"operationId": "health", "responses": {"200": {"description": "ok"}}}
    }
    plan = oc.plan_spec(spec)
    assert "health" not in plan.tables and "health" not in plan.queries
    assert plan.commands["health"].reads


@pytest.mark.parametrize(
    "parameter, schema",
    [
        ("ids", {"type": "integer"}),  # not a list
        ("names", {"type": "array", "items": {"type": "string"}}),  # no key's plural
        ("ownerIds", {"type": "array", "items": {"type": "integer"}}),  # no row field ownerId
    ],
)
def test_a_parameterised_read_that_looks_up_no_list_of_row_keys_is_no_loader(parameter, schema):
    spec = json.loads(json.dumps(SPEC))
    lookup = spec["paths"]["/pets/lookup"]["get"]
    lookup["parameters"] = [{"name": parameter, "in": "query", "required": True, "schema": schema}]
    plan = oc.plan_spec(spec)
    assert plan.tables["listPets"].lookup is None
    assert plan.queries["findPetsByIds"].table == "listPets"


def test_discovery_records_the_generated_tables_queries_and_commands(prepared):
    manifest = prepared.manifest
    kinds = {e.name: (e.kind, e.handler) for e in manifest.exports}
    assert kinds == {
        "discover": ("discover", None),
        "listPets": ("table", "serve_table"),
        "listOwners": ("table", "serve_table"),
        "listOwnerVisits": ("table", "serve_table"),
        "findPetsByIds": ("query", "serve_query"),
        "getPetById": ("query", "serve_query"),
        "createPet": ("command", "serve_command"),
        "deletePet": ("command", "serve_command"),
    }
    pets = manifest.export("listPets")
    assert pets.description == "Every pet"
    assert pets.loader and pets.loader_batch == 2
    assert [(p.name, p.required) for p in pets.parameters] == [("status", False), ("id", False)]
    visits = manifest.export("listOwnerVisits")
    assert [(p.name, p.required) for p in visits.parameters] == [
        ("ownerId", True),
        ("since", False),
    ]
    # A query returns the rows of its table.
    for query in ("findPetsByIds", "getPetById"):
        assert manifest.export(query).many and manifest.export(query).returns == pets.returns
    assert [(p.name, p.required) for p in manifest.export("getPetById").parameters] == [
        ("petId", True)
    ]
    assert [p.name for p in manifest.export("createPet").parameters] == ["body"]
    assert [p.name for p in manifest.export("deletePet").parameters] == ["petId"]
    assert set(discovered_handlers(manifest)) == set(kinds) - {"discover"}


def test_the_hosts_hello_offers_every_generated_item(prepared):
    assert sorted(prepared.worker().exports) == sorted(e.name for e in prepared.manifest.exports)


# -- parity: tables, columns, parameters -----------------------------------------------------------


@pytest.mark.parametrize("table", ["listPets", "listOwners", "listOwnerVisits"])
def test_a_generated_tables_columns_are_those_register_table_offers(prepared, table):
    manifest = prepared.manifest
    generated = declared_columns(manifest, manifest.export(table))
    existing = _existing_columns(table)
    lookup = oc.plan_spec(SPEC).tables[table].lookup
    # The one addition: a table with a loader takes its key as an input (REQ-2100).
    extra = [] if lookup is None else [f"_nf_{lookup.key}"]
    assert [c["name"] for c in generated] == [c["name"] for c in existing] + extra
    for got, want in zip(generated, existing, strict=False):
        assert got["data_type"] == _ir(want["type"]), got
        assert got["comment"] == (want.get("description") or None), got
        if want.get("native_filter_type") is None:
            assert got["native_filter_type"] is None, got
        else:
            # Every code-source parameter is a query argument; required as the spec says.
            assert got["native_filter_type"] == "query_arg", got
            assert got["native_filter_required"] == want["native_filter_required"], got


async def test_the_tables_are_the_existing_sources_plain_lists_and_unlisted_functions():
    from provisa.executor.source_operation import _openapi_reads, plain_lists

    state = _existing_state()
    plain = [p.table for p in await plain_lists(state, "pets-src")]
    functions = _openapi_reads(state, "pets-src")
    listed_types = {_existing_query(t).returns_type for t in plain}
    # REQ-1986's default: a function's table is on where no plain list answers its type.
    exposed = [f.table for f in functions if f.returns_type not in listed_types]
    assert list(oc.plan_spec(SPEC).tables) == plain + exposed


def test_the_queries_are_the_existing_sources_functions_on_their_plain_lists():
    from provisa.executor.source_operation import _openapi_reads

    plan = oc.plan_spec(SPEC)
    functions = {f.name: f for f in _openapi_reads(_existing_state(), "pets-src")}
    for name, query in plan.queries.items():
        function = functions[name]
        q = query.operation
        assert tuple(p["name"] for p in [*q.path_params, *q.query_params]) == function.arguments
        assert _existing_query(query.table).returns_type == function.returns_type


def test_the_commands_are_the_existing_sources_commands(prepared):
    from provisa.executor.source_operation import _openapi_operations

    manifest = prepared.manifest
    existing = {
        op.name: op.arguments
        for op in _openapi_operations(_existing_state(), "pets-src") or []
        if op.table is None
    }
    generated = {
        e.name: tuple(p.name for p in e.parameters) for e in manifest.exports if e.kind == "command"
    }
    assert generated == existing


# -- parity: rows -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "table, inputs",
    [
        ("listPets", {}),
        ("listPets", {"status": "sold"}),
        ("listOwners", {}),
        ("listOwnerVisits", {"ownerId": 1}),
    ],
)
async def test_a_generated_table_reads_the_rows_the_existing_source_reads(
    prepared, code, table, inputs
):
    manifest = prepared.manifest
    read = await code_table.read(None, _table(manifest, table), inputs)
    columns = [f.name for f in manifest.types[f"{table}Row"].fields]
    existing = await _existing_rows(code.api.url, table, table, inputs)
    assert existing, "the fixture answers rows"
    assert _project(read.rows, columns) == _project(existing, columns)


async def test_status_is_sent_as_the_query_parameter_a_statement_binds(prepared, code):
    await code_table.read(None, _table(prepared.manifest, "listPets"), {"status": "sold"})
    assert ("GET", "/pets", {"status": ["sold"]}, None) in code.api.calls


async def test_a_generated_query_returns_the_rows_the_existing_function_reads(prepared, code):
    manifest = prepared.manifest
    worker = prepared.worker()
    query = manifest.export("getPetById")
    rows = worker.command(
        "getPetById", [3], ["petId"], schema=schema_text(row_schema(manifest, query))
    )
    existing = await _existing_rows(code.api.url, "getPetById", "listPets", {"petId": 3})
    columns = [f.name for f in manifest.types["listPetsRow"].fields]
    got = _project(rows, columns)
    assert got == _project(existing, columns) and len(got) == 1


async def test_a_generated_command_answers_as_the_existing_command_answers(prepared, code):
    from provisa.executor.source_operation import call_operation

    worker = prepared.worker()
    pet = {"id": 9, "name": "new"}
    made = worker.command("createPet", [pet], ["body"])
    existing = await call_operation(
        _existing_state(code.api.url), "pets-src", "createPet", {"body": pet}
    )
    assert made == existing == [pet]
    assert worker.command("deletePet", [4], ["petId"]) == [{"deleted": 4}]
    assert ("DELETE", "/pets/4", {}, None) in code.api.calls
    assert [c for c in code.api.calls if c[0] == "POST"] == [("POST", "/pets", {}, pet)] * 2


# -- the loader -------------------------------------------------------------------------------------


async def test_a_join_sends_its_keys_to_the_lookup_in_batches_of_its_max_items(prepared, code):
    sets = [{"id": i} for i in (1, 2, 3, 4, 5)]
    read = await code_table.read_each(
        None, _table(prepared.manifest, "listPets"), sets, joined=True
    )
    assert sorted((r["id"], r["name"]) for r in read.rows) == [(i, f"pet{i}") for i in range(1, 6)]
    lookups = [c[2]["ids"] for c in code.api.calls if c[1] == "/pets/lookup"]
    assert sorted(len(ids) for ids in lookups) == [1, 2, 2]
    assert sorted(int(i) for ids in lookups for i in ids) == [1, 2, 3, 4, 5]
    assert not [c for c in code.api.calls if c[1] == "/pets"]


async def test_a_key_given_as_an_input_is_read_through_the_lookup(prepared, code):
    read = await code_table.read(None, _table(prepared.manifest, "listPets"), {"id": 3})
    assert [(r["id"], r["name"]) for r in read.rows] == [(3, "pet3")]
    assert [c[1:3] for c in code.api.calls] == [("/pets/lookup", {"ids": ["3"]})]


async def test_a_key_given_with_another_input_is_refused_by_name(prepared):
    with pytest.raises(
        Exception, match="its key id is read by findPetsByIds, which takes none of status"
    ):
        await code_table.read(
            None, _table(prepared.manifest, "listPets"), {"id": 3, "status": "sold"}
        )

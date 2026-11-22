# Copyright (c) 2026 Kenneth Stott
# Canary: 67ace8fa-c76b-41e4-87ec-1f3aa95a292f
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The built-in remote GraphQL code source (REQ-2267 part 2, v0): its @discover wraps the
graphql_remote mapping and REQ-1986's grouping by row type, a table fetched by a key list declares
a loader (REQ-2100), and one handler per kind reads as the source reads today.

The remote is a real GraphQL server (graphql-core over HTTP, on an ephemeral local port); the code
is read by the real Python reader and served by a real host process (unsandboxed). Parity: what the
code source generates and reads matches what the graphql_remote registration offers and reads."""

# Requirements: REQ-2267, REQ-1986, REQ-1865, REQ-2100
from __future__ import annotations

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from graphql import build_schema, get_introspection_query, graphql_sync

from provisa.codesource import source as code_source
from provisa.codesource import table as code_table
from provisa.codesource import worker as w
from provisa.codesource.arrow_rows import row_schema, schema_text
from provisa.codesource.discovery import declared_columns
from provisa.codesource.manifest import PYTHON_RULES, discovered_handlers, read_manifest
from provisa.graphql_remote import discovered as gd
from provisa.python.source import HOST, READER

pytestmark = pytest.mark.unit

SDL = """
interface Node { id: ID! }

type Address { city: String street: String }

type Author implements Node {
  id: ID!
  name: String!
  born: Int
  rating: Float
  active: Boolean
  address: Address
}

type Book implements Node { id: ID! title: String! authorId: ID! author: Author }

type Publisher { code: String! name: String }

type DeleteResult { affectedRows: Int! }

input IdComparison { _eq: ID _in: [ID!] }
input StringComparison { _eq: String _in: [String!] }
input AuthorBoolExp { id: IdComparison name: StringComparison }
input BookBoolExp { id: IdComparison authorId: IdComparison }

type Query {
  "Every author"
  authors(where: AuthorBoolExp, limit: Int): [Author!]!
  author(id: ID!): Author
  books(where: BookBoolExp): [Book!]!
  nodes(ids: [ID!]!): [Node]!
  publisherByCode(code: String!): Publisher
}

type Mutation {
  insertAuthor(name: String!, born: Int): Author
  deleteBooks(where: BookBoolExp!): DeleteResult
  ping: String
}
"""

AUTHORS = [
    {"__typename": "Author", "id": "a1", "name": "Ada", "born": 1815, "rating": 4.5,
     "active": True, "address": {"city": "London", "street": "St James"}},
    {"__typename": "Author", "id": "a2", "name": "Grace", "born": 1906, "rating": 4.9,
     "active": False, "address": None},
    {"__typename": "Author", "id": "a3", "name": "Edsger", "born": 1930, "rating": None,
     "active": True, "address": {"city": "Austin", "street": None}},
]  # fmt: skip
BOOKS = [
    {"__typename": "Book", "id": "b1", "title": "Notes", "authorId": "a1"},
    {"__typename": "Book", "id": "b2", "title": "Compilers", "authorId": "a2"},
    {"__typename": "Book", "id": "b3", "title": "Discipline", "authorId": "a3"},
]
PUBLISHERS = [{"code": "p1", "name": "Pearson"}]


def _matches(row: dict, where: dict | None) -> bool:
    for column, test in (where or {}).items():
        if "_eq" in test and row[column] != test["_eq"]:
            return False
        if "_in" in test and row[column] not in test["_in"]:
            return False
    return True


class _Root:
    def __init__(self) -> None:
        self.authors_added: list[dict] = []

    def authors(self, _info, where=None, limit=None):
        found = [a for a in AUTHORS + self.authors_added if _matches(a, where)]
        return found[:limit] if limit is not None else found

    def author(self, _info, id):  # noqa: A002 -- the remote's argument
        return next((a for a in AUTHORS if a["id"] == id), None)

    def books(self, _info, where=None):
        return [{**b, "author": None} for b in BOOKS if _matches(b, where)]

    def nodes(self, _info, ids):
        every = {r["id"]: r for r in AUTHORS + BOOKS}
        return [every.get(i) for i in ids]

    def publisherByCode(self, _info, code):  # noqa: N802 -- the remote's field
        return next((p for p in PUBLISHERS if p["code"] == code), None)

    def insertAuthor(self, _info, name, born=None):  # noqa: N802
        added = {"__typename": "Author", "id": f"n{len(self.authors_added) + 1}", "name": name,
                 "born": born, "rating": None, "active": None, "address": None}  # fmt: skip
        self.authors_added.append(added)
        return added

    def deleteBooks(self, _info, where):  # noqa: N802
        return {"affectedRows": len([b for b in BOOKS if _matches(b, where)])}

    def ping(self, _info):
        return "pong"


SCHEMA = build_schema(SDL)


class Remote:
    """The fixture remote: every request it answered, in order."""

    def __init__(self) -> None:
        self.root = _Root()
        self.requests: list[dict] = []
        remote = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                remote.requests.append(body)
                result = graphql_sync(
                    SCHEMA,
                    body["query"],
                    root_value=remote.root,
                    variable_values=body.get("variables"),
                )
                answer: dict = {"data": result.data}
                if result.errors:
                    answer["errors"] = [e.formatted for e in result.errors]
                data = json.dumps(answer).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args) -> None:  # noqa: A002 -- its base's name
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/graphql"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def _introspected() -> dict:
    result = graphql_sync(SCHEMA, get_introspection_query())
    assert result.errors is None
    assert result.data is not None
    return result.data["__schema"]


@pytest.fixture
def remote():
    found = Remote()
    yield found
    found.stop()


def _config(url: str) -> dict:
    return {
        "url": url,
        "auth": None,
        "namespace": "",
        "schema": _introspected(),
        "max_object_depth": 5,
        "max_list_depth": 2,
        "max_list_items": 100,
        "max_rows": 10_000,
        "loader_batch": 2,
        "field_overrides": None,
    }


def _read_code(code) -> dict:
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(code), gd.ENTRY], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout)


@pytest.fixture
def prepared(tmp_path, monkeypatch, remote):
    code = gd.write_code(tmp_path / "code", _config(remote.url))
    raw = _read_code(code)
    assert raw["diagnostics"] == []
    manifest = read_manifest(raw, PYTHON_RULES)
    command = w.HostCommand(
        argv=lambda address: [sys.executable, "-I", str(HOST), address, str(code), gd.ENTRY],
        env=w.private_environment(PYTHONDONTWRITEBYTECODE="1"),
        cwd=code,
        discovered=discovered_handlers(manifest),
    )
    ready = code_source.Prepared("gql-src", "h1", manifest, command, "acme")
    source = SimpleNamespace(id="gql-src", type=SimpleNamespace(value="python"), mapping={})

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
    w.pool.stop_source("gql-src")


def _code_table(manifest, name: str) -> dict:
    columns = [
        {**c, "column_name": c["name"]} for c in declared_columns(manifest, manifest.export(name))
    ]
    return {"table_name": name, "source_id": "gql-src", "columns": columns}


# -- the plan: the mapping, grouped by row type ----------------------------------------------------


def test_the_plan_groups_the_mapping_by_row_type():
    plan = gd.plan(_config("http://remote.invalid/graphql"))
    assert list(plan.tables) == ["authors", "books", "publisher_by_code"]
    # A parameterised read of a type with a plain list is a query on that list.
    assert list(plan.queries) == ["author"]
    assert plan.queries["author"].table == "authors"
    assert [(a.name, a.remote, a.required) for a in plan.queries["author"].read.arguments] == [
        ("id", "id", True)
    ]
    # One with no plain list of its type is a table of its own, its argument a required input.
    publisher = plan.tables["publisher_by_code"]
    assert [(a.name, a.required) for a in publisher.read.arguments] == [("code", True)]
    assert publisher.key is None
    assert list(plan.commands) == ["insertAuthor", "deleteBooks", "ping"]
    assert plan.commands["insertAuthor"].table == "authors"
    assert plan.commands["deleteBooks"].table is None
    assert plan.commands["ping"].returns == "text"


def test_a_table_the_remote_fetches_by_a_key_list_gets_that_key_as_its_loader():
    plan = gd.plan(_config("http://remote.invalid/graphql"))
    authors = plan.tables["authors"]
    assert authors.key == "id"
    assert authors.key_fetch is not None and authors.key_fetch["style"] == "where_in"
    assert plan.tables["books"].key == "id"


def test_a_configuration_lacking_a_field_is_refused_by_name(tmp_path):
    config = _config("http://remote.invalid/graphql")
    del config["loader_batch"]
    with pytest.raises(gd.ConfigRefused, match="lacks loader_batch"):
        gd.write_code(tmp_path / "code", config)


# -- discovery records what the plan generates -----------------------------------------------------


def test_discovery_records_the_generated_tables_queries_and_commands(prepared):
    manifest = prepared.manifest
    kinds = {e.name: (e.kind, e.handler) for e in manifest.exports}
    assert kinds == {
        "discover": ("discover", None),
        "authors": ("table", "serve_table"),
        "books": ("table", "serve_table"),
        "publisher_by_code": ("table", "serve_table"),
        "author": ("query", "serve_query"),
        "insertAuthor": ("command", "serve_command"),
        "deleteBooks": ("command", "serve_command"),
        "ping": ("command", "serve_command"),
    }
    authors = manifest.export("authors")
    assert authors.description == "Every author"
    assert authors.loader and authors.loader_batch == 2
    assert [(p.name, p.required) for p in authors.parameters] == [
        ("where", False),
        ("id", False),
    ]
    assert [(p.name, p.required) for p in manifest.export("publisher_by_code").parameters] == [
        ("code", True)
    ]
    author = manifest.export("author")
    assert author.many and author.returns == authors.returns
    assert [(p.name, p.required) for p in author.parameters] == [("id", True)]
    insert = manifest.export("insertAuthor")
    assert insert.returns == authors.returns
    assert [(p.name, p.required) for p in insert.parameters] == [("name", True), ("born", False)]


# -- parity with the graphql_remote registration ---------------------------------------------------

# A column's stored type (the graphql_remote registration's) as the IR type a code source's column
# of the same Python type registers with. Python has one integer type, so a GraphQL Int is a bigint.
_STORED_AS_IR = {
    "varchar": "text",
    "integer": "bigint",
    "double": "double",
    "boolean": "boolean",
    "json": "json",
}


def _registration_offer(url: str):
    from provisa.api.admin._graphql_table_registration import (
        offered_columns_of_every_table,
        offered_tables,
    )
    from provisa.graphql_remote.brands import LiveSchema

    offer = LiveSchema("gql-src", _introspected(), 5, 2, 100)
    reg = {"namespace": "", "source_id": "gql-src", "url": url, "auth": None}
    return offer, reg, offered_tables(offer, reg), offered_columns_of_every_table(offer, reg)


def test_the_generated_items_offer_what_the_registration_offers(prepared, remote):
    manifest = prepared.manifest
    _, _, offered, columns = _registration_offer(remote.url)
    generated = {e.name for e in manifest.exports if e.kind in ("table", "query")}
    assert {t["name"] for t in offered} == generated
    for name, registered in columns.items():
        export = manifest.export(name)
        # A query returns the rows of its row type's table; its parameters are its own.
        rows_of = export if export.kind == "table" else manifest.export(name)
        declared = declared_columns(manifest, rows_of)
        data = [(c["name"], c["data_type"], c["comment"]) for c in declared]
        expected = [(n, _STORED_AS_IR[t], d) for n, t, d in registered]
        if export.kind == "table" and export.loader:
            # The loader's key is an input of its own (REQ-2100): the one column added.
            key = next(p for p in export.parameters if p.name == "id")
            assert data[-1] == (f"_nf_{key.name}", "text", None)
            data = data[:-1]
        assert data == expected, name


def test_the_relationships_the_registration_detects_are_the_plans():
    from provisa.graphql_remote.mapper import map_schema

    plan = gd.plan(_config("http://remote.invalid/graphql"))
    _, _, detected = map_schema(_introspected(), "", "")
    assert list(plan.relationships) == detected
    assert [(r["source_table_id"], r["source_column"], r["target_table_id"]) for r in detected] == [
        ("books", "author_id", "authors")
    ]


async def _registration_rows(url: str, name: str, variables: dict | None) -> list[dict]:
    from provisa.api.data.materialization import _fetch_gql_remote_rows
    from provisa.graphql_remote.brands import LiveSchema, available_tables, map_tables
    from provisa.graphql_remote.executor import column_selection

    offer = LiveSchema("gql-src", _introspected(), 5, 2, 100)
    # Every table mapped at once, as offered_columns_of_every_table maps them: a field offered as
    # a relationship between two of them is not a column too (REQ-1929).
    every = [t["sql_name"] for t in available_tables(offer, "")]
    table = map_tables(offer, "", "gql-src", "", every)[name]
    reg = {"source_id": "gql-src", "url": url, "auth": None}
    rows, cut = await _fetch_gql_remote_rows(
        name,
        reg,
        table,
        [column_selection(c) for c in table["columns"]],
        variables,
        {},
        100,
        10_000,
    )
    assert not cut
    return rows


def _plain(rows: list[dict]) -> list[dict]:
    return sorted((dict(r) for r in rows), key=lambda r: json.dumps(r, sort_keys=True))


@pytest.mark.parametrize(
    "name, inputs, variables",
    [
        ("authors", {}, None),
        ("books", {}, None),
        ("publisher_by_code", {"code": "p1"}, {"code": "p1"}),
    ],
)
async def test_a_generated_table_reads_the_rows_the_registration_reads(
    prepared, remote, name, inputs, variables
):
    read = await code_table.read(None, _code_table(prepared.manifest, name), inputs)
    expected = await _registration_rows(remote.url, name, variables)
    assert expected
    got = [{k: v for k, v in r.items() if not k.startswith("_nf_")} for r in read.rows]
    assert _plain(got) == _plain(expected)


def test_a_generated_query_reads_the_rows_the_registration_reads(prepared, remote):
    import asyncio

    manifest = prepared.manifest
    query = manifest.export("author")
    rows = prepared.worker().command(
        "author", ["a2"], ["id"], schema=schema_text(row_schema(manifest, query))
    )
    expected = asyncio.run(_registration_rows(remote.url, "author", {"id": "a2"}))
    assert _plain(rows) == _plain(expected)
    assert [r["name"] for r in rows] == ["Grace"]


# -- a generated table's loader fetches a batch of keys in one request -----------------------------


async def test_a_generated_tables_loader_fetches_each_batch_by_its_key_list(prepared, remote):
    sets = [{"id": i} for i in ("a1", "a2", "a3", "zz")]
    before = len(remote.requests)
    read = await code_table.read_each(None, _code_table(prepared.manifest, "authors"), sets,
                                      joined=True)  # fmt: skip
    sent = remote.requests[before:]
    # Four keys at a declared batch of two: two requests, each a where _in on the key list.
    assert [r["variables"] for r in sent] == [
        {"where": {"id": {"_in": ["a1", "a2"]}}},
        {"where": {"id": {"_in": ["a3", "zz"]}}},
    ]
    assert sorted((r["id"], r["name"]) for r in read.rows) == [
        ("a1", "Ada"),
        ("a2", "Grace"),
        ("a3", "Edsger"),
    ]


async def test_a_key_given_as_an_input_is_read_by_the_key_fetch(prepared, remote):
    before = len(remote.requests)
    read = await code_table.read(None, _code_table(prepared.manifest, "books"), {"id": "b2"})
    assert [r["variables"] for r in remote.requests[before:]] == [
        {"where": {"id": {"_in": ["b2"]}}}
    ]
    assert [(r["id"], r["title"]) for r in read.rows] == [("b2", "Compilers")]


async def test_a_relay_nodes_key_fetch_selects_the_type_on_the_interface(remote):
    plan = gd.plan(_config(remote.url))
    books = plan.tables["books"]
    by_ids = {"style": "ids", "field": "nodes", "arg": "ids", "arg_type": "[ID!]!", "on": "Book"}
    relay = gd.Plan(plan.config, {"books": gd.Table(**{**books.__dict__, "key_fetch": by_ids})})
    answered = await gd.read_table(relay, "books", {}, {"h1": {"id": "b1"}, "h2": {"id": "a1"}})
    assert answered == {"h1": [{"id": "b1", "title": "Notes", "author_id": "a1"}], "h2": []}
    assert "... on Book" in remote.requests[-1]["query"]


# -- commands ---------------------------------------------------------------------------------------


def test_a_generated_command_returning_a_tables_row_answers_with_its_columns(prepared, remote):
    manifest = prepared.manifest
    insert = manifest.export("insertAuthor")
    rows = prepared.worker().command(
        "insertAuthor",
        ["Barbara", 1939],
        ["name", "born"],
        schema=schema_text(row_schema(manifest, insert)),
    )
    assert [(r["name"], r["born"]) for r in rows] == [("Barbara", 1939)]
    assert remote.root.authors_added[0]["name"] == "Barbara"


def test_a_generated_command_answering_with_one_value_returns_it(prepared):
    worker = prepared.worker()
    assert worker.command("ping", [], []) == "pong"
    deleted = worker.command("deleteBooks", [{"authorId": {"_eq": "a1"}}], ["where"])
    assert deleted == {"__typename": "DeleteResult", "affectedRows": 1}

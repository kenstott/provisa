# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e5c1a-4d92-4f38-a6c1-9e2f7d3b8a54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Relay-shaped remote GraphQL schema maps to tables that can be read (REQ-308, REQ-309).

The schema here has the shapes GitHub's has: root fields that return one object and need
arguments, connections under them, a root connection, a connection with no ``nodes`` shorthand,
a connection of a union, fields that need an argument, plain lists that take no ``first``, and
types that refer back to themselves.
"""

import json

import httpx
import pytest
import respx
from graphql import build_schema, parse, validate

from provisa.graphql_remote.executor import column_selection, execute_remote, table_query
from provisa.graphql_remote.mapper import map_schema
from provisa.graphql_remote.probe import fit_table_to_credential

URL = "https://remote.example/graphql"

SDL = """
type Query {
  repository(owner: String!, name: String!): Repository
  advisories(first: Int, after: String): AdvisoryConnection!
  releases(first: Int, after: String): ReleaseConnection!
  search(query: String!, first: Int): SearchConnection!
  licenses: [License]!
}
type Repository {
  id: ID!
  name: String!
  private(login: String!): Boolean
  parent: Repository
  owner: Owner!
  labels: [Label!]
  tags(first: Int): [Tag!]
  issues(first: Int, after: String, states: [String!]): IssueConnection!
  watchers(login: String!, first: Int): IssueConnection!
  pinned: PinnedConnection!
}
interface Owner { login: String! url: String! }
type User implements Owner { login: String! url: String! email: String }
type Label { name: String! }
type Tag { name: String! }
type License { key: String! name: String! }
type Issue { id: ID! title: String! author: User comments(first: Int): CommentConnection! }
type Comment { id: ID! body: String! }
type Advisory { id: ID! summary: String! }
type Release { id: ID! tag: String! }
type PageInfo { hasNextPage: Boolean! endCursor: String }
type IssueConnection { nodes: [Issue] pageInfo: PageInfo! totalCount: Int! }
type CommentConnection { nodes: [Comment] pageInfo: PageInfo! totalCount: Int! }
type AdvisoryConnection { nodes: [Advisory] pageInfo: PageInfo! totalCount: Int! }
type ReleaseEdge { cursor: String! node: Release }
type ReleaseConnection { edges: [ReleaseEdge] pageInfo: PageInfo! }
type PinnedConnection { nodes: [Label] pageInfo: PageInfo! }
union SearchItem = Issue | Repository
type SearchConnection { nodes: [SearchItem] pageInfo: PageInfo! }
"""

SCHEMA = build_schema(SDL)


def _type_ref(t) -> dict:
    from graphql import GraphQLList, GraphQLNonNull, is_enum_type, is_interface_type
    from graphql import is_object_type, is_scalar_type, is_union_type

    if isinstance(t, GraphQLNonNull):
        return {"kind": "NON_NULL", "name": None, "ofType": _type_ref(t.of_type)}
    if isinstance(t, GraphQLList):
        return {"kind": "LIST", "name": None, "ofType": _type_ref(t.of_type)}
    kind = (
        "SCALAR"
        if is_scalar_type(t)
        else "ENUM"
        if is_enum_type(t)
        else "INTERFACE"
        if is_interface_type(t)
        else "UNION"
        if is_union_type(t)
        else "OBJECT"
    )
    assert kind != "OBJECT" or is_object_type(t)
    return {"kind": kind, "name": t.name, "ofType": None}


def _introspected() -> dict:
    """The schema in the shape provisa.graphql_remote.introspect returns it."""
    types = []
    for name, t in SCHEMA.type_map.items():
        if name.startswith("__"):
            continue
        ref = _type_ref(t)
        fields = None
        if ref["kind"] in ("OBJECT", "INTERFACE"):
            fields = [
                {
                    "name": fname,
                    "description": None,
                    "type": _type_ref(f.type),
                    "args": [
                        {
                            "name": an,
                            "description": None,
                            "defaultValue": None,
                            "type": _type_ref(a.type),
                        }
                        for an, a in f.args.items()
                    ],
                }
                for fname, f in t.fields.items()
            ]
        types.append({"kind": ref["kind"], "name": name, "description": None, "fields": fields})
    return {"queryType": {"name": "Query"}, "mutationType": None, "types": types}


def _tables(depth: int = 5) -> dict[str, dict]:
    tables, _, _ = map_schema(_introspected(), "gh", "src", "", max_object_depth=depth)
    return {t["name"]: t for t in tables}


def _query(table: dict) -> str:
    return table_query(table, [column_selection(c) for c in table["columns"]], page_size=50)


# --- every mapped table's query is one the remote accepts ---


@pytest.mark.parametrize("depth", [0, 1, 5])
def test_every_table_query_is_valid_against_the_schema(depth):
    for table in _tables(depth).values():
        errors = validate(SCHEMA, parse(_query(table)))
        assert not errors, f"{table['name']}: {[e.message for e in errors]}"


def test_a_field_that_needs_an_argument_is_not_a_column():
    columns = {c["name"] for c in _tables()["gh__repository"]["columns"]}
    assert "private" not in columns  # private(login: String!)
    assert {"id", "name", "owner", "parent", "labels", "tags"} <= columns


def test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it():
    by_name = {c["name"]: c for c in _tables()["gh__repository"]["columns"]}
    # labels: [Label!] declares no arguments; tags(first: Int) does.
    parent = by_name["parent"]["gql_selection"]
    assert "labels {" in parent and "labels(" not in parent
    assert "tags(first: 100) {" in parent


def test_an_interface_column_selects_the_fields_the_interface_declares():
    owner = {c["name"]: c for c in _tables()["gh__repository"]["columns"]}["owner"]
    assert owner["gql_selection"] == "owner { login url }"


def test_a_connection_is_a_table_of_its_own_and_never_a_column():
    tables = _tables()
    columns = {c["name"] for c in tables["gh__repository"]["columns"]}
    assert not {"issues", "pinned", "watchers"} & columns
    assert "gh__repositoryIssues" in tables
    # Nor is it selected inside a nested object: an issue's author has no connections, but the
    # issue's own comments connection is not a column of the issues table either.
    assert "comments" not in {c["name"] for c in tables["gh__repositoryIssues"]["columns"]}


def test_a_type_is_entered_once_along_a_path():
    parent = {c["name"]: c for c in _tables()["gh__repository"]["columns"]}["parent"]
    # parent is a Repository; inside it, its own parent is not selected again.
    assert parent["gql_selection"].count("parent") == 1


# --- connection tables ---


def test_child_connection_table_takes_the_root_fields_arguments():
    t = _tables()["gh__repositoryIssues"]
    assert t["field_name"] == "repository"
    assert t["rows_path"] == ["issues", "nodes"]
    assert [a["name"] for a in t["required_args"]] == ["owner", "name"]
    assert t["gql_type_name"] == "Issue"
    assert t["sql_name"] == "gh__repository_issues"
    assert {"id", "title", "author"} == {c["name"] for c in t["columns"]}


def test_a_connection_that_needs_an_argument_is_not_a_table():
    assert "gh__repositoryWatchers" not in _tables()


def test_root_connection_is_a_table_of_its_nodes():
    t = _tables()["gh__advisories"]
    assert t["rows_path"] == ["nodes"]
    assert {c["name"] for c in t["columns"]} == {"id", "summary"}
    assert t["pagination"]["limit_arg"] == "first" and t["pagination"]["cursor_arg"] == "after"


def test_a_connection_without_nodes_is_read_through_its_edges():
    assert _tables()["gh__releases"]["rows_path"] == ["edges", "node"]


def test_a_connection_of_a_union_is_not_a_table():
    assert "gh__search" not in _tables()


def test_connection_query_places_the_page_arguments_on_the_connection():
    q = " ".join(_query(_tables()["gh__repositoryIssues"]).split())
    assert q.startswith("query($owner: String!, $name: String!, $pageCursor: String) {")
    assert "repository(owner: $owner, name: $name) { issues(first: 50, after: $pageCursor) {" in q
    assert q.rstrip(" }").endswith("pageInfo { hasNextPage endCursor")


# --- reading a connection ---


def _page(rows: list[dict], cursor: str | None) -> httpx.Response:
    connection = {
        "nodes": rows,
        "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
    }
    return httpx.Response(200, json={"data": {"repository": {"issues": connection}}})


@respx.mock
async def test_a_connection_is_read_page_by_page_until_the_remote_has_no_next_page():
    route = respx.post(URL).mock(
        side_effect=[_page([{"id": "1"}, {"id": "2"}], "c1"), _page([{"id": "3"}, None], None)]
    )
    t = _tables()["gh__repositoryIssues"]
    rows = (
        await execute_remote(
            URL,
            {"type": "bearer", "token": "t"},
            t["field_name"],
            ["id"],
            variables={"owner": "o", "name": "n"},
            required_args=t["required_args"],
            rows_path=t["rows_path"],
        )
    ).rows
    assert [r["id"] for r in rows] == ["1", "2", "3"]  # a null node is not a row
    sent = [json.loads(c.request.content)["variables"] for c in route.calls]
    assert [v["pageCursor"] for v in sent] == [None, "c1"]
    assert sent[0]["owner"] == "o"


@respx.mock
async def test_a_request_read_stops_at_max_rows_and_says_it_was_cut():
    """REQ-1350: a request gets the rows up to max_rows, and the statement carries
    api.answer_row_cut; the answer is marked cut so it is never cached as the table's."""
    from provisa.core.statement_warnings import collecting

    route = respx.post(URL).mock(return_value=_page([{"id": "1"}, {"id": "2"}], "more"))
    t = _tables()["gh__repositoryIssues"]
    with collecting() as found:
        answer = await execute_remote(
            URL, None, t["field_name"], ["id"], variables={"owner": "o", "name": "n"},
            required_args=t["required_args"], rows_path=t["rows_path"], max_rows=3,
        )  # fmt: skip
    assert len(answer.rows) == 3 and route.call_count == 2 and answer.cut
    assert [(w.code, w.params) for w in found] == [
        ("api.answer_row_cut", {"table": "repository.issues", "max_rows": 3})
    ]


@respx.mock
async def test_a_read_that_ends_exactly_at_max_rows_is_whole():
    from provisa.core.statement_warnings import collecting

    respx.post(URL).mock(side_effect=[_page([{"id": "1"}], "c1"), _page([{"id": "2"}], None)])
    t = _tables()["gh__repositoryIssues"]
    with collecting() as found:
        answer = await execute_remote(
            URL, None, t["field_name"], ["id"], variables={"owner": "o", "name": "n"},
            required_args=t["required_args"], rows_path=t["rows_path"], max_rows=2,
        )  # fmt: skip
    assert [r["id"] for r in answer.rows] == ["1", "2"] and not answer.cut and found == []


def _root_page(field: str, rows: list[dict], cursor: str | None) -> httpx.Response:
    """One page of a root connection (rows_path ``["nodes"]``)."""
    connection = {
        "nodes": rows,
        "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
    }
    return httpx.Response(200, json={"data": {field: connection}})


@respx.mock
async def test_a_build_reads_a_connection_a_page_at_a_time_and_fails_by_name_at_max_rows():
    """REQ-1915: a replica is the whole table. The build pulls one page per batch, and a read
    that reaches max_rows with more to read fails with replication.row_limit_reached."""
    from provisa.graphql_remote.executor import RowLimitReached, whole_connection

    t = _tables()["gh__advisories"]
    f = t["field_name"]
    route = respx.post(URL).mock(
        side_effect=[
            _root_page(f, [{"id": "1"}, {"id": "2"}], "c1"),
            _root_page(f, [{"id": "3"}], "c2"),
        ]
    )
    pages = whole_connection(
        URL, None, t["field_name"], ["id"], t["rows_path"], table="gh.advisories", max_rows=3
    )
    first = await anext(pages)
    assert [r["id"] for r in first] == ["1", "2"] and route.call_count == 1  # one page held
    with pytest.raises(RowLimitReached) as failed:
        await anext(pages)
    assert failed.value.code == "replication.row_limit_reached"
    assert failed.value.params == {"table": "gh.advisories", "max_rows": 3, "rows": 3}


@respx.mock
async def test_a_build_of_a_connection_within_max_rows_reads_it_whole():
    from provisa.graphql_remote.executor import whole_connection

    t = _tables()["gh__advisories"]
    f = t["field_name"]
    respx.post(URL).mock(
        side_effect=[_root_page(f, [{"id": "1"}], "c1"), _root_page(f, [{"id": "2"}], None)]
    )
    pages = whole_connection(
        URL, None, t["field_name"], ["id"], t["rows_path"], table="gh.advisories", max_rows=2
    )
    assert [[r["id"] for r in page] async for page in pages] == [["1"], ["2"]]


@respx.mock
async def test_a_missing_parent_has_no_rows():
    respx.post(URL).mock(return_value=httpx.Response(200, json={"data": {"repository": None}}))
    t = _tables()["gh__repositoryIssues"]
    rows = (await execute_remote(
        URL, None, t["field_name"], ["id"], variables={"owner": "o", "name": "n"},
        required_args=t["required_args"], rows_path=t["rows_path"],
    )).rows  # fmt: skip
    assert rows == []


@respx.mock
async def test_a_connection_read_raises_what_the_remote_reports():
    respx.post(URL).mock(return_value=httpx.Response(200, json={"errors": [{"message": "boom"}]}))
    t = _tables()["gh__advisories"]
    with pytest.raises(ValueError, match="boom"):
        await execute_remote(URL, None, t["field_name"], ["id"], rows_path=t["rows_path"])


@respx.mock
async def test_edges_connection_rows_are_the_edge_nodes():
    body = {
        "edges": [{"node": {"id": "r1"}}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }
    respx.post(URL).mock(return_value=httpx.Response(200, json={"data": {"releases": body}}))
    t = _tables()["gh__releases"]
    rows = (await execute_remote(URL, None, t["field_name"], ["id"], rows_path=t["rows_path"])).rows
    assert rows == [{"id": "r1"}]


@respx.mock
async def test_a_rate_limit_with_a_wait_time_is_waited_out():
    page = {"nodes": [{"id": "1"}], "pageInfo": {"hasNextPage": False, "endCursor": None}}
    route = respx.post(URL).mock(
        side_effect=[
            httpx.Response(403, headers={"retry-after": "0"}),
            httpx.Response(200, json={"data": {"advisories": page}}),
        ]
    )
    t = _tables()["gh__advisories"]
    rows = (await execute_remote(URL, None, t["field_name"], ["id"], rows_path=t["rows_path"])).rows
    assert rows == [{"id": "1"}] and route.call_count == 2


@respx.mock
async def test_a_refusal_that_names_no_wait_is_raised():
    respx.post(URL).mock(return_value=httpx.Response(403))
    t = _tables()["gh__advisories"]
    with pytest.raises(httpx.HTTPStatusError):
        await execute_remote(URL, None, t["field_name"], ["id"], rows_path=t["rows_path"])


# --- fitting a table to the credential ---


def _refusal(query: str, field: str) -> dict:
    """The error a remote returns for a field the credential may not read: its type and where
    in the query the field starts (1-based line and column)."""
    offset = query.index(field)
    line = query.count("\n", 0, offset) + 1
    column = offset - (query.rfind("\n", 0, offset) + 1) + 1
    return {
        "type": "INSUFFICIENT_SCOPES",
        "locations": [{"line": line, "column": column}],
        "message": f"The '{field}' field requires one of the following scopes: ['read:user']",
    }


@respx.mock
async def test_refused_fields_are_taken_out_and_reported():
    table = _tables()["gh__repositoryIssues"]
    seen: list[str] = []

    def answer(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        seen.append(query)
        errors = [_refusal(query, f) for f in ("email", "title") if f in query]
        # The remote reports one refused field at a time.
        return httpx.Response(200, json={"errors": errors[:1]} if errors else {"data": {}})

    respx.post(URL).mock(side_effect=answer)
    fitted, omitted = await fit_table_to_credential(
        URL, None, table, frozenset({"INSUFFICIENT_SCOPES"})
    )
    assert [o["field"] for o in omitted] == ["email", "title"]
    assert "read:user" in omitted[0]["reason"]
    by_name = {c["name"]: c for c in fitted["columns"]}
    assert "title" not in by_name  # a refused column is gone
    assert "email" not in by_name["author"]["gql_selection"]  # a refused nested field is gone
    assert "login" in by_name["author"]["gql_selection"]
    assert "email" not in {f["name"] for f in by_name["author"]["gql_object_fields"]}
    assert not validate(SCHEMA, parse(_query(fitted)))
    assert len(seen) == 3  # two refusals, then a clean answer


@respx.mock
async def test_errors_of_other_types_do_not_change_the_table():
    table = _tables()["gh__repositoryIssues"]
    not_found = {"type": "NOT_FOUND", "path": ["repository"], "message": "no such repository"}
    route = respx.post(URL).mock(return_value=httpx.Response(200, json={"errors": [not_found]}))
    fitted, omitted = await fit_table_to_credential(
        URL, None, table, frozenset({"INSUFFICIENT_SCOPES"})
    )
    assert fitted == table and omitted == [] and route.call_count == 1


@respx.mock
async def test_a_source_that_declares_no_refusal_types_is_not_probed():
    route = respx.post(URL).mock(return_value=httpx.Response(200, json={"data": {}}))
    table = _tables()["gh__advisories"]
    assert await fit_table_to_credential(URL, None, table, frozenset()) == (table, [])
    assert route.call_count == 0

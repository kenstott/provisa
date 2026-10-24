# Copyright (c) 2026 Kenneth Stott
# Canary: b1c25bb7-10ad-48ee-a9e1-c637bc40eac3
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865: a remote GraphQL table fetches its rows by its key where its remote's schema has a
field returning them for a list of keys, and the fetch is one request for the whole list.

The schemas are real (graphql-core), introspected with the query Provisa sends, and the remote
answering the fetch executes the same schema."""

# Requirements: REQ-1865

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
import respx
from graphql import build_schema, graphql_sync

from provisa.graphql_remote.by_keys import key_fetches
from provisa.graphql_remote.introspect import INTROSPECTION_QUERY
from provisa.graphql_remote.mapper import map_schema

_URL = "http://remote.test/graphql"

_ALBUMS = {1: "Let It Be", 2: "Abbey Road", 3: "Revolver"}

_HASURA = build_schema(
    """
    input Int_comparison_exp { _eq: Int, _in: [Int!] }
    input String_comparison_exp { _eq: String, _like: String }
    input albums_bool_exp {
      _and: [albums_bool_exp!]
      id: Int_comparison_exp
      title: String_comparison_exp
    }
    type albums { id: Int! title: String }
    type tracks { id: Int! name: String }
    type Query {
      albums(where: albums_bool_exp, limit: Int): [albums!]!
      tracks: [tracks!]!
    }
    """
)

_RELAY = build_schema(
    """
    interface Node { id: ID! }
    type Album implements Node { id: ID! title: String }
    type Query {
      albums: [Album!]!
      nodes(ids: [ID!]!): [Node]!
    }
    """
)


def _introspected(schema) -> dict:
    result = graphql_sync(schema, INTROSPECTION_QUERY)
    assert not result.errors, result.errors
    assert result.data is not None
    return result.data["__schema"]


def _table(schema, name: str) -> dict:
    tables, _, _ = map_schema(_introspected(schema), "", "remote", "music")
    return next(t for t in tables if t["field_name"] == name)


def test_a_where_filter_with_an_in_list_is_a_fetch_by_that_column():
    fetches = key_fetches(_introspected(_HASURA), _table(_HASURA, "albums"))
    assert fetches == {
        "id": {
            "style": "where_in",
            "field": "albums",
            "arg": "where",
            "arg_type": "albums_bool_exp",
            "key": "id",
        }
    }  # title's comparison takes no list


def test_a_table_without_one_has_no_fetch():
    assert key_fetches(_introspected(_HASURA), _table(_HASURA, "tracks")) == {}


def test_a_field_taking_a_list_of_ids_and_returning_the_type_is_a_fetch_by_id():
    fetches = key_fetches(_introspected(_RELAY), _table(_RELAY, "albums"))
    assert fetches == {
        "id": {"style": "ids", "field": "nodes", "arg": "ids", "arg_type": "[ID!]!", "on": "Album"}
    }


def test_a_table_its_root_field_requires_arguments_for_has_no_fetch():
    schema = build_schema(
        """
        input Int_comparison_exp { _in: [Int!] }
        input albums_bool_exp { id: Int_comparison_exp }
        type albums { id: Int! }
        type Query { albums(artist: Int!, where: albums_bool_exp): [albums!]! }
        """
    )
    assert key_fetches(_introspected(schema), _table(schema, "albums")) == {}


# -- the fetch ----------------------------------------------------------------------------------


class _Remote:
    """The remote, executing ``schema`` over ``_ALBUMS``; every request it is sent is kept."""

    def __init__(self, schema) -> None:
        self.schema = schema
        self.requests: list[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.requests.append(body)
        result = graphql_sync(
            self.schema,
            body["query"],
            root_value=_ROOT,
            variable_values=body.get("variables"),
        )
        assert not result.errors, result.errors
        return httpx.Response(200, json={"data": result.data})


def _albums(_info, where=None, limit=None):
    keys = (where or {}).get("id", {}).get("_in")
    return [
        {"id": i, "title": t, "__typename": "Album"}
        for i, t in _ALBUMS.items()
        if keys is None or i in keys
    ]


def _nodes(_info, ids):
    return [
        {"id": i, "title": _ALBUMS[int(i)], "__typename": "Album"} if int(i) in _ALBUMS else None
        for i in ids
    ]


_ROOT = SimpleNamespace(albums=_albums, nodes=_nodes)


@pytest.fixture(autouse=True)
def _deployment_shape():
    """The API layer says at import what deployment this is, which the fetch's egress hooks
    read (security.sandbox_egress)."""
    import provisa.api.app  # noqa: F401


def _loaded(schema, field: str):
    """A keyed loader over a registration of ``field``'s table as its registration stores it."""
    from provisa.events.source_loader import make_graphql_remote_keyed_loader

    table = _table(schema, field)
    table["by_keys"] = key_fetches(_introspected(schema), table)
    state = SimpleNamespace(
        graphql_remote_sources={"remote": {"url": _URL, "auth": None, "tables": [table]}}
    )
    return (
        make_graphql_remote_keyed_loader(state),
        SimpleNamespace(id="remote"),
        SimpleNamespace(table_name=table["sql_name"]),
    )


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("schema", [_HASURA, _RELAY], ids=["where _in", "nodes(ids:)"])
async def test_the_rows_of_a_list_of_keys_are_fetched_in_one_request(schema):
    remote = _Remote(schema)
    respx.post(_URL).mock(side_effect=remote)
    load, source, table = _loaded(schema, "albums")
    rows = await load(source, table, ["id"], [(1,), (3,)])
    assert sorted((int(r["id"]), r["title"]) for r in rows) == [(1, "Let It Be"), (3, "Revolver")]
    assert len(remote.requests) == 1
    (sent,) = remote.requests
    assert sent["variables"] in ({"where": {"id": {"_in": [1, 3]}}}, {"ids": [1, 3]})


@pytest.mark.asyncio
@respx.mock
async def test_a_key_the_remote_does_not_know_is_no_row():
    remote = _Remote(_RELAY)
    respx.post(_URL).mock(side_effect=remote)
    load, source, table = _loaded(_RELAY, "albums")
    rows = await load(source, table, ["id"], [(2,), (9,)])
    assert [(int(r["id"]), r["title"]) for r in rows] == [(2, "Abbey Road")]


@pytest.mark.asyncio
async def test_a_table_without_a_fetch_for_its_key_is_refused_by_name():
    from provisa.events.source_loader import UnsupportedSourceFetch

    load, source, table = _loaded(_HASURA, "tracks")
    with pytest.raises(UnsupportedSourceFetch, match="tracks.*no field returning its rows"):
        await load(source, table, ["id"], [(1,)])

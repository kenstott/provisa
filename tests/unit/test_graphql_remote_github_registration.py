# Copyright (c) 2026 Kenneth Stott
# Canary: a3af3a7b-0573-439f-8b90-8c266c9b4b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registration path for a bearer-authed remote schema (REQ-307/309), modeled on
GitHub's public GraphQL API (https://api.github.com/graphql). No live network call —
the generic graphql_remote machinery is what's under test, not GitHub itself."""

import httpx
import pytest
import respx

from provisa.graphql_remote.introspect import introspect_schema
from provisa.graphql_remote.mapper import map_schema

GITHUB_LIKE_URL = "https://api.github.com/graphql"

GITHUB_LIKE_SCHEMA = {
    "queryType": {"name": "Query"},
    "mutationType": None,
    "types": [
        {
            "kind": "OBJECT",
            "name": "Query",
            "fields": [
                {
                    "name": "viewer",
                    "type": {"kind": "OBJECT", "name": "User", "ofType": None},
                    "args": [],
                }
            ],
        },
        {
            "kind": "OBJECT",
            "name": "User",
            "fields": [
                {
                    "name": "login",
                    "type": {"kind": "SCALAR", "name": "String", "ofType": None},
                    "args": [],
                },
            ],
        },
    ],
}


@pytest.mark.anyio
@respx.mock
async def test_introspect_sends_bearer_pat_and_returns_schema():
    route = respx.post(GITHUB_LIKE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": GITHUB_LIKE_SCHEMA}})
    )
    schema = await introspect_schema(
        GITHUB_LIKE_URL, auth={"type": "bearer", "token": "ghp_faketoken"}
    )
    assert schema == GITHUB_LIKE_SCHEMA
    assert route.calls[0].request.headers["Authorization"] == "Bearer ghp_faketoken"


@pytest.mark.anyio
@respx.mock
async def test_introspected_schema_maps_to_registerable_table():
    respx.post(GITHUB_LIKE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"__schema": GITHUB_LIKE_SCHEMA}})
    )
    schema = await introspect_schema(
        GITHUB_LIKE_URL, auth={"type": "bearer", "token": "ghp_faketoken"}
    )
    tables, functions, relationships = map_schema(schema, "gh", "github-graphql", "")
    assert [t["name"] for t in tables] == ["gh__viewer"]
    assert [c["name"] for c in tables[0]["columns"]] == ["login"]
    assert functions == []
    assert relationships == []

# Copyright (c) 2026 Kenneth Stott
# Canary: a24a7afb-976e-4c82-84f7-d6ace38e48de
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for GraphQL Remote Schema Connector (Phase AP).

Uses the live FastAPI test client with respx mocking the remote GraphQL server.
No external services required for these tests.
"""

import os

import httpx
import pytest
import pytest_asyncio
import respx
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

REMOTE_URL = "https://remote-graphql.example.com/graphql"

SAMPLE_INTROSPECTION_RESPONSE = {
    "data": {
        "__schema": {
            "queryType": {"name": "Query"},
            "mutationType": {"name": "Mutation"},
            "types": [
                {
                    "kind": "OBJECT",
                    "name": "Query",
                    "fields": [
                        {
                            "name": "users",
                            "type": {
                                "kind": "LIST",
                                "name": None,
                                "ofType": {"kind": "OBJECT", "name": "User", "ofType": None},
                            },
                            "args": [],
                        }
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "Mutation",
                    "fields": [
                        {
                            "name": "createUser",
                            "type": {"kind": "OBJECT", "name": "CreateUserResult", "ofType": None},
                            "args": [
                                {
                                    "name": "name",
                                    "type": {"kind": "SCALAR", "name": "String", "ofType": None},
                                },
                            ],
                        }
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "User",
                    "fields": [
                        {
                            "name": "id",
                            "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                            "args": [],
                        },
                        {
                            "name": "name",
                            "type": {"kind": "SCALAR", "name": "String", "ofType": None},
                            "args": [],
                        },
                    ],
                },
                {
                    "kind": "OBJECT",
                    "name": "CreateUserResult",
                    "fields": [
                        {
                            "name": "id",
                            "type": {"kind": "SCALAR", "name": "ID", "ofType": None},
                            "args": [],
                        },
                        {
                            "name": "ok",
                            "type": {"kind": "SCALAR", "name": "Boolean", "ofType": None},
                            "args": [],
                        },
                    ],
                },
            ],
        }
    }
}


_REGISTERED_SOURCE_IDS = [
    "test-remote",
    "list-test-remote",
    "refresh-remote",
    "it-github",
    "it-gitlab",
]


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c
            from tests.helpers import delete_source_and_its_tables

            for sid in _REGISTERED_SOURCE_IDS:
                await delete_source_and_its_tables(c, sid)


class TestGraphQLRemoteSourceRegistration:
    # integration: mock-justified — respx intercepts outbound HTTP to
    # REMOTE_URL ("https://remote-graphql.example.com/graphql"), a 3rd-party
    # external GraphQL server that is not part of the docker-compose stack.
    # Tests exercise the real FastAPI ASGI transport and real app lifespan.

    @respx.mock
    async def test_register_source(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "test-remote",
                "url": REMOTE_URL,
                "namespace": "testns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_id"] == "test-remote"
        assert body["tables"] == 1
        assert body["functions"] == 1
        assert "testns__users" in body["table_names"]
        assert "testns__createUser" in body["function_names"]

    @respx.mock
    async def test_list_sources(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        # Register first
        await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "list-test-remote",
                "url": REMOTE_URL,
                "namespace": "listns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        resp = await client.get("/admin/sources/graphql-remote")
        assert resp.status_code == 200
        sources = resp.json()
        source_ids = [s["source_id"] for s in sources]
        assert "list-test-remote" in source_ids

    @respx.mock
    async def test_refresh_source(self, client):
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        # Register first
        await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "refresh-remote",
                "url": REMOTE_URL,
                "namespace": "refreshns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        # Now refresh
        respx.post(REMOTE_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_INTROSPECTION_RESPONSE)
        )
        resp = await client.post("/admin/sources/graphql-remote/refresh-remote/refresh")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source_id"] == "refresh-remote"
        assert body["tables"] == 1
        assert body["functions"] == 1

    async def test_refresh_unknown_source_returns_404(self, client):
        resp = await client.post("/admin/sources/graphql-remote/nonexistent-source/refresh")
        assert resp.status_code == 404

    @respx.mock
    async def test_registration_failure_on_bad_url(self, client):
        respx.post(REMOTE_URL).mock(return_value=httpx.Response(500, text="Server Error"))
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "bad-remote",
                "url": REMOTE_URL,
                "namespace": "badns",
                "domain_id": "",
                "auth": None,
                "cache_ttl": 300,
            },
        )
        assert resp.status_code == 422
        assert "Introspection failed" in resp.json()["detail"]


GITHUB_URL = "https://api.github.com/graphql"
_GITHUB_SOURCE = "it-github"
_GITHUB_DOMAIN = "it_github"


async def _gql(client, query: str) -> dict:
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def _github(request: httpx.Request) -> httpx.Response:
    """GitHub's endpoint for these tests: the credential check answers, a query naming a field
    outside the token's scopes is refused where that field starts, and a table read returns one
    page of issues."""
    import json

    query = json.loads(request.content)["query"]
    if request.headers.get("authorization") != "Bearer ghp_integration":
        return httpx.Response(401, json={"message": "Bad credentials"})
    if "viewer { login }" in query:
        return httpx.Response(200, json={"data": {"viewer": {"login": "octocat"}}})
    if "body_text: bodyText" in query and "\n" not in query:
        refusal = {
            "type": "INSUFFICIENT_SCOPES",
            "locations": [{"line": 1, "column": query.index("body_text: bodyText") + 1}],
            "message": "The 'bodyText' field requires one of the following scopes: ['read:x']",
        }
        return httpx.Response(200, json={"errors": [refusal]})
    page = {
        "nodes": [{"title": "First issue", "number": 1}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }
    return httpx.Response(200, json={"data": {"repository": {"issues": page}}})


class TestGitHubBrandedSource:
    """REQ-1923: GitHub is added by its brand and a token, registers no tables, offers every
    table of its shipped schema, and registers the ones the steward picks through the same
    mutation every source uses.

    integration: mock-justified -- respx intercepts outbound HTTP to api.github.com, a
    third-party service outside the compose stack. The app, its control plane, the secrets
    vault and the model store are real.
    """

    @respx.mock
    async def test_a_rejected_token_adds_nothing(self, client):
        respx.post(GITHUB_URL).mock(side_effect=_github)
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "it-github-bad",
                "brand": "github",
                "auth": {"type": "bearer", "token": "wrong"},
            },
        )
        assert resp.status_code == 422, resp.text
        assert "did not accept the access token" in resp.json()["detail"]
        listed = (await _gql(client, "{ sources { id } }"))["sources"]
        assert "it-github-bad" not in {s["id"] for s in listed}

    @respx.mock
    async def test_adding_github_registers_no_tables_and_offers_them_all(self, client):
        respx.post(GITHUB_URL).mock(side_effect=_github)
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": _GITHUB_SOURCE,
                "brand": "github",
                "auth": {"type": "bearer", "token": "ghp_integration"},
                "description": "GitHub",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["tables"] == 0 and body["available_tables"] > 100

        data = await _gql(client, "{ sources { id type federationHintsJson } tables { sourceId } }")
        mine = next(s for s in data["sources"] if s["id"] == _GITHUB_SOURCE)
        assert mine["type"] == "graphql_remote"
        assert '"brand": "github"' in mine["federationHintsJson"]
        assert "ghp_integration" not in str(mine)  # the token is in the vault, not on the row
        listed = (await client.get("/admin/sources/graphql-remote")).json()
        entry = next(r for r in listed if r["source_id"] == _GITHUB_SOURCE)
        assert entry["auth"] == {"type": "bearer"}  # the scheme leaves the server, not the token
        assert "ghp_integration" not in str(listed)
        assert _GITHUB_SOURCE not in {t["sourceId"] for t in data["tables"]}

        offered = await _gql(
            client,
            f'{{ availableSchemas(sourceId: "{_GITHUB_SOURCE}") '
            f'availableTables(sourceId: "{_GITHUB_SOURCE}", schemaName: "graphql") '
            "{ name comment } }",
        )
        assert offered["availableSchemas"] == ["graphql"]
        names = {t["name"] for t in offered["availableTables"]}
        assert {"gh__repository", "gh__repository_issues", "gh__viewer"} <= names

        columns = (
            await _gql(
                client,
                f'{{ availableColumnsMetadata(sourceId: "{_GITHUB_SOURCE}", '
                'schemaName: "graphql", tableName: "gh__repository_issues") { name dataType } }',
            )
        )["availableColumnsMetadata"]
        types = {c["name"]: c["dataType"] for c in columns}
        assert types["title"] == "varchar" and types["_nf_owner"] == "varchar"

    @respx.mock
    async def test_registering_a_table_fits_it_to_the_token_and_survives_a_reload(self, client):
        respx.post(GITHUB_URL).mock(side_effect=_github)
        await _gql(
            client,
            f'mutation {{ createDomain(input: {{ id: "{_GITHUB_DOMAIN}", description: "" }}) '
            "{ success } }",
        )
        result = (
            await _gql(
                client,
                f'mutation {{ registerTable(input: {{ sourceId: "{_GITHUB_SOURCE}", '
                f'domainId: "{_GITHUB_DOMAIN}", schemaName: "graphql", '
                'tableName: "gh__repository_issues", columns: [] }) '
                "{ success message code params } }",
            )
        )["registerTable"]
        assert result["success"], result
        assert result["code"] == "schema.table_registered_fields_omitted", result
        assert "body_text" in result["params"]["fields"]

        from provisa.api.app import state
        from provisa.api.app_loaders import _load_graphql_remote_sources_from_db

        def registered() -> dict:
            reg = state.graphql_remote_sources[_GITHUB_SOURCE]
            return next(t for t in reg["tables"] if t["sql_name"] == "gh__repository_issues")

        table = registered()
        assert table["field_name"] == "repository"
        assert table["rows_path"] == ["issues", "nodes"]
        names = {c["name"] for c in table["columns"]}
        assert "title" in names and "body_text" not in names

        # A process that never took the registration rebuilds it from the registry: the token
        # from the vault, how the table is read from the shipped schema.
        del state.graphql_remote_sources[_GITHUB_SOURCE]
        await _load_graphql_remote_sources_from_db()
        reg = state.graphql_remote_sources[_GITHUB_SOURCE]
        assert reg["auth"] == {"type": "bearer", "token": "ghp_integration"}
        assert reg["brand"] == "github"
        table = registered()
        assert table["rows_path"] == ["issues", "nodes"]
        assert [a["name"] for a in table["required_args"]] == ["owner", "name"]

        # And the table reads: the query goes to the connection, with the token.
        from provisa.graphql_remote.executor import execute_remote

        rows = await execute_remote(
            reg["url"],
            reg["auth"],
            table["field_name"],
            ["title", "number"],
            variables={"owner": "apache", "name": "calcite"},
            required_args=table["required_args"],
            rows_path=table["rows_path"],
            error_policy=reg["error_policy"],
        )
        assert rows == [{"title": "First issue", "number": 1}]

    @respx.mock
    async def test_a_branded_source_is_not_refreshed(self, client):
        respx.post(GITHUB_URL).mock(side_effect=_github)
        resp = await client.post(f"/admin/sources/graphql-remote/{_GITHUB_SOURCE}/refresh")
        assert resp.status_code == 409, resp.text


GITLAB_URL = "https://gitlab.com/api/graphql"
_GITLAB_SOURCE = "it-gitlab"


def _gitlab(request: httpx.Request) -> httpx.Response:
    """GitLab's endpoint for these tests: it knows one token, and it prices a query -- one that
    selects more than a handful of fields is refused with its score."""
    import json

    query = json.loads(request.content)["query"]
    if "currentUser" in query:
        known = request.headers.get("authorization") == "Bearer glpat_integration"
        user = {"username": "octo"} if known else None
        return httpx.Response(200, json={"data": {"currentUser": user}})
    if len(query) > 400:
        reason = "Query has complexity of 1733, which exceeds max complexity of 200"
        return httpx.Response(200, json={"errors": [{"message": reason}]})
    return httpx.Response(200, json={"data": {"project": None}})


class TestGitLabBrandedSource:
    """REQ-1923: GitLab prices a query and caps the price, so a wide table is registered with
    the columns wanted, and the remote's own answer decides whether a selection is served.

    integration: mock-justified -- respx intercepts outbound HTTP to gitlab.com, a third-party
    service outside the compose stack.
    """

    @respx.mock
    async def test_a_token_gitlab_does_not_recognize_adds_nothing(self, client):
        respx.post(GITLAB_URL).mock(side_effect=_gitlab)
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": "it-gitlab-bad",
                "brand": "gitlab",
                "auth": {"type": "bearer", "token": "stale"},
            },
        )
        assert resp.status_code == 422, resp.text
        assert "did not recognize the credential" in resp.json()["detail"]

    @respx.mock
    async def test_a_wide_table_is_refused_with_gitlabs_numbers_and_a_narrow_one_registers(
        self, client
    ):
        respx.post(GITLAB_URL).mock(side_effect=_gitlab)
        resp = await client.post(
            "/admin/sources/graphql-remote",
            json={
                "source_id": _GITLAB_SOURCE,
                "brand": "gitlab",
                "auth": {"type": "bearer", "token": "glpat_integration"},
            },
        )
        assert resp.status_code == 200, resp.text
        await _gql(
            client,
            f'mutation {{ createDomain(input: {{ id: "{_GITHUB_DOMAIN}", description: "" }}) '
            "{ success } }",
        )

        def register(columns: str) -> str:
            return (
                f'mutation {{ registerTable(input: {{ sourceId: "{_GITLAB_SOURCE}", '
                f'domainId: "{_GITHUB_DOMAIN}", schemaName: "graphql", '
                f'tableName: "gl__project_issues", columns: [{columns}] }}) '
                "{ success message code params } }"
            )

        wide = (await _gql(client, register("")))["registerTable"]
        assert wide["success"] is False
        assert wide["code"] == "schema.table_too_complex", wide
        assert "complexity of 1733" in wide["params"]["reason"]
        tables = (await _gql(client, "{ tables { sourceId tableName } }"))["tables"]
        assert _GITLAB_SOURCE not in {t["sourceId"] for t in tables}

        narrow = (
            await _gql(
                client,
                register(
                    '{ name: "iid", visibleTo: ["org_admin"] }, '
                    '{ name: "title", visibleTo: ["org_admin"] }'
                ),
            )
        )["registerTable"]
        assert narrow["success"], narrow

        from provisa.api.app import state

        table = next(
            t
            for t in state.graphql_remote_sources[_GITLAB_SOURCE]["tables"]
            if t["sql_name"] == "gl__project_issues"
        )
        assert {c["name"] for c in table["columns"]} == {"iid", "title"}
        assert [a["name"] for a in table["required_args"]] == ["fullPath"]

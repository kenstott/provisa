# Copyright (c) 2026 Kenneth Stott
# Canary: 2e6b9f40-1c7a-4d85-b3e2-8a5d0c4f7e19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GitHub as a branded source carried by the remote GraphQL source (REQ-1923).

These run against the schema that ships with Provisa. The remote is mocked: what GitHub itself
accepts is checked by reading every table live, which no unit test can do.
"""

import json
from types import SimpleNamespace

import httpx
import pytest
import respx
from graphql import parse

from provisa.api.admin import _graphql_table_registration as registration
from provisa.core.models import Column
from provisa.graphql_remote.brands import (
    BRANDS,
    available_tables,
    brand_of,
    brand_schema,
    map_table,
    table_spec,
)
from provisa.graphql_remote.mapper import map_schema
from provisa.graphql_remote.probe import QueryTooComplex
from provisa.graphql_remote.executor import column_selection, execute_remote, table_query

GITHUB = BRANDS["github"]


def _reg(**over) -> dict:
    return {
        "source_id": "my-github",
        "url": GITHUB.url,
        "namespace": "gh",
        "auth": GITHUB.auth("t0ken"),
        "brand": "github",
        "tables": [],
        **over,
    }


# --- the brand ---


def test_a_source_row_names_its_brand_in_its_hints():
    assert brand_of({"brand": "github", "namespace": "gh"}) is GITHUB
    assert brand_of({"namespace": "shop"}) is None
    assert brand_of(None) is None


def test_a_row_naming_a_brand_this_build_does_not_carry_is_an_error():
    with pytest.raises(ValueError, match="does not carry"):
        brand_of({"brand": "gitlab-of-the-future"})


def test_the_shipped_schema_offers_githubs_tables_under_the_sources_namespace():
    names = {t["sql_name"] for t in available_tables(GITHUB, "gh")}
    assert {"gh__viewer", "gh__repository", "gh__repository_issues"} <= names
    assert {"gh__repository_pull_requests", "gh__organization_repositories"} <= names
    assert {t["sql_name"] for t in available_tables(GITHUB, "hub")} >= {"hub__repository_issues"}


def test_offering_tables_does_not_map_their_columns():
    assert all(t["columns"] == [] for t in available_tables(GITHUB, "gh"))


def test_a_table_spec_says_how_the_table_is_read():
    spec = table_spec(GITHUB, "gh", "gh__repository_issues")
    assert spec is not None
    assert spec["field_name"] == "repository"
    assert spec["rows_path"] == ["issues", "nodes"]
    assert [a["name"] for a in spec["required_args"]] == ["owner", "name"]
    assert table_spec(GITHUB, "gh", "gh__no_such_table") is None


def test_every_offered_table_maps_to_a_query_that_parses():
    tables, _, _ = map_schema(
        brand_schema("github"), "gh", "my-github", "", max_object_depth=GITHUB.max_object_depth
    )
    assert {t["sql_name"] for t in tables} == {
        t["sql_name"] for t in available_tables(GITHUB, "gh")
    }
    for table in tables:
        assert table["columns"], table["sql_name"]
        parse(table_query(table, [column_selection(c) for c in table["columns"]]))


def test_one_table_maps_the_same_alone_as_with_the_rest():
    alone = map_table(GITHUB, "gh", "my-github", "", "gh__repository_issues")
    tables, _, _ = map_schema(
        brand_schema("github"), "gh", "my-github", "", max_object_depth=GITHUB.max_object_depth
    )
    assert alone == next(t for t in tables if t["sql_name"] == "gh__repository_issues")


def test_no_github_column_is_a_connection_or_needs_an_argument():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__repository")
    names = {c["name"] for c in table["columns"]}
    # Connections are tables of their own; ref(qualifiedName: String!) needs an argument.
    assert not {"issues", "pullRequests", "stargazers", "ref", "release"} & names
    assert {"name", "createdAt", "isPrivate", "owner", "licenseInfo"} <= names
    for column in table["columns"]:
        assert "(first:" not in (column.get("gql_selection") or ""), column["name"]


# --- what the Register Table picker is given ---


async def test_the_picker_is_offered_every_table_by_its_registered_name():
    state = SimpleNamespace(graphql_remote_sources={"my-github": _reg()})
    offered = await registration.source_offer(state, "my-github")
    assert offered is not None and offered[0] is GITHUB
    tables = {t["name"]: t for t in registration.offered_tables(*offered)}
    assert "gh__repository_issues" in tables
    assert tables["gh__repository_issues"]["description"]
    assert await registration.source_offer(state, "absent") is None


def test_offered_columns_are_typed_and_include_the_required_filters():
    columns = {
        name: data_type
        for name, data_type, _ in registration.offered_columns(
            GITHUB, _reg(), "gh__repository_issues"
        )
    }
    assert columns["title"] == "varchar"
    assert columns["number"] == "integer"
    assert columns["closed"] == "boolean"
    assert columns["author"] == "json"
    assert columns["_nf_owner"] == "varchar" and columns["_nf_name"] == "varchar"
    assert all(columns.values())


# --- registering a table ---


def _refuse(query: str, field: str) -> dict:
    """GitHub's answer for a field outside the token's scopes: where in the (one-line) query
    the field starts -- at its alias, when it has one."""
    return {
        "type": "INSUFFICIENT_SCOPES",
        "locations": [{"line": 1, "column": query.index(f" {field}") + 2}],
        "message": f"The '{field}' field requires one of the following scopes: ['read:x']",
    }


@respx.mock
async def test_registering_leaves_out_what_the_token_may_not_read_and_says_so():
    def answer(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        assert "\n" not in query  # the probe is one line, so a column locates a field
        errors = [_refuse(query, f) for f in ("body_text: bodyText",) if f in query]
        return httpx.Response(200, json={"errors": errors} if errors else {"data": {}})

    route = respx.post(GITHUB.url).mock(side_effect=answer)
    columns, omitted, _ = await registration.columns_to_register(
        GITHUB, _reg(), "gh__repository_issues", "eng", [], 100
    )
    assert route.calls[0].request.headers["authorization"] == "Bearer t0ken"
    assert [o["field"] for o in omitted] == ["body_text"]
    assert "read:x" in omitted[0]["reason"]
    by_name = {c.name: c for c in columns}
    assert "body_text" not in by_name and "title" in by_name
    assert by_name["_nf_owner"].native_filter_type == "query_param"
    assert (by_name["author"].gql_selection or "").startswith("author {")
    assert all(c.data_type for c in columns)


@respx.mock
async def test_only_the_chosen_columns_register_and_keep_their_governance():
    respx.post(GITHUB.url).mock(return_value=httpx.Response(200, json={"data": {}}))
    chosen = [
        Column(name="title", visible_to=["analyst"]),
        Column(name="number", visible_to=["analyst"], alias="issueNumber"),
    ]
    columns, omitted, _ = await registration.columns_to_register(
        GITHUB, _reg(), "gh__repository_issues", "eng", chosen, 100
    )
    assert omitted == []
    by_name = {c.name: c for c in columns}
    # The two picked, plus the filters the table cannot be read without.
    assert set(by_name) == {"title", "number", "_nf_owner", "_nf_name"}
    assert by_name["title"].visible_to == ["analyst"] and by_name["title"].data_type == "varchar"
    assert by_name["number"].alias == "issueNumber" and by_name["number"].data_type == "integer"


@respx.mock
async def test_a_table_the_token_may_not_read_at_all_has_no_columns_to_register():
    def answer(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        refusal = {
            "type": "INSUFFICIENT_SCOPES",
            "locations": [{"line": 1, "column": query.index("savedReplies") + 1}],
            "message": "The 'savedReplies' field requires one of the following scopes: ['read:user']",
        }
        return httpx.Response(200, json={"errors": [refusal]})

    respx.post(GITHUB.url).mock(side_effect=answer)
    columns, omitted, _ = await registration.columns_to_register(
        GITHUB, _reg(), "gh__viewer_saved_replies", "eng", [], 100
    )
    assert not [c for c in columns if c.native_filter_type is None]
    assert "read:user" in omitted[0]["reason"]


# --- reading ---


@respx.mock
async def test_a_field_github_withholds_on_one_row_is_null_in_that_row():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__repository_forks")
    forbidden = {
        "type": "FORBIDDEN",
        "path": ["repository", "forks", "nodes", 0, "viewerPermission"],
        "message": "You do not have permission",
    }
    page = {
        "nodes": [{"name": "calcite", "viewerPermission": None}],
        "pageInfo": {"hasNextPage": False, "endCursor": None},
    }
    respx.post(GITHUB.url).mock(
        return_value=httpx.Response(
            200, json={"data": {"repository": {"forks": page}}, "errors": [forbidden]}
        )
    )
    rows = (await execute_remote(
        GITHUB.url, None, table["field_name"], ["name", "viewerPermission"],
        variables={"owner": "o", "name": "n"}, required_args=table["required_args"],
        rows_path=table["rows_path"], error_policy=GITHUB.error_policy,
    )).rows  # fmt: skip
    assert rows == [{"name": "calcite", "viewerPermission": None}]


@respx.mock
async def test_github_refusing_the_table_itself_fails_the_read():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__organization_domains")
    forbidden = {
        "type": "FORBIDDEN",
        "path": ["organization", "domains"],
        "message": "does not have the right permission to retrieve domains",
    }
    respx.post(GITHUB.url).mock(
        return_value=httpx.Response(
            200, json={"data": {"organization": {"domains": None}}, "errors": [forbidden]}
        )
    )
    with pytest.raises(ValueError, match="retrieve domains"):
        await execute_remote(
            GITHUB.url, None, table["field_name"], ["domain"], variables={"login": "apache"},
            required_args=table["required_args"], rows_path=table["rows_path"],
            error_policy=GITHUB.error_policy,
        )  # fmt: skip


@respx.mock
async def test_a_page_the_gateway_gives_up_on_is_asked_for_again_at_half_the_size():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__repository_forks")
    page = {"nodes": [{"name": "a"}], "pageInfo": {"hasNextPage": False, "endCursor": None}}
    route = respx.post(GITHUB.url).mock(
        side_effect=[
            httpx.Response(502),
            httpx.Response(200, json={"data": {"repository": {"forks": page}}}),
        ]
    )
    rows = (await execute_remote(
        GITHUB.url, None, table["field_name"], ["name"], variables={"owner": "o", "name": "n"},
        required_args=table["required_args"], rows_path=table["rows_path"], limit=100,
    )).rows  # fmt: skip
    assert rows == [{"name": "a"}]
    sent = [json.loads(c.request.content)["query"] for c in route.calls]
    assert "first: 100" in sent[0] and "first: 50" in sent[1]


@respx.mock
async def test_a_page_that_costs_more_than_github_computes_is_asked_for_at_half_the_size():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__repository_watchers")
    page = {"nodes": [{"login": "a"}], "pageInfo": {"hasNextPage": False, "endCursor": None}}
    too_much = {
        "type": "RESOURCE_LIMITS_EXCEEDED",
        "path": ["repository", "watchers", "nodes", 13, "contributionsCollection"],
        "message": "Resource limits for this query exceeded.",
    }
    route = respx.post(GITHUB.url).mock(
        side_effect=[
            httpx.Response(200, json={"data": {"repository": None}, "errors": [too_much]}),
            httpx.Response(200, json={"data": {"repository": {"watchers": page}}}),
        ]
    )
    rows = (await execute_remote(
        GITHUB.url, None, table["field_name"], ["login"], variables={"owner": "o", "name": "n"},
        required_args=table["required_args"], rows_path=table["rows_path"], limit=40,
        error_policy=GITHUB.error_policy,
    )).rows  # fmt: skip
    assert rows == [{"login": "a"}]
    sent = [json.loads(c.request.content)["query"] for c in route.calls]
    assert "first: 40" in sent[0] and "first: 20" in sent[1]


@respx.mock
async def test_a_single_row_github_will_not_compute_fails_the_read_and_says_why():
    table = map_table(GITHUB, "gh", "my-github", "", "gh__topic_stargazers")
    timeout = {"message": "Something went wrong while executing your query on 2026-10-02."}
    route = respx.post(GITHUB.url).mock(
        return_value=httpx.Response(200, json={"errors": [timeout]})
    )
    with pytest.raises(ValueError, match="fewer columns"):
        await execute_remote(
            GITHUB.url, None, table["field_name"], ["login"], variables={"name": "sql"},
            required_args=table["required_args"], rows_path=table["rows_path"], limit=4,
            error_policy=GITHUB.error_policy,
        )  # fmt: skip
    assert route.call_count == 3  # 4 rows, then 2, then 1


def test_githubs_untyped_timeout_counts_as_a_page_that_asked_too_much():
    policy = GITHUB.error_policy
    timeout = {"message": "Something went wrong while executing your query on 2026-10-02T18:55Z."}
    assert policy.overloaded_by([timeout]) == ["Something went wrong while executing your query"]
    assert policy.overloaded_by([{"type": "RESOURCE_LIMITS_EXCEEDED", "message": "x"}]) == [
        "RESOURCE_LIMITS_EXCEEDED"
    ]
    assert policy.overloaded_by([{"type": "FORBIDDEN", "message": "Something went wrong"}]) == []
    assert policy.overloaded_by([{"message": "Field 'x' doesn't exist"}]) == []


# --- coming back after a restart ---


def test_a_registered_table_gets_its_read_details_back_from_the_shipped_schema():
    from provisa.api.app_loaders import _apply_brand_table_spec

    # What the registry rebuilds on its own: the name, and a root field guessed from it.
    table = {"sql_name": "gh__repository_issues", "field_name": "repositoryIssues", "columns": []}
    assert _apply_brand_table_spec(table, GITHUB, "gh") is True
    assert table["field_name"] == "repository"
    assert table["rows_path"] == ["issues", "nodes"]
    assert [a["gql_type"] for a in table["required_args"]] == ["String!", "String!"]
    assert table["pagination"]["cursor_arg"] == "after"


def test_a_registered_table_the_shipped_schema_no_longer_offers_is_reported():
    from provisa.api.app_loaders import _apply_brand_table_spec

    assert _apply_brand_table_spec({"sql_name": "gh__gone"}, GITHUB, "gh") is False


async def test_the_token_comes_back_from_the_sources_row(monkeypatch):
    from provisa.api.app_loaders import _graphql_remote_auth

    monkeypatch.setenv("GH_PAT", "from-env")
    state = SimpleNamespace(active_org_id="org", admin_db=None)
    row = {
        "id": "my-github",
        "username": "",
        "password_ref": "${env:GH_PAT}",
        "federation_hints": {"brand": "github", "namespace": "gh", "auth_type": "bearer"},
    }
    assert await _graphql_remote_auth(state, row) == {"type": "bearer", "token": "from-env"}
    assert await _graphql_remote_auth(state, {**row, "federation_hints": {}}) is None
    with pytest.raises(ValueError, match="unknown auth_type"):
        await _graphql_remote_auth(state, {**row, "federation_hints": {"auth_type": "x"}})


def test_a_sources_credential_does_not_leave_the_server():
    from provisa.api.admin.graphql_remote_router import _auth_without_secret

    assert _auth_without_secret({"type": "bearer", "token": "t0ken"}) == {"type": "bearer"}
    assert _auth_without_secret({"type": "basic", "username": "u", "password": "p"}) == {
        "type": "basic",
        "username": "u",
    }
    assert _auth_without_secret(None) is None


# --- GitLab: a remote that prices a query and caps the price ---

GITLAB = BRANDS["gitlab"]


def _gitlab_reg() -> dict:
    return {
        "source_id": "my-gitlab",
        "url": GITLAB.url,
        "namespace": "gl",
        "auth": GITLAB.auth("glpat"),
        "brand": "gitlab",
    }


def test_gitlabs_shipped_schema_offers_its_tables():
    names = {t["sql_name"] for t in available_tables(GITLAB, "gl")}
    assert {"gl__project", "gl__project_issues", "gl__project_merge_requests"} <= names
    spec = table_spec(GITLAB, "gl", "gl__project_issues")
    assert spec is not None
    assert spec["rows_path"] == ["issues", "nodes"]
    assert [a["name"] for a in spec["required_args"]] == ["fullPath"]


@respx.mock
async def test_a_table_the_remote_prices_too_high_is_not_registered_and_carries_its_numbers():
    reason = "Query has complexity of 1733, which exceeds max complexity of 200"
    route = respx.post(GITLAB.url).mock(
        return_value=httpx.Response(200, json={"errors": [{"message": reason}]})
    )
    with pytest.raises(QueryTooComplex) as refused:
        await registration.columns_to_register(
            GITLAB, _gitlab_reg(), "gl__project_issues", "eng", [], 100
        )
    assert refused.value.reason == reason
    # The query is priced at the page size a read will ask for.
    assert "first: 100" in json.loads(route.calls[0].request.content)["query"]


@respx.mock
async def test_a_query_refused_as_too_large_with_a_client_error_status_says_so():
    respx.post(GITLAB.url).mock(
        return_value=httpx.Response(422, json={"errors": [{"message": "Query too large"}]})
    )
    with pytest.raises(QueryTooComplex, match="Query too large"):
        await registration.columns_to_register(GITLAB, _gitlab_reg(), "gl__project", "eng", [], 100)


@respx.mock
async def test_the_columns_picked_are_what_the_remote_is_asked_to_price():
    route = respx.post(GITLAB.url).mock(return_value=httpx.Response(200, json={"data": {}}))
    chosen = [Column(name="iid", visible_to=["analyst"]), Column(name="title", visible_to=[])]
    columns, omitted, _ = await registration.columns_to_register(
        GITLAB, _gitlab_reg(), "gl__project_issues", "eng", chosen, 100
    )
    assert omitted == []
    assert {c.name for c in columns} == {"iid", "title", "_nf_full_path"}
    query = json.loads(route.calls[0].request.content)["query"]
    assert "nodes { iid title }" in query


@respx.mock
async def test_an_error_that_is_not_about_cost_does_not_refuse_the_table():
    not_found = {"message": "The resource that you are attempting to access does not exist"}
    respx.post(GITLAB.url).mock(return_value=httpx.Response(200, json={"errors": [not_found]}))
    columns, _, _ = await registration.columns_to_register(
        GITLAB, _gitlab_reg(), "gl__project_issues", "eng", [Column(name="iid", visible_to=[])], 100
    )
    assert {c.name for c in columns} == {"iid", "_nf_full_path"}


@respx.mock
async def test_a_credential_the_remote_answers_with_null_is_not_accepted():
    from provisa.api.admin.graphql_remote_router import _verify_live_auth

    respx.post(GITLAB.url).mock(
        side_effect=[
            httpx.Response(200, json={"data": {"currentUser": None}}),
            httpx.Response(200, json={"data": {"currentUser": {"username": "octo"}}}),
        ]
    )
    with pytest.raises(ValueError, match="did not recognize the credential"):
        await _verify_live_auth(GITLAB.url, GITLAB.auth("stale"), GITLAB.verify_query)
    await _verify_live_auth(GITLAB.url, GITLAB.auth("good"), GITLAB.verify_query)

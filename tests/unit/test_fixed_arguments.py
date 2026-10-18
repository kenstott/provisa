# Copyright (c) 2026 Kenneth Stott
# Canary: 3638ee21-665c-4fe5-b36a-fc32eba46b81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2061: a table may fix the arguments its source takes by name.

A fixed argument is sent on every read of the table -- an OpenAPI operation's path or query
parameter, a GraphQL root field's argument -- and is no parameter of the table, so a table whose
arguments are all fixed is a whole that lands like any table (a TTL -1 replica among them)."""

# Requirements: REQ-2061

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
import respx
from sqlalchemy import insert, select

from provisa.core.models import Column, ProvisaConfig, Table
from provisa.core.schema_org import domains, registered_tables, sources

_VIS = ["admin"]


def _columns(*names: str) -> list[Column]:
    return [Column(name=n, data_type="text", visible_to=_VIS) for n in names]


def test_an_argument_is_fixed_or_a_parameter_never_both():
    with pytest.raises(ValueError, match=r"owner are fixed and also parameters"):
        Table(
            source_id="api",
            domain_id="d",
            schema="openapi",
            table="t",
            arguments={"owner": "acme"},
            columns=[
                *_columns("id"),
                Column(
                    name="_nf_owner",
                    data_type="text",
                    visible_to=[],
                    native_filter_type="path_param",
                    native_filter_required=True,
                ),
            ],
        )
    fixed = Table(
        source_id="api", domain_id="d", schema="openapi", table="t", arguments={"owner": "acme"},
        columns=_columns("id"),
    )  # fmt: skip
    assert fixed.arguments == {"owner": "acme"}


def test_a_table_with_its_arguments_fixed_is_copied_whole():
    from provisa.federation.replica_converge import whole_copy

    table = {"columns": [{"name": "id", "native_filter_type": None}], "row_materialize": False}
    assert whole_copy(SimpleNamespace(type="openapi"), table, engine=None)


def test_arguments_are_fixed_only_on_an_api_that_takes_them_by_name():
    from provisa.core.config_loader import _validate_table_arguments

    config = ProvisaConfig.model_validate(
        {
            "sources": [
                {"id": "pg", "type": "postgresql", "host": "h", "port": 5432, "database": "d",
                 "username": "u", "password": "p"},
            ],
            "domains": [{"id": "d"}],
            "roles": [],
            "tables": [
                {"source_id": "pg", "domain_id": "d", "schema": "public", "table": "t",
                 "arguments": {"x": 1}, "columns": [{"name": "id", "data_type": "integer",
                                                     "visible_to": _VIS}]},
            ],
        }
    )  # fmt: skip
    with pytest.raises(
        ValueError, match=r"graphql_remote, openapi or jira source, not 'postgresql'"
    ):
        _validate_table_arguments(config)


# --- OpenAPI: a path parameter fixed, the table landed and read with it ---

BASE = "https://orgs.test"
SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/orgs/{org}/alerts": {
            "get": {
                "operationId": "listAlerts",
                "parameters": [
                    {"name": "org", "in": "path", "required": True, "schema": {"type": "string"}},
                    {"name": "state", "in": "query", "schema": {"type": "string"}},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"id": {"type": "string"}},
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}


def _alerts(arguments: dict) -> Table:
    return Table(
        source_id="gh",
        domain_id="d",
        schema="openapi",
        table="listAlerts",
        arguments=arguments,
        columns=_columns("id"),
    )


@pytest.fixture
async def org_db(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="gh", type="openapi"))
    return db


async def _register(db, table: Table):
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import register_openapi_source
    from provisa.core.repositories import table as table_repo

    state = SimpleNamespace(
        openapi_specs={"gh": {"spec": SPEC, "base_url": BASE, "auth_config": None}},
        api_endpoints={},
        api_sources={},
    )
    async with db.acquire() as conn:
        await register_openapi_source(conn, "gh", BASE)
        await table_repo.upsert(conn, table)
        assert await persist_openapi_endpoint(state, conn, table) is None
        state.api_endpoints, state.api_sources = await load_api_sources(conn, {})
        stored = (await conn.execute_core(select(registered_tables.c.arguments))).scalar_one()
    return state, stored


@respx.mock
async def test_an_openapi_table_is_read_with_its_fixed_path_and_query_parameters(org_db):
    from provisa.api_source.caller import answer_rows, call_api

    state, stored = await _register(org_db, _alerts({"org": "acme", "state": "open"}))
    assert stored == {"org": "acme", "state": "open"}
    route = respx.get(f"{BASE}/orgs/acme/alerts").mock(
        return_value=httpx.Response(200, json=[{"id": "a1"}, {"id": "a2"}])
    )
    endpoint = state.api_endpoints[("gh", "listAlerts")]
    answer = await call_api(endpoint, dict(endpoint.default_params), base_url=BASE, auth=None)
    rows, _ = answer_rows(endpoint, answer)
    assert [r["id"] for r in rows] == ["a1", "a2"]
    assert route.calls.last.request.url.params["state"] == "open"


async def test_an_argument_the_operation_does_not_take_is_refused_by_name(org_db):
    with pytest.raises(ValueError, match=r"argument\(s\) team are not parameters of its operation"):
        await _register(org_db, _alerts({"org": "acme", "team": "x"}))


# --- GraphQL: a root field's arguments fixed, sent on every page of a landing ---

URL = "https://api.github.test/graphql"


@respx.mock
async def test_a_connection_lands_with_its_fixed_arguments_on_every_page():
    from provisa.graphql_remote.executor import whole_connection
    from tests.unit.test_graphql_remote_relay import _tables

    t = _tables()["gh__repositoryIssues"]

    def page(nodes, cursor):
        info = {"hasNextPage": cursor is not None, "endCursor": cursor}
        return httpx.Response(
            200, json={"data": {"repository": {"issues": {"nodes": nodes, "pageInfo": info}}}}
        )

    route = respx.post(URL).mock(side_effect=[page([{"id": "1"}], "c1"), page([{"id": "2"}], None)])
    pages = whole_connection(
        URL, None, t["field_name"], ["id"], t["rows_path"], table="gh.issues", max_rows=10,
        variables={"owner": "acme", "name": "payments"}, required_args=t["required_args"],
    )  # fmt: skip
    assert [[r["id"] for r in p] async for p in pages] == [["1"], ["2"]]
    for call in route.calls:
        body = json.loads(call.request.content)
        assert body["variables"]["owner"] == "acme" and body["variables"]["name"] == "payments"
        assert ": String!" in body["query"] and "repository(owner: " in body["query"]


async def test_a_live_read_sends_the_fixed_arguments_with_the_ones_it_gives(monkeypatch):
    from provisa.api.data import materialization
    from provisa.graphql_remote import executor

    sent: dict = {}

    async def execute_remote(**kwargs):
        sent.update(kwargs)
        return executor.RemoteAnswer([])

    monkeypatch.setattr(executor, "execute_remote", execute_remote)
    table = {"name": "t", "arguments": {"owner": "acme"}, "required_args": [], "rows_path": None}
    await materialization._fetch_gql_remote_rows(
        {"url": URL}, table, ["id"], {"name": "payments"}, {}, None, None
    )
    assert sent["variables"] == {"owner": "acme", "name": "payments"}

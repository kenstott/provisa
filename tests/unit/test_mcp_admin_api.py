# Copyright (c) 2026 Kenneth Stott
# Canary: 33392bbe-2173-414d-897f-f4152edc90df
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114: Polly's reach into the admin API the UI uses.

She reads the real admin GraphQL schema and runs its queries as the user (a mutation is never run
here: it is confirmed by the user and sent by their browser); she lists the UI's REST routes; she
runs the Relationships page's own discovery for one table as the user; and an Explore question
reaches all nine surfaces."""

# Requirements: REQ-2114

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI, UploadFile
from pydantic import BaseModel

from provisa.api.mcp import admin_api
from provisa.api.mcp.tools import science_surfaces

pytestmark = pytest.mark.unit


def test_the_schema_lists_its_mutations_as_signatures_narrowed_by_a_word():
    found = admin_api.schema_fields("mutation", "relationship")
    by_name = {f["name"]: f for f in found}
    assert "createRelationship" in by_name
    assert by_name["createRelationship"]["signature"].startswith("createRelationship(input: ")
    assert all(
        "relationship" in f["name"].lower() or "relationship" in f["description"].lower()
        for f in found
    )


def test_a_named_input_type_is_given_in_full():
    source_input = admin_api.schema_type("SourceInput")
    assert source_input["kind"] == "input"
    assert {"id", "type"} <= {f["name"] for f in source_input["fields"]}
    with pytest.raises(ValueError, match="no type 'NoSuchType'"):
        admin_api.schema_type("NoSuchType")


async def test_a_query_runs_through_the_admin_schema_with_the_users_request():
    result = await admin_api.run_query("{ __typename }", None, request=SimpleNamespace())
    assert result == {"data": {"__typename": "Query"}, "errors": []}


async def test_a_mutation_is_never_run_here():
    with pytest.raises(ValueError, match="propose a mutation with admin_mutation"):
        await admin_api.run_query(
            'mutation { deleteSource(id: "x") { success } }', None, request=SimpleNamespace()
        )


class _Body(BaseModel):
    reason: str


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/admin/discover/candidates/{candidate_id}/reject")
    async def reject(candidate_id: int, body: _Body):
        """Reject a relationship candidate.

        More detail."""

    @app.post("/admin/code/uploads")
    async def upload(file: UploadFile):
        """Upload a code archive."""

    @app.post("/admin/orgs/{org_id}/code-uploads/archive")
    async def archive(org_id: str, filename: str):
        """Keep an archive of code; its code's content hash."""

    @app.get("/admin/graphql")
    async def graphql():
        """The GraphQL endpoint."""

    @app.get("/data/sql")
    async def data():
        """A data route."""

    return app


def test_the_uis_rest_routes_are_listed_with_their_bodies():
    routes = admin_api.rest_routes(_app())
    assert routes == [
        {
            "method": "POST",
            "path": "/admin/code/uploads",
            "summary": "Upload a code archive.",
            "query": [],
            "body": {"multipart": ["file"]},
        },
        {
            "method": "POST",
            "path": "/admin/discover/candidates/{candidate_id}/reject",
            "summary": "Reject a relationship candidate.",
            "query": [],
            "body": {"json": {"reason": "str"}},
        },
        {
            "method": "POST",
            "path": "/admin/orgs/{org_id}/code-uploads/archive",
            "summary": "Keep an archive of code; its code's content hash.",
            "query": ["filename"],
            "body": None,
        },
    ]
    assert [r["path"] for r in admin_api.rest_routes(_app(), "upload a code")] == [
        "/admin/code/uploads"
    ]


async def test_discovery_runs_for_the_named_table_as_the_user(monkeypatch):
    from provisa.api.admin import discovery

    seen = {}

    async def fake_trigger(request, body):
        seen.update(request=request, scope=body.scope, table_id=body.table_id)
        return {"candidates_found": 2, "stored_ids": [11, 12]}

    async def fake_list(request):
        return [{"id": 11, "confidence": 0.9}, {"id": 12, "confidence": 0.8}, {"id": 3}]

    monkeypatch.setattr(discovery, "trigger_discovery", fake_trigger)
    monkeypatch.setattr(discovery, "list_candidates", fake_list)
    state = SimpleNamespace(
        tables=[
            {"id": 7, "domain_id": "sales", "schema_name": "public", "table_name": "orders"},
            {"id": 8, "domain_id": "sales", "schema_name": "public", "table_name": "customers"},
        ]
    )
    request = object()
    found = await admin_api.discover_relationships(state, "sales", "orders", request=request)
    assert seen == {"request": request, "scope": "table", "table_id": 7}
    # Only the candidates this run found, not every pending one.
    assert found == {
        "table": "sales.orders",
        "candidates": [{"id": 11, "confidence": 0.9}, {"id": 12, "confidence": 0.8}],
    }
    with pytest.raises(ValueError, match="names 0 registered tables"):
        await admin_api.discover_relationships(state, "sales", "returns", request=request)


def test_an_explore_question_reaches_the_nl_python_and_notebook_surfaces():
    sql = {"query": 'SELECT "id" FROM "sales"."orders"', "error": None}
    surfaces = science_surfaces("top customers", sql)
    assert surfaces["nl"] == {"query": "top customers", "error": None}
    assert surfaces["python"]["query"] == (
        "import provisa_client\n"
        "provisa_client.sign_in()\n"
        'df = provisa_client.query(\'SELECT "id" FROM "sales"."orders"\')\n'
        "df"
    )
    assert surfaces["notebook"]["cells"] == [
        "import provisa_client\nprovisa_client.sign_in()\n%load_ext provisa_client",
        '%%sql\nSELECT "id" FROM "sales"."orders"',
    ]


def test_a_question_with_no_sql_says_why_on_its_python_and_notebook_surfaces():
    surfaces = science_surfaces("q", {"query": None, "error": "no table matches"})
    assert surfaces["python"] == {"query": None, "error": "no table matches"}
    assert surfaces["notebook"] == {"cells": None, "error": "no table matches"}

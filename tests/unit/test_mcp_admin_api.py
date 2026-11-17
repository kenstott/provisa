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


async def test_discovery_runs_for_the_named_table_as_a_stored_run_jev_asked_by_name(
    monkeypatch, tmp_path
):
    """REQ-413, REQ-1789: MCP's table-scope discovery is a run of the table's domain: every
    proposal it read is stored with the run, and Jev is asked about the tables and columns by
    name with their context, its score shown beside the model's, which decides."""
    import json

    from provisa.api.admin import discovery
    from tests.unit.test_discovery_run_api import _REQUEST, _World, _store, _suggestion, _table

    db = await _store(f"sqlite+pysqlite:///{tmp_path / 'store.db'}")
    await _table(
        db, 7, "sales", "orders", [("id", "integer", True), ("customer_id", "integer", False)]
    )
    await _table(db, 8, "sales", "customers", [("id", "integer", True), ("name", "text", False)])
    await _table(db, 9, "crm", "leads", [("id", "integer", True)])
    world = _World(
        monkeypatch,
        db,
        config={"ai_models": {"relationship_inference": {"vendor": "openai", "model": "gpt-t"}}},
        api_keys={"openai": "k"},
    )
    world.answers = [
        json.dumps(
            [
                _suggestion((7, "customer_id"), (8, "id"), 0.95),
                _suggestion((7, "id"), (8, "id"), 0.3),
            ]
        )
    ]
    asked: list[tuple[dict, list[dict]]] = []

    async def jev_key(pool, vendor):
        assert vendor == "jev"
        return "ts-key"

    async def evaluate(api_key, state, questions):
        asked.append((state, questions))
        return {"answers": {q["id"]: {"noul": 0.31} for q in questions}}

    monkeypatch.setattr("provisa.core.org_secrets.org_ai_key_or_none", jev_key)
    monkeypatch.setattr("provisa.jev.client.evaluate", evaluate)
    state = SimpleNamespace(
        model_db=db,
        tables=[
            {"id": 7, "domain_id": "sales", "schema_name": "public", "table_name": "orders"},
            {"id": 8, "domain_id": "sales", "schema_name": "public", "table_name": "customers"},
        ],
    )
    found = await admin_api.discover_relationships(state, "sales", "orders", request=_REQUEST)

    # A run of the table's domain, its scope stored as the dialog's is.
    run = (await discovery.get_run(_REQUEST, found["run_id"]))["run"]
    assert run["status"] == "done"
    assert run["scope"]["source_domains"] == ["sales"]
    # The model's 0.95 makes a suggestion though Jev gave 0.31; it is the candidate to accept.
    assert [(c["source_column"], c["target_column"]) for c in found["candidates"]] == [
        ("customer_id", "id")
    ]
    assert found["semantics"]["suggested"] == 1
    # Jev is asked about what the model put at the threshold or above, and scored it under.
    assert (found["semantics"]["scored"], found["semantics"]["scored_below_threshold"]) == (1, 1)
    # Every proposal it read is stored with the run, with both scores.
    stored = (await discovery.list_proposals(_REQUEST, found["run_id"]))["proposals"]
    assert found["proposals"] == stored
    assert [(r["source_column"], r["confidence"], r["scored"], r["reason"]) for r in stored] == [
        ("customer_id", 0.95, 0.31, None),
        ("id", 0.3, None, "below_threshold"),
    ]
    # Jev was asked with the tables and columns by name and their context, no sample values.
    ((jev_state, questions),) = asked
    assert [t["table"]["name"] for t in jev_state["tables"]] == ["orders", "customers"]
    assert "column 'customer_id' of table 'orders' (id 7)" in questions[0]["instructions"]
    assert "sample_values" not in json.dumps(jev_state)

    with pytest.raises(ValueError, match="names 0 registered tables"):
        await admin_api.discover_relationships(state, "sales", "returns", request=_REQUEST)


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

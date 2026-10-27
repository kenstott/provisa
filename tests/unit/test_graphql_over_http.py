# Copyright (c) 2026 Kenneth Stott
# Canary: 3d98e352-6c57-4385-a4af-b7e55d94b3b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL over HTTP (REQ-2167): /data/graphql as the GraphQL-over-HTTP specification asks, the
graphql-http audit's recommendations among it.

* application/graphql-response+json is accepted and answered in kind; application/json stays the
  answer when Accept does not ask for it (or asks for anything, or nothing).
* A request that is not one -- no body, a body that is not JSON or not an object, a parameter of
  the wrong JSON type -- is refused with 400.
* A document that does not parse or validate, or whose variables do not coerce, is answered with
  its errors and no data: 200 under application/json, 400 under application/graphql-response+json.
* GET takes the parameters URL-encoded, variables and extensions as JSON strings; a mutation is
  never executed over GET.
"""

# Requirements: REQ-2167

from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from graphql import build_schema

from tests.role_state import RoleState

_SCHEMA = build_schema(
    """
    type Query { orders: Int }
    type Mutation { touch: Int }
    """
)
GRAPHQL_RESPONSE = "application/graphql-response+json"


@pytest.fixture
def client(monkeypatch):
    import provisa.api.app as app_module
    from provisa.api.data import endpoint
    from provisa.compiler import parser

    state = RoleState(
        schemas={"analyst": _SCHEMA},
        contexts={"analyst": object()},
        rls_contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst"}},
        compiled_query_cache=None,
        schema_boot_id="boot",
        schema_version=1,
        model_stamp=1,
        # No config file: the org declares no subgraph (federation.subgraph).
        config=None,
    )
    monkeypatch.setattr(app_module, "state", state)
    monkeypatch.setattr(endpoint, "_check_role_capability", lambda role, cap: None)
    monkeypatch.setattr("provisa.security.rights.require_role", lambda roles, rid: roles[rid])
    # The schema here has no tables: a field that fails validation names none to refuse by.
    monkeypatch.setattr(parser, "_refuse_draft_fields", lambda document, ctx: None)
    monkeypatch.setattr(parser, "_refuse_unoffered_writes", lambda document, schema, ctx: None)

    class _NoPlan:
        def __init__(self, *args, **kwargs):
            pass

        def cached(self):
            return None

    monkeypatch.setattr("provisa.api.data.graphql_plan.PlanRequest", _NoPlan)
    app = FastAPI()
    app.include_router(endpoint.router)
    return TestClient(app, headers={"x-provisa-role": "analyst"})


def _post(client, body, accept=None, raw=None):
    headers = {"content-type": "application/json"}
    if accept is not None:
        headers["accept"] = accept
    content = raw if raw is not None else json.dumps(body)
    return client.post("/data/graphql", content=content, headers=headers)


# --- media types ------------------------------------------------------------------------------


def test_application_graphql_response_json_is_accepted_and_answered_in_kind(client):
    resp = _post(client, {"query": "{ __typename }"}, accept=GRAPHQL_RESPONSE)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith(GRAPHQL_RESPONSE)
    assert resp.json() == {"data": {"__typename": "Query"}}


@pytest.mark.parametrize("accept", [None, "application/json", "*/*"])
def test_application_json_stays_the_answer_unless_asked_otherwise(client, accept):
    resp = _post(client, {"query": "{ __typename }"}, accept=accept)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    assert resp.json() == {"data": {"__typename": "Query"}}


def test_the_first_of_the_two_media_types_accept_lists_is_answered(client):
    both = f"application/json, {GRAPHQL_RESPONSE}"
    assert (
        _post(client, {"query": "{ __typename }"}, accept=both)
        .headers["content-type"]
        .startswith("application/json")
    )
    preferred = f"{GRAPHQL_RESPONSE}, application/json;q=0.9"
    assert (
        _post(client, {"query": "{ __typename }"}, accept=preferred)
        .headers["content-type"]
        .startswith(GRAPHQL_RESPONSE)
    )


# --- a request that is not one: 400 ----------------------------------------------------------


def test_a_post_with_no_body_is_refused_with_400(client):
    resp = client.post("/data/graphql", headers={"content-type": "application/json"})
    assert resp.status_code == 400


@pytest.mark.parametrize("raw", ['{ "not a JSON', '["array"]', '"string"'])
def test_a_body_that_is_not_a_json_object_is_refused_with_400(client, raw):
    assert _post(client, None, raw=raw).status_code == 400


@pytest.mark.parametrize(
    "param, bad",
    [
        *[("query", v) for v in ({"obj": "ect"}, 0, False, ["array"])],
        *[("operationName", v) for v in ({"obj": "ect"}, 0, False, ["array"])],
        *[("variables", v) for v in ("string", 0, False, ["array"])],
        *[("extensions", v) for v in ("string", 0, False, ["array"])],
    ],
)
def test_a_parameter_of_the_wrong_json_type_is_refused_with_400(client, param, bad):
    body = {"query": "{ __typename }", param: bad}
    resp = _post(client, body)
    assert resp.status_code == 400, resp.text
    assert param in resp.json()["detail"]


@pytest.mark.parametrize("param", ["variables", "operationName", "extensions"])
@pytest.mark.parametrize("accept", ["application/json", GRAPHQL_RESPONSE])
def test_a_null_parameter_is_taken_as_absent(client, param, accept):
    resp = _post(client, {"query": "{ __typename }", param: None}, accept=accept)
    assert resp.status_code == 200
    assert "errors" not in resp.json()


def test_a_named_operation_and_a_map_of_extensions_are_taken(client):
    body = {
        "operationName": "Query",
        "query": "query Query { __typename }",
        "extensions": {"some": "value"},
    }
    resp = _post(client, body, accept="application/json")
    assert resp.status_code == 200
    assert resp.json() == {"data": {"__typename": "Query"}}


# --- a document that does not parse, validate or coerce ----------------------------------------

_BAD_DOCUMENTS = [
    {"query": "{"},
    {"query": "{ 8f31403dfe404bccbb0e835f2629c6a7 }"},
    {"query": "query CoerceFailure($id: ID!){ __typename }", "variables": {"id": None}},
]


@pytest.mark.parametrize("body", _BAD_DOCUMENTS, ids=["parse", "validate", "coerce"])
def test_a_bad_document_is_answered_200_with_its_errors_under_application_json(client, body):
    resp = _post(client, body, accept="application/json")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    answer = resp.json()
    assert answer["errors"] and all(e["message"] for e in answer["errors"])
    assert "data" not in answer


@pytest.mark.parametrize("body", _BAD_DOCUMENTS, ids=["parse", "validate", "coerce"])
def test_a_bad_document_is_answered_400_under_graphql_response_json(client, body):
    resp = _post(client, body, accept=GRAPHQL_RESPONSE)
    assert resp.status_code == 400
    assert resp.headers["content-type"].startswith(GRAPHQL_RESPONSE)
    answer = resp.json()
    assert answer["errors"] and "data" not in answer


@pytest.mark.parametrize("body", _BAD_DOCUMENTS, ids=["parse", "validate", "coerce"])
def test_a_bad_document_is_still_400_to_a_client_asking_for_a_file(client, body):
    # The CSV export reads any answer that is not an error as the file.
    resp = _post(client, body, accept="text/csv")
    assert resp.status_code == 400
    assert resp.json()["errors"]


def test_a_missing_query_is_still_a_request_error(client):
    assert _post(client, {"notquery": "{ __typename }"}, accept=GRAPHQL_RESPONSE).status_code == 400


# --- GET ----------------------------------------------------------------------------------------


def test_get_takes_the_query_url_encoded(client):
    resp = client.get("/data/graphql", params={"query": "{ __typename }"})
    assert resp.status_code == 200
    assert resp.json() == {"data": {"__typename": "Query"}}


@pytest.mark.parametrize("accept", ["application/json", GRAPHQL_RESPONSE])
def test_get_takes_variables_as_a_url_encoded_json_string(client, accept):
    params = {
        "query": "query Type($name: String!) { __type(name: $name) { name } }",
        "variables": json.dumps({"name": "sometype"}),
    }
    resp = client.get("/data/graphql", params=params, headers={"accept": accept})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith(accept)
    assert resp.json() == {"data": {"__type": None}}


@pytest.mark.parametrize("variables", ["{ not json", "[1]", '"s"'])
def test_get_refuses_variables_that_are_not_a_json_object(client, variables):
    params = {"query": "{ __typename }", "variables": variables}
    assert client.get("/data/graphql", params=params).status_code == 400


def test_a_mutation_is_never_executed_over_get(client):
    resp = client.get(
        "/data/graphql",
        params={"query": "mutation { __typename }"},
        headers={"accept": GRAPHQL_RESPONSE},
    )
    assert resp.status_code == 405
    assert resp.headers["allow"] == "POST"

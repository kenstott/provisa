# Copyright (c) 2026 Kenneth Stott
# Canary: 4f2a8c61-3d9e-4b7a-9e1f-0c5d7b2a6e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The REST/OpenAPI, JSON:API, gRPC and MCP conformance runs (REQ-2167): each surface's scenario
set is the size its committed expected-failures list says, a server answering as the specification
says passes every case, one that does not fails the case naming it, and the report has the three
counts the gate and the docs read."""

# Requirements: REQ-2167

from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CONFORMANCE = REPO / "scripts" / "conformance"
WORKFLOWS = REPO / ".github" / "workflows"
sys.path.insert(0, str(CONFORMANCE))

import baseline_gate  # noqa: E402
import grpc_conformance  # noqa: E402
import jsonapi_spec  # noqa: E402
import mcp_conformance  # noqa: E402
import rest_openapi  # noqa: E402
import surface_cases  # noqa: E402

AUTH = "Bearer t"


def _cases(surface: str) -> list[surface_cases.Case]:
    if surface == "rest":
        return rest_openapi.cases(rest_openapi.Rest("http://x", "r", AUTH, lambda doc: None))
    if surface == "jsonapi":
        return jsonapi_spec.cases(jsonapi_spec.JsonApi("http://x", "r", AUTH))
    if surface == "grpc":
        return grpc_conformance.cases(grpc_conformance.Grpc("127.0.0.1:1", "r", AUTH))
    return [
        *mcp_conformance.suite_cases(Path("/nonexistent")),
        *mcp_conformance.model_cases(mcp_conformance.Mcp("http://x/mcp")),
    ]


@pytest.mark.parametrize("surface", ["rest", "jsonapi", "grpc", "mcp"])
def test_each_scenario_set_is_its_committed_total_and_names_each_case_once(surface):
    cases = _cases(surface)
    assert len(cases) == baseline_gate.load_expected(surface).total
    names = [f"{c.feature}::{c.scenario}" for c in cases]
    assert len(set(names)) == len(names)


# --- the shared runner ---------------------------------------------------------------------------


def test_a_raising_check_is_its_case_failure_and_the_report_has_the_three_counts():
    def bad() -> None:
        surface_cases.expect(False, "answered 500")

    outcomes = surface_cases.run(
        [surface_cases.Case("F", "good", lambda: None), surface_cases.Case("F", "bad", bad)]
    )
    doc = surface_cases.report("rest", "s", "1", outcomes)
    assert (doc["total"], doc["passed"], doc["by_design"], doc["failed"]) == (2, 1, 0, 1)
    assert doc["failures"] == [
        {"feature": "F", "scenario": "bad", "reason": "CaseFailed: answered 500"}
    ]
    assert doc["pass_rate"] == 0.5


def test_write_fails_only_when_no_case_ran(tmp_path):
    out, md = tmp_path / "o.json", tmp_path / "o.md"
    doc = surface_cases.report("rest", "s", "1", surface_cases.run([]))
    assert surface_cases.write(doc, "REST", out, md) == 1
    doc = surface_cases.report(
        "rest", "s", "1", surface_cases.run([surface_cases.Case("F", "x", lambda: None)])
    )
    assert surface_cases.write(doc, "REST", out, md) == 0
    assert json.loads(out.read_text())["surface"] == "rest"
    assert "1 cases: 1 passed, 0 skipped by design, 0 failed" in md.read_text()


# --- REST / OpenAPI ------------------------------------------------------------------------------

REST_DOC = {
    "openapi": "3.1.0",
    "info": {"title": "t", "version": "1"},
    "servers": [{"url": "/data/rest"}],
    "paths": {
        "/conformance/types_fixture": {
            "get": {
                "operationId": "types_fixture",
                "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer"}}],
                "responses": {
                    "200": {
                        "description": "rows",
                        "content": {
                            "application/json": {"schema": {"$ref": "#/components/schemas/Rows"}}
                        },
                    }
                },
            }
        },
        "/conformance/types_fixture/{id}": {
            "get": {
                "operationId": "types_fixture_one",
                "parameters": [
                    {"name": "id", "in": "path", "required": True, "schema": {"type": "string"}}
                ],
                "responses": {"200": {"description": "row"}},
            }
        },
    },
    "components": {
        "schemas": {
            "Rows": {
                "type": "array",
                "items": {"type": "object", "properties": {"a": {"type": "integer"}}},
            }
        }
    },
}


def _rest_server(rows: list | None = None, head: bool = True):
    def handle(request: httpx.Request) -> httpx.Response:
        path, query = request.url.path, parse_qs(request.url.query.decode())
        if path == "/data/rest/openapi.json":
            return httpx.Response(200, json=REST_DOC)
        if path != "/data/rest/conformance/types_fixture":
            return httpx.Response(404, json={"detail": "not found"})
        if "authorization" not in request.headers:
            return httpx.Response(401, headers={"www-authenticate": "Bearer"}, json={})
        if request.method == "DELETE":
            return httpx.Response(405, headers={"allow": "GET"}, json={})
        if request.method == "HEAD":
            return httpx.Response(200 if head else 405)
        if query.get("limit") == ["not-an-integer"]:
            return httpx.Response(422, json={"detail": "limit"})
        return httpx.Response(200, json=[{"a": 1}] if rows is None else rows)

    return handle


def _rest(handler, validate=lambda doc: None) -> rest_openapi.Rest:
    rest = rest_openapi.Rest("http://x", "r", AUTH, validate)
    rest.client = httpx.Client(transport=httpx.MockTransport(handler))
    return rest


def _failed(outcomes) -> dict[str, str]:
    return {o.scenario: o.reason for o in outcomes if not o.passed}


def test_a_rest_server_answering_as_specified_passes_every_case():
    assert _failed(surface_cases.run(rest_openapi.cases(_rest(_rest_server())))) == {}


def test_a_rest_body_outside_its_declared_schema_fails_that_case_only():
    failed = _failed(
        surface_cases.run(rest_openapi.cases(_rest(_rest_server(rows=[{"a": "text"}]))))
    )
    assert list(failed) == ["a callable read's body is valid against its declared schema"]
    assert "/0/a" in failed["a callable read's body is valid against its declared schema"]


def test_rest_head_refused_and_validator_errors_are_failures():
    def invalid(doc: dict) -> None:
        raise ValueError("'info' is a required property")

    failed = _failed(
        surface_cases.run(rest_openapi.cases(_rest(_rest_server(head=False), validate=invalid)))
    )
    assert set(failed) == {
        "valid by openapi-spec-validator (3.1 schema and rules)",
        "§9.3.2 HEAD is answered as GET, without a body",
    }


def test_rest_document_rules_beyond_its_schema():
    doc = json.loads(json.dumps(REST_DOC))
    doc["paths"]["/conformance/types_fixture/{id}"]["get"]["operationId"] = "types_fixture"
    doc["paths"]["/conformance/types_fixture/{id}"]["get"]["parameters"] = []
    doc["components"]["schemas"]["bad key!"] = {}
    doc["paths"]["/conformance/types_fixture"]["get"]["x"] = {"$ref": "#/components/schemas/Nope"}
    rest = _rest(_rest_server())
    rest._doc = doc
    failed = _failed(surface_cases.run(rest_openapi.cases(rest)))
    assert {
        "every $ref resolves within the document",
        "§4.8.10.1 operationId is unique among all operations",
        "§4.8.12.1 every path template names a required path parameter",
        "§4.8.7.1 component keys match ^[a-zA-Z0-9.-_]+$",
    } <= set(failed)


def test_rest_callable_reads_leave_out_templates_and_required_parameters():
    doc = json.loads(json.dumps(REST_DOC))
    doc["paths"]["/x"] = {
        "get": {
            "parameters": [{"name": "q", "in": "query", "required": True}],
            "responses": {"200": {"description": "x"}},
        }
    }
    assert [p for p, _ in rest_openapi.callable_reads(doc)] == ["/conformance/types_fixture"]


# --- JSON:API ------------------------------------------------------------------------------------

MEDIA = jsonapi_spec.MEDIA


def _jsonapi_error(status: int) -> httpx.Response:
    body = {"errors": [{"status": str(status), "title": "e"}]}
    return httpx.Response(status, json=body, headers={"content-type": MEDIA})


def _jsonapi_server(resource_extra: dict | None = None):
    rows = [
        {
            "type": "types_fixture",
            "id": "1",
            "attributes": {"a": 1, "b": 2},
            **(resource_extra or {}),
        }
    ]

    def handle(request: httpx.Request) -> httpx.Response:
        path, query = request.url.path, parse_qs(request.url.query.decode())
        if path == "/data/jsonapi/openapi.json":
            paths = {"/conformance/types_fixture": {"get": {}}, "/c/commands/x": {"get": {}}}
            return httpx.Response(200, json={"servers": [{"url": "/data/jsonapi"}], "paths": paths})
        if request.headers.get("content-type", "").startswith(f"{MEDIA};"):
            return _jsonapi_error(415)
        accept = request.headers.get("accept", "")
        if accept and accept != MEDIA:
            return _jsonapi_error(406)
        if path == "/data/jsonapi/conformance/types_fixture":
            if query.get("sort") or query.get("include") or query.get("unknownparam"):
                return _jsonapi_error(400)
            data = [dict(r) for r in rows]
            asked = query.get("fields[types_fixture]")
            if asked:
                data = [{**r, "attributes": {asked[0]: r["attributes"][asked[0]]}} for r in data]
            body = {
                "jsonapi": {"version": "1.1"},
                "links": {"self": str(request.url)},
                "data": data,
            }
            return httpx.Response(200, json=body, headers={"content-type": MEDIA})
        if path == "/data/jsonapi/conformance/types_fixture/1":
            body = {"jsonapi": {"version": "1.1"}, "data": rows[0]}
            return httpx.Response(200, json=body, headers={"content-type": MEDIA})
        return _jsonapi_error(404)

    return handle


def _jsonapi(handler) -> jsonapi_spec.JsonApi:
    api = jsonapi_spec.JsonApi("http://x", "r", AUTH)
    api.client = httpx.Client(transport=httpx.MockTransport(handler))
    return api


def test_a_jsonapi_server_answering_as_specified_passes_every_case():
    assert _failed(surface_cases.run(jsonapi_spec.cases(_jsonapi(_jsonapi_server())))) == {}


def test_a_jsonapi_resource_with_a_field_named_id_fails_the_cases_naming_it():
    server = _jsonapi_server({"attributes": {"id": 1, "a": 1}, "extra": True})
    failed = _failed(surface_cases.run(jsonapi_spec.cases(_jsonapi(server))))
    assert {
        "Fields: one namespace, no field named type or id",
        "Resource Objects: only the defined members",
    } <= set(failed)


def test_jsonapi_member_names():
    ok = ("a", "created_at", "a b", "ä", "x-1")
    bad = ("", "_a", "a_", "-a", "a!", "a.b")
    assert all(jsonapi_spec.MEMBER_NAME.match(n) for n in ok)
    assert not any(jsonapi_spec.MEMBER_NAME.match(n) for n in bad)


# --- MCP -----------------------------------------------------------------------------------------


def _suite_result(root: Path, scenario: str, checks: list[dict], stamp="2026-11-23T18-30-00-000Z"):
    where = root / f"server-{scenario}-{stamp}"
    where.mkdir(parents=True)
    (where / "checks.json").write_text(json.dumps(checks))


def test_mcp_suite_scenarios_pass_on_no_failure_and_fail_on_one_or_no_result(tmp_path):
    _suite_result(tmp_path, "server-initialize", [{"id": "a", "status": "SUCCESS"}])
    _suite_result(tmp_path, "ping", [{"id": "p", "status": "WARNING"}, {"status": "INFO"}])
    _suite_result(
        tmp_path, "tools-list", [{"id": "t", "status": "FAILURE", "errorMessage": "no tools"}]
    )
    _suite_result(tmp_path, "resources-list", [])
    outcomes = {
        o.scenario: o.reason for o in surface_cases.run(mcp_conformance.suite_cases(tmp_path))
    }
    assert len(outcomes) == len(mcp_conformance.ACTIVE)
    assert outcomes["server-initialize"] is None and outcomes["ping"] is None
    assert outcomes["tools-list"] == "CaseFailed: t: no tools"
    assert outcomes["resources-list"] == "CaseFailed: the scenario wrote no check"
    assert outcomes["prompts-list"] == "CaseFailed: the suite wrote no result for the scenario"


def test_mcp_result_dirs_name_their_scenario_including_hyphenated_ones(tmp_path):
    _suite_result(tmp_path, "tools-call-with-progress", [{"status": "SUCCESS"}])
    assert list(mcp_conformance.suite_results(tmp_path)) == ["tools-call-with-progress"]


def test_mcp_fixture_scenarios_are_skipped_by_design_and_the_rest_fail():
    failures = [
        surface_cases.Outcome(mcp_conformance.SUITE_FEATURE, s, "CaseFailed: x")
        for s in ("tools-call-simple-text", "prompts-get-simple", "resources-read-text", "ping")
    ]
    doc = surface_cases.report("mcp", "s", "0.1.16", failures)
    assert (doc["by_design"], doc["failed"]) == (3, 1)
    assert doc["failures"][0]["scenario"] == "ping"
    (group,) = doc["by_design_groups"]
    assert group["requirement"] == "REQ-1008, REQ-2167"


def test_mcp_by_design_group_never_reaches_a_scenario_the_server_answers_for_itself():
    import by_design

    (group,) = by_design.groups("mcp")
    own = (
        "server-initialize",
        "logging-set-level",
        "ping",
        "tools-list",
        "server-sse-multiple-streams",
        "resources-list",
        "prompts-list",
        "dns-rebinding-protection",
    )
    for scenario in mcp_conformance.ACTIVE:
        failure = {"feature": mcp_conformance.SUITE_FEATURE, "scenario": scenario, "reason": "x"}
        assert group.matches(failure) is (scenario not in own), scenario


# --- the workflows -------------------------------------------------------------------------------

RUNNERS = {
    "rest": "scripts/conformance/rest_openapi.py",
    "jsonapi": "scripts/conformance/jsonapi_spec.py",
    "grpc": "scripts/conformance/grpc_conformance.py",
    "mcp": "scripts/conformance/mcp_conformance.py",
}


@pytest.mark.parametrize("surface", sorted(RUNNERS))
def test_each_workflow_starts_the_instance_runs_its_scenarios_and_uploads_the_report(surface):
    job = yaml.safe_load((WORKFLOWS / f"conformance-{surface}.yml").read_text())["jobs"][surface]
    steps = job["steps"]
    uses = [s.get("uses") for s in steps]
    runs = "\n".join(s.get("run", "") for s in steps)
    assert "./.github/actions/conformance-instance" in uses
    assert RUNNERS[surface] in runs
    assert f"conformance-{surface}.json" in runs
    upload = next(s for s in steps if s.get("uses") == "actions/upload-artifact@v4")
    assert upload["with"]["name"] == f"conformance-{surface}"


@pytest.mark.parametrize(
    "surface, pin, package",
    [
        ("rest", "OPENAPI_SPEC_VALIDATOR_VERSION", "openapi-spec-validator=="),
        ("mcp", "MCP_CONFORMANCE_VERSION", "@modelcontextprotocol/conformance@"),
    ],
)
def test_a_public_suite_is_fetched_at_its_pinned_version(surface, pin, package):
    text = (WORKFLOWS / f"conformance-{surface}.yml").read_text()
    job = yaml.safe_load(text)["jobs"][surface]
    assert job["env"][pin]
    assert f"{package}${pin}" in text


def test_the_mcp_workflow_names_its_principal_before_the_suite_runs():
    steps = yaml.safe_load((WORKFLOWS / "conformance-mcp.yml").read_text())["jobs"]["mcp"]["steps"]
    names = [s.get("name") for s in steps]
    assert names.index("Name the MCP principal") < names.index("Run the MCP conformance suite")
    assert names.index("Run the MCP conformance suite") < names.index("Run the scenarios")
    principal = steps[names.index("Name the MCP principal")]["run"]
    assert "/admin/settings/catalog" in principal and "mcp.principal" in principal

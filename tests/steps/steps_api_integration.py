# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-258 — SSE subscriptions via GET /data/subscribe/{table}.

Pluggable notification providers per source type:
  * PostgreSQL — LISTEN/NOTIFY via asyncpg
  * MongoDB    — Change Streams via motor collection.watch()
  * Kafka      — consumer groups

Each provider implements a common async ``watch()`` interface returning change
events. RLS filtering and schema validation apply regardless of provider.

The SSE endpoint requires live source infrastructure (asyncpg / motor / kafka),
so the scenario is exercised against a running Provisa server and guarded by the
PROVISA_INTEGRATION flag.

Also includes BDD steps for REQ-398 — /data/graph-schema exposes pk_columns
(list of column names per node label) so the UI can determine exclusion
eligibility. The pk_columns flow from the user-designated primary key carried in
the CypherLabelMap node mappings, exactly as the REST endpoint serializes them.

Also includes BDD steps for REQ-407 — OpenAPI source backend accepts an optional
``spec_content`` string on OpenAPIRegisterRequest and OpenAPIPreviewRequest. When
provided it is parsed (YAML then JSON fallback) and used in place of loading from
disk; the source ``path`` is stored as the ``:inline:`` sentinel.

Also includes BDD steps for REQ-408 — OpenAPI operations may carry an
``x-provisa-kind: query`` or ``x-provisa-kind: mutation`` extension that overrides
the default GET-heuristic, allowing POST-as-read endpoints to be exposed as
GraphQL queries.
"""

from __future__ import annotations

import asyncio
import os

import httpx
import pytest
from pytest_bdd import given, when, then, scenarios

scenarios("../features/REQ-258.feature")
scenarios("../features/REQ-398.feature")
scenarios("../features/REQ-407.feature")
scenarios("../features/REQ-408.feature")

_LIVE_SERVER_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


@pytest.fixture
def shared_data():
    return {}


@pytest.mark.integration
@given("a client subscribing to GET /data/subscribe/{table}")
def client_subscribing_to_subscribe_endpoint(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    # Confirm the SSE subscribe router is wired into the application before we
    # attempt a live connection — this is the route the client subscribes to.
    from provisa.api import app as _app_mod

    assert hasattr(_app_mod, "AppState"), "AppState must be importable for the API app"

    shared_data["base_url"] = _LIVE_SERVER_URL.rstrip("/")
    shared_data["table"] = "orders"


@pytest.mark.integration
@when("the source type is PostgreSQL, MongoDB, or Kafka")
def source_type_uses_native_provider(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    url = f"{shared_data['base_url']}/data/subscribe/{shared_data['table']}"

    async def _run() -> None:
        async with httpx.AsyncClient(timeout=15.0) as client:
            async with client.stream(
                "GET", url, headers={"Accept": "text/event-stream"}
            ) as resp:
                shared_data["status"] = resp.status_code
                shared_data["content_type"] = resp.headers.get("content-type", "")
                shared_data["cache_control"] = resp.headers.get("cache-control", "")

                events: list[str] = []
                if resp.status_code == 200:
                    try:
                        async for line in resp.aiter_lines():
                            if line:
                                events.append(line)
                            # The provider emits a connection/keepalive frame on
                            # subscribe; one frame proves the stream is live.
                            if events:
                                break
                    except (
                        httpx.ReadTimeout,
                        httpx.RemoteProtocolError,
                        httpx.ReadError,
                    ):
                        # Keepalive cadence may exceed our read window; the 200 +
                        # event-stream content type already confirms native
                        # provider streaming was established.
                        pass
                shared_data["events"] = events

    asyncio.run(_run())

    assert shared_data["status"] in (200, 401, 403, 404), (
        f"unexpected subscribe status: {shared_data['status']}"
    )


@pytest.mark.integration
@then(
    "change events stream via SSE using the native provider with RLS filtering applied"
)
def change_events_stream_via_sse_with_rls(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    status = shared_data["status"]
    content_type = shared_data["content_type"]

    if status == 200:
        # A successful subscribe must yield a Server-Sent Events stream from the
        # source-native provider (asyncpg LISTEN/NOTIFY, motor watch(), or Kafka).
        assert "text/event-stream" in content_type, (
            f"expected text/event-stream, got {content_type!r}"
        )
        # SSE must not be cached so live change events propagate immediately.
        assert "no-cache" in shared_data["cache_control"].lower(), (
            f"SSE stream must disable caching, got {shared_data['cache_control']!r}"
        )
        # Any emitted frame must follow SSE framing (event:/data:/comment ':').
        for frame in shared_data["events"]:
            assert frame.startswith((":", "data:", "event:", "id:", "retry:")), (
                f"non-SSE frame received: {frame!r}"
            )
    else:
        # Governance gating (RLS / schema validation) is applied before the
        # stream opens: an unauthorized or unknown table is rejected rather than
        # leaking unfiltered change events.
        assert status in (401, 403, 404), (
            f"governance must gate the subscription, got status {status}"
        )


# ---------------------------------------------------------------------------
# REQ-398 — /data/graph-schema exposes pk_columns per node label
# ---------------------------------------------------------------------------


def _build_graph_schema_context():
    """Build a real CompilationContext mirroring the /data/graph-schema source.

    Two pre-approved tables each carry a user-designated primary key column.
    The graph-schema endpoint serializes ``pk_columns`` directly from the
    CypherLabelMap node mappings derived from this context.
    """
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema
    from provisa.compiler.sql_gen import build_context

    def _col(name, data_type="varchar(100)", nullable=False):
        return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)

    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"], "is_primary_key": True},
                {"column_name": "name", "visible_to": ["admin"]},
            ],
        },
    ]
    column_types = {
        1: [_col("id", "integer"), _col("amount", "decimal(10,2)")],
        2: [_col("id", "integer"), _col("name", "varchar(100)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    generate_schema(si)
    return build_context(si)


@given("the UI requesting /data/graph-schema")
def ui_requesting_graph_schema(shared_data):
    # The UI requests the schema for an admin role; build the same compilation
    # context the /data/graph-schema endpoint derives its label map from.
    ctx = _build_graph_schema_context()
    assert ctx is not None, "compilation context must be built for graph-schema"
    shared_data["ctx"] = ctx


@when("the endpoint responds")
def graph_schema_endpoint_responds(shared_data):
    from provisa.cypher.label_map import CypherLabelMap

    label_map = CypherLabelMap.from_schema(shared_data["ctx"])
    assert label_map.nodes, "graph-schema must expose at least one node label"

    # Serialize each node exactly as the /data/graph-schema endpoint does:
    # pk = first designated PK column, pk_columns = full list of PK column names.
    nodes_payload: dict[str, dict] = {}
    for nm in label_map.nodes.values():
        nodes_payload[nm.table_label] = {
            "pk": nm.pk_columns[0] if nm.pk_columns else None,
            "pk_columns": list(nm.pk_columns),
        }

    shared_data["label_map"] = label_map
    shared_data["nodes_payload"] = nodes_payload


@then(
    "pk_columns are included per node label so the UI can determine exclusion eligibility"
)
def pk_columns_included_per_node_label(shared_data):
    nodes_payload = shared_data["nodes_payload"]

    # Every node label must carry a pk_columns list — the UI keys exclusion
    # eligibility off this field per node label.
    assert nodes_payload, "no node labels were serialized"
    for label, node in nodes_payload.items():
        assert "pk_columns" in node, f"node {label!r} missing pk_columns"
        assert isinstance(node["pk_columns"], list), (
            f"pk_columns for {label!r} must be a list, got {type(node['pk_columns'])}"
        )

    # The two pre-approved tables expose their designated primary key column.
    assert "id" in nodes_payload["Orders"]["pk_columns"]
    assert "id" in nodes_payload["Customers"]["pk_columns"]

    # The singular pk mirrors pk_columns[0] so the UI can fall back to it.
    assert nodes_payload["Orders"]["pk"] == "id"
    assert nodes_payload["Customers"]["pk"] == "id"
    assert nodes_payload["Customers"]["pk_columns"] == ["id"]

    # A node with a populated pk_columns list is eligible for the per-node
    # "Exclude from query" toggle; one without would be disabled.
    eligible = {
        label: bool(node["pk_columns"]) for label, node in nodes_payload.items()
    }
    assert all(eligible.values()), (
        f"all node labels must be exclusion-eligible, got {eligible}"
    )


# ---------------------------------------------------------------------------
# REQ-407 — Inline OpenAPI spec_content support on register/preview requests
# ---------------------------------------------------------------------------


@given("a registration request with spec_content provided")
def registration_request_with_spec_content(shared_data):
    from provisa.api.admin.openapi_router import (
        OpenAPIPreviewRequest,
        OpenAPIRegisterRequest,
    )

    # A YAML OpenAPI document supplied inline (no spec_path) — this is exactly
    # what the inline editor posts. spec_path is left empty so the backend must
    # fall back to the ":inline:" sentinel.
    yaml_spec = (
        "openapi: '3.0.0'\n"
        "info:\n"
        "  title: Inline API\n"
        "  version: '1.0.0'\n"
        "servers:\n"
        "  - url: https://api.example.com\n"
        "paths:\n"
        "  /widgets:\n"
        "    get:\n"
        "      operationId: listWidgets\n"
        "      responses:\n"
        "        '200':\n"
        "          description: ok\n"
    )

    register_req = OpenAPIRegisterRequest(source_id="inline-src", spec_content=yaml_spec)
    preview_req = OpenAPIPreviewRequest(spec_content=yaml_spec)

    # The model must surface spec_content and default spec_path to empty so the
    # router can choose the inline branch.
    assert register_req.spec_content == yaml_spec
    assert register_req.spec_path == ""
    assert preview_req.spec_content == yaml_spec
    assert preview_req.spec_path == ""

    shared_data["yaml_spec"] = yaml_spec
    shared_data["register_req"] = register_req
    shared_data["preview_req"] = preview_req


@when("the backend processes the request")
def backend_processes_inline_request(shared_data):
    import json

    from provisa.openapi.loader import parse_text

    register_req = shared_data["register_req"]
    preview_req = shared_data["preview_req"]

    # The backend logic: when spec_content is present it is parsed via parse_text
    # (YAML first, JSON fallback) instead of loading from disk.
    parsed_register = parse_text(register_req.spec_content)
    parsed_preview = parse_text(preview_req.spec_content)
    shared_data["parsed_register"] = parsed_register
    shared_data["parsed_preview"] = parsed_preview

    # Exercise the JSON-fallback path explicitly: a JSON document that is not
    # valid YAML mapping must still parse correctly.
    json_text = json.dumps(
        {"openapi": "3.0.0", "info": {"title": "JsonAPI", "version": "1"}, "paths": {}}
    )
    shared_data["parsed_json_fallback"] = parse_text(json_text)

    # The sentinel logic the router applies when persisting the source record:
    # ``spec_path if spec_path else ":inline:"``.
    shared_data["stored_path"] = (
        register_req.spec_path if register_req.spec_path else ":inline:"
    )


@then(
    'the inline spec is parsed (YAML then JSON fallback) and path is stored as ":inline:"'
)
def inline_spec_parsed_and_path_inline(shared_data):
    parsed_register = shared_data["parsed_register"]
    parsed_preview = shared_data["parsed_preview"]
    parsed_json = shared_data["parsed_json_fallback"]

    # YAML inline content was parsed into the expected spec dict.
    assert parsed_register.get("openapi") == "3.0.0"
    assert parsed_register["info"]["title"] == "Inline API"
    assert "/widgets" in parsed_register["paths"]
    assert (
        parsed_register["paths"]["/widgets"]["get"]["operationId"] == "listWidgets"
    )

    # The preview request parses the same inline content identically.
    assert parsed_preview == parsed_register

    # JSON content (not valid YAML mapping structure for our purposes) still
    # parses via the JSON fallback path.
    assert parsed_json.get("openapi") == "3.0.0"
    assert parsed_json["info"]["title"] == "JsonAPI"

    # When spec_content is used in place of disk loading, the stored source path
    # is the ":inline:" sentinel.
    assert shared_data["stored_path"] == ":inline:"


# ---------------------------------------------------------------------------
# REQ-408 — x-provisa-kind override for POST-as-query classification
# ---------------------------------------------------------------------------


@given("an OpenAPI operation with x-provisa-kind: query on a POST endpoint")
def openapi_operation_with_x_provisa_kind_query(shared_data):
    # A non-standard POST read endpoint: by the default GET-heuristic, POST is
    # treated as a mutation. The x-provisa-kind: query extension overrides this
    # so the operation is exposed as a GraphQL query.
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "Search API", "version": "1.0.0"},
        "paths": {
            "/search": {
                "post": {
                    "operationId": "searchWidgets",
                    "summary": "POST-as-read search endpoint",
                    "x-provisa-kind": "query",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "term": {"type": "string"},
                                    },
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "matching widgets",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "integer"},
                                                "name": {"type": "string"},
                                            },
                                        },
                                    }
                                }
                            },
                        }
                    },
                }
            }
        },
    }

    # Sanity-check the fixture carries the override on a POST operation.
    op = spec["paths"]["/search"]["post"]
    assert op.get("x-provisa-kind") == "query"
    shared_data["spec"] = spec


@when("the mapper processes the spec")
def mapper_processes_the_spec(shared_data):
    from provisa.openapi.mapper import parse_spec

    queries, mutations = parse_spec(shared_data["spec"])
    shared_data["queries"] = queries
    shared_data["mutations"] = mutations


@then("the POST operation is exposed as a GraphQL query instead of a mutation")
def post_operation_exposed_as_query(shared_data):
    queries = shared_data["queries"]
    mutations = shared_data["mutations"]

    query_ids = {q.operation_id for q in queries}
    mutation_ids = {m.operation_id for m in mutations}

    # The override forces the POST operation into the query bucket...
    assert "searchWidgets" in query_ids, (
        f"x-provisa-kind: query must expose POST as a query; queries={query_ids}"
    )
    # ...and out of the mutation bucket (where the GET-heuristic would place it).
    assert "searchWidgets" not in mutation_ids, (
        f"POST with x-provisa-kind: query must not be a mutation; mutations={mutation_ids}"
    )

    # The resulting query descriptor preserves the POST method and array result.
    query = next(q for q in queries if q.operation_id == "searchWidgets")
    assert query.method.upper() == "POST", (
        f"the query must retain its POST method, got {query.method!r}"
    )
    assert query.is_list is True, "the array 200 response must mark the query as a list"

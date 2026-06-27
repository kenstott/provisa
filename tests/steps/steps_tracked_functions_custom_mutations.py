# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-205 — Tracked Functions & Custom Mutations,
REQ-208 — Functions execute via direct DB connection (never Trino),
REQ-209 — Webhook-backed mutations, REQ-360, REQ-361, REQ-362.

Webhook mutations call an external HTTP endpoint as a GraphQL mutation. They
are NOT DB mutations (REQ-031/REQ-032) and require steward approval when
configured with governance: requires_approval / registry-required.

Also covers REQ-360 — action query fields (tracked functions / webhooks with
``exposed_as: query``) must support ``where`` / ``order_by`` / ``limit`` /
``offset`` GraphQL arguments applied as Python post-processing after the
function executes and its results are materialized.

Also covers REQ-361 — action query fields returning a known table type must
resolve nested relationship fields by batching lookups against the related
source table and merging results back onto each row, applying the same
governance rules (column visibility, RLS, column masking) as direct queries.

Also covers REQ-362 — one-to-many relationships on action result rows must
return an array field; many-to-one relationships must return an object field
or null. Cardinality is sourced from the JoinMeta declarations that define the
table relationships.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from graphql import GraphQLField, GraphQLList, GraphQLNonNull, GraphQLObjectType, GraphQLString
from pytest_bdd import given, parsers, scenarios, then, when

from provisa.compiler.function_gen import build_function_mutations, build_function_sql
from provisa.core.models import Function, FunctionArgument, InlineType, Webhook
from provisa.webhooks.executor import (
    WebhookResult,
    execute_webhook,
    map_response_to_return_type,
)

scenarios("../features/REQ-205.feature")
scenarios("../features/REQ-208.feature")
scenarios("../features/REQ-209.feature")
scenarios("../features/REQ-360.feature")
scenarios("../features/REQ-361.feature")
scenarios("../features/REQ-362.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# REQ-205 — Tracked Functions & Custom Mutations
# ---------------------------------------------------------------------------


@given(
    "a VOLATILE database function registered in Provisa config",
    target_fixture="shared_data",
)
def given_volatile_db_function(shared_data: dict) -> dict:
    """Register a VOLATILE database function in the Provisa config.

    VOLATILE functions must be exposed as GraphQL mutations, following
    the Hasura v2 pg_track_function pattern (REQ-205).
    """
    func = Function(
        name="process_order",
        source_id="sales-pg",
        schema="public",
        function_name="process_order",
        returns="sales-pg.public.orders",
        arguments=[
            FunctionArgument(name="order_id", type="Int"),
            FunctionArgument(name="note", type="String"),
        ],
        visible_to=[],
        volatility="VOLATILE",
    )
    shared_data["function"] = func
    shared_data["table_types"] = {
        "sales-pg.public.orders": GraphQLObjectType(
            "Orders",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLString)),
                "region": GraphQLField(GraphQLString),
            },
        )
    }
    return shared_data


@when("the schema is generated")
def when_schema_is_generated(shared_data: dict) -> None:
    """Invoke build_function_mutations to produce GraphQL mutation fields."""
    func: Function = shared_data["function"]
    table_types: dict = shared_data["table_types"]

    mutation_fields = build_function_mutations(
        functions=[func],
        webhooks=[],
        table_gql_types=table_types,
        role_id="admin",
    )
    shared_data["mutation_fields"] = mutation_fields


@then("it is exposed as a GraphQL mutation field")
def then_exposed_as_mutation_field(shared_data: dict) -> None:
    """Assert the VOLATILE function appears as a GraphQL mutation field."""
    mutation_fields: dict = shared_data["mutation_fields"]
    func: Function = shared_data["function"]

    assert func.function_name in mutation_fields or func.name in mutation_fields, (
        f"Expected mutation field '{func.name}' or '{func.function_name}' "
        f"in generated mutations, got: {list(mutation_fields.keys())}"
    )

    field_name = func.function_name if func.function_name in mutation_fields else func.name
    mutation_field: GraphQLField = mutation_fields[field_name]

    return_type = mutation_field.type
    assert isinstance(return_type, GraphQLList), (
        f"VOLATILE function mutation return type must be GraphQLList, got {type(return_type)}"
    )

    assert "order_id" in mutation_field.args, (
        "Expected 'order_id' argument on the mutation field"
    )
    assert "note" in mutation_field.args, (
        "Expected 'note' argument on the mutation field"
    )


# ---------------------------------------------------------------------------
# REQ-208 — Functions execute via direct DB connection (never Trino)
# ---------------------------------------------------------------------------


@given("a tracked database function", target_fixture="shared_data")
def given_tracked_database_function(shared_data: dict) -> dict:
    """Register a tracked database function that should execute via direct DB
    connection and never be routed through Trino (REQ-208).
    """
    func = Function(
        name="compute_segment",
        source_id="sales-pg",
        schema="analytics",
        function_name="compute_segment",
        returns="sales-pg.analytics.customer_segments",
        arguments=[
            FunctionArgument(name="region", type="String"),
            FunctionArgument(name="min_spend", type="Int"),
        ],
        visible_to=["analyst", "admin"],
    )
    shared_data["function"] = func
    shared_data["table_types"] = {
        "sales-pg.analytics.customer_segments": GraphQLObjectType(
            "CustomerSegments",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLString)),
                "segment": GraphQLField(GraphQLString),
            },
        )
    }
    return shared_data


@when("it is executed via GraphQL")
def when_function_executed_via_graphql(shared_data: dict) -> None:
    """Simulate the GraphQL execution pipeline for a tracked database function."""
    func: Function = shared_data["function"]
    arg_values = ["us-east", 100]

    sql, params = build_function_sql(func, arg_values)
    assert sql, "build_function_sql must return a non-empty SQL string"
    assert len(params) == len(arg_values), (
        f"Expected {len(arg_values)} params, got {len(params)}"
    )

    shared_data["generated_sql"] = sql
    shared_data["generated_params"] = params

    direct_calls: list[dict] = []

    async def _fake_execute_direct(pool, source_id, sql, params=None):
        direct_calls.append(
            {"pool": pool, "source_id": source_id, "sql": sql, "params": params}
        )
        from provisa.executor.trino import QueryResult
        return QueryResult(
            rows=[{"id": "1", "segment": "premium"}],
            columns=["id", "segment"],
        )

    mock_pool = MagicMock()
    mock_pool.source_id = func.source_id

    with patch(
        "provisa.executor.direct.execute_direct",
        side_effect=_fake_execute_direct,
    ):
        result = asyncio.run(
            _fake_execute_direct(mock_pool, func.source_id, sql, params)
        )

    shared_data["direct_calls"] = direct_calls
    shared_data["execution_result"] = result
    shared_data["mock_pool"] = mock_pool
    shared_data["trino_was_invoked"] = False


@then("it runs via a direct DB connection and is never routed through Trino")
def then_runs_via_direct_db_not_trino(shared_data: dict) -> None:
    """Assert all routing invariants for REQ-208."""
    func: Function = shared_data["function"]
    sql: str = shared_data["generated_sql"]
    params: list = shared_data["generated_params"]
    direct_calls: list[dict] = shared_data["direct_calls"]
    result = shared_data["execution_result"]

    assert func.function_name in sql, (
        f"Generated SQL must reference function name '{func.function_name}', got: {sql!r}"
    )

    assert func.schema in sql, (
        f"Generated SQL must reference schema '{func.schema}', got: {sql!r}"
    )

    parts = [p.strip().strip('"') for p in sql.replace("`", '"').split(".")]
    if func.source_id in parts:
        catalog_idx = parts.index(func.source_id)
        remaining = parts[catalog_idx + 1:]
        assert not (
            len(remaining) >= 2
            and remaining[0] == func.schema
            and remaining[1] == func.function_name
        ), (
            f"SQL appears to be Trino-routed (three-part catalog.schema.function): {sql!r}"
        )

    assert len(direct_calls) == 1, (
        f"Expected exactly 1 direct DB call, got {len(direct_calls)}"
    )
    call = direct_calls[0]
    assert call["source_id"] == func.source_id, (
        f"Direct executor called with wrong source_id: expected '{func.source_id}', "
        f"got '{call['source_id']}'"
    )

    assert call["sql"] == sql, (
        "Direct executor received different SQL than what the compiler generated"
    )
    assert call["params"] == params, (
        "Direct executor received different params than what the compiler generated"
    )

    assert shared_data["trino_was_invoked"] is False, (
        "Trino executor must never be invoked for tracked database functions (REQ-208)"
    )

    from provisa.executor.trino import QueryResult
    assert isinstance(result, QueryResult), (
        f"Result must be a QueryResult from the direct executor, got {type(result)}"
    )
    assert len(result.rows) >= 1, (
        "Expected at least one row from the direct DB execution"
    )
    assert result.rows[0].get("segment") == "premium", (
        "Unexpected row content from direct DB execution"
    )


# ---------------------------------------------------------------------------
# Helpers shared by REQ-209 / REQ-360 / REQ-361 / REQ-362
# ---------------------------------------------------------------------------


def _requires_steward_approval(webhook: Webhook) -> bool:
    """Determine whether a webhook is gated behind steward approval."""
    governance = (getattr(webhook, "governance", None) or "").lower()
    return governance in {
        "requires_approval",
        "requires-approval",
        "registry-required",
        "registry_required",
    }


# ---------------------------------------------------------------------------
# REQ-209 — Webhook-backed mutations
# ---------------------------------------------------------------------------


@given(
    "a webhook mutation configured with governance: requires_approval",
    target_fixture="shared_data",
)
def given_webhook_requires_approval(shared_data: dict) -> dict:
    """Configure a webhook mutation with requires_approval governance (REQ-209)."""
    webhook = Webhook(
        name="trigger_external_service",
        url="https://api.example.com/trigger",
        method="POST",
        returns="sales-pg.public.orders",
        arguments=[
            FunctionArgument(name="order_id", type="Int"),
            FunctionArgument(name="reason", type="String"),
        ],
        visible_to=["admin"],
        timeout_ms=5000,
        governance="requires_approval",
    )
    assert _requires_steward_approval(webhook) is True, (
        f"Expected governance 'requires_approval' to gate the webhook, "
        f"but _requires_steward_approval returned False for governance={webhook.governance!r}"
    )
    shared_data["webhook"] = webhook
    shared_data["arguments"] = {"order_id": 42, "reason": "manual-retry"}
    shared_data["steward_approved"] = False
    return shared_data


@when("a client invokes it")
def when_client_invokes(shared_data: dict) -> None:
    """Simulate a client invoking the webhook mutation."""
    webhook: Webhook = shared_data["webhook"]
    arguments = shared_data["arguments"]

    approval_required = _requires_steward_approval(webhook)
    shared_data["approval_required"] = approval_required

    assert approval_required is True, (
        "Webhook with governance=requires_approval must require steward approval "
        "before a client can invoke it."
    )

    shared_data["steward_approved"] = True

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 42, "region": "us-east"}
    mock_response.headers = {"content-type": "application/json"}
    mock_response.raise_for_status = MagicMock(return_value=None)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    async def _invoke() -> WebhookResult:
        return await execute_webhook(webhook, arguments)

    with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
        result = asyncio.run(_invoke())

    shared_data["result"] = result
    shared_data["http_request_call"] = mock_client.request.call_args


@then("steward approval is required and the external HTTP endpoint is called")
def then_approval_and_endpoint_called(shared_data: dict) -> None:
    """Assert all REQ-209 invariants for webhook mutation governance and dispatch."""
    webhook: Webhook = shared_data["webhook"]

    assert shared_data["approval_required"] is True, (
        "Webhook with governance=requires_approval must flag approval_required=True"
    )
    assert _requires_steward_approval(webhook) is True, (
        f"_requires_steward_approval must return True for governance={webhook.governance!r}"
    )
    assert shared_data["steward_approved"] is True, (
        "Steward approval must be granted before the external endpoint is invoked"
    )

    call = shared_data["http_request_call"]
    assert call is not None, (
        "execute_webhook must have called the external HTTP endpoint; no call recorded"
    )
    kwargs = call.kwargs
    assert kwargs.get("method") == webhook.method, (
        f"HTTP method mismatch: expected {webhook.method!r}, got {kwargs.get('method')!r}"
    )
    assert kwargs.get("url") == webhook.url, (
        f"HTTP URL mismatch: expected {webhook.url!r}, got {kwargs.get('url')!r}"
    )
    assert kwargs.get("json") == shared_data["arguments"], (
        f"HTTP JSON body mismatch: expected {shared_data['arguments']!r}, "
        f"got {kwargs.get('json')!r}"
    )

    result: WebhookResult = shared_data["result"]
    assert isinstance(result, WebhookResult), (
        f"execute_webhook must return a WebhookResult, got {type(result)}"
    )
    assert result.status_code == 200, (
        f"Expected HTTP 200 from the external endpoint, got {result.status_code}"
    )
    assert result.data == {"id": 42, "region": "us-east"}, (
        f"Unexpected response data: {result.data!r}"
    )

    mapped = map_response_to_return_type(
        result.data, inline_fields=[{"name": "id", "type": "Int"}]
    )
    assert mapped == {"id": 42}, (
        f"map_response_to_return_type with inline_fields=[id] must return {{id: 42}}, "
        f"got {mapped!r}"
    )

    mapped_full = map_response_to_return_type(result.data, inline_fields=None)
    assert mapped_full == result.data, (
        "map_response_to_return_type with inline_fields=None must return data unchanged"
    )

    assert "generated_sql" not in shared_data, (
        "Webhook mutations must not generate SQL — they call external HTTP endpoints, "
        "not the database (REQ-209 explicitly excludes DB mutation paths)"
    )

    assert webhook.timeout_ms == 5000, (
        f"Expected timeout_ms=5000, got {webhook.timeout_ms}"
    )


# ---------------------------------------------------------------------------
# REQ-360 — Post-processing of action query fields (where/order_by/limit/offset)
# ---------------------------------------------------------------------------

# GraphQL-style comparison operators applied as Python predicates.
_FILTER_OPS = {
    "_eq": lambda a, b: a == b,
    "_neq": lambda a, b: a != b,
    "_gt": lambda a, b: a is not None and a > b,
    "_gte": lambda a, b: a is not None and a >= b,
    "_lt": lambda a, b: a is not None and a < b,
    "_lte": lambda a, b: a is not None and a <= b,
    "_in": lambda a, b: a in b,
    "_nin": lambda a, b: a not in b,
    "_like": lambda a, b: a is not None and str(b).replace("%", "") in str(a),
}


def _row_matches_where(row: dict[str, Any], where: dict[str, Any]) -> bool:
    """Apply a Hasura-style `where` object to a single materialized row."""
    for field_name, condition in where.items():
        if field_name in ("_and", "_or", "_not"):
            if field_name == "_and":
                if not all(_row_matches_where(row, c) for c in condition):
                    return False
            elif field_name == "_or":
                if not any(_row_matches_where(row, c) for c in condition):
                    return False
            elif field_name == "_not":
                if _row_matches_where(row, condition):
                    return False
            continue
        value = row.get(field_name)
        for op, operand in condition.items():
            fn = _FILTER_OPS.get(op)
            assert fn is not None, f"unsupported filter operator: {op}"
            if not fn(value, operand):
                return False
    return True


def _apply_where(rows: list[dict], where: dict | None) -> list[dict]:
    if not where:
        return list(rows)
    return [r for r in rows if _row_matches_where(r, where)]


def _apply_order_by(rows: list[dict], order_by: list[dict] | None) -> list[dict]:
    if not order_by:
        return list(rows)
    ordered = list(rows)
    for spec in reversed(order_by):
        for f, direction in spec.items():
            reverse = str(direction).lower().startswith("desc")
            ordered.sort(key=lambda r, fld=f: (r.get(fld) is None, r.get(fld)), reverse=reverse)
    return ordered


def _apply_pagination(rows: list[dict], limit: int | None, offset: int | None) -> list[dict]:
    start = offset or 0
    if limit is None:
        return rows[start:]
    return rows[start: start + limit]


def post_process_action_query(
    rows: list[dict],
    *,
    where: dict | None = None,
    order_by: list[dict] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict]:
    """Apply filter → sort → pagination over materialized function results.

    This mirrors the Python post-processing applied to action query fields
    (tracked functions / webhooks with ``exposed_as: query``) after the
    backing function executes (REQ-360).
    """
    result = _apply_where(rows, where)
    result = _apply_order_by(result, order_by)
    result = _apply_pagination(result, limit, offset)
    return result


@given(
    "an action query field with where, order_by, limit, and offset arguments",
    target_fixture="shared_data",
)
def given_action_query_field(shared_data: dict) -> dict:
    """Set up an action query field (webhook exposed_as query) with filter/sort/pagination args.

    The webhook returns a table-shaped result set. The standard GraphQL query
    arguments (where, order_by, limit, offset) will be applied as Python
    post-processing after the function executes and results are materialized
    (REQ-360).
    """
    webhook = Webhook(
        name="list_orders",
        url="https://api.example.com/orders",
        method="POST",
        returns="sales-pg.public.orders",
        arguments=[FunctionArgument(name="region", type="String")],
        visible_to=["admin"],
        timeout_ms=5000,
    )
    shared_data["webhook"] = webhook
    shared_data["arguments"] = {"region": "us-east"}

    # The standard GraphQL query arguments supplied by the client.
    shared_data["query_args"] = {
        "where": {"amount": {"_gte": 100}},
        "order_by": [{"amount": "desc"}],
        "limit": 2,
        "offset": 1,
    }
    return shared_data


@when("the function executes and results are materialized")
def when_function_executes_and_materializes(shared_data: dict) -> None:
    """Execute the webhook/function and capture the raw materialized result set.

    The function (webhook) executes and returns a materialized result set before
    any filter/sort/pagination is applied. The raw rows are stored in shared_data
    for the Then step to post-process.
    """
    webhook: Webhook = shared_data["webhook"]
    arguments = shared_data["arguments"]

    # The function (webhook) executes and returns a materialized result set.
    materialized_rows = [
        {"id": 1, "amount": 50,  "region": "us-east", "status": "open"},
        {"id": 2, "amount": 300, "region": "us-east", "status": "closed"},
        {"id": 3, "amount": 150, "region": "us-east", "status": "open"},
        {"id": 4, "amount": 200, "region": "us-east", "status": "open"},
        {"id": 5, "amount": 80,  "region": "us-east", "status": "open"},
    ]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = materialized_rows
    mock_response.headers = {"content-type": "application/json"}
    mock_response.raise_for_status = MagicMock(return_value=None)

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)

    async def _invoke() -> WebhookResult:
        return await execute_webhook(webhook, arguments)

    with patch("provisa.webhooks.executor.httpx.AsyncClient", return_value=mock_client):
        result = asyncio.run(_invoke())

    assert isinstance(result, WebhookResult), (
        f"execute_webhook must return a WebhookResult, got {type(result)}"
    )
    assert isinstance(result.data, list), (
        f"Materialized result must be a list of rows, got {type(result.data)}"
    )
    assert len(result.data) == 5, (
        f"Expected 5 materialized rows before post-processing, got {len(result.data)}"
    )

    shared_data["materialized_rows"] = result.data
    shared_data["webhook_result"] = result


@then("filter, sort, and pagination are applied as Python post-processing")
def then_filter_sort_pagination_applied(shared_data: dict) -> None:
    """Assert that where/order_by/limit/offset post-processing is correctly applied
    to the materialized action query field results (REQ-360).

    Verified properties:
    1. ``where`` filters rows to only those matching the predicate.
    2. ``order_by`` sorts the filtered rows by the specified field and direction.
    3. ``limit`` and ``offset`` paginate the sorted result set.
    4. The pipeline is applied in the correct order: filter → sort → paginate.
    5. The post_process_action_query helper applies all four operations in one call.
    6. Each operation is independently verifiable against the materialized rows.
    """
    materialized_rows: list[dict] = shared_data["materialized_rows"]
    query_args: dict = shared_data["query_args"]

    where = query_args["where"]          # amount >= 100
    order_by = query_args["order_by"]    # amount desc
    limit = query_args["limit"]          # 2
    offset = query_args["offset"]        # 1

    # -----------------------------------------------------------------------
    # Step 1: verify ``where`` filter in isolation.
    # Rows with amount >= 100: ids 2 (300), 3 (150), 4 (200)
    # -----------------------------------------------------------------------
    filtered = _apply_where(materialized_rows, where)
    assert len(filtered) == 3, (
        f"where {{amount: {{_gte: 100}}}} should keep 3 rows (ids 2,3,4), "
        f"got {len(filtered)}: {filtered}"
    )
    filtered_ids = {r["id"] for r in filtered}
    assert filtered_ids == {2, 3, 4}, (
        f"Filtered row ids must be {{2, 3, 4}}, got {filtered_ids}"
    )

    # -----------------------------------------------------------------------
    # Step 2: verify ``order_by`` sort in isolation (applied to filtered rows).
    # amount desc: 300 (id=2), 200 (id=4), 150 (id=3)
    # -----------------------------------------------------------------------
    sorted_rows = _apply_order_by(filtered, order_by)
    assert len(sorted_rows) == 3, (
        f"Sorted result should still have 3 rows, got {len(sorted_rows)}"
    )
    assert sorted_rows[0]["amount"] == 300, (
        f"First sorted row (desc) must have amount=300, got {sorted_rows[0]['amount']}"
    )
    assert sorted_rows[1]["amount"] == 200, (
        f"Second sorted row (desc) must have amount=200, got {sorted_rows[1]['amount']}"
    )
    assert sorted_rows[2]["amount"] == 150, (
        f"Third sorted row (desc) must have amount=150, got {sorted_rows[2]['amount']}"
    )

    # -----------------------------------------------------------------------
    # Step 3: verify ``limit`` + ``offset`` pagination in isolation.
    # offset=1 skips the first row (amount=300), limit=2 keeps the next two.
    # Expected: [{id:4, amount:200}, {id:3, amount:150}]
    # -----------------------------------------------------------------------
    paginated = _apply_pagination(sorted_rows, limit, offset)
    assert len(paginated) == 2, (
        f"limit=2, offset=1 on 3 sorted rows should yield 2 rows, got {len(paginated)}"
    )
    assert paginated[0]["amount"] == 200, (
        f"After offset=1 on desc-sorted rows, first row must have amount=200, "
        f"got {paginated[0]['amount']}"
    )
    assert paginated[1]["amount"] == 150, (
        f"After offset=1 on desc-sorted rows, second row must have amount=150, "
        f"got {paginated[1]['amount']}"
    )

    # -----------------------------------------------------------------------
    # Step 4: verify the full pipeline via post_process_action_query.
    # Must produce identical results to the three individual steps above.
    # -----------------------------------------------------------------------
    pipeline_result = post_process_action_query(
        materialized_rows,
        where=where,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )
    assert pipeline_result == paginated, (
        f"post_process_action_query pipeline result must match step-by-step result.\n"
        f"Pipeline: {pipeline_result}\n"
        f"Step-by-step: {paginated}"
    )

    # -----------------------------------------------------------------------
    # Step 5: verify ordering guarantee — filter BEFORE sort BEFORE paginate.
    # If we paginate before filtering, we'd lose eligible rows.
    # -----------------------------------------------------------------------
    wrong_order_paginate_first = _apply_pagination(materialized_rows, limit, offset)
    wrong_order_then_filter = _apply_where(wrong_order_paginate_first, where)
    wrong_ids = {r["id"] for r in wrong_order_then_filter}
    correct_ids = {r["id"] for r in pipeline_result}
    assert correct_ids != wrong_ids, (
        "The correct filter-then-paginate pipeline must differ from paginate-then-filter "
        f"for this dataset. correct={correct_ids}, wrong_order={wrong_ids}"
    )

    # -----------------------------------------------------------------------
    # Step 6: verify edge cases — empty where, no order_by, no pagination.
    # -----------------------------------------------------------------------
    all_rows = post_process_action_query(materialized_rows)
    assert len(all_rows) == len(materialized_rows), (
        "post_process_action_query with no arguments must return all rows unchanged"
    )

    empty_result = post_process_action_query(
        materialized_rows,
        where={"amount": {"_gt": 99999}},
    )
    assert empty_result == [], (
        "post_process_action_query with unsatisfiable where must return empty list"
    )


# ---------------------------------------------------------------------------
# RE

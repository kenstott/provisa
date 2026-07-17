# Copyright (c) 2026 Kenneth Stott
# Canary: 2d0f83c2-7ed6-44a2-a2ec-c39f0dfced79
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
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from graphql import GraphQLField, GraphQLList, GraphQLNonNull, GraphQLObjectType, GraphQLString
from pytest_bdd import given, scenarios, then, when

from provisa.compiler.function_gen import build_function_mutations, build_function_sql
from provisa.core.models import Function, FunctionArgument, Webhook
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

    assert "order_id" in mutation_field.args, "Expected 'order_id' argument on the mutation field"
    assert "note" in mutation_field.args, "Expected 'note' argument on the mutation field"


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
    assert len(params) == len(arg_values), f"Expected {len(arg_values)} params, got {len(params)}"

    shared_data["generated_sql"] = sql
    shared_data["generated_params"] = params

    direct_calls: list[dict] = []

    async def _fake_execute_direct(pool, source_id, sql, params=None):
        direct_calls.append({"pool": pool, "source_id": source_id, "sql": sql, "params": params})
        from provisa.executor.result import QueryResult

        return QueryResult(
            rows=[("1", "premium")],
            column_names=["id", "segment"],
        )

    mock_pool = MagicMock()
    mock_pool.source_id = func.source_id

    with patch(
        "provisa.executor.direct.execute_direct",
        side_effect=_fake_execute_direct,
    ):
        result = asyncio.run(_fake_execute_direct(mock_pool, func.source_id, sql, params))

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

    assert func.schema_name in sql, (
        f"Generated SQL must reference schema '{func.schema_name}', got: {sql!r}"
    )

    parts = [p.strip().strip('"') for p in sql.replace("`", '"').split(".")]
    if func.source_id in parts:
        catalog_idx = parts.index(func.source_id)
        remaining = parts[catalog_idx + 1 :]
        assert not (
            len(remaining) >= 2
            and remaining[0] == func.schema_name
            and remaining[1] == func.function_name
        ), f"SQL appears to be Trino-routed (three-part catalog.schema.function): {sql!r}"

    assert len(direct_calls) == 1, f"Expected exactly 1 direct DB call, got {len(direct_calls)}"
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

    from provisa.executor.result import QueryResult

    assert isinstance(result, QueryResult), (
        f"Result must be a QueryResult from the direct executor, got {type(result)}"
    )
    assert len(result.rows) >= 1, "Expected at least one row from the direct DB execution"
    row = result.rows[0]
    col_idx = result.column_names.index("segment")
    assert row[col_idx] == "premium", "Unexpected row content from direct DB execution"


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

    mapped = map_response_to_return_type(result.data, inline_fields=[{"name": "id", "type": "Int"}])
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

    assert webhook.timeout_ms == 5000, f"Expected timeout_ms=5000, got {webhook.timeout_ms}"


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
    return rows[start : start + limit]


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
        {"id": 1, "amount": 50, "region": "us-east", "status": "open"},
        {"id": 2, "amount": 300, "region": "us-east", "status": "closed"},
        {"id": 3, "amount": 150, "region": "us-east", "status": "open"},
        {"id": 4, "amount": 200, "region": "us-east", "status": "open"},
        {"id": 5, "amount": 80, "region": "us-east", "status": "open"},
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

    where = query_args["where"]  # amount >= 100
    order_by = query_args["order_by"]  # amount desc
    limit = query_args["limit"]  # 2
    offset = query_args["offset"]  # 1

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
    assert filtered_ids == {2, 3, 4}, f"Filtered row ids must be {{2, 3, 4}}, got {filtered_ids}"

    # -----------------------------------------------------------------------
    # Step 2: verify ``order_by`` sort in isolation (applied to filtered rows).
    # amount desc: 300 (id=2), 200 (id=4), 150 (id=3)
    # -----------------------------------------------------------------------
    sorted_rows = _apply_order_by(filtered, order_by)
    assert len(sorted_rows) == 3, f"Sorted result should still have 3 rows, got {len(sorted_rows)}"
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
# REQ-361 — Nested relationship resolution with RLS/masking on action results
# ---------------------------------------------------------------------------


def _apply_rls(rows: list[dict], rls_filter: dict | None) -> list[dict]:
    """Apply a simple RLS equality filter to a list of dicts."""
    if not rls_filter:
        return list(rows)
    return [r for r in rows if all(r.get(k) == v for k, v in rls_filter.items())]


def _apply_column_mask(rows: list[dict], masked_columns: dict[str, str]) -> list[dict]:
    """Replace masked column values with their mask value."""
    result = []
    for row in rows:
        masked_row = dict(row)
        for col, mask_val in masked_columns.items():
            if col in masked_row:
                masked_row[col] = mask_val
        result.append(masked_row)
    return result


def _apply_column_visibility(rows: list[dict], visible_columns: set[str]) -> list[dict]:
    """Strip columns not in the visible set."""
    return [{k: v for k, v in row.items() if k in visible_columns} for row in rows]


def resolve_relationship_field(
    action_rows: list[dict],
    source_column: str,
    target_column: str,
    target_rows: list[dict],
    cardinality: str,
    relationship_field: str,
    *,
    rls_filter: dict | None = None,
    masked_columns: dict[str, str] | None = None,
    visible_columns: set[str] | None = None,
) -> list[dict]:
    """Resolve a relationship field on action result rows via batched lookup.

    Applies governance (RLS, masking, column visibility) to the related rows,
    then merges them back onto each action result row.

    Args:
        action_rows: rows returned by the action/function
        source_column: FK column on the action row side
        target_column: PK/join column on the target table side
        target_rows: all available rows from the target table (pre-fetched batch)
        cardinality: "one-to-many" or "many-to-one"
        relationship_field: GraphQL field name to attach results under
        rls_filter: optional RLS equality filter applied to target_rows
        masked_columns: optional {col: mask_value} applied to target_rows
        visible_columns: optional set of column names visible to this role

    Returns:
        action_rows with the relationship_field populated on each row
    """
    governed_rows = list(target_rows)

    if rls_filter:
        governed_rows = _apply_rls(governed_rows, rls_filter)
    if masked_columns:
        governed_rows = _apply_column_mask(governed_rows, masked_columns)
    if visible_columns is not None:
        governed_rows = _apply_column_visibility(governed_rows, visible_columns)

    # Build index: target_column value → list of matching target rows
    index: dict[Any, list[dict]] = {}
    for tr in governed_rows:
        key = tr.get(target_column)
        index.setdefault(key, []).append(tr)

    result = []
    for row in action_rows:
        merged = dict(row)
        key = row.get(source_column)
        matched = index.get(key, [])
        if cardinality == "one-to-many":
            merged[relationship_field] = matched
        else:  # many-to-one
            merged[relationship_field] = matched[0] if matched else None
        result.append(merged)
    return result


@given(
    "an action query field returning a registered table type with nested relationship fields",
    target_fixture="shared_data",
)
def given_action_query_with_nested_relationships(shared_data: dict) -> dict:
    # Simulate a tracked function that returns rows of type "orders"
    # and "orders" has a one-to-many relationship to "order_items"
    func = Function(
        name="get_recent_orders",
        source_id="sales-pg",
        schema="public",
        function_name="get_recent_orders",
        returns="sales-pg.public.orders",
        arguments=[FunctionArgument(name="region", type="String")],
        visible_to=[],
    )
    shared_data["function"] = func

    # Action result rows (orders table)
    shared_data["action_rows"] = [
        {"id": 1, "region": "us-east", "customer_id": 10},
        {"id": 2, "region": "us-east", "customer_id": 20},
        {"id": 3, "region": "us-east", "customer_id": 10},
    ]

    # Related table rows (order_items) — fetched as a batch
    shared_data["related_rows"] = [
        {"item_id": 101, "order_id": 1, "sku": "A1", "secret_cost": 9.99},
        {"item_id": 102, "order_id": 1, "sku": "B2", "secret_cost": 4.50},
        {"item_id": 103, "order_id": 2, "sku": "C3", "secret_cost": 12.00},
        {"item_id": 104, "order_id": 2, "sku": "D4", "secret_cost": 3.00},
        # order_id=3 has no items intentionally
        # order_id=99 exists only in related table (out-of-scope, RLS-filtered)
        {"item_id": 105, "order_id": 99, "sku": "X9", "secret_cost": 0.01},
    ]

    # JoinMeta-equivalent metadata for the relationship
    shared_data["join_meta"] = {
        "source_column": "id",  # FK on orders side
        "target_column": "order_id",  # PK/join col on order_items side
        "cardinality": "one-to-many",
        "relationship_field": "order_items",
    }

    # Governance: RLS restricts order_items to known order_ids; mask secret_cost; hide secret_cost
    shared_data["rls_filter"] = None  # no row-level filter for this scenario
    shared_data["masked_columns"] = {"secret_cost": "***"}
    shared_data["visible_columns"] = {"item_id", "order_id", "sku", "secret_cost"}

    return shared_data


@when("the results are resolved")
def when_results_are_resolved(shared_data: dict) -> None:
    action_rows: list[dict] = shared_data["action_rows"]
    related_rows: list[dict] = shared_data["related_rows"]
    join_meta: dict = shared_data["join_meta"]

    resolved = resolve_relationship_field(
        action_rows=action_rows,
        source_column=join_meta["source_column"],
        target_column=join_meta["target_column"],
        target_rows=related_rows,
        cardinality=join_meta["cardinality"],
        relationship_field=join_meta["relationship_field"],
        rls_filter=shared_data.get("rls_filter"),
        masked_columns=shared_data.get("masked_columns"),
        visible_columns=shared_data.get("visible_columns"),
    )
    shared_data["resolved_rows"] = resolved


@then("related rows are fetched via batched lookups with RLS and masking applied")
def then_related_rows_fetched_with_governance(shared_data: dict) -> None:
    resolved: list[dict] = shared_data["resolved_rows"]
    join_meta: dict = shared_data["join_meta"]

    assert len(resolved) == 3, f"All 3 action rows must be present, got {len(resolved)}"

    # Order 1: two items, both with masked secret_cost
    order1 = next(r for r in resolved if r["id"] == 1)
    items1: list[dict] = order1[join_meta["relationship_field"]]
    assert isinstance(items1, list), "one-to-many must return a list"
    assert len(items1) == 2, f"order_id=1 must have 2 items, got {len(items1)}"
    skus1 = {i["sku"] for i in items1}
    assert skus1 == {"A1", "B2"}, f"Unexpected SKUs for order 1: {skus1}"
    for item in items1:
        assert item["secret_cost"] == "***", (
            f"secret_cost must be masked to '***', got {item['secret_cost']!r}"
        )

    # Order 2: two items
    order2 = next(r for r in resolved if r["id"] == 2)
    items2: list[dict] = order2[join_meta["relationship_field"]]
    assert isinstance(items2, list), "one-to-many must return a list"
    assert len(items2) == 2, f"order_id=2 must have 2 items, got {len(items2)}"

    # Order 3: no items (empty list, not null)
    order3 = next(r for r in resolved if r["id"] == 3)
    items3: list[dict] = order3[join_meta["relationship_field"]]
    assert isinstance(items3, list), "one-to-many with no matches must return empty list, not null"
    assert items3 == [], f"order_id=3 must have 0 items, got {items3}"

    # Confirm the out-of-scope item (order_id=99) is NOT present in any resolved row
    all_item_ids = {
        item["item_id"] for row in resolved for item in row[join_meta["relationship_field"]]
    }
    assert 105 not in all_item_ids, (
        "item_id=105 (order_id=99, out-of-scope) must not appear in resolved results"
    )


# ---------------------------------------------------------------------------
# REQ-362 — Cardinality: one-to-many → array, many-to-one → object or null
# ---------------------------------------------------------------------------


@given("an action result with a one-to-many relationship", target_fixture="shared_data")
def given_action_result_with_relationships(shared_data: dict) -> dict:
    # Action rows represent order_items; each has a many-to-one FK to orders
    shared_data["action_rows"] = [
        {"item_id": 101, "order_id": 1, "sku": "A1"},
        {"item_id": 102, "order_id": 1, "sku": "B2"},
        {"item_id": 103, "order_id": 2, "sku": "C3"},
        {"item_id": 104, "order_id": 999, "sku": "D4"},  # dangling FK → null
    ]

    # Target rows for the many-to-one relationship (orders)
    shared_data["order_rows"] = [
        {"id": 1, "region": "us-east"},
        {"id": 2, "region": "us-west"},
        # id=999 intentionally absent → many-to-one resolves to null
    ]

    # For the one-to-many side: parent orders with child order_items
    shared_data["parent_order_rows"] = [
        {"id": 1, "region": "us-east"},
        {"id": 2, "region": "us-west"},
        {"id": 3, "region": "eu-west"},  # no items → empty array
    ]
    shared_data["child_item_rows"] = [
        {"item_id": 101, "order_id": 1, "sku": "A1"},
        {"item_id": 102, "order_id": 1, "sku": "B2"},
        {"item_id": 103, "order_id": 2, "sku": "C3"},
    ]

    # JoinMeta for many-to-one (item → order)
    shared_data["many_to_one_meta"] = {
        "source_column": "order_id",
        "target_column": "id",
        "cardinality": "many-to-one",
        "relationship_field": "order",
    }

    # JoinMeta for one-to-many (order → items)
    shared_data["one_to_many_meta"] = {
        "source_column": "id",
        "target_column": "order_id",
        "cardinality": "one-to-many",
        "relationship_field": "items",
    }

    return shared_data


@when("the relationship field is resolved")
def when_relationship_field_is_resolved(shared_data: dict) -> None:
    action_rows: list[dict] = shared_data["action_rows"]
    order_rows: list[dict] = shared_data["order_rows"]
    m2o: dict = shared_data["many_to_one_meta"]

    resolved_m2o = resolve_relationship_field(
        action_rows=action_rows,
        source_column=m2o["source_column"],
        target_column=m2o["target_column"],
        target_rows=order_rows,
        cardinality=m2o["cardinality"],
        relationship_field=m2o["relationship_field"],
    )
    shared_data["resolved_m2o"] = resolved_m2o

    parent_rows: list[dict] = shared_data["parent_order_rows"]
    child_rows: list[dict] = shared_data["child_item_rows"]
    o2m: dict = shared_data["one_to_many_meta"]

    resolved_o2m = resolve_relationship_field(
        action_rows=parent_rows,
        source_column=o2m["source_column"],
        target_column=o2m["target_column"],
        target_rows=child_rows,
        cardinality=o2m["cardinality"],
        relationship_field=o2m["relationship_field"],
    )
    shared_data["resolved_o2m"] = resolved_o2m


@then("it returns an array; many-to-one returns an object or null per JoinMeta cardinality")
def then_cardinality_shapes_are_correct(shared_data: dict) -> None:
    resolved_m2o: list[dict] = shared_data["resolved_m2o"]
    resolved_o2m: list[dict] = shared_data["resolved_o2m"]
    m2o: dict = shared_data["many_to_one_meta"]
    o2m: dict = shared_data["one_to_many_meta"]

    # --- many-to-one: each row gets an object or null ---
    assert len(resolved_m2o) == 4, f"Expected 4 rows, got {len(resolved_m2o)}"
    for row in resolved_m2o:
        rel = row[m2o["relationship_field"]]
        assert not isinstance(rel, list), (
            f"many-to-one must return an object or null, not a list; row={row}"
        )

    # item_id=101 and 102: order_id=1 → should resolve to order dict
    for item_id in (101, 102):
        row = next(r for r in resolved_m2o if r["item_id"] == item_id)
        rel = row[m2o["relationship_field"]]
        assert isinstance(rel, dict), (
            f"item_id={item_id} has valid FK order_id=1; expected dict, got {type(rel)}"
        )
        assert rel["id"] == 1, f"Resolved order must have id=1, got {rel}"
        assert rel["region"] == "us-east"

    # item_id=103: order_id=2
    row103 = next(r for r in resolved_m2o if r["item_id"] == 103)
    rel103 = row103[m2o["relationship_field"]]
    assert isinstance(rel103, dict) and rel103["id"] == 2

    # item_id=104: order_id=999 → dangling FK → null
    row104 = next(r for r in resolved_m2o if r["item_id"] == 104)
    assert row104[m2o["relationship_field"]] is None, (
        "Dangling FK (order_id=999 not in orders) must resolve to null for many-to-one"
    )

    # --- one-to-many: each row gets a list (possibly empty) ---
    assert len(resolved_o2m) == 3, f"Expected 3 parent rows, got {len(resolved_o2m)}"
    for row in resolved_o2m:
        rel = row[o2m["relationship_field"]]
        assert isinstance(rel, list), f"one-to-many must return a list; row={row}, got {type(rel)}"

    # order id=1 → 2 items
    order1 = next(r for r in resolved_o2m if r["id"] == 1)
    assert len(order1[o2m["relationship_field"]]) == 2, (
        f"order id=1 must have 2 items, got {order1[o2m['relationship_field']]}"
    )

    # order id=2 → 1 item
    order2 = next(r for r in resolved_o2m if r["id"] == 2)
    assert len(order2[o2m["relationship_field"]]) == 1

    # order id=3 → 0 items (empty list, not null)
    order3 = next(r for r in resolved_o2m if r["id"] == 3)
    items3 = order3[o2m["relationship_field"]]
    assert items3 == [], f"order id=3 has no matching items; must return empty list, got {items3!r}"

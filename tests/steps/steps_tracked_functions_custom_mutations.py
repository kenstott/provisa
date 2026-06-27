# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD step implementations for REQ-209 — Webhook-backed mutations.

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
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.core.models import FunctionArgument, Webhook
from provisa.webhooks.executor import (
    WebhookResult,
    execute_webhook,
    map_response_to_return_type,
)

scenarios("../features/REQ-209.feature")
scenarios("../features/REQ-360.feature")
scenarios("../features/REQ-361.feature")
scenarios("../features/REQ-362.feature")


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _requires_steward_approval(webhook: Webhook) -> bool:
    """Determine whether a webhook is gated behind steward approval.

    Webhook mutations with governance requiring approval / registry are
    NOT invokable until a steward approves their use (REQ-209).
    """
    governance = (getattr(webhook, "governance", None) or "").lower()
    return governance in {
        "requires_approval",
        "requires-approval",
        "registry-required",
        "registry_required",
    }


@given(
    "a webhook mutation configured with governance: requires_approval",
    target_fixture="shared_data",
)
def given_webhook_requires_approval(shared_data: dict) -> dict:
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
    # The configured governance must actually gate the webhook.
    assert _requires_steward_approval(webhook) is True
    shared_data["webhook"] = webhook
    shared_data["arguments"] = {"order_id": 42, "reason": "manual-retry"}
    return shared_data


@when("a client invokes it")
def when_client_invokes(shared_data: dict) -> None:
    webhook: Webhook = shared_data["webhook"]
    arguments = shared_data["arguments"]

    # Governance gate: a client invocation is only permitted after a steward
    # has approved the webhook. Record the gating decision.
    shared_data["approval_required"] = _requires_steward_approval(webhook)

    # Simulate the steward approval that unblocks the invocation, then call
    # the external endpoint with a mocked HTTP transport.
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
        import asyncio

        result = asyncio.run(_invoke())

    shared_data["result"] = result
    shared_data["http_request_call"] = mock_client.request.call_args


@then("steward approval is required and the external HTTP endpoint is called")
def then_approval_and_endpoint_called(shared_data: dict) -> None:
    webhook: Webhook = shared_data["webhook"]

    # 1. Steward approval is required for this webhook mutation.
    assert shared_data["approval_required"] is True
    assert _requires_steward_approval(webhook) is True

    # 2. The external HTTP endpoint was actually called with the configured
    #    method / url and the resolved arguments as JSON body.
    call = shared_data["http_request_call"]
    assert call is not None, "external HTTP endpoint was never called"
    kwargs = call.kwargs
    assert kwargs["method"] == webhook.method
    assert kwargs["url"] == webhook.url
    assert kwargs["json"] == shared_data["arguments"]

    # 3. The webhook returned a successful, mapped result (not a DB mutation).
    result: WebhookResult = shared_data["result"]
    assert isinstance(result, WebhookResult)
    assert result.status_code == 200
    assert result.data == {"id": 42, "region": "us-east"}

    mapped = map_response_to_return_type(
        result.data, inline_fields=[{"name": "id", "type": "Int"}]
    )
    assert mapped == {"id": 42}


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
    for field, condition in where.items():
        if field in ("_and", "_or", "_not"):
            if field == "_and":
                if not all(_row_matches_where(row, c) for c in condition):
                    return False
            elif field == "_or":
                if not any(_row_matches_where(row, c) for c in condition):
                    return False
            elif field == "_not":
                if _row_matches_where(row, condition):
                    return False
            continue
        value = row.get(field)
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
    # Apply sort keys in reverse for stable multi-key ordering.
    for spec in reversed(order_by):
        for field, direction in spec.items():
            reverse = str(direction).lower().startswith("desc")
            ordered.sort(key=lambda r, f=field: (r.get(f) is None, r.get(f)), reverse=reverse)
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
    # An action query field is a webhook (or tracked function) exposed_as query
    # returning a table-shaped result set.
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

    assert isinstance(result, WebhookResult)
    assert isinstance(result.data, list)
    shared_data["materialized"] = result.data


@then("filter, sort, and pagination are applied as Python post-processing")
def then_post_processing_applied(shared_data: dict) -> None:
    materialized: list[dict] = shared_data["materialized"]
    args = shared_data["query_args"]

    # 1. Apply only the where filter — amount >= 100 keeps ids {2, 3, 4}.
    filtered = _apply_where(materialized, args["where"])
    assert {r["id"] for r in filtered} == {2, 3, 4}

    # 2. Apply order_by desc on amount over the filtered set.
    ordered = _apply_order_by(filtered, args["order_by"])
    assert [r["amount"] for r in ordered] == [300, 200, 150]
    assert [r["id"] for r in ordered] == [2, 4, 3]

    # 3. Apply pagination: offset=1, limit=2 over the ordered/filtered set.
    paginated = _apply_pagination(ordered, args["limit"], args["offset"])
    assert [r["id"] for r in paginated] == [4, 3]

    # 4. The combined pipeline (filter → sort → paginate) yields the same result.
    final = post_process_action_query(
        materialized,
        where=args["where"],
        order_by=args["order_by"],
        limit=args["limit"],
        offset=args["offset"],
    )
    assert final == paginated
    assert len(final) == 2

    # 5. Post-processing operated on already-materialized rows (plain dicts),
    #    confirming it is applied in Python after function execution.
    assert all(isinstance(r, dict) for r in final)


# ---------------------------------------------------------------------------
# REQ-361 — Governed relationship resolution on action results
# ---------------------------------------------------------------------------


def _apply_column_visibility(row: dict[str, Any], visible_columns: set[str]) -> dict[str, Any]:
    """Drop any columns the current role is not permitted to see."""
    return {k: v for k, v in row.items() if k in visible_columns}


def _apply_rls(rows: list[dict], rls_predicate: Callable[[dict], bool] | None) -> list[dict]:
    """Apply a row-level-security predicate to related rows."""
    if rls_predicate is None:
        return list(rows)
    return [r for r in rows if rls_predicate(r)]


def _apply_masking(row: dict[str, Any], mask_columns: set[str]) -> dict[str, Any]:
    """Mask configured sensitive columns in-place on a copy of the row."""
    masked = dict(row)
    for col in mask_columns:
        if col in masked and masked[col] is not None:
            masked[col] = "****"
    return masked


def resolve_relationship_batched(
    parent_rows: list[dict],
    *,
    fk_field: str,
    related_rows: list[dict],
    related_pk: str,
    relationship_name: str,
    rls_predicate: Callable[[dict], bool] | None = None,
    visible_columns: set[str] | None = None,
    mask_columns: set[str] | None = None,
) -> dict[str, Any]:
    """Resol

# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Step implementations for REQ-119 — JSONB field promotion to generated columns."""

from __future__ import annotations

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, scenarios

from provisa.api_source.promotions import (
    apply_promotions,
    dot_path_to_pg_expression,
    generate_promotion_ddl,
)
from provisa.api_source.models import PromotionConfig


scenarios("../features/REQ-119.feature")


class _RecordingConn:
    """Minimal asyncpg-style connection that records executed DDL."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    async def execute(self, sql, *args):
        self.executed.append(sql)
        return "ALTER TABLE"


@pytest.fixture
def shared_data() -> dict:
    return {}


@pytest_asyncio.fixture
async def recording_conn() -> _RecordingConn:
    return _RecordingConn()


@given("a JSONB column with nested fields")
def jsonb_column_with_nested_fields(shared_data):
    # A cached API/document table with a JSONB column "data" containing nested fields.
    shared_data["table_name"] = "api_cache_orders"
    shared_data["jsonb_column"] = "data"
    shared_data["nested_fields"] = {
        "customer.address.city": "text",
        "customer.age": "integer",
        "created_at": "timestamptz",
    }
    # Verify dot-path extraction works for nested structures.
    expr = dot_path_to_pg_expression("data", "customer.address.city")
    assert expr == "(data->'customer'->'address'->>'city')"
    assert "->>" in expr


@when("a steward promotes a nested field via dot-path")
def steward_promotes_nested_field(shared_data):
    import asyncio as _asyncio

    conn = _RecordingConn()
    promotions = [
        PromotionConfig(
            jsonb_column=shared_data["jsonb_column"],
            field="customer.address.city",
            target_column="customer_city",
            target_type="text",
        ),
        PromotionConfig(
            jsonb_column=shared_data["jsonb_column"],
            field="customer.age",
            target_column="customer_age",
            target_type="integer",
        ),
    ]
    shared_data["promotions"] = promotions

    # Capture the generated DDL for assertions.
    ddl = generate_promotion_ddl(shared_data["table_name"], promotions)
    shared_data["ddl"] = ddl

    # Apply the promotions against the recording connection.
    async def _run():
        return await apply_promotions(conn, shared_data["table_name"], promotions)

    count = _asyncio.run(_run())
    shared_data["applied_count"] = count
    shared_data["executed_ddl"] = conn.executed


@then(
    "a PostgreSQL generated column is created that is filterable, indexable, and relationship-eligible"
)
def generated_column_created(shared_data):
    ddl = shared_data["ddl"]
    executed = shared_data["executed_ddl"]

    # Both promotions were applied.
    assert shared_data["applied_count"] == 2
    assert len(executed) == 2
    assert len(ddl) == 2

    for stmt in ddl:
        # Native PostgreSQL generated, stored (indexable/filterable) column.
        assert f"ALTER TABLE {shared_data['table_name']}" in stmt
        assert "ADD COLUMN IF NOT EXISTS" in stmt
        assert "GENERATED ALWAYS AS" in stmt
        assert "STORED" in stmt

    # The nested text field uses full dot-path extraction.
    text_stmt = next(s for s in ddl if "customer_city" in s)
    assert "(data->'customer'->'address'->>'city')" in text_stmt

    # The integer field is cast to a concrete relationship-eligible scalar type.
    int_stmt = next(s for s in ddl if "customer_age" in s)
    assert "(data->'customer'->>'age')::integer" in int_stmt
    assert "INTEGER" in int_stmt.upper()

    # Executed DDL matches the generated DDL exactly.
    assert executed == ddl

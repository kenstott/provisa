# Copyright (c) 2025 Kenneth Stott
# Canary: cbe736f3-6451-4309-b052-9f4daa4abfdd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for direct execution via pluggable drivers."""

import os

import pytest

from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def source_pool():
    sp = SourcePool()
    await sp.add(
        "test-pg",
        source_type="postgresql",
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    yield sp
    await sp.close_all()


class TestDirectExecution:
    async def test_simple_select(self, source_pool):
        result = await execute_direct(
            source_pool, "test-pg",
            'SELECT "id", "amount" FROM "public"."orders" LIMIT 3',
        )
        assert len(result.rows) <= 3
        assert "id" in result.column_names
        assert "amount" in result.column_names

    async def test_parameterized_query(self, source_pool):
        result = await execute_direct(
            source_pool, "test-pg",
            'SELECT "id", "region" FROM "public"."orders" WHERE "region" = $1',
            ["us-east"],
        )
        for row in result.rows:
            assert row[1] == "us-east"

    async def test_empty_result(self, source_pool):
        result = await execute_direct(
            source_pool, "test-pg",
            'SELECT "id" FROM "public"."orders" WHERE "id" = $1',
            [-999],
        )
        assert result.rows == []
        assert "id" in result.column_names

    async def test_join_query(self, source_pool):
        result = await execute_direct(
            source_pool, "test-pg",
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" '
            'ON "t0"."customer_id" = "t1"."id" '
            'LIMIT 5',
        )
        assert len(result.rows) <= 5
        assert "id" in result.column_names
        assert "name" in result.column_names

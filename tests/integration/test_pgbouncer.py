# Copyright (c) 2026 Kenneth Stott
# Canary: 3d954f17-4c44-48b3-9f4b-642ec43efd6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for PgBouncer connection pooling.

Requires PgBouncer running in Docker Compose (port 6432).
"""

import os

import pytest

from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

PGBOUNCER_PORT = int(os.environ.get("PGBOUNCER_PORT", "6432"))


class TestPgBouncerPool:
    async def test_connect_through_pgbouncer(self):
        """PostgreSQL driver connects through PgBouncer with statement_cache_size=0."""
        sp = SourcePool()
        await sp.add(
            source_id="test-pgb",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=5432,
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
            use_pgbouncer=True,
            pgbouncer_port=PGBOUNCER_PORT,
        )
        assert sp.has("test-pgb")
        result = await sp.execute("test-pgb", "SELECT count(*) AS n FROM orders")
        assert result.rows[0][0] >= 25  # seed data; CDC tests may add rows
        assert result.column_names == ["n"]
        await sp.close_all()

    async def test_parameterized_query_via_pgbouncer(self):
        sp = SourcePool()
        await sp.add(
            source_id="test-pgb",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=5432,
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
            use_pgbouncer=True,
            pgbouncer_port=PGBOUNCER_PORT,
        )
        result = await sp.execute(
            "test-pgb",
            'SELECT "id", "region" FROM "public"."orders" WHERE "region" = $1',
            ["us-east"],
        )
        for row in result.rows:
            assert row[1] == "us-east"
        await sp.close_all()

    async def test_direct_and_pgbouncer_coexist(self):
        """Both direct PG and PgBouncer pools can exist simultaneously."""
        sp = SourcePool()
        # Direct
        await sp.add(
            source_id="direct",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database="provisa", user="provisa",
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )
        # PgBouncer
        await sp.add(
            source_id="bounced",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=5432,
            database="provisa", user="provisa",
            password=os.environ.get("PG_PASSWORD", "provisa"),
            use_pgbouncer=True,
            pgbouncer_port=PGBOUNCER_PORT,
        )
        r1 = await sp.execute("direct", "SELECT count(*) FROM orders")
        r2 = await sp.execute("bounced", "SELECT count(*) FROM orders")
        assert r1.rows[0][0] == r2.rows[0][0]
        await sp.close_all()


class TestPoolSizing:
    async def test_custom_pool_sizes(self):
        """Pool min/max are configurable per source."""
        sp = SourcePool()
        await sp.add(
            source_id="sized",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database="provisa", user="provisa",
            password=os.environ.get("PG_PASSWORD", "provisa"),
            min_size=2, max_size=10,
        )
        result = await sp.execute("sized", "SELECT 1")
        assert result.rows
        await sp.close_all()

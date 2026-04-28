# Copyright (c) 2026 Kenneth Stott
# Canary: e358dbaf-5110-43b2-9736-1cb720c39d5d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for SourcePool lifecycle against a live PostgreSQL instance.

Requires docker-compose stack (postgres service). No skipping.
"""

import os

import pytest

from provisa.executor.pool import SourcePool


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


@pytest.fixture
def pool_params():
    return {
        "source_type": "postgresql",
        "host": os.environ.get("PG_HOST", "localhost"),
        "port": int(os.environ.get("PG_PORT", "5432")),
        "database": os.environ.get("PG_DATABASE", "provisa"),
        "user": os.environ.get("PG_USER", "provisa"),
        "password": os.environ.get("PG_PASSWORD", "provisa"),
    }


class TestSourcePool:
    async def test_add_and_get(self, pool_params):
        sp = SourcePool()
        await sp.add("test-src", **pool_params)
        assert sp.has("test-src")
        driver = sp.get("test-src")
        assert driver is not None
        assert driver.is_connected
        await sp.close_all()

    async def test_double_add_is_noop(self, pool_params):
        sp = SourcePool()
        await sp.add("src", **pool_params)
        driver1 = sp.get("src")
        await sp.add("src", **pool_params)
        driver2 = sp.get("src")
        assert driver1 is driver2
        await sp.close_all()

    async def test_close_single(self, pool_params):
        sp = SourcePool()
        await sp.add("src", **pool_params)
        await sp.close("src")
        assert not sp.has("src")

    async def test_close_all(self, pool_params):
        sp = SourcePool()
        await sp.add("src1", **pool_params)
        await sp.add("src2", **pool_params)
        assert len(sp.source_ids) == 2
        await sp.close_all()
        assert len(sp.source_ids) == 0

    async def test_execute(self, pool_params):
        sp = SourcePool()
        await sp.add("src", **pool_params)
        result = await sp.execute("src", "SELECT 1 AS n")
        assert result.rows == [(1,)]
        assert result.column_names == ["n"]
        await sp.close_all()

    async def test_source_ids(self, pool_params):
        sp = SourcePool()
        await sp.add("a", **pool_params)
        await sp.add("b", **pool_params)
        assert set(sp.source_ids) == {"a", "b"}
        await sp.close_all()

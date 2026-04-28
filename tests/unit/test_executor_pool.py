# Copyright (c) 2026 Kenneth Stott
# Canary: 413ae908-76b8-4a53-85d7-bdf8bd18c970
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for SourcePool lifecycle and DuckDB driver (no external services).

DuckDB is used as the test driver because it is in-process and requires no
network connection. Integration tests for PostgreSQL/MySQL live in test_pool.py.
"""

from __future__ import annotations

import pytest

from provisa.executor.pool import SourcePool
from provisa.executor.trino import QueryResult
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.duckdb_driver import DuckDBDriver


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_duckdb_pool(source_id: str = "duck") -> SourcePool:
    sp = SourcePool()
    await sp.add(
        source_id,
        source_type="duckdb",
        host="",
        port=0,
        database=":memory:",
        user="",
        password="",
    )
    return sp


# ---------------------------------------------------------------------------
# SourcePool — structural tests (no connection needed)
# ---------------------------------------------------------------------------

class TestSourcePoolStructural:
    """Tests that do not require any real database connection."""

    async def test_empty_pool_has_no_sources(self):
        sp = SourcePool()
        assert sp.source_ids == []

    async def test_has_returns_false_for_unknown(self):
        sp = SourcePool()
        assert sp.has("nope") is False

    async def test_get_raises_key_error_for_unknown(self):
        sp = SourcePool()
        with pytest.raises(KeyError):
            sp.get("nonexistent")

    async def test_source_ids_empty_initially(self):
        sp = SourcePool()
        assert sp.source_ids == []

    async def test_close_nonexistent_source_is_noop(self):
        """Closing an unregistered source must not raise."""
        sp = SourcePool()
        await sp.close("ghost")  # should not raise

    async def test_close_all_on_empty_pool_is_noop(self):
        sp = SourcePool()
        await sp.close_all()  # should not raise


# ---------------------------------------------------------------------------
# SourcePool — DuckDB integration (in-process, no network)
# ---------------------------------------------------------------------------

class TestSourcePoolDuckDB:
    async def test_add_registers_source(self):
        sp = await _make_duckdb_pool("src")
        assert sp.has("src")
        await sp.close_all()

    async def test_get_returns_driver(self):
        sp = await _make_duckdb_pool("src")
        driver = sp.get("src")
        assert driver is not None
        assert isinstance(driver, DirectDriver)
        await sp.close_all()

    async def test_driver_is_connected_after_add(self):
        sp = await _make_duckdb_pool("src")
        driver = sp.get("src")
        assert driver.is_connected is True
        await sp.close_all()

    async def test_double_add_is_noop(self):
        """Adding the same source_id twice keeps the original driver."""
        sp = await _make_duckdb_pool("src")
        driver1 = sp.get("src")
        await sp.add(
            "src",
            source_type="duckdb",
            host="",
            port=0,
            database=":memory:",
            user="",
            password="",
        )
        driver2 = sp.get("src")
        assert driver1 is driver2
        await sp.close_all()

    async def test_source_ids_lists_all(self):
        sp = SourcePool()
        await sp.add("a", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        await sp.add("b", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        assert set(sp.source_ids) == {"a", "b"}
        await sp.close_all()

    async def test_close_single_removes_source(self):
        sp = await _make_duckdb_pool("src")
        await sp.close("src")
        assert sp.has("src") is False

    async def test_close_single_leaves_other_sources(self):
        sp = SourcePool()
        await sp.add("keep", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        await sp.add("drop", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        await sp.close("drop")
        assert sp.has("keep") is True
        assert sp.has("drop") is False
        await sp.close_all()

    async def test_close_all_clears_all_sources(self):
        sp = SourcePool()
        await sp.add("a", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        await sp.add("b", source_type="duckdb", host="", port=0,
                     database=":memory:", user="", password="")
        await sp.close_all()
        assert sp.source_ids == []

    async def test_driver_not_connected_after_close(self):
        sp = await _make_duckdb_pool("src")
        driver = sp.get("src")
        await sp.close("src")
        assert driver.is_connected is False

    async def test_execute_simple_select(self):
        sp = await _make_duckdb_pool()
        result = await sp.execute("duck", "SELECT 42 AS answer")
        assert isinstance(result, QueryResult)
        assert result.rows == [(42,)]
        assert result.column_names == ["answer"]
        await sp.close_all()

    async def test_execute_multiple_columns(self):
        sp = await _make_duckdb_pool()
        result = await sp.execute("duck", "SELECT 1 AS a, 2 AS b, 3 AS c")
        assert result.rows == [(1, 2, 3)]
        assert result.column_names == ["a", "b", "c"]
        await sp.close_all()

    async def test_execute_multiple_rows(self):
        sp = await _make_duckdb_pool()
        result = await sp.execute(
            "duck",
            "SELECT * FROM (VALUES (1, 'x'), (2, 'y'), (3, 'z')) AS t(id, name)",
        )
        assert len(result.rows) == 3
        assert result.rows[0] == (1, "x")
        assert result.rows[2] == (3, "z")
        await sp.close_all()

    async def test_execute_with_params(self):
        sp = await _make_duckdb_pool()
        result = await sp.execute(
            "duck",
            "SELECT $1 AS v",
            params=["hello"],
        )
        assert result.rows[0][0] == "hello"
        await sp.close_all()

    async def test_execute_raises_for_unknown_source(self):
        sp = SourcePool()
        with pytest.raises(KeyError):
            await sp.execute("ghost", "SELECT 1")


# ---------------------------------------------------------------------------
# DuckDBDriver — unit-level tests
# ---------------------------------------------------------------------------

class TestDuckDBDriver:
    async def test_not_connected_before_connect(self):
        driver = DuckDBDriver()
        assert driver.is_connected is False

    async def test_connected_after_connect(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        assert driver.is_connected is True
        await driver.close()

    async def test_not_connected_after_close(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        await driver.close()
        assert driver.is_connected is False

    async def test_double_close_is_safe(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        await driver.close()
        await driver.close()  # must not raise

    async def test_execute_returns_query_result(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        result = await driver.execute("SELECT 1 AS n")
        assert isinstance(result, QueryResult)
        assert result.rows == [(1,)]
        assert result.column_names == ["n"]
        await driver.close()

    async def test_execute_with_params(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        result = await driver.execute("SELECT $1 AS v", params=[99])
        assert result.rows == [(99,)]
        await driver.close()

    async def test_execute_string_param(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        result = await driver.execute("SELECT $1 AS s", params=["duckdb"])
        assert result.rows == [("duckdb",)]
        await driver.close()

    async def test_execute_create_and_query_table(self):
        driver = DuckDBDriver()
        await driver.connect("", 0, ":memory:", "", "")
        await driver.execute(
            "CREATE TABLE t (id INTEGER, val VARCHAR)"
        )
        await driver.execute(
            "INSERT INTO t VALUES (1, 'one'), (2, 'two')"
        )
        result = await driver.execute("SELECT id, val FROM t ORDER BY id")
        assert result.rows == [(1, "one"), (2, "two")]
        assert result.column_names == ["id", "val"]
        await driver.close()

# Copyright (c) 2026 Kenneth Stott
# Canary: a438716c-0176-40c1-a634-88bf251c2cb4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-2194: a warehouse driver whose client returns Arrow (Snowflake, Databricks, BigQuery)
reads a DIRECT result through that interface and hands the executor the Arrow table: no Python
row is built at the fetch, and the DIRECT terminal builds none to count the result. Rows are
built from the table only when a consumer that takes rows reads them."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from provisa.executor.result import ArrowResult, is_arrow

_TABLE = pa.table(
    {
        "ID": pa.array([1, 2], pa.int64()),
        "PRICE": pa.array([Decimal("1.50"), None], pa.decimal128(10, 2)),
    }
)


class _ArrowCursor:
    """A cursor whose row fetches fail the test: only its Arrow fetch may be called."""

    def __init__(self, table: pa.Table | None, *, result_format: str = "arrow") -> None:
        self._table = table
        self._query_result_format = result_format
        self.description = None if table is None else [(n,) for n in table.column_names]
        self.closed = False
        self.arrow_kwargs: dict = {}

    def execute(self, sql, params=None, **_kw) -> None:
        self.sql = sql

    def fetch_arrow_all(self, **kwargs):
        self.arrow_kwargs = kwargs
        return self._table

    def fetchall_arrow(self):
        return self._table

    def fetchall(self):
        raise AssertionError("rows fetched from a driver that has an Arrow interface")

    fetchmany = fetchall

    def cancel(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def _assert_arrow_unbuilt(result) -> None:
    assert isinstance(result, ArrowResult) and is_arrow(result)
    assert result._rows is None  # no Python row built yet
    assert result.table is _TABLE
    assert result.stats.row_count == 2


def _assert_rows(result) -> None:
    assert result.column_names == ["ID", "PRICE"]
    assert result.rows == [(1, Decimal("1.50")), (2, None)]


# -- Snowflake -----------------------------------------------------------------------------------


def test_snowflake_hands_the_executor_its_arrow_table():
    from provisa.executor.drivers.snowflake import SnowflakeDriver

    cursor = _ArrowCursor(_TABLE)
    driver = SnowflakeDriver()
    driver._conn = SimpleNamespace(cursor=lambda: cursor)
    result = asyncio.run(driver.execute("SELECT id, price FROM t"))
    _assert_arrow_unbuilt(result)
    # An empty result still comes back as a table (the connector's default is None).
    assert cursor.arrow_kwargs == {"force_return_table": True}
    assert cursor.closed
    _assert_rows(result)


def test_snowflake_json_format_result_is_its_rows():
    """A SHOW or a DDL status comes back in Snowflake's JSON format, for which the connector has
    no Arrow interface: its rows are the result."""
    from provisa.executor.drivers.snowflake import SnowflakeDriver

    class _JsonCursor(_ArrowCursor):
        def fetchall(self):
            return [("Statement executed successfully.",)]

        def fetch_arrow_all(self, **kwargs):
            raise AssertionError("Arrow fetched from a JSON-format result")

    cursor = _JsonCursor(pa.table({"status": ["x"]}), result_format="json")
    driver = SnowflakeDriver()
    driver._conn = SimpleNamespace(cursor=lambda: cursor)
    result = asyncio.run(driver.execute("CREATE SCHEMA s"))
    assert not is_arrow(result)
    assert result.column_names == ["status"]
    assert result.rows == [("Statement executed successfully.",)]


def test_snowflake_statement_without_result_has_no_columns():
    from provisa.executor.drivers.snowflake import SnowflakeDriver

    cursor = _ArrowCursor(None)
    driver = SnowflakeDriver()
    driver._conn = SimpleNamespace(cursor=lambda: cursor)
    result = asyncio.run(driver.execute("USE WAREHOUSE w"))
    assert result.rows == [] and result.column_names == []


def test_snowflake_connects_with_scaled_numbers_as_decimal(monkeypatch):
    """A NUMBER with a scale arrives in Arrow as decimal128 (the row's exact Decimal), not as
    float64, the connector's default for its Arrow fetch."""
    pytest.importorskip("snowflake.connector")
    import snowflake.connector as sf

    from provisa.executor.drivers.snowflake import SnowflakeDriver

    seen: dict = {}
    monkeypatch.setattr(sf, "connect", lambda **kw: seen.update(kw) or object())
    driver = SnowflakeDriver()
    driver.configure({"account": "acct"})
    asyncio.run(driver.connect("", 443, "DB", "u", "p"))
    assert seen["arrow_number_to_decimal"] is True


# -- Databricks ----------------------------------------------------------------------------------


def test_databricks_hands_the_executor_its_arrow_table():
    from provisa.executor.drivers.databricks import DatabricksDriver

    cursor = _ArrowCursor(_TABLE)
    driver = DatabricksDriver()
    result = driver._run(SimpleNamespace(cursor=lambda: cursor), "SELECT id, price FROM t", None)
    _assert_arrow_unbuilt(result)
    assert cursor.closed
    _assert_rows(result)


def test_databricks_statement_without_result_has_no_columns():
    from provisa.executor.drivers.databricks import DatabricksDriver

    cursor = _ArrowCursor(None)
    result = DatabricksDriver()._run(SimpleNamespace(cursor=lambda: cursor), "SET x = 1", None)
    assert result.rows == [] and result.column_names == []


# -- BigQuery ------------------------------------------------------------------------------------


def test_bigquery_hands_the_executor_its_arrow_table():
    from provisa.executor.drivers.bigquery import BigQueryDriver

    def _result():
        raise AssertionError("rows fetched from a driver that has an Arrow interface")

    job = SimpleNamespace(to_arrow=lambda: _TABLE, result=_result)
    driver = BigQueryDriver()
    driver._client = SimpleNamespace(query=lambda sql: job)
    result = asyncio.run(driver.execute("SELECT id, price FROM t"))
    _assert_arrow_unbuilt(result)
    _assert_rows(result)


# -- the DIRECT terminal -------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_direct_terminal_counts_an_arrow_result_without_building_rows():
    from provisa.executor.direct import execute_direct

    def _no_rows(_table):
        raise AssertionError("the DIRECT terminal built rows to count the result")

    arrow = ArrowResult(_TABLE, to_rows=_no_rows)

    async def _execute(*_):
        return arrow

    pool = SimpleNamespace(execute=_execute)
    with patch("provisa.executor.direct._retry_budget", return_value=0.0):
        result = await execute_direct(pool, "src1", "SELECT id, price FROM t")  # type: ignore[arg-type]
    assert result is arrow and result._rows is None

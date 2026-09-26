# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouseDriver.execute() must substitute the governed pipeline's `@N`/`$N` positional
placeholders before sending SQL to ClickHouse — confirmed live (federated_join via GraphQL,
perf-bench) that ClickHouse rejects `@N` outright as a syntax error when it reaches the wire
unsubstituted; the driver previously discarded `params` entirely on the assumption the SQL
"arrives fully formed," which only held for the single-source path it was first written
against. Mirrors tests/unit/test_executor_trino.py's TestExecuteTrinoParameterSubstitution
pattern for the equivalent Trino bug class.
"""

from __future__ import annotations

import asyncio

from provisa.executor.drivers.clickhouse import ClickHouseDriver


class _FakeResult:
    def __init__(self, rows, cols):
        self.result_rows = rows
        self.column_names = cols


class _FakeClient:
    def __init__(self, rows=None, cols=None):
        self._rows = rows or []
        self._cols = cols or []
        self.last_sql: str | None = None
        self.last_parameters = None

    def query(self, sql, parameters=None):
        self.last_sql = sql
        self.last_parameters = parameters
        return _FakeResult(self._rows, self._cols)


def _make_driver(rows=None, cols=None) -> tuple[ClickHouseDriver, _FakeClient]:
    driver = ClickHouseDriver()
    client = _FakeClient(rows=rows, cols=cols)
    driver._client = client
    return driver, client


class TestClickHouseDriverParameterSubstitution:
    def test_no_params_passed_as_is(self):
        driver, client = _make_driver(rows=[(42,)], cols=["n"])
        result = asyncio.run(driver.execute("SELECT 42 AS n"))
        assert result.rows == [(42,)]
        assert result.column_names == ["n"]
        assert client.last_sql == "SELECT 42 AS n"
        assert client.last_parameters is None

    def test_at_param_replaced_and_bound(self):
        driver, client = _make_driver(rows=[("x",)], cols=["v"])
        asyncio.run(driver.execute("SELECT @1 AS v", params=["x"]))
        assert client.last_sql is not None
        assert "@1" not in client.last_sql
        assert client.last_parameters == {"p1": "x"}

    def test_dollar_param_replaced_and_bound(self):
        driver, client = _make_driver(rows=[], cols=["v"])
        asyncio.run(driver.execute("SELECT $1 AS v", params=["x"]))
        assert client.last_sql is not None
        assert "$1" not in client.last_sql
        assert client.last_parameters == {"p1": "x"}

    def test_multiple_params_all_replaced(self):
        driver, client = _make_driver(rows=[], cols=["a", "b"])
        asyncio.run(driver.execute("SELECT @1, @2", params=["lo", "hi"]))
        assert client.last_sql is not None
        assert "@1" not in client.last_sql
        assert "@2" not in client.last_sql
        assert client.last_parameters == {"p1": "lo", "p2": "hi"}

    def test_replacement_order_no_prefix_collision(self):
        """@10 must not be corrupted by a naive forward-order @1 replacement first."""
        driver, client = _make_driver(rows=[], cols=["a"])
        params = list(range(10))  # 10 values -> placeholders @1..@10
        asyncio.run(driver.execute("SELECT @1, @10", params=params))
        assert client.last_sql is not None
        assert client.last_sql.count("%(p1)s") == 1
        assert "@10" not in client.last_sql
        assert client.last_parameters is not None
        assert client.last_parameters["p1"] == 0
        assert client.last_parameters["p10"] == 9

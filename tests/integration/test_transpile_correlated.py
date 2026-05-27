# Copyright (c) 2026 Kenneth Stott
# Canary: d3e4f5a6-b7c8-9012-def0-123456789abc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: rewrite_correlated_subqueries_for_trino → live Trino execution.

Requires Trino on localhost:8080 with the tpch catalog.
Tests skip automatically when Trino is unreachable.
"""

from __future__ import annotations

import os
import socket

import pytest
import trino

from provisa.transpiler.transpile import rewrite_correlated_subqueries_for_trino, transpile

pytestmark = [pytest.mark.integration]

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


def _trino_reachable() -> bool:
    try:
        with socket.create_connection((TRINO_HOST, TRINO_PORT), timeout=3):
            return True
    except OSError:
        return False


def _tpch_catalog_exists() -> bool:
    try:
        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="test")
        cur = conn.cursor()
        cur.execute("SHOW SCHEMAS FROM tpch")
        cur.fetchone()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def trino_cur():
    if not _trino_reachable():
        pytest.skip(f"Trino not reachable at {TRINO_HOST}:{TRINO_PORT}")
    if not _tpch_catalog_exists():
        pytest.skip("Trino 'tpch' catalog not available")
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="test",
        catalog="tpch",
        schema="tiny",
    )
    yield conn.cursor()
    conn.close()


def _run(cur, pg_sql: str) -> list:
    """Rewrite, transpile to Trino dialect, then execute."""
    rewritten = rewrite_correlated_subqueries_for_trino(pg_sql)
    trino_sql = transpile(rewritten, "trino")
    cur.execute(trino_sql)
    return cur.fetchall()


class TestCorrelatedSubqueriesOnTrino:
    """Execute rewritten SQL on a live Trino instance using tpch.tiny tables."""

    def test_scalar_correlated_executes(self, trino_cur):
        # tpch.tiny.orders has custkey; tpch.tiny.customer has custkey, name
        sql = (
            "SELECT o.orderkey,"
            " (SELECT c.name FROM tpch.tiny.customer c WHERE c.custkey = o.custkey) AS cust_name"
            " FROM tpch.tiny.orders o"
            " LIMIT 5"
        )
        rows = _run(trino_cur, sql)
        assert len(rows) > 0
        # cust_name must be non-null (every order has a customer in tpch)
        assert all(row[1] is not None for row in rows)

    def test_scalar_correlated_with_local_filter_executes(self, trino_cur):
        sql = (
            "SELECT o.orderkey,"
            " (SELECT c.name FROM tpch.tiny.customer c"
            "  WHERE c.custkey = o.custkey AND c.acctbal > 0) AS cust_name"
            " FROM tpch.tiny.orders o"
            " LIMIT 5"
        )
        rows = _run(trino_cur, sql)
        assert isinstance(rows, list)

    def test_two_correlated_subqueries_execute(self, trino_cur):
        sql = (
            "SELECT o.orderkey,"
            " (SELECT c.name FROM tpch.tiny.customer c WHERE c.custkey = o.custkey) AS cust,"
            " (SELECT c.mktsegment FROM tpch.tiny.customer c WHERE c.custkey = o.custkey) AS seg"
            " FROM tpch.tiny.orders o"
            " LIMIT 5"
        )
        rows = _run(trino_cur, sql)
        assert len(rows) > 0
        assert all(len(row) == 3 for row in rows)

    def test_aggregate_correlated_executes(self, trino_cur):
        # Count lineitems per order via correlated subquery
        sql = (
            "SELECT o.orderkey,"
            " (SELECT COUNT(l.linenumber) FROM tpch.tiny.lineitem l WHERE l.orderkey = o.orderkey) AS line_count"
            " FROM tpch.tiny.orders o"
            " LIMIT 5"
        )
        rows = _run(trino_cur, sql)
        assert len(rows) > 0
        assert all(isinstance(row[1], (int, type(None))) for row in rows)

    def test_sampling_wrapper_executes(self, trino_cur):
        inner = (
            "SELECT o.orderkey,"
            " (SELECT c.name FROM tpch.tiny.customer c WHERE c.custkey = o.custkey) AS cust_name"
            " FROM tpch.tiny.orders o"
        )
        sql = f"SELECT * FROM ({inner}) AS _sample LIMIT 10"
        rows = _run(trino_cur, sql)
        assert len(rows) > 0

    def test_no_correlated_subquery_executes(self, trino_cur):
        sql = "SELECT orderkey, totalprice FROM tpch.tiny.orders LIMIT 3"
        rows = _run(trino_cur, sql)
        assert len(rows) == 3

    def test_hot_ctes_preserved_before_rel_ctes(self, trino_cur):
        # Simulate a _hot_ VALUES CTE followed by a correlated subquery
        sql = (
            "WITH _hot_t AS (SELECT CAST(1 AS BIGINT) AS custkey, 'hot_name' AS name)"
            " SELECT o.orderkey,"
            " (SELECT c.name FROM tpch.tiny.customer c WHERE c.custkey = o.custkey) AS cust"
            " FROM tpch.tiny.orders o"
            " LIMIT 3"
        )
        rows = _run(trino_cur, sql)
        assert isinstance(rows, list)

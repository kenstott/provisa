# Copyright (c) 2026 Kenneth Stott
# Canary: 75e4568c-b50f-451b-b262-3024e5a0d397
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for provisa.compiler.schema_service."""

import time
from unittest.mock import MagicMock

import pytest

from provisa.compiler import schema_service


@pytest.fixture(autouse=True)
def _clear_cache():
    schema_service._cache.clear()
    schema_service._conn = None
    yield
    schema_service._cache.clear()
    schema_service._conn = None


def _make_conn(rows: list[tuple]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_returns_varchar_with_no_conn():
    result = schema_service.get_column_type("cat", "sch", "tbl", "col")
    assert result == "varchar"


def test_fetches_and_caches():
    conn = _make_conn([("id", "INTEGER"), ("name", "VARCHAR")])
    schema_service.init(conn)

    assert schema_service.get_column_type("cat", "sch", "tbl", "id") == "integer"
    assert schema_service.get_column_type("cat", "sch", "tbl", "name") == "varchar"
    # Second call must not re-query Trino
    conn.cursor.return_value.execute.assert_called_once()


def test_cache_miss_returns_varchar():
    conn = _make_conn([("id", "INTEGER")])
    schema_service.init(conn)
    assert schema_service.get_column_type("cat", "sch", "tbl", "missing") == "varchar"


def test_expired_entry_refetches():
    conn = _make_conn([("id", "INTEGER")])
    schema_service.init(conn)
    schema_service.get_column_type("cat", "sch", "tbl", "id")

    # Expire the entry
    key = ("cat", "sch", "tbl")
    schema_service._cache[key].expiry = time.monotonic() - 1

    # Should re-fetch
    conn2 = _make_conn([("id", "BIGINT")])
    schema_service._conn = conn2
    result = schema_service.get_column_type("cat", "sch", "tbl", "id")
    assert result == "bigint"
    conn2.cursor.return_value.execute.assert_called_once()


def test_invalidate_clears_entry():
    conn = _make_conn([("id", "INTEGER")])
    schema_service.init(conn)
    schema_service.get_column_type("cat", "sch", "tbl", "id")
    schema_service.invalidate("cat", "sch", "tbl")
    assert ("cat", "sch", "tbl") not in schema_service._cache


def test_preload_populates_cache():
    conn = _make_conn([("x", "DOUBLE")])
    schema_service.init(conn)
    schema_service.preload("cat", "sch", "tbl")
    assert ("cat", "sch", "tbl") in schema_service._cache
    assert schema_service._cache[("cat", "sch", "tbl")].columns["x"] == "double"

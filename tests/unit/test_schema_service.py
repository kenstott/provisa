# Copyright (c) 2026 Kenneth Stott
# Canary: 75e4568c-b50f-451b-b262-3024e5a0d397
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests for provisa.compiler.schema_service."""

import time

import pytest

from provisa.compiler import schema_service


@pytest.fixture(autouse=True)
def _clear_cache():
    schema_service._cache.clear()
    schema_service._engine = None
    yield
    schema_service._cache.clear()
    schema_service._engine = None


class _FakeEngine:
    """Introspection seam stand-in: introspect_by_catalog returns configured column types."""

    def __init__(self, cols: dict[str, str]):
        self._cols = cols
        self.calls = 0

    def introspect_by_catalog(self, catalog, schema, table):
        self.calls += 1
        return dict(self._cols)


def test_returns_varchar_with_no_conn():
    result = schema_service.get_column_type("cat", "sch", "tbl", "col")
    assert result == "varchar"


def test_fetches_and_caches():
    engine = _FakeEngine({"id": "INTEGER", "name": "VARCHAR"})
    schema_service.init(engine)

    assert schema_service.get_column_type("cat", "sch", "tbl", "id") == "integer"
    assert schema_service.get_column_type("cat", "sch", "tbl", "name") == "varchar"
    # Second call must not re-introspect
    assert engine.calls == 1


def test_cache_miss_returns_varchar():
    schema_service.init(_FakeEngine({"id": "INTEGER"}))
    assert schema_service.get_column_type("cat", "sch", "tbl", "missing") == "varchar"


def test_expired_entry_refetches():
    schema_service.init(_FakeEngine({"id": "INTEGER"}))
    schema_service.get_column_type("cat", "sch", "tbl", "id")

    # Expire the entry
    key = ("cat", "sch", "tbl")
    schema_service._cache[key].expiry = time.monotonic() - 1

    # Should re-fetch
    engine2 = _FakeEngine({"id": "BIGINT"})
    schema_service._engine = engine2
    result = schema_service.get_column_type("cat", "sch", "tbl", "id")
    assert result == "bigint"
    assert engine2.calls == 1


def test_invalidate_clears_entry():
    schema_service.init(_FakeEngine({"id": "INTEGER"}))
    schema_service.get_column_type("cat", "sch", "tbl", "id")
    schema_service.invalidate("cat", "sch", "tbl")
    assert ("cat", "sch", "tbl") not in schema_service._cache


def test_preload_populates_cache():
    schema_service.init(_FakeEngine({"x": "DOUBLE"}))
    schema_service.preload("cat", "sch", "tbl")
    assert ("cat", "sch", "tbl") in schema_service._cache
    assert schema_service._cache[("cat", "sch", "tbl")].columns["x"] == "double"

# Copyright (c) 2026 Kenneth Stott
"""REQ-1093: declared UNIQUE constraint introspection (PG/MySQL/SQLite)."""

from __future__ import annotations

import pytest

from provisa.discovery.fk_introspect import (
    _pg_uniques,
    _sqlite_uniques,
    introspect_unique_constraints,
)


class _Result:
    def __init__(self, rows):
        self.rows = rows


class _FakeDriver:
    """Returns canned rows per SQL substring; records calls."""

    def __init__(self, by_sql):
        self._by_sql = by_sql
        self.calls: list[str] = []

    async def execute(self, sql, params=None):
        self.calls.append(sql)
        for needle, rows in self._by_sql.items():
            if needle in sql:
                return _Result(rows)
        return _Result([])


class _Pools:
    def __init__(self, driver):
        self._driver = driver

    def has(self, _source_id):
        return self._driver is not None

    def get(self, _source_id):
        return self._driver


@pytest.mark.asyncio
async def test_pg_uniques_groups_composite_in_order():
    driver = _FakeDriver(
        {
            "information_schema.table_constraints": [
                ("users_tenant_email_key", "tenant_id", 1),
                ("users_tenant_email_key", "email", 2),
                ("users_sku_key", "sku", 1),
            ]
        }
    )
    result = await _pg_uniques(driver, "public", "users")
    assert {"name": "users_tenant_email_key", "columns": ["tenant_id", "email"]} in result
    assert {"name": "users_sku_key", "columns": ["sku"]} in result
    assert len(result) == 2


@pytest.mark.asyncio
async def test_sqlite_uniques_only_origin_u():
    driver = _FakeDriver(
        {
            "index_list": [
                (0, "sqlite_autoindex_users_1", 1, "pk", 0),  # PK — excluded
                (1, "uq_email", 1, "u", 0),  # declared UNIQUE — included
                (2, "ix_created", 0, "c", 0),  # non-unique index — excluded
            ],
            'index_info("uq_email")': [(0, 3, "email")],
        }
    )
    result = await _sqlite_uniques(driver, "main", "users")
    assert result == [{"name": "uq_email", "columns": ["email"]}]


@pytest.mark.asyncio
async def test_introspect_dispatch_and_missing_source():
    driver = _FakeDriver({"information_schema.table_constraints": [("uq", "id", 1)]})
    got = await introspect_unique_constraints(_Pools(driver), "postgresql", "s", "public", "t")
    assert got == [{"name": "uq", "columns": ["id"]}]

    # source not in pools → empty, no crash
    assert await introspect_unique_constraints(_Pools(None), "postgresql", "s", "public", "t") == []

    # unsupported source type → empty
    assert await introspect_unique_constraints(_Pools(driver), "mongodb", "s", "public", "t") == []

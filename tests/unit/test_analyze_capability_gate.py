# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""ANALYZE is issued only where the connector collects statistics (REQ-636).

Every SQLite/JDBC/adapter-backed catalog answered ANALYZE with NOT_SUPPORTED, and a catalog
whose tables are not yet landed answered TABLE_NOT_FOUND — one failed query per registered
table, on every config load.
"""

import types

from provisa.core.catalog import analyze_source_tables


class _Cursor:
    def __init__(self, capable: list[str], executed: list[str]) -> None:
        self._capable = capable
        self._executed = executed
        self._rows: list[tuple] = []

    def execute(self, sql: str) -> None:
        self._executed.append(sql)
        if "analyze_properties" in sql:
            self._rows = [(name,) for name in self._capable]
        else:
            self._rows = []

    def fetchall(self) -> list[tuple]:
        return self._rows


class _Conn:
    def __init__(self, capable: list[str]) -> None:
        self.capable = capable
        self.executed: list[str] = []

    def cursor(self) -> _Cursor:
        return _Cursor(self.capable, self.executed)


def _source():
    return types.SimpleNamespace(id="pet-store-pg")


def _table():
    return types.SimpleNamespace(source_id="pet-store-pg", schema_name="public", table_name="pets")


def test_analyze_runs_where_the_connector_collects_statistics():
    conn = _Conn(["org_ks__lake"])

    analyze_source_tables(conn, _source(), [_table()], catalog_name="org_ks__lake")

    assert "ANALYZE org_ks__lake.public.pets" in conn.executed


def test_analyze_skipped_where_the_connector_collects_none():
    conn = _Conn(["otel", "results"])

    analyze_source_tables(conn, _source(), [_table()], catalog_name="org_ks__pet_store_sqlite")

    assert [sql for sql in conn.executed if sql.startswith("ANALYZE")] == []

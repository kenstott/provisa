# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-987: the Databricks materialization write face. Driver-free — a fake cursor records the SQL so
the DDL/type mapping, catalog+schema creation, replace/append shapes, and bulk (non-per-row) insert
are pinned without a live warehouse. The live land→query round-trip is exercised in integration."""

from __future__ import annotations

import json

import pytest

from provisa.federation.databricks_store import (
    _ddl_type,
    land_databricks_native,
    reconcile_databricks_native,
)

COLS = [("id", "bigint"), ("s", "text"), ("amt", "numeric"), ("j", "json")]


class _FakeCursor:
    """Records executed SQL (+ params); answers information_schema column probes from a settable set."""

    def __init__(self):
        self.sql: list[tuple[str, list | None]] = []
        self._existing: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append((sql, params))

    def fetchall(self):
        # only the _existing_columns probe calls fetchall
        return [(c,) for c in self._existing]

    def joined(self) -> str:
        return " | ".join(s for s, _ in self.sql)


def test_ddl_type_maps_ir_and_raises_on_unknown():
    assert _ddl_type("bigint") == "BIGINT"
    assert _ddl_type("text") == "STRING"
    assert _ddl_type("json") == "STRING"
    assert _ddl_type("numeric") == "DECIMAL(38,9)"
    assert _ddl_type("timestamptz") == "TIMESTAMP"  # native spelling normalizes through to_ir
    # A type outside the IR vocabulary raises (via to_ir) rather than silently widening.
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        _ddl_type("geography")


def test_reconcile_creates_catalog_schema_and_table():
    cur = _FakeCursor()
    assert (
        reconcile_databricks_native(
            cur, catalog="sales_db", schema="public", table="orders", columns=COLS
        )
        == "created"
    )
    j = cur.joined()
    assert "CREATE CATALOG IF NOT EXISTS `sales_db`" in j
    assert "CREATE SCHEMA IF NOT EXISTS `sales_db`.`public`" in j
    assert "CREATE TABLE IF NOT EXISTS `sales_db`.`public`.`orders`" in j
    assert "USING DELTA" in j


def test_reconcile_keeps_matching_and_recreates_on_drift():
    cur = _FakeCursor()
    cur._existing = ["id", "s", "amt", "j"]
    assert (
        reconcile_databricks_native(cur, catalog="c", schema="s", table="t", columns=COLS) == "kept"
    )
    cur2 = _FakeCursor()
    cur2._existing = ["id", "s"]  # drift
    assert (
        reconcile_databricks_native(cur2, catalog="c", schema="s", table="t", columns=COLS)
        == "recreated"
    )
    assert "DROP TABLE IF EXISTS `c`.`s`.`t`" in cur2.joined()


def test_land_replace_truncates_then_bulk_inserts():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[
            {"id": 1, "s": "a", "amt": 10, "j": json.dumps({"k": 1})},
            {"id": 2, "s": "b", "amt": 20, "j": None},
        ],
    )
    j = cur.joined()
    assert "TRUNCATE TABLE `c`.`s`.`t`" in j  # replace = full refresh
    inserts = [(s, p) for s, p in cur.sql if s.startswith("INSERT INTO")]
    assert len(inserts) == 1  # ONE bulk multi-row INSERT, never a per-row loop (REQ-987)
    sql, params = inserts[0]
    assert params is not None
    assert sql.count("(?, ?, ?, ?)") == 2  # two value tuples in one statement
    assert len(params) == 8  # 2 rows × 4 columns, flattened


def test_land_append_does_not_truncate():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[{"id": 3, "s": "c", "amt": 30, "j": None}],
        change_signal="poll",
        watermark_column="id",
    )
    assert "TRUNCATE" not in cur.joined()  # append amends, never truncates


def test_land_coerces_dict_json_to_text():
    cur = _FakeCursor()
    land_databricks_native(
        cur,
        catalog="c",
        schema="s",
        table="t",
        columns=COLS,
        rows=[{"id": 1, "s": "a", "amt": 1, "j": {"k": 2}}],  # dict, not str
    )
    _, params = [(s, p) for s, p in cur.sql if s.startswith("INSERT INTO")][0]
    assert params is not None
    assert params[3] == '{"k": 2}'  # dict re-serialized to JSON text for the STRING column


def test_land_no_rows_creates_but_no_insert():
    cur = _FakeCursor()
    land_databricks_native(cur, catalog="c", schema="s", table="t", columns=COLS, rows=[])
    assert not any(s.startswith("INSERT INTO") for s, _ in cur.sql)
    assert "CREATE TABLE IF NOT EXISTS" in cur.joined()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])

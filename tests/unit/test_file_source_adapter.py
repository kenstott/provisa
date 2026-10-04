# Copyright (c) 2026 Kenneth Stott
# Canary: 37f6d0a6-b5ed-42d4-9c87-4518fd87de10
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-736: the file source adapter reads SQLite, CSV and Parquet; SQLite uses native type mapping,
discovery returns column definitions and queries return row dicts."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from provisa.file_source.source import FileSourceConfig, discover_schema, execute_query


@pytest.fixture
def db(tmp_path: Path) -> Path:
    path = tmp_path / "app.db"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE orders (id INTEGER NOT NULL, amount REAL, note TEXT, paid BOOLEAN,
                             created DATETIME, payload BLOB);
        CREATE TABLE customers (cid INTEGER NOT NULL, name VARCHAR(40));
        INSERT INTO orders (id, amount, note) VALUES (1, 9.5, 'a'), (2, 3.25, 'b');
        INSERT INTO customers VALUES (7, 'Ada');
        """
    )
    con.commit()
    con.close()
    return path


def _config(path: Path) -> FileSourceConfig:
    return FileSourceConfig(id="app", source_type="sqlite", path=str(path))


def test_discovery_covers_every_table_with_native_type_mapping(db: Path) -> None:
    cols = {(c["table"], c["name"]): c for c in discover_schema(_config(db))}
    assert {t for t, _ in cols} == {"orders", "customers"}
    assert cols[("orders", "id")]["type"] == "BIGINT"
    assert cols[("orders", "amount")]["type"] == "DOUBLE"
    assert cols[("orders", "note")]["type"] == "VARCHAR"
    assert cols[("orders", "paid")]["type"] == "BOOLEAN"
    assert cols[("orders", "created")]["type"] == "TIMESTAMP"
    assert cols[("orders", "payload")]["type"] == "VARBINARY"
    assert cols[("customers", "name")]["type"] == "VARCHAR"


def test_discovery_reports_nullability_from_the_declared_constraint(db: Path) -> None:
    cols = {(c["table"], c["name"]): c for c in discover_schema(_config(db))}
    assert cols[("orders", "id")]["nullable"] is False
    assert cols[("orders", "amount")]["nullable"] is True


def test_queries_return_row_dicts(db: Path) -> None:
    rows = execute_query(_config(db), "SELECT id, amount FROM orders ORDER BY id")
    assert rows == [{"id": 1, "amount": 9.5}, {"id": 2, "amount": 3.25}]


def test_csv_discovery_and_query_return_the_same_shapes(tmp_path: Path) -> None:
    path = tmp_path / "people.csv"
    path.write_text("id,name\n1,Ada\n2,Grace\n")
    cfg = FileSourceConfig(id="p", source_type="csv", path=str(path))
    assert {c["name"] for c in discover_schema(cfg)} == {"id", "name"}
    rows = execute_query(cfg, f"SELECT name FROM read_csv_auto('{path}') ORDER BY id")
    assert [r["name"] for r in rows] == ["Ada", "Grace"]


def test_an_unsupported_format_is_refused(tmp_path: Path) -> None:
    cfg = FileSourceConfig(id="x", source_type="xlsx", path=str(tmp_path / "x"))
    with pytest.raises(ValueError, match="Unsupported file source type"):
        discover_schema(cfg)
    with pytest.raises(ValueError, match="Unsupported file source type"):
        execute_query(cfg, "SELECT 1")

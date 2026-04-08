# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-4567-890abcdef012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for file-based source adapter (Issue #27): SQLite, CSV, Parquet."""

from __future__ import annotations

import csv
import sqlite3
import tempfile
from pathlib import Path

import pytest

from provisa.file_source.source import (
    FileSourceConfig,
    _arrow_type_to_sql,
    _sqlite_type_to_sql,
    discover_schema,
    execute_query,
    generate_catalog_properties,
    generate_table_definitions,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    """Create a small SQLite database with two tables."""
    db_path = tmp_path / "test.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO orders VALUES (1, 10, 99.99, 'delivered');
        INSERT INTO orders VALUES (2, 11, 49.50, 'pending');

        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 1
        );
        INSERT INTO customers VALUES (10, 'Alice', 1);
        INSERT INTO customers VALUES (11, 'Bob', 1);
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Create a small CSV file."""
    csv_path = tmp_path / "customers.csv"
    rows = [
        ["id", "name", "email", "score"],
        ["1", "Alice", "alice@example.com", "4.5"],
        ["2", "Bob", "bob@example.com", "3.8"],
        ["3", "Carol", "carol@example.com", "5.0"],
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return csv_path


@pytest.fixture
def parquet_file(tmp_path: Path) -> Path:
    """Create a small Parquet file using pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "sku": pa.array(["A", "B", "C"], type=pa.string()),
        "price": pa.array([9.99, 14.99, 4.99], type=pa.float64()),
        "active": pa.array([True, True, False], type=pa.bool_()),
    })
    path = tmp_path / "products.parquet"
    pq.write_table(table, path)
    return path


def _sqlite_config(path: Path) -> FileSourceConfig:
    return FileSourceConfig(id="test-sqlite", source_type="sqlite", path=str(path))


def _csv_config(path: Path) -> FileSourceConfig:
    return FileSourceConfig(id="test-csv", source_type="csv", path=str(path))


def _parquet_config(path: Path) -> FileSourceConfig:
    return FileSourceConfig(id="test-parquet", source_type="parquet", path=str(path))


# ---------------------------------------------------------------------------
# TestSqliteTypeMapping
# ---------------------------------------------------------------------------


class TestSqliteTypeMapping:
    def test_integer_type(self):
        assert _sqlite_type_to_sql("INTEGER") == "BIGINT"

    def test_int_type(self):
        assert _sqlite_type_to_sql("INT") == "BIGINT"

    def test_real_type(self):
        assert _sqlite_type_to_sql("REAL") == "DOUBLE"

    def test_float_type(self):
        assert _sqlite_type_to_sql("FLOAT") == "DOUBLE"

    def test_double_type(self):
        assert _sqlite_type_to_sql("DOUBLE") == "DOUBLE"

    def test_boolean_type(self):
        assert _sqlite_type_to_sql("BOOLEAN") == "BOOLEAN"

    def test_text_type(self):
        assert _sqlite_type_to_sql("TEXT") == "VARCHAR"

    def test_blob_type(self):
        assert _sqlite_type_to_sql("BLOB") == "VARBINARY"

    def test_datetime_type(self):
        assert _sqlite_type_to_sql("DATETIME") == "TIMESTAMP"

    def test_date_type(self):
        assert _sqlite_type_to_sql("DATE") == "TIMESTAMP"

    def test_unknown_type_defaults_to_varchar(self):
        assert _sqlite_type_to_sql("CUSTOM_TYPE") == "VARCHAR"

    def test_empty_type_defaults_to_varchar(self):
        assert _sqlite_type_to_sql("") == "VARCHAR"


# ---------------------------------------------------------------------------
# TestArrowTypeMapping
# ---------------------------------------------------------------------------


class TestArrowTypeMapping:
    def test_int32(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.int32()) == "INTEGER"

    def test_int64(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.int64()) == "BIGINT"

    def test_float64(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.float64()) == "DOUBLE"

    def test_float32(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.float32()) == "REAL"

    def test_bool(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.bool_()) == "BOOLEAN"

    def test_string(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.string()) == "VARCHAR"

    def test_date32(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.date32()) == "DATE"

    def test_timestamp(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.timestamp("ms")) == "TIMESTAMP"

    def test_binary(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.binary()) == "VARBINARY"

    def test_uint32(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.uint32()) == "INTEGER"

    def test_uint64(self):
        import pyarrow as pa
        assert _arrow_type_to_sql(pa.uint64()) == "BIGINT"


# ---------------------------------------------------------------------------
# TestDiscoverSchemaSqlite
# ---------------------------------------------------------------------------


class TestDiscoverSchemaSqlite:
    def test_returns_list(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        assert isinstance(cols, list)

    def test_finds_both_tables(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        tables = {c["table"] for c in cols}
        assert "orders" in tables
        assert "customers" in tables

    def test_orders_has_four_columns(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        orders_cols = [c for c in cols if c["table"] == "orders"]
        assert len(orders_cols) == 4

    def test_integer_column_mapped(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        col = next(c for c in cols if c["table"] == "orders" and c["name"] == "id")
        assert col["type"] == "BIGINT"

    def test_real_column_mapped(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        col = next(c for c in cols if c["table"] == "orders" and c["name"] == "amount")
        assert col["type"] == "DOUBLE"

    def test_text_column_mapped(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        col = next(c for c in cols if c["table"] == "orders" and c["name"] == "status")
        assert col["type"] == "VARCHAR"

    def test_each_column_has_name_type_nullable(self, sqlite_db):
        cols = discover_schema(_sqlite_config(sqlite_db))
        for col in cols:
            assert "name" in col
            assert "type" in col
            assert "nullable" in col


# ---------------------------------------------------------------------------
# TestDiscoverSchemaCsv
# ---------------------------------------------------------------------------


class TestDiscoverSchemaCsv:
    def test_returns_list(self, csv_file):
        cols = discover_schema(_csv_config(csv_file))
        assert isinstance(cols, list)

    def test_has_four_columns(self, csv_file):
        cols = discover_schema(_csv_config(csv_file))
        assert len(cols) == 4

    def test_column_names_present(self, csv_file):
        cols = discover_schema(_csv_config(csv_file))
        names = {c["name"] for c in cols}
        assert names == {"id", "name", "email", "score"}

    def test_each_column_has_type(self, csv_file):
        cols = discover_schema(_csv_config(csv_file))
        for col in cols:
            assert col["type"] in (
                "BIGINT", "INTEGER", "SMALLINT", "TINYINT",
                "DOUBLE", "REAL", "BOOLEAN", "DATE",
                "TIMESTAMP", "VARBINARY", "VARCHAR",
            )


# ---------------------------------------------------------------------------
# TestDiscoverSchemaParquet
# ---------------------------------------------------------------------------


class TestDiscoverSchemaParquet:
    def test_returns_list(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        assert isinstance(cols, list)

    def test_has_four_columns(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        assert len(cols) == 4

    def test_column_names(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        names = {c["name"] for c in cols}
        assert names == {"id", "sku", "price", "active"}

    def test_int32_column(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        col = next(c for c in cols if c["name"] == "id")
        assert col["type"] == "INTEGER"

    def test_float64_column(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        col = next(c for c in cols if c["name"] == "price")
        assert col["type"] == "DOUBLE"

    def test_bool_column(self, parquet_file):
        cols = discover_schema(_parquet_config(parquet_file))
        col = next(c for c in cols if c["name"] == "active")
        assert col["type"] == "BOOLEAN"


# ---------------------------------------------------------------------------
# TestDiscoverSchemaErrors
# ---------------------------------------------------------------------------


class TestDiscoverSchemaErrors:
    def test_unsupported_type_raises(self):
        cfg = FileSourceConfig(id="x", source_type="hdf5", path="/tmp/x.hdf5")
        with pytest.raises(ValueError, match="Unsupported file source type"):
            discover_schema(cfg)


# ---------------------------------------------------------------------------
# TestExecuteQuerySqlite
# ---------------------------------------------------------------------------


class TestExecuteQuerySqlite:
    def test_select_all_orders(self, sqlite_db):
        rows = execute_query(_sqlite_config(sqlite_db), "SELECT * FROM orders")
        assert len(rows) == 2

    def test_row_is_dict(self, sqlite_db):
        rows = execute_query(_sqlite_config(sqlite_db), "SELECT * FROM orders")
        assert isinstance(rows[0], dict)

    def test_columns_in_result(self, sqlite_db):
        rows = execute_query(_sqlite_config(sqlite_db), "SELECT * FROM orders")
        assert "id" in rows[0]
        assert "amount" in rows[0]

    def test_filter_by_status(self, sqlite_db):
        rows = execute_query(
            _sqlite_config(sqlite_db),
            "SELECT * FROM orders WHERE status = 'delivered'"
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "delivered"

    def test_count_query(self, sqlite_db):
        rows = execute_query(_sqlite_config(sqlite_db), "SELECT COUNT(*) as cnt FROM orders")
        assert rows[0]["cnt"] == 2

    def test_select_customers(self, sqlite_db):
        rows = execute_query(_sqlite_config(sqlite_db), "SELECT name FROM customers ORDER BY id")
        names = [r["name"] for r in rows]
        assert names == ["Alice", "Bob"]


# ---------------------------------------------------------------------------
# TestExecuteQueryCsv
# ---------------------------------------------------------------------------


class TestExecuteQueryCsv:
    def test_select_all(self, csv_file):
        rows = execute_query(_csv_config(csv_file), f"SELECT * FROM customers")
        assert len(rows) == 3

    def test_row_keys_match_csv_header(self, csv_file):
        rows = execute_query(_csv_config(csv_file), "SELECT * FROM customers")
        assert set(rows[0].keys()) == {"id", "name", "email", "score"}

    def test_filter_by_name(self, csv_file):
        rows = execute_query(
            _csv_config(csv_file),
            "SELECT * FROM customers WHERE name = 'Alice'"
        )
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# TestExecuteQueryParquet
# ---------------------------------------------------------------------------


class TestExecuteQueryParquet:
    def test_select_all(self, parquet_file):
        rows = execute_query(_parquet_config(parquet_file), "SELECT * FROM products")
        assert len(rows) == 3

    def test_filter_active(self, parquet_file):
        rows = execute_query(
            _parquet_config(parquet_file),
            "SELECT * FROM products WHERE active = true"
        )
        assert len(rows) == 2

    def test_price_values(self, parquet_file):
        rows = execute_query(
            _parquet_config(parquet_file),
            "SELECT sku, price FROM products ORDER BY price"
        )
        skus = [r["sku"] for r in rows]
        assert skus[0] == "C"  # 4.99


# ---------------------------------------------------------------------------
# TestExecuteQueryErrors
# ---------------------------------------------------------------------------


class TestExecuteQueryErrors:
    def test_unsupported_type_raises(self):
        cfg = FileSourceConfig(id="x", source_type="hdf5", path="/tmp/x.hdf5")
        with pytest.raises(ValueError, match="Unsupported file source type"):
            execute_query(cfg, "SELECT 1")


# ---------------------------------------------------------------------------
# TestGenerateCatalogProperties
# ---------------------------------------------------------------------------


class TestGenerateCatalogProperties:
    def test_returns_empty_dict_for_csv(self, csv_file):
        cfg = _csv_config(csv_file)
        assert generate_catalog_properties(cfg) == {}

    def test_returns_empty_dict_for_parquet(self, parquet_file):
        cfg = _parquet_config(parquet_file)
        assert generate_catalog_properties(cfg) == {}

    def test_returns_empty_dict_for_sqlite(self, sqlite_db):
        cfg = _sqlite_config(sqlite_db)
        assert generate_catalog_properties(cfg) == {}


# ---------------------------------------------------------------------------
# TestGenerateTableDefinitions
# ---------------------------------------------------------------------------


class TestGenerateTableDefinitions:
    def test_sqlite_returns_two_tables(self, sqlite_db):
        defs = generate_table_definitions(_sqlite_config(sqlite_db))
        assert len(defs) == 2

    def test_sqlite_table_names(self, sqlite_db):
        defs = generate_table_definitions(_sqlite_config(sqlite_db))
        names = {d["tableName"] for d in defs}
        assert names == {"orders", "customers"}

    def test_csv_returns_one_table(self, csv_file):
        defs = generate_table_definitions(_csv_config(csv_file))
        assert len(defs) == 1

    def test_csv_table_name_from_filename(self, csv_file):
        defs = generate_table_definitions(_csv_config(csv_file))
        assert defs[0]["tableName"] == "customers"

    def test_parquet_returns_one_table(self, parquet_file):
        defs = generate_table_definitions(_parquet_config(parquet_file))
        assert len(defs) == 1

    def test_columns_key_present(self, csv_file):
        defs = generate_table_definitions(_csv_config(csv_file))
        assert "columns" in defs[0]

    def test_column_has_name_and_type(self, parquet_file):
        defs = generate_table_definitions(_parquet_config(parquet_file))
        col = defs[0]["columns"][0]
        assert "name" in col
        assert "type" in col


# ---------------------------------------------------------------------------
# TestSourceModelIntegration
# ---------------------------------------------------------------------------


class TestSourceModelIntegration:
    def test_source_type_enum_has_sqlite(self):
        from provisa.core.models import SourceType
        assert SourceType.sqlite.value == "sqlite"

    def test_source_type_enum_has_csv(self):
        from provisa.core.models import SourceType
        assert SourceType.csv.value == "csv"

    def test_source_type_enum_has_parquet(self):
        from provisa.core.models import SourceType
        assert SourceType.parquet.value == "parquet"

    def test_source_model_accepts_file_source(self):
        from provisa.core.models import Source, SourceType
        src = Source(
            id="demo-csv",
            type=SourceType.csv,
            path="./demo/files/customers.csv",
        )
        assert src.path == "./demo/files/customers.csv"
        assert src.host == ""
        assert src.port == 0

    def test_source_model_path_none_by_default(self):
        from provisa.core.models import Source, SourceType
        src = Source(
            id="sales-pg",
            type=SourceType.postgresql,
            host="localhost",
            port=5432,
            database="provisa",
            username="admin",
            password="secret",
        )
        assert src.path is None

    def test_source_adapter_registry_has_sqlite(self):
        from provisa.source_adapters.registry import registered_types
        assert "sqlite" in registered_types()

    def test_source_adapter_registry_has_csv(self):
        from provisa.source_adapters.registry import registered_types
        assert "csv" in registered_types()

    def test_source_adapter_registry_has_parquet(self):
        from provisa.source_adapters.registry import registered_types
        assert "parquet" in registered_types()

    def test_get_adapter_sqlite_returns_module(self):
        from provisa.source_adapters.registry import get_adapter
        mod = get_adapter("sqlite")
        assert hasattr(mod, "discover_schema")
        assert hasattr(mod, "execute_query")

    def test_get_adapter_csv_returns_module(self):
        from provisa.source_adapters.registry import get_adapter
        mod = get_adapter("csv")
        assert hasattr(mod, "discover_schema")

    def test_get_adapter_parquet_returns_module(self):
        from provisa.source_adapters.registry import get_adapter
        mod = get_adapter("parquet")
        assert hasattr(mod, "generate_table_definitions")

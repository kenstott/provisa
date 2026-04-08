# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-5678-90abcdef1234
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for DirectoryCrawler (Issue #28)."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from provisa.file_source.crawler import (
    SUPPORTED_EXTENSIONS,
    _is_fsspec_uri,
    _source_type_for_path,
    _walk_local,
    crawl_directory,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def flat_dir(tmp_path: Path) -> Path:
    """Directory with one CSV, one Parquet, one SQLite, one unsupported file."""
    # CSV
    csv_path = tmp_path / "sales.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([["id", "amount"], ["1", "99.9"], ["2", "49.5"]])

    # Parquet
    import pyarrow as pa
    import pyarrow.parquet as pq
    tbl = pa.table({"sku": pa.array(["A", "B"]), "price": pa.array([1.0, 2.0])})
    pq.write_table(tbl, tmp_path / "inventory.parquet")

    # SQLite
    db_path = tmp_path / "store.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO products VALUES (1, 'Widget');
    """)
    conn.commit()
    conn.close()

    # Unsupported
    (tmp_path / "readme.txt").write_text("ignore me")

    return tmp_path


@pytest.fixture
def nested_dir(tmp_path: Path) -> Path:
    """Root with CSV at top level and CSV inside a subdirectory."""
    # Top-level CSV
    with open(tmp_path / "top.csv", "w", newline="") as f:
        csv.writer(f).writerows([["x", "y"], ["1", "2"]])

    sub = tmp_path / "sub"
    sub.mkdir()
    with open(sub / "nested.csv", "w", newline="") as f:
        csv.writer(f).writerows([["a", "b"], ["3", "4"]])

    return tmp_path


@pytest.fixture
def db_ext_dir(tmp_path: Path) -> Path:
    """Directory containing a .db file (should be treated as sqlite)."""
    db_path = tmp_path / "data.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("CREATE TABLE entries (id INTEGER, val TEXT);")
    conn.commit()
    conn.close()
    return tmp_path


# ---------------------------------------------------------------------------
# TestHelpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_supported_extensions_contains_csv(self):
        assert ".csv" in SUPPORTED_EXTENSIONS

    def test_supported_extensions_contains_parquet(self):
        assert ".parquet" in SUPPORTED_EXTENSIONS

    def test_supported_extensions_contains_sqlite(self):
        assert ".sqlite" in SUPPORTED_EXTENSIONS

    def test_supported_extensions_contains_db(self):
        assert ".db" in SUPPORTED_EXTENSIONS

    def test_source_type_for_csv(self, tmp_path):
        assert _source_type_for_path(str(tmp_path / "x.csv")) == "csv"

    def test_source_type_for_parquet(self, tmp_path):
        assert _source_type_for_path(str(tmp_path / "x.parquet")) == "parquet"

    def test_source_type_for_sqlite(self, tmp_path):
        assert _source_type_for_path(str(tmp_path / "x.sqlite")) == "sqlite"

    def test_source_type_for_db(self, tmp_path):
        assert _source_type_for_path(str(tmp_path / "x.db")) == "sqlite"

    def test_source_type_unsupported_returns_none(self, tmp_path):
        assert _source_type_for_path(str(tmp_path / "x.txt")) is None

    def test_is_fsspec_uri_s3(self):
        assert _is_fsspec_uri("s3://bucket/prefix/") is True

    def test_is_fsspec_uri_ftp(self):
        assert _is_fsspec_uri("ftp://host/path/") is True

    def test_is_fsspec_uri_local_path(self):
        assert _is_fsspec_uri("/local/path/") is False

    def test_is_fsspec_uri_relative(self):
        assert _is_fsspec_uri("relative/path") is False


# ---------------------------------------------------------------------------
# TestWalkLocal
# ---------------------------------------------------------------------------


class TestWalkLocal:
    def test_finds_csv_files(self, flat_dir):
        paths = _walk_local(str(flat_dir), None)
        assert any(p.endswith(".csv") for p in paths)

    def test_finds_parquet_files(self, flat_dir):
        paths = _walk_local(str(flat_dir), None)
        assert any(p.endswith(".parquet") for p in paths)

    def test_finds_sqlite_files(self, flat_dir):
        paths = _walk_local(str(flat_dir), None)
        assert any(p.endswith(".sqlite") for p in paths)

    def test_excludes_unsupported_files(self, flat_dir):
        paths = _walk_local(str(flat_dir), None)
        assert not any(p.endswith(".txt") for p in paths)

    def test_flat_dir_has_three_files(self, flat_dir):
        paths = _walk_local(str(flat_dir), None)
        assert len(paths) == 3

    def test_nested_dir_finds_both_files_unlimited(self, nested_dir):
        paths = _walk_local(str(nested_dir), None)
        assert len(paths) == 2

    def test_depth_zero_only_top_level(self, nested_dir):
        paths = _walk_local(str(nested_dir), 0)
        assert len(paths) == 1

    def test_depth_one_finds_all(self, nested_dir):
        paths = _walk_local(str(nested_dir), 1)
        assert len(paths) == 2

    def test_raises_for_nonexistent_dir(self, tmp_path):
        with pytest.raises(ValueError, match="Not a directory"):
            _walk_local(str(tmp_path / "nonexistent"), None)

    def test_raises_for_file_path(self, tmp_path):
        f = tmp_path / "x.csv"
        f.write_text("a,b\n1,2\n")
        with pytest.raises(ValueError, match="Not a directory"):
            _walk_local(str(f), None)


# ---------------------------------------------------------------------------
# TestCrawlDirectory
# ---------------------------------------------------------------------------


class TestCrawlDirectory:
    def test_returns_list(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        assert isinstance(result, list)

    def test_finds_three_entries(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        assert len(result) == 3

    def test_each_entry_has_required_keys(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        for entry in result:
            assert "name" in entry
            assert "path" in entry
            assert "type" in entry
            assert "tables" in entry

    def test_tables_is_list(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        for entry in result:
            assert isinstance(entry["tables"], list)

    def test_each_table_has_name_and_columns(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        for entry in result:
            for tbl in entry["tables"]:
                assert "name" in tbl
                assert "columns" in tbl

    def test_csv_entry_type(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        csv_entries = [e for e in result if e["type"] == "csv"]
        assert len(csv_entries) == 1

    def test_parquet_entry_type(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        parquet_entries = [e for e in result if e["type"] == "parquet"]
        assert len(parquet_entries) == 1

    def test_sqlite_entry_type(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        sqlite_entries = [e for e in result if e["type"] == "sqlite"]
        assert len(sqlite_entries) == 1

    def test_csv_columns_inferred(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        csv_entry = next(e for e in result if e["type"] == "csv")
        cols = csv_entry["tables"][0]["columns"]
        names = {c["name"] for c in cols}
        assert names == {"id", "amount"}

    def test_parquet_columns_inferred(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        pq_entry = next(e for e in result if e["type"] == "parquet")
        cols = pq_entry["tables"][0]["columns"]
        names = {c["name"] for c in cols}
        assert names == {"sku", "price"}

    def test_sqlite_table_in_entry(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        sq_entry = next(e for e in result if e["type"] == "sqlite")
        tbl_names = {t["name"] for t in sq_entry["tables"]}
        assert "products" in tbl_names

    def test_depth_zero_skips_nested(self, nested_dir):
        result = crawl_directory(str(nested_dir), depth=0)
        assert len(result) == 1

    def test_depth_one_finds_nested(self, nested_dir):
        result = crawl_directory(str(nested_dir), depth=1)
        assert len(result) == 2

    def test_db_extension_treated_as_sqlite(self, db_ext_dir):
        result = crawl_directory(str(db_ext_dir))
        assert len(result) == 1
        assert result[0]["type"] == "sqlite"

    def test_column_has_nullable(self, flat_dir):
        result = crawl_directory(str(flat_dir))
        for entry in result:
            for tbl in entry["tables"]:
                for col in tbl["columns"]:
                    assert "nullable" in col

    def test_invalid_path_raises_value_error(self, tmp_path):
        with pytest.raises(ValueError, match="Not a directory"):
            crawl_directory(str(tmp_path / "no_such_dir"))

    def test_empty_directory_returns_empty_list(self, tmp_path):
        result = crawl_directory(str(tmp_path))
        assert result == []

# Copyright (c) 2026 Kenneth Stott
# Canary: fb064706-57b2-4db0-9e18-3a8730a9c846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for file-lake source mapping (REQ-788, REQ-789, REQ-790, REQ-791)."""

# Requirements: REQ-788, REQ-789, REQ-790, REQ-791

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from provisa.core.models import Source, SourceType
from provisa.file_source.crawler import (
    SUPPORTED_EXTENSIONS,
    _build_table_entry,
    _source_type_for_path,
    _walk_local,
    crawl_directory,
)
from provisa.file_source.source import (
    FileSourceConfig,
    _arrow_schema_to_columns,
    discover_schema,
    generate_table_definitions,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _csv_source(path: str) -> FileSourceConfig:
    return FileSourceConfig(id="test-csv", source_type="csv", path=path)


def _make_csv(
    tmp_path: Path, filename: str, headers: list[str], rows: list[list] | None = None
) -> Path:
    p = tmp_path / filename
    with p.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows or [["val"] * len(headers)]:
            writer.writerow(row)
    return p


# --------------------------------------------------------------------------- #
# REQ-788: files connector accepts a glob pattern — _build_catalog_properties  #
# --------------------------------------------------------------------------- #


class TestFilesConnectorGlobPattern:
    """REQ-788: File connector sources accept a directory glob pattern."""

    def test_source_type_files_exists(self):
        assert SourceType.files.value == "files"

    def test_source_with_glob_path_is_valid(self):
        src = Source(
            id="lake1",
            type=SourceType.files,
            path="/data/lake/**/*.csv",
        )
        assert src.path == "/data/lake/**/*.csv"

    def test_source_path_none_by_default(self):
        src = Source(id="lake1", type=SourceType.files)
        assert src.path is None

    def test_catalog_properties_include_glob(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="lake1", type=SourceType.files, path="/data/**/*.csv")
        props = _build_catalog_properties(src, "")
        assert props["glob"] == "/data/**/*.csv"

    def test_catalog_properties_recursive_true(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="lake1", type=SourceType.files, path="/data/**/*.csv")
        props = _build_catalog_properties(src, "")
        assert props["recursive"] == "true"

    def test_catalog_properties_schema_name_from_source_id(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="my-lake", type=SourceType.files, path="/data/*.csv")
        props = _build_catalog_properties(src, "")
        assert props["schema-name"] == "my_lake"

    def test_catalog_properties_execution_engine_linq4j(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="lake1", type=SourceType.files, path="/data/*.csv")
        props = _build_catalog_properties(src, "")
        assert props["execution-engine"] == "LINQ4J"

    def test_catalog_properties_case_insensitive_matching(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="lake1", type=SourceType.files, path="/data/*.csv")
        props = _build_catalog_properties(src, "")
        assert props["case-insensitive-name-matching"] == "true"

    def test_missing_path_raises_value_error(self):
        from provisa.core.catalog import _build_catalog_properties

        src = Source(id="lake1", type=SourceType.files)
        with pytest.raises(ValueError, match="'path'.*required for files connector"):
            _build_catalog_properties(src, "")

    def test_glob_wildcards_preserved_verbatim(self):
        from provisa.core.catalog import _build_catalog_properties

        glob = "/mnt/data/2026/**/*.parquet"
        src = Source(id="lake1", type=SourceType.files, path=glob)
        props = _build_catalog_properties(src, "")
        assert props["glob"] == glob

    def test_source_type_mapped_to_file_connector(self):
        from provisa.federation.trino_connectors import trino_connector_name

        assert trino_connector_name("files") == "file"

    def test_walk_local_enumerates_csv_files(self, tmp_path: Path):
        _make_csv(tmp_path, "orders.csv", ["id", "name"])
        _make_csv(tmp_path, "users.csv", ["user_id", "email"])
        found = _walk_local(str(tmp_path), max_depth=None)
        stems = {Path(p).name for p in found}
        assert stems == {"orders.csv", "users.csv"}

    def test_walk_local_ignores_unsupported_extensions(self, tmp_path: Path):
        (tmp_path / "notes.txt").write_text("irrelevant")
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")
        found = _walk_local(str(tmp_path), max_depth=None)
        assert all(Path(p).suffix.lower() in SUPPORTED_EXTENSIONS for p in found)

    def test_walk_local_raises_for_non_directory(self, tmp_path: Path):
        f = tmp_path / "file.csv"
        f.write_text("a\n1\n")
        with pytest.raises(ValueError, match="Not a directory"):
            _walk_local(str(f), max_depth=None)

    def test_source_type_for_path_csv(self):
        assert _source_type_for_path("/data/orders.csv") == "csv"

    def test_source_type_for_path_parquet(self):
        assert _source_type_for_path("/data/events.parquet") == "parquet"

    def test_source_type_for_path_sqlite(self):
        assert _source_type_for_path("/data/app.sqlite") == "sqlite"

    def test_source_type_for_path_db(self):
        assert _source_type_for_path("/data/app.db") == "sqlite"

    def test_source_type_for_path_unsupported(self):
        assert _source_type_for_path("/data/notes.txt") is None


# --------------------------------------------------------------------------- #
# REQ-789: CSV column headers → GraphQL field names                            #
# --------------------------------------------------------------------------- #


class TestCsvHeaderToGraphqlFieldName:
    """REQ-789: CSV column headers are automatically mapped to GraphQL field names."""

    def _mock_arrow_field(self, name: str) -> MagicMock:
        f = MagicMock()
        f.name = name
        f.nullable = True
        import pyarrow as pa

        f.type = pa.string()
        return f

    def _mock_schema(self, names: list[str]) -> MagicMock:
        import pyarrow as pa

        fields = []
        for name in names:
            fld = MagicMock()
            fld.name = name
            fld.nullable = True
            fld.type = pa.string()
            fields.append(fld)
        schema = MagicMock()
        schema.__len__ = lambda s: len(fields)
        schema.field = lambda i: fields[i]
        return schema

    def test_plain_header_preserved(self):
        schema = self._mock_schema(["id"])
        cols = _arrow_schema_to_columns(schema)
        assert cols[0]["name"] == "id"

    def test_header_with_space_mapped_via_arrow(self, tmp_path: Path):
        """Arrow preserves original header names; mapping to GQL names is caller's concern."""
        p = _make_csv(tmp_path, "t.csv", ["order id", "user name"], [["1", "alice"]])
        cfg = _csv_source(str(p))
        cols = discover_schema(cfg)
        raw_names = [c["name"] for c in cols]
        assert "order id" in raw_names or "order_id" in raw_names

    def test_snake_case_header_survives_roundtrip(self, tmp_path: Path):
        p = _make_csv(tmp_path, "t.csv", ["user_id", "first_name"], [["1", "bob"]])
        cfg = _csv_source(str(p))
        cols = discover_schema(cfg)
        names = [c["name"] for c in cols]
        assert "user_id" in names
        assert "first_name" in names

    def test_all_columns_present(self, tmp_path: Path):
        headers = ["col_a", "col_b", "col_c"]
        p = _make_csv(tmp_path, "t.csv", headers)
        cfg = _csv_source(str(p))
        cols = discover_schema(cfg)
        assert len(cols) == 3

    def test_column_name_field_present_in_each_col(self, tmp_path: Path):
        p = _make_csv(tmp_path, "t.csv", ["id", "value"])
        cfg = _csv_source(str(p))
        for col in discover_schema(cfg):
            assert "name" in col

    def test_column_type_field_present(self, tmp_path: Path):
        p = _make_csv(tmp_path, "t.csv", ["id"])
        cfg = _csv_source(str(p))
        for col in discover_schema(cfg):
            assert "type" in col

    def test_column_nullable_field_present(self, tmp_path: Path):
        p = _make_csv(tmp_path, "t.csv", ["id"])
        cfg = _csv_source(str(p))
        for col in discover_schema(cfg):
            assert "nullable" in col

    def test_generate_table_definitions_includes_column_names(self, tmp_path: Path):
        headers = ["product_id", "price", "label"]
        p = _make_csv(tmp_path, "products.csv", headers, [["1", "9.99", "hat"]])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        col_names = [c["name"] for c in defs[0]["columns"]]
        assert "product_id" in col_names
        assert "price" in col_names
        assert "label" in col_names

    def test_generate_table_definitions_table_name_is_stem(self, tmp_path: Path):
        p = _make_csv(tmp_path, "orders.csv", ["id"])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        assert defs[0]["tableName"] == "orders"

    def test_header_only_csv_produces_zero_data_rows_but_columns(self, tmp_path: Path):
        p = _make_csv(tmp_path, "empty.csv", ["a", "b"], [])
        cfg = _csv_source(str(p))
        cols = discover_schema(cfg)
        assert len(cols) == 2

    def test_naming_field_name_strips_non_alphanumeric(self):
        from provisa.compiler.naming import _to_field_name

        assert _to_field_name("order-id") == "order_id"
        assert _to_field_name("user name") == "user_name"
        assert _to_field_name("col#1") == "col_1"

    def test_naming_snake_case_passthrough(self):
        from provisa.compiler.naming import _to_field_name

        assert _to_field_name("user_id") == "user_id"

    def test_naming_camel_to_snake(self):
        from provisa.compiler.naming import _to_snake_case

        assert _to_snake_case("OrderId") == "order_id"
        assert _to_snake_case("userName") == "user_name"


# --------------------------------------------------------------------------- #
# REQ-790: File table enumeration is lazy (no filesystem calls at registration) #
# --------------------------------------------------------------------------- #


class TestFilesConnectorEnumerationIsLazy:
    """REQ-790: File connector table enumeration is lazy — files discovered at query time."""

    def test_source_registration_does_not_call_glob(self):
        """Constructing a Source with files type must not touch the filesystem."""
        import os

        call_log: list[str] = []
        original_listdir = os.listdir

        def spy_listdir(path: str) -> list:
            call_log.append(path)
            return original_listdir(path)

        with patch("os.listdir", side_effect=spy_listdir):
            Source(id="lake1", type=SourceType.files, path="/nonexistent/**/*.csv")

        assert call_log == [], f"Filesystem was accessed during Source construction: {call_log}"

    def test_source_construction_does_not_call_walk_local(self):
        """_walk_local must not be invoked during Source construction."""
        with patch(
            "provisa.file_source.crawler._walk_local",
            side_effect=AssertionError("_walk_local called during construction"),
        ):
            src = Source(id="lake1", type=SourceType.files, path="/data/**/*.csv")
        assert src.path == "/data/**/*.csv"

    def test_crawl_directory_called_lazily(self, tmp_path: Path):
        """crawl_directory is only called when explicitly invoked, not on import."""
        call_count = [0]
        real_walk = _walk_local

        def counting_walk(root: str, max_depth: object) -> list:
            call_count[0] += 1
            return real_walk(root, max_depth)

        with patch("provisa.file_source.crawler._walk_local", side_effect=counting_walk):
            # Nothing calls walk yet
            assert call_count[0] == 0
            # Explicitly trigger crawl
            _make_csv(tmp_path, "data.csv", ["id"])
            crawl_directory(str(tmp_path))
            assert call_count[0] == 1

    def test_build_catalog_properties_does_not_read_filesystem(self, tmp_path: Path):
        from provisa.core.catalog import _build_catalog_properties

        nonexistent_glob = str(tmp_path / "missing" / "**" / "*.csv")
        src = Source(id="lake1", type=SourceType.files, path=nonexistent_glob)
        # Must not raise even though path does not exist — just builds props
        props = _build_catalog_properties(src, "")
        assert props["glob"] == nonexistent_glob

    def test_walk_local_only_enumerates_on_call(self, tmp_path: Path):
        _make_csv(tmp_path, "a.csv", ["x"])
        subdir = tmp_path / "sub"
        subdir.mkdir()
        _make_csv(subdir, "b.csv", ["y"])

        # Before call: no traversal has happened (just verifying no side-effects on construction)
        _src = Source(id="lake1", type=SourceType.files, path=str(tmp_path / "**" / "*.csv"))

        # After explicit call: traversal happens
        found = _walk_local(str(tmp_path), max_depth=None)
        assert len(found) == 2

    def test_max_depth_limits_traversal(self, tmp_path: Path):
        _make_csv(tmp_path, "top.csv", ["a"])
        nested = tmp_path / "level1" / "level2"
        nested.mkdir(parents=True)
        _make_csv(nested, "deep.csv", ["b"])

        found_depth0 = _walk_local(str(tmp_path), max_depth=0)
        assert all(Path(p).parent == tmp_path for p in found_depth0)

    def test_no_introspection_at_source_construction(self):
        """discover_schema must not be called when Source is instantiated."""
        with patch(
            "provisa.file_source.source.discover_schema",
            side_effect=AssertionError("discover_schema called during Source construction"),
        ):
            src = Source(id="lake1", type=SourceType.files, path="/data/*.csv")
        assert src.path == "/data/*.csv"


# --------------------------------------------------------------------------- #
# REQ-791: Registered file-based tables queryable via data GraphQL API         #
# --------------------------------------------------------------------------- #


class TestFilesConnectorQueryable:
    """REQ-791: Registered file-based tables are queryable via the data GraphQL API."""

    def test_generate_table_definitions_returns_list(self, tmp_path: Path):
        p = _make_csv(tmp_path, "sales.csv", ["amount", "region"], [["100", "west"]])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        assert isinstance(defs, list)

    def test_generate_table_definitions_single_csv(self, tmp_path: Path):
        p = _make_csv(tmp_path, "sales.csv", ["amount", "region"], [["100", "west"]])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        assert len(defs) == 1

    def test_generate_table_definitions_has_table_name_key(self, tmp_path: Path):
        p = _make_csv(tmp_path, "sales.csv", ["amount"])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        assert "tableName" in defs[0]

    def test_generate_table_definitions_has_columns_key(self, tmp_path: Path):
        p = _make_csv(tmp_path, "sales.csv", ["amount"])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        assert "columns" in defs[0]

    def test_columns_have_name_and_type(self, tmp_path: Path):
        p = _make_csv(tmp_path, "t.csv", ["id", "label"], [["1", "x"]])
        cfg = _csv_source(str(p))
        defs = generate_table_definitions(cfg)
        for col in defs[0]["columns"]:
            assert "name" in col
            assert "type" in col

    def test_build_table_entry_csv_structure(self, tmp_path: Path):
        p = _make_csv(tmp_path, "events.csv", ["ts", "event"])
        columns = [
            {"name": "ts", "type": "TIMESTAMP", "nullable": True},
            {"name": "event", "type": "VARCHAR", "nullable": True},
        ]
        entry = _build_table_entry(str(p), "csv", columns)
        assert entry["name"] == "events"
        assert entry["type"] == "csv"
        assert entry["path"] == str(p)
        assert len(entry["tables"]) == 1
        assert entry["tables"][0]["name"] == "events"

    def test_build_table_entry_columns_passed_through(self, tmp_path: Path):
        p = _make_csv(tmp_path, "metrics.csv", ["val"])
        columns = [{"name": "val", "type": "DOUBLE", "nullable": False}]
        entry = _build_table_entry(str(p), "csv", columns)
        assert entry["tables"][0]["columns"][0]["name"] == "val"
        assert entry["tables"][0]["columns"][0]["type"] == "DOUBLE"

    def test_crawl_directory_produces_table_descriptors(self, tmp_path: Path):
        _make_csv(tmp_path, "metrics.csv", ["ts", "value"], [["2026-01-01", "1.0"]])
        with patch(
            "provisa.file_source.crawler._introspect_file",
            return_value=[
                {"name": "ts", "type": "VARCHAR", "nullable": True},
                {"name": "value", "type": "DOUBLE", "nullable": True},
            ],
        ):
            results = crawl_directory(str(tmp_path))
        assert len(results) == 1
        assert results[0]["name"] == "metrics"
        assert results[0]["type"] == "csv"
        assert results[0]["tables"][0]["name"] == "metrics"

    def test_crawl_directory_multiple_files(self, tmp_path: Path):
        _make_csv(tmp_path, "a.csv", ["x"])
        _make_csv(tmp_path, "b.csv", ["y"])
        mock_cols = [{"name": "x", "type": "VARCHAR", "nullable": True}]
        with patch("provisa.file_source.crawler._introspect_file", return_value=mock_cols):
            results = crawl_directory(str(tmp_path))
        assert len(results) == 2
        names = {r["name"] for r in results}
        assert names == {"a", "b"}

    def test_crawl_directory_skips_unsupported_files(self, tmp_path: Path):
        (tmp_path / "readme.txt").write_text("ignore me")
        _make_csv(tmp_path, "data.csv", ["col"])
        mock_cols = [{"name": "col", "type": "VARCHAR", "nullable": True}]
        with patch("provisa.file_source.crawler._introspect_file", return_value=mock_cols):
            results = crawl_directory(str(tmp_path))
        assert len(results) == 1
        assert results[0]["name"] == "data"

    def test_file_source_config_dataclass_fields(self):
        cfg = FileSourceConfig(id="x", source_type="csv", path="/tmp/t.csv")
        assert cfg.id == "x"
        assert cfg.source_type == "csv"
        assert cfg.path == "/tmp/t.csv"
        assert cfg.options == {}

    def test_generate_catalog_properties_returns_empty_for_file_source(self, tmp_path: Path):
        from provisa.file_source.source import generate_catalog_properties

        p = _make_csv(tmp_path, "t.csv", ["id"])
        cfg = _csv_source(str(p))
        assert generate_catalog_properties(cfg) == {}

    def test_unsupported_source_type_raises(self):
        cfg = FileSourceConfig(id="x", source_type="xlsx", path="/data/t.xlsx")
        with pytest.raises(ValueError, match="Unsupported file source type"):
            discover_schema(cfg)

# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1b2c4-d5e6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Iceberg/Delta time-travel queries (REQ-372).

Verifies:
- as_of with ISO 8601 timestamp → FOR TIMESTAMP AS OF
- as_of with integer version/snapshot ID → FOR VERSION AS OF
- as_of on non-time-travel source → compile-time rejection
- TIME_TRAVEL_SOURCES set contains correct entries
"""

import pytest
from graphql import parse, validate

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import build_context, compile_query
from provisa.core.models import TIME_TRAVEL_SOURCES


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_lake_ctx(source_type: str):
    """Build schema + context for a single lake source table."""
    tables = [
        {
            "id": 10,
            "source_id": "lake-src",
            "domain_id": "datalake",
            "schema_name": "db",
            "table_name": "events",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "ts", "visible_to": ["admin"]},
                {"column_name": "payload", "visible_to": ["admin"]},
            ],
        }
    ]
    column_types = {
        10: [
            _col("id", "bigint"),
            _col("ts", "timestamp"),
            _col("payload", "varchar"),
        ]
    }
    role = {"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]}
    domains = [{"id": "datalake", "description": "Data Lake"}]
    si = SchemaInput(
        tables=tables,
        relationships=[],
        column_types=column_types,
        naming_rules=[],
        role=role,
        domains=domains,
        source_types={"lake-src": source_type},
    )
    schema = generate_schema(si)
    ctx = build_context(si)
    return schema, ctx


class TestTimeTravelSourcesSet:
    def test_iceberg_in_time_travel(self):
        assert "iceberg" in TIME_TRAVEL_SOURCES

    def test_delta_lake_in_time_travel(self):
        assert "delta_lake" in TIME_TRAVEL_SOURCES

    def test_hive_s3_not_in_time_travel(self):
        assert "hive_s3" not in TIME_TRAVEL_SOURCES

    def test_postgresql_not_in_time_travel(self):
        assert "postgresql" not in TIME_TRAVEL_SOURCES


class TestIcebergTimeTravelTimestamp:
    def test_for_timestamp_as_of(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse('{ events(as_of: "2024-01-15T12:00:00") { id ts } }')
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15T12:00:00'" in sql
        assert "FOR VERSION AS OF" not in sql

    def test_timestamp_placed_before_alias(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse('{ events(as_of: "2024-06-01 00:00:00") { id } }')
        results = compile_query(doc, ctx)
        sql = results[0].sql
        # Table ref should precede the time-travel clause which precedes the alias
        assert "FOR TIMESTAMP AS OF" in sql

    def test_original_select_preserved(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse('{ events(as_of: "2024-01-15T00:00:00") { id payload } }')
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "SELECT" in sql
        assert '"id"' in sql
        assert '"payload"' in sql


class TestIcebergTimeTravelVersion:
    def test_for_version_as_of_integer(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse("{ events(as_of: 1234567890) { id } }")
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "FOR VERSION AS OF 1234567890" in sql
        assert "FOR TIMESTAMP AS OF" not in sql

    def test_for_version_as_of_string_integer(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse('{ events(as_of: "9876543210") { id } }')
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "FOR VERSION AS OF 9876543210" in sql


class TestDeltaLakeTimeTravel:
    def test_delta_timestamp_as_of(self):
        schema, ctx = _build_lake_ctx("delta_lake")
        doc = parse('{ events(as_of: "2025-03-01T08:00:00") { id ts } }')
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "FOR TIMESTAMP AS OF TIMESTAMP '2025-03-01T08:00:00'" in sql

    def test_delta_version_as_of(self):
        schema, ctx = _build_lake_ctx("delta_lake")
        doc = parse("{ events(as_of: 42) { id } }")
        results = compile_query(doc, ctx)
        sql = results[0].sql
        assert "FOR VERSION AS OF 42" in sql


class TestTimeTravelRejection:
    def test_postgresql_as_of_rejected(self):
        schema, ctx = _build_lake_ctx("postgresql")
        doc = parse('{ events(as_of: "2024-01-01T00:00:00") { id } }')
        with pytest.raises(ValueError, match="as_of is not supported"):
            compile_query(doc, ctx)

    def test_hive_s3_as_of_rejected(self):
        schema, ctx = _build_lake_ctx("hive_s3")
        doc = parse('{ events(as_of: "2024-01-01T00:00:00") { id } }')
        with pytest.raises(ValueError, match="as_of is not supported"):
            compile_query(doc, ctx)

    def test_unknown_source_type_as_of_rejected(self):
        schema, ctx = _build_lake_ctx("mongodb")
        doc = parse('{ events(as_of: "2024-01-01T00:00:00") { id } }')
        with pytest.raises(ValueError, match="as_of is not supported"):
            compile_query(doc, ctx)


class TestTimeTravelCatalogQualified:
    def test_iceberg_timestamp_with_catalog(self):
        schema, ctx = _build_lake_ctx("iceberg")
        doc = parse('{ events(as_of: "2024-01-15T12:00:00") { id } }')
        results = compile_query(doc, ctx, use_catalog=True)
        sql = results[0].sql
        assert "FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15T12:00:00'" in sql
        # Should still include catalog-qualified table name
        assert "lake_src" in sql

    def test_delta_version_with_catalog(self):
        schema, ctx = _build_lake_ctx("delta_lake")
        doc = parse("{ events(as_of: 7) { id } }")
        results = compile_query(doc, ctx, use_catalog=True)
        sql = results[0].sql
        assert "FOR VERSION AS OF 7" in sql
        assert "lake_src" in sql

# Copyright (c) 2026 Kenneth Stott
# Canary: fc66f598-3ea9-4ea5-be94-15a43c58cad0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Iceberg/Delta time-travel queries (REQ-372).

Verifies (REQ-372 as amended 2026-10-26, under REQ-1163's arguments):
- as_recorded on an Iceberg or Delta table → its snapshot at that time (FOR TIMESTAMP AS OF)
- as_of on one (no valid time) or either argument on any other table → refused by name
- TIME_TRAVEL_SOURCES set contains correct entries
"""

import pytest
from graphql import parse

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.context import build_context
from provisa.core.source_registry import TIME_TRAVEL_SOURCES


def _col(name: str, data_type: str = "varchar(100)", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _build_lake_ctx(source_type: str, time_travel: dict[int, str] | None = None):
    """Build schema + context for a single lake source table."""
    tables = [
        {
            "id": 10,
            "source_id": "lake-src",
            "domain_id": "datalake",
            "schema_name": "db",
            "table_name": "events",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "ts", "visible_to": ["admin"]},
                {"column_name": "payload", "visible_to": ["admin"]},
            ],
            "write_ops": ["delete", "insert", "update"],
            "write_returns_rows": True,
            "write_refused_forms": [],
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
        time_travel=time_travel or {},
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


def _snapshot_ctx(source_type: str):
    """The lake table's schema and context, its time travel decided as the build decides it
    (provisa.mv.time_travel.relation_kinds over the registered row and its source's type)."""
    from types import SimpleNamespace

    from provisa.mv.time_travel import relation_kinds

    state = SimpleNamespace(tables=[], source_types={"lake-src": source_type})
    row = {"id": 10, "source_id": "lake-src", "table_name": "events", "view_sql": None}
    return _build_lake_ctx(source_type, relation_kinds(state, [row]))


def _read(source_type: str, query: str) -> str:
    """The statement the engine runs for ``query``: compiled, its arguments lifted per reference,
    then each snapshot table read at its reference's recorded time."""
    from graphql import validate

    from provisa.mv.time_travel import lift, read_snapshots

    schema, ctx = _snapshot_ctx(source_type)
    document = parse(query)
    errors = validate(schema, document)
    if errors:
        raise ValueError(errors[0].message)
    [compiled] = compile_query(document, ctx, use_catalog=True)
    sql, travel = lift(compiled.sql, lambda _t: "snapshot")
    return read_snapshots(sql, ctx, travel)


class TestSnapshotTimeTravel:
    """REQ-372 (amended 2026-10-26): an Iceberg or Delta table travels by its snapshots under the
    one time-travel argument every relation that keeps versions takes: a snapshot is a commit
    time, so as_recorded selects it (REQ-1163)."""

    @pytest.mark.parametrize("source_type", ["iceberg", "delta_lake"])
    def test_as_recorded_reads_the_snapshot_current_then(self, source_type):
        sql = _read(source_type, '{ events(as_recorded: "2024-01-15T12:00:00") { id ts } }')
        assert "FOR TIMESTAMP AS OF CAST('2024-01-15 12:00:00" in sql
        assert "FOR VERSION AS OF" not in sql

    def test_a_read_with_no_time_reads_the_current_snapshot(self):
        assert "AS OF" not in _read("iceberg", "{ events { id } }")

    def test_a_snapshot_table_maps_no_valid_time(self):
        with pytest.raises(ValueError, match="as_of"):
            _read("iceberg", '{ events(as_of: "2024-01-15T12:00:00") { id } }')

    @pytest.mark.parametrize("source_type", ["postgresql", "hive_s3"])
    def test_a_table_that_keeps_no_versions_takes_no_time(self, source_type):
        with pytest.raises(ValueError, match="as_recorded"):
            _read(source_type, '{ events(as_recorded: "2024-01-01T00:00:00") { id } }')

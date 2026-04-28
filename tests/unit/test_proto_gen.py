# Copyright (c) 2026 Kenneth Stott
# Canary: d01f4449-3432-44a4-8942-7c9cbe503e07
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for gRPC proto generation from SchemaInput."""

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput
from provisa.grpc.proto_gen import generate_proto, _trino_to_proto


def _make_si(
    tables=None,
    relationships=None,
    column_types=None,
    role=None,
    naming_rules=None,
    domains=None,
    source_types=None,
):
    """Build a minimal SchemaInput for testing."""
    if tables is None:
        tables = [
            {
                "id": 1,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "created_at", "visible_to": ["admin"]},
                ],
            }
        ]
    if column_types is None:
        column_types = {
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="amount", data_type="decimal(10,2)", is_nullable=True),
                ColumnMetadata(column_name="created_at", data_type="timestamp", is_nullable=True),
            ]
        }
    if role is None:
        role = {"id": "admin", "domain_access": ["*"], "capabilities": []}
    if naming_rules is None:
        naming_rules = []
    if domains is None:
        domains = [{"id": "sales", "description": "Sales domain"}]
    if relationships is None:
        relationships = []

    return SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=naming_rules,
        role=role,
        domains=domains,
        source_types=source_types,
    )


class TestTypeMapping:
    def test_integer(self):
        assert _trino_to_proto("integer") == "int32"

    def test_bigint(self):
        assert _trino_to_proto("bigint") == "int64"

    def test_varchar(self):
        assert _trino_to_proto("varchar") == "string"

    def test_varchar_parameterized(self):
        assert _trino_to_proto("varchar(255)") == "string"

    def test_boolean(self):
        assert _trino_to_proto("boolean") == "bool"

    def test_decimal(self):
        assert _trino_to_proto("decimal(10,2)") == "double"

    def test_real(self):
        assert _trino_to_proto("real") == "double"

    def test_double(self):
        assert _trino_to_proto("double") == "double"

    def test_timestamp(self):
        assert _trino_to_proto("timestamp") == "google.protobuf.Timestamp"

    def test_date(self):
        assert _trino_to_proto("date") == "string"

    def test_unmapped_raises(self):
        with pytest.raises(ValueError, match="Unmapped Trino type"):
            _trino_to_proto("hyperloglog")

    def test_array_inner(self):
        assert _trino_to_proto("array(varchar)") == "string"


class TestGenerateProto:
    def test_basic_message_fields(self):
        si = _make_si()
        proto = generate_proto(si)

        assert 'syntax = "proto3";' in proto
        assert "package provisa.v1;" in proto
        assert "message Orders {" in proto
        # Sorted columns: amount, created_at, id
        assert "double amount = 1;" in proto
        assert "google.protobuf.Timestamp created_at = 2;" in proto
        assert "int32 id = 3;" in proto

    def test_timestamp_import(self):
        si = _make_si()
        proto = generate_proto(si)
        assert 'import "google/protobuf/timestamp.proto";' in proto

    def test_no_timestamp_import_when_unneeded(self):
        si = _make_si(
            tables=[{
                "id": 1,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "users",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "name", "visible_to": ["admin"]},
                ],
            }],
            column_types={
                1: [
                    ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                    ColumnMetadata(column_name="name", data_type="varchar", is_nullable=False),
                ],
            },
        )
        proto = generate_proto(si)
        assert "timestamp.proto" not in proto

    def test_filter_input_generated(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "message OrdersFilter {" in proto
        # Filter uses string for timestamp
        assert "string created_at" in proto

    def test_request_message(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "message OrdersRequest {" in proto
        assert "OrdersFilter filter = 1;" in proto
        assert "int32 limit = 2;" in proto
        assert "int32 offset = 3;" in proto

    def test_service_query_rpc(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "service ProvisaService {" in proto
        assert "rpc QueryOrders(OrdersRequest) returns (stream Orders);" in proto

    def test_service_mutation_rpc(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "rpc InsertOrders(OrdersInput) returns (MutationResponse);" in proto

    def test_mutation_input_message(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "message OrdersInput {" in proto

    def test_mutation_response(self):
        si = _make_si()
        proto = generate_proto(si)
        assert "message MutationResponse {" in proto
        assert "int32 affected_rows = 1;" in proto


class TestRoleFiltering:
    def test_invisible_columns_excluded(self):
        si = _make_si(
            tables=[{
                "id": 1,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin", "viewer"]},
                    {"column_name": "amount", "visible_to": ["admin"]},
                    {"column_name": "secret", "visible_to": ["admin"]},
                ],
            }],
            column_types={
                1: [
                    ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                    ColumnMetadata(column_name="amount", data_type="decimal", is_nullable=True),
                    ColumnMetadata(column_name="secret", data_type="varchar", is_nullable=True),
                ],
            },
            role={"id": "viewer", "domain_access": ["*"], "capabilities": []},
        )
        proto = generate_proto(si)
        assert "int32 id = 1;" in proto
        assert "amount" not in proto
        assert "secret" not in proto

    def test_no_visible_tables_raises(self):
        si = _make_si(
            role={"id": "nobody", "domain_access": ["other"], "capabilities": []},
        )
        with pytest.raises(ValueError, match="No tables visible"):
            generate_proto(si)


class TestRelationships:
    def _two_table_si(self):
        tables = [
            {
                "id": 1,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "orders",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "customer_id", "visible_to": ["admin"]},
                ],
            },
            {
                "id": 2,
                "source_id": "pg1",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "customers",
                "columns": [
                    {"column_name": "id", "visible_to": ["admin"]},
                    {"column_name": "name", "visible_to": ["admin"]},
                ],
            },
        ]
        column_types = {
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="customer_id", data_type="integer", is_nullable=True),
            ],
            2: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="name", data_type="varchar", is_nullable=False),
            ],
        }
        return tables, column_types

    def test_many_to_one_singular_field(self):
        tables, column_types = self._two_table_si()
        relationships = [{
            "id": 1,
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }]
        si = _make_si(
            tables=tables,
            column_types=column_types,
            relationships=relationships,
        )
        proto = generate_proto(si)
        # Many-to-one: singular, not repeated
        assert "Customers customers = 3;" in proto
        assert "repeated Customers" not in proto

    def test_one_to_many_repeated_field(self):
        tables, column_types = self._two_table_si()
        relationships = [{
            "id": 1,
            "source_table_id": 2,
            "target_table_id": 1,
            "source_column": "id",
            "target_column": "customer_id",
            "cardinality": "one-to-many",
        }]
        si = _make_si(
            tables=tables,
            column_types=column_types,
            relationships=relationships,
        )
        proto = generate_proto(si)
        assert "repeated Orders orders = 3;" in proto

# Copyright (c) 2026 Kenneth Stott
# Canary: ed408a8c-63ca-46ea-a75c-2339f6a48fd2
# Canary: PENDING
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Flight SQL catalog and approved modes (REQ-126)."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight.catalog import (
    ApprovedQuery,
    CatalogColumn,
    CatalogTable,
    approved_query_to_flight_info,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
    _trino_type_to_arrow,
)
from provisa.api.flight.server import ProvisaFlightServer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class FakeState:
    """Minimal AppState substitute for testing."""

    def __init__(self):
        self.schemas = {}
        self.contexts = {}
        self.rls_contexts = {}
        self.roles = {}
        self.source_pools = None
        self.source_types = {}
        self.source_dialects = {}
        self.pg_pool = None
        self.trino_conn = None
        self.flight_client = None


@pytest.fixture()
def fake_state():
    return FakeState()


def _make_table(
    domain="sales",
    table_name="orders",
    description="All orders",
    columns=None,
) -> CatalogTable:
    if columns is None:
        columns = [
            CatalogColumn("id", "integer", False, "Primary key"),
            CatalogColumn("customer_name", "varchar", True, "Customer full name"),
            CatalogColumn("total", "decimal(10,2)", True, "Order total"),
        ]
    return CatalogTable(
        domain_id=domain,
        table_name=table_name,
        description=description,
        columns=columns,
    )


def _make_approved_query(
    stable_id="abc-123",
    query_text="{ orders { id } }",
    compiled_sql="SELECT id FROM orders",
) -> ApprovedQuery:
    return ApprovedQuery(
        stable_id=stable_id,
        query_text=query_text,
        compiled_sql=compiled_sql,
    )


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------

class TestTrinoTypeToArrow:
    def test_varchar(self):
        assert _trino_type_to_arrow("varchar") == pa.utf8()

    def test_integer(self):
        assert _trino_type_to_arrow("integer") == pa.int32()

    def test_bigint(self):
        assert _trino_type_to_arrow("bigint") == pa.int64()

    def test_boolean(self):
        assert _trino_type_to_arrow("boolean") == pa.bool_()

    def test_double(self):
        assert _trino_type_to_arrow("double") == pa.float64()

    def test_decimal_parameterized(self):
        assert _trino_type_to_arrow("decimal(10,2)") == pa.float64()

    def test_timestamp(self):
        assert _trino_type_to_arrow("timestamp") == pa.timestamp("us")

    def test_unknown_raises(self):
        with pytest.raises(KeyError, match="Unmapped Trino type"):
            _trino_type_to_arrow("unknown_fancy_type")


# ---------------------------------------------------------------------------
# Catalog schema
# ---------------------------------------------------------------------------

class TestCatalogTableToArrowSchema:
    def test_field_count(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        assert len(schema) == 3

    def test_field_names(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        assert schema.names == ["id", "customer_name", "total"]

    def test_field_types(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        assert schema.field("id").type == pa.int32()
        assert schema.field("customer_name").type == pa.utf8()
        # decimal(10,2) maps to float64
        assert schema.field("total").type == pa.float64()

    def test_nullable(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        assert schema.field("id").nullable is False
        assert schema.field("customer_name").nullable is True

    def test_description_metadata(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        meta = schema.field("id").metadata
        assert meta[b"description"] == b"Primary key"

    def test_schema_metadata(self):
        table = _make_table()
        schema = catalog_table_to_arrow_schema(table)
        assert schema.metadata[b"domain"] == b"sales"
        assert schema.metadata[b"description"] == b"All orders"


# ---------------------------------------------------------------------------
# FlightInfo for catalog
# ---------------------------------------------------------------------------

class TestCatalogFlightInfo:
    def test_descriptor_path(self):
        table = _make_table()
        info = catalog_table_to_flight_info(table)
        assert list(info.descriptor.path) == [b"sales", b"orders"]

    def test_schema_matches(self):
        table = _make_table()
        info = catalog_table_to_flight_info(table)
        assert info.schema.names == ["id", "customer_name", "total"]

    def test_no_endpoints_without_location(self):
        table = _make_table()
        info = catalog_table_to_flight_info(table)
        assert len(info.endpoints) == 0


# ---------------------------------------------------------------------------
# FlightInfo for approved queries
# ---------------------------------------------------------------------------

class TestApprovedFlightInfo:
    def test_descriptor_path(self):
        q = _make_approved_query()
        info = approved_query_to_flight_info(q)
        assert list(info.descriptor.path) == [b"approved", b"abc-123"]

    def test_schema_fields(self):
        q = _make_approved_query()
        info = approved_query_to_flight_info(q)
        assert info.schema.names == ["stable_id", "query_text", "compiled_sql"]


# ---------------------------------------------------------------------------
# Server mode parsing
# ---------------------------------------------------------------------------

class TestModeFromTicket:
    def test_default_none(self):
        assert ProvisaFlightServer._parse_mode(None) == "default"

    def test_default_empty(self):
        assert ProvisaFlightServer._parse_mode(b"") == "default"

    def test_default_garbage(self):
        assert ProvisaFlightServer._parse_mode(b"\xff\xfe") == "default"

    def test_catalog(self):
        buf = json.dumps({"mode": "catalog"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "catalog"

    def test_approved(self):
        buf = json.dumps({"mode": "approved"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "approved"

    def test_default_explicit(self):
        buf = json.dumps({"mode": "default"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "default"

    def test_no_mode_key(self):
        buf = json.dumps({"query": "test"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "default"


# ---------------------------------------------------------------------------
# Server catalog streams
# ---------------------------------------------------------------------------

class TestServerCatalogTables:
    def test_catalog_tables(self):
        tables = [
            _make_table(domain="sales", table_name="orders"),
            _make_table(domain="hr", table_name="employees"),
        ]
        result = ProvisaFlightServer._build_catalog_table(tables)
        assert result.num_rows == 2
        assert result.column("schema_name").to_pylist() == ["sales", "hr"]
        assert result.column("table_name").to_pylist() == ["orders", "employees"]

    def test_catalog_tables_filtered(self):
        tables = [
            _make_table(domain="sales", table_name="orders"),
            _make_table(domain="hr", table_name="employees"),
        ]
        result = ProvisaFlightServer._build_catalog_table(tables, "sales")
        assert result.num_rows == 1
        assert result.column("table_name").to_pylist() == ["orders"]

    def test_table_columns(self):
        table = _make_table()
        result = ProvisaFlightServer._build_columns_table(table)
        assert result.num_rows == 3
        assert result.column("column_name").to_pylist() == [
            "id", "customer_name", "total",
        ]
        assert result.column("data_type").to_pylist() == [
            "integer", "varchar", "decimal(10,2)",
        ]

    def test_empty_catalog(self):
        result = ProvisaFlightServer._build_catalog_table([])
        assert result.num_rows == 0


# ---------------------------------------------------------------------------
# Server approved streams
# ---------------------------------------------------------------------------

class TestServerApprovedTables:
    def test_approved_queries_table(self):
        queries = [
            _make_approved_query("id-1", "q1", "sql1"),
            _make_approved_query("id-2", "q2", "sql2"),
        ]
        result = ProvisaFlightServer._build_approved_queries_table(queries)
        assert result.num_rows == 2
        assert result.column("stable_id").to_pylist() == ["id-1", "id-2"]

    def test_single_approved_query_table(self):
        q = _make_approved_query()
        result = ProvisaFlightServer._build_approved_query_table(q)
        assert result.num_rows == 1
        assert result.column("stable_id").to_pylist() == ["abc-123"]
        assert result.column("compiled_sql").to_pylist() == ["SELECT id FROM orders"]

    def test_empty_approved(self):
        result = ProvisaFlightServer._build_approved_queries_table([])
        assert result.num_rows == 0


# ---------------------------------------------------------------------------
# Default mode unchanged
# ---------------------------------------------------------------------------

class TestDefaultMode:
    def test_missing_query_raises(self, fake_state):
        """Default mode still requires a query in the ticket."""
        server = ProvisaFlightServer.__new__(ProvisaFlightServer)
        server._state = fake_state
        server._session_modes = {}
        ticket = flight.Ticket(json.dumps({"role": "admin"}).encode())
        with pytest.raises(flight.FlightServerError, match="must include 'query'"):
            server.do_get(None, ticket)

    def test_missing_role_raises(self, fake_state):
        """Default mode raises when the role schema is missing."""
        server = ProvisaFlightServer.__new__(ProvisaFlightServer)
        server._state = fake_state
        server._session_modes = {}
        ticket = flight.Ticket(json.dumps({"query": "{ x }", "role": "nope"}).encode())
        with pytest.raises(flight.FlightServerError, match="No schema for role"):
            server.do_get(None, ticket)

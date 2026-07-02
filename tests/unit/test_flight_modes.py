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

"""Unit tests for Flight SQL catalog (REQ-126)."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight.catalog import (
    CatalogColumn,
    CatalogTable,
    catalog_table_to_arrow_schema,
    catalog_table_to_flight_info,
    _trino_type_to_arrow,
)
from provisa.api.flight.server import ProvisaFlightServer
from provisa.api.flight.server import _parse_limit_value


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
        self.tenant_db = None
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

    def test_ticket_has_no_mode_key(self):
        """Ticket JSON must not contain a 'mode' key."""
        table = _make_table()
        loc = flight.Location.for_grpc_tcp("localhost", 8815)
        info = catalog_table_to_flight_info(table, location=loc)
        assert len(info.endpoints) == 1
        ticket_data = json.loads(info.endpoints[0].ticket.ticket.decode())
        assert "mode" not in ticket_data
        assert ticket_data["domain"] == "sales"
        assert ticket_data["table"] == "orders"


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
            "id",
            "customer_name",
            "total",
        ]
        assert result.column("data_type").to_pylist() == [
            "integer",
            "varchar",
            "decimal(10,2)",
        ]

    def test_empty_catalog(self):
        result = ProvisaFlightServer._build_catalog_table([])
        assert result.num_rows == 0


# ---------------------------------------------------------------------------
# Limit value parsing
# ---------------------------------------------------------------------------


class TestParseLimitValue:
    def test_rejects_negative(self):
        with pytest.raises(flight.FlightServerError, match="non-negative integer"):
            _parse_limit_value(-1)

    def test_rejects_bool(self):
        with pytest.raises(flight.FlightServerError, match="non-negative integer"):
            _parse_limit_value(True)

    def test_none_passthrough(self):
        assert _parse_limit_value(None) is None

    def test_zero(self):
        assert _parse_limit_value(0) == 0

    def test_positive(self):
        assert _parse_limit_value(10) == 10


# ---------------------------------------------------------------------------
# WHERE variable parsing (REQ-302)
# ---------------------------------------------------------------------------


class TestParseWhereVariables:
    """Unit tests for _parse_where_variables — JDBC WHERE-clause variable extraction."""

    def test_integer_equality(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE id = 42"
        assert _parse_where_variables(sql) == {"id": 42}

    def test_string_equality(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE region = 'us-east'"
        assert _parse_where_variables(sql) == {"region": "us-east"}

    def test_multiple_predicates(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE region = 'eu-west' AND id = 99"
        result = _parse_where_variables(sql)
        assert result["region"] == "eu-west"
        assert result["id"] == 99

    def test_no_where_returns_empty(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders"
        assert _parse_where_variables(sql) == {}

    def test_stops_at_limit(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE id = 5 LIMIT 10"
        result = _parse_where_variables(sql)
        assert result == {"id": 5}

    def test_float_value(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE amount = 3.14"
        assert _parse_where_variables(sql) == {"amount": 3.14}

    def test_negative_integer(self):
        from provisa.api.flight.server import _parse_where_variables

        sql = "SELECT * FROM orders WHERE offset_val = -1"
        assert _parse_where_variables(sql) == {"offset_val": -1}


# ---------------------------------------------------------------------------
# do_get routing
# ---------------------------------------------------------------------------


class TestDoGet:
    def test_missing_query_falls_through_to_catalog(self, fake_state):
        """No query in ticket routes to catalog fetch (tenant_db=None → empty)."""
        server = ProvisaFlightServer.__new__(ProvisaFlightServer)
        server._state = fake_state
        ticket = flight.Ticket(json.dumps({"role": "admin"}).encode())
        # tenant_db is None, so _do_get_catalog returns empty table stream
        stream = server.do_get(None, ticket)
        assert stream is not None

    def test_missing_role_raises(self, fake_state):
        """Query with unknown role raises FlightServerError."""
        server = ProvisaFlightServer.__new__(ProvisaFlightServer)
        server._state = fake_state
        ticket = flight.Ticket(json.dumps({"query": "{ x }", "role": "nope"}).encode())
        with pytest.raises(flight.FlightServerError, match="No schema for role"):
            server.do_get(None, ticket)

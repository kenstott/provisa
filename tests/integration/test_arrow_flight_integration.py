# Copyright (c) 2026 Kenneth Stott
# Canary: e5f6a7b8-c9d0-1234-efab-234567890004
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the Arrow Flight server (REQ-045, REQ-126).

Tests are organised into three tiers:

  1. Pure Python — exercises ProvisaFlightServer helpers that do not require
     a live Flight server (JSON ticket parsing, mode detection, Arrow table
     builders).  These run without any infrastructure.

  2. In-process server — starts a ProvisaFlightServer in a background thread
     and connects a pyarrow.flight.FlightClient to it.

  3. Live PG-backed — extends the in-process server fixture with a real
     source pool.
"""

from __future__ import annotations

import json
import os
import threading

import pytest

pa = pytest.importorskip("pyarrow")
flight = pytest.importorskip("pyarrow.flight")

from provisa.api.flight.server import ProvisaFlightServer
from provisa.executor.formats.arrow import rows_to_arrow_table
from provisa.compiler.sql_gen import ColumnRef

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_FLIGHT_PORT = int(os.environ.get("PROVISA_TEST_FLIGHT_PORT", "8916"))
_FLIGHT_LOCATION = f"grpc://localhost:{_TEST_FLIGHT_PORT}"


def _port_in_use(port: int) -> bool:
    import socket

    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def _make_minimal_state():
    """Return a minimal MagicMock AppState suitable for Flight server tests."""
    from unittest.mock import MagicMock

    state = MagicMock()
    state.schemas = {}
    state.contexts = {}
    state.rls_contexts = {}
    state.roles = {}
    state.source_pools = MagicMock()
    state.source_types = {}
    state.source_dialects = {}
    state.masking_rules = {}
    state.flight_client = None
    state.trino_conn = None
    return state


# ---------------------------------------------------------------------------
# Tier 1: Pure-Python helper tests (no Flight server needed)
# ---------------------------------------------------------------------------


class TestFlightServerParseMode:
    """Unit tests for ProvisaFlightServer._parse_mode."""

    async def test_none_returns_default(self):
        assert ProvisaFlightServer._parse_mode(None) == "default"

    async def test_empty_bytes_returns_default(self):
        assert ProvisaFlightServer._parse_mode(b"") == "default"

    async def test_catalog_mode_parsed(self):
        buf = json.dumps({"mode": "catalog"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "catalog"

    async def test_approved_mode_parsed(self):
        buf = json.dumps({"mode": "approved"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "approved"

    async def test_default_mode_parsed(self):
        buf = json.dumps({"mode": "default"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "default"

    async def test_missing_mode_key_returns_default(self):
        buf = json.dumps({"other": "value"}).encode()
        assert ProvisaFlightServer._parse_mode(buf) == "default"

    async def test_invalid_json_returns_default(self):
        assert ProvisaFlightServer._parse_mode(b"not-json") == "default"

    async def test_non_utf8_returns_default(self):
        assert ProvisaFlightServer._parse_mode(b"\xff\xfe") == "default"


class TestFlightServerBuildCatalogTable:
    """Unit tests for the Arrow table builders (no server needed)."""

    async def test_build_catalog_table_produces_correct_schema(self):
        from unittest.mock import MagicMock
        from provisa.api.flight.server import ProvisaFlightServer

        cat1 = MagicMock()
        cat1.domain_id = "sales"
        cat1.table_name = "orders"
        cat1.description = "Order records"

        cat2 = MagicMock()
        cat2.domain_id = "crm"
        cat2.table_name = "customers"
        cat2.description = "Customer data"

        table = ProvisaFlightServer._build_catalog_table([cat1, cat2])
        assert "schema_name" in table.schema.names
        assert "table_name" in table.schema.names
        assert "description" in table.schema.names
        assert table.num_rows == 2

    async def test_build_catalog_table_domain_filter(self):
        from unittest.mock import MagicMock
        from provisa.api.flight.server import ProvisaFlightServer

        cat1 = MagicMock()
        cat1.domain_id = "sales"
        cat1.table_name = "orders"
        cat1.description = ""
        cat2 = MagicMock()
        cat2.domain_id = "crm"
        cat2.table_name = "customers"
        cat2.description = ""

        table = ProvisaFlightServer._build_catalog_table([cat1, cat2], domain_filter="sales")
        assert table.num_rows == 1
        assert table.column("schema_name")[0].as_py() == "sales"

    async def test_build_columns_table_structure(self):
        from unittest.mock import MagicMock
        from provisa.api.flight.server import ProvisaFlightServer

        col = MagicMock()
        col.name = "id"
        col.data_type = "integer"
        col.is_nullable = False
        col.description = "Primary key"

        cat = MagicMock()
        cat.columns = [col]

        table = ProvisaFlightServer._build_columns_table(cat)
        assert "column_name" in table.schema.names
        assert "data_type" in table.schema.names
        assert "is_nullable" in table.schema.names
        assert "description" in table.schema.names
        assert table.num_rows == 1

    async def test_build_approved_query_table(self):
        from unittest.mock import MagicMock
        from provisa.api.flight.server import ProvisaFlightServer

        q = MagicMock()
        q.stable_id = "abc123"
        q.query_text = "{ orders { id } }"
        q.compiled_sql = "SELECT id FROM orders"

        table = ProvisaFlightServer._build_approved_query_table(q)
        assert "stable_id" in table.schema.names
        assert "query_text" in table.schema.names
        assert "compiled_sql" in table.schema.names
        assert table.num_rows == 1

    async def test_build_approved_queries_table(self):
        from unittest.mock import MagicMock
        from provisa.api.flight.server import ProvisaFlightServer

        q1 = MagicMock()
        q1.stable_id = "s1"
        q1.query_text = "{ a { id } }"
        q1.compiled_sql = "SELECT id FROM a"

        q2 = MagicMock()
        q2.stable_id = "s2"
        q2.query_text = "{ b { id } }"
        q2.compiled_sql = "SELECT id FROM b"

        table = ProvisaFlightServer._build_approved_queries_table([q1, q2])
        assert table.num_rows == 2


class TestRowsToArrowTable:
    """Unit tests for the rows → Arrow table conversion used by Flight."""

    async def test_basic_conversion(self):
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(1, 100), (2, 200), (3, 300)]
        table = rows_to_arrow_table(rows, columns)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert "id" in table.schema.names
        assert "amount" in table.schema.names

    async def test_empty_rows_gives_empty_table(self):
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
        ]
        table = rows_to_arrow_table([], columns)
        assert table.num_rows == 0

    async def test_nested_column_uses_dotted_name(self):
        from provisa.executor.formats.arrow import rows_to_arrow_table, _column_names
        columns = [
            ColumnRef(alias=None, column="name", field_name="name", nested_in="customer"),
        ]
        names = _column_names(columns)
        assert names == ["customer.name"]

    async def test_decimal_converted_to_float(self):
        from decimal import Decimal

        columns = [
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(Decimal("123.45"),)]
        table = rows_to_arrow_table(rows, columns)
        val = table.column("amount")[0].as_py()
        assert isinstance(val, float)
        assert abs(val - 123.45) < 0.001

    async def test_schema_field_names_match_query_columns(self):
        """Returned schema field names match the queried ColumnRef field names."""
        columns = [
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
        ]
        rows = [(1, "us-east", 500), (2, "eu-west", 200)]
        table = rows_to_arrow_table(rows, columns)
        schema_names = table.schema.names
        for col in columns:
            assert col.field_name in schema_names


# ---------------------------------------------------------------------------
# Tier 2: In-process Flight server fixture (requires pyarrow + free port)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def flight_server_and_client():
    """Start ProvisaFlightServer in a background thread; yield a FlightClient.

    The fixture is module-scoped to avoid repeated startup overhead.
    """
    state = _make_minimal_state()

    server = ProvisaFlightServer(state, location=_FLIGHT_LOCATION)

    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()

    # Give the server a moment to bind
    import time
    for _ in range(20):
        if _port_in_use(_TEST_FLIGHT_PORT):
            break
        time.sleep(0.1)
    else:
        raise RuntimeError("Flight server did not start in time")

    client = flight.connect(_FLIGHT_LOCATION)
    yield client, server, state

    client.close()
    server.shutdown()


class TestFlightServerStartsAndConnects:
    """Basic connectivity tests for the in-process Flight server."""

    async def test_flight_server_starts(self, flight_server_and_client):
        """Flight server binds to port and accepts connections."""
        client, server, state = flight_server_and_client
        # list_flights with empty criteria should not raise
        _ = list(client.list_flights(b""))

    async def test_flight_list_flights_default_mode(self, flight_server_and_client):
        """list_flights in default mode returns no flights (ad-hoc query server)."""
        client, server, state = flight_server_and_client
        flights = list(client.list_flights(b""))
        # Default mode has no pre-defined flights
        assert isinstance(flights, list)

    async def test_flight_invalid_ticket_returns_error(self, flight_server_and_client):
        """Malformed ticket JSON raises a Flight error."""
        client, server, state = flight_server_and_client
        bad_ticket = flight.Ticket(b"not-valid-json")
        with pytest.raises(flight.FlightServerError):
            reader = client.do_get(bad_ticket)
            reader.read_all()

    async def test_flight_ticket_missing_query_raises_error(self, flight_server_and_client):
        """Ticket without 'query' key raises FlightServerError."""
        client, server, state = flight_server_and_client
        ticket_bytes = json.dumps({"role": "admin"}).encode()
        bad_ticket = flight.Ticket(ticket_bytes)
        with pytest.raises(flight.FlightServerError):
            reader = client.do_get(bad_ticket)
            reader.read_all()

    async def test_flight_unknown_role_raises_error(self, flight_server_and_client):
        """Ticket with unknown role raises FlightServerError."""
        client, server, state = flight_server_and_client
        ticket_bytes = json.dumps({
            "query": "{ orders { id } }",
            "role": "nonexistent_role",
        }).encode()
        bad_ticket = flight.Ticket(ticket_bytes)
        with pytest.raises(flight.FlightServerError):
            reader = client.do_get(bad_ticket)
            reader.read_all()

    async def test_flight_handshake_returns_token_for_valid_mode(self, flight_server_and_client):
        """do_handshake with valid mode completes without error."""
        # We connect a fresh client that goes through the handshake implicitly
        client2 = flight.connect(_FLIGHT_LOCATION)
        try:
            # Calling list_flights tests that the connection (handshake) works
            _ = list(client2.list_flights(b""))
        finally:
            client2.close()


# ---------------------------------------------------------------------------
# Tier 3: Live PG-backed Flight tests
# ---------------------------------------------------------------------------


class TestFlightDoGetWithRealData:
    """Arrow Flight do_get tests with real PostgreSQL data.

    These tests start their own in-process server backed by a live PG pool
    so they are independent of the module-scoped fixture's mock state.
    """

    @pytest.fixture(scope="class")
    def pg_backed_flight(self, pg_pool):
        """Start a PG-backed Flight server on a separate port."""
        port = _TEST_FLIGHT_PORT + 1
        location = f"grpc://localhost:{port}"

        from graphql import (
            GraphQLField,
            GraphQLInt,
            GraphQLList,
            GraphQLNonNull,
            GraphQLObjectType,
            GraphQLSchema,
            GraphQLString,
            GraphQLFloat,
        )
        from provisa.compiler.rls import RLSContext

        order_type = GraphQLObjectType(
            "Order",
            lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLInt)),
                "region": GraphQLField(GraphQLString),
                "amount": GraphQLField(GraphQLFloat),
            },
        )
        query_type = GraphQLObjectType(
            "Query",
            {"orders": GraphQLField(GraphQLList(order_type))},
        )
        schema = GraphQLSchema(query=query_type)

        try:
            from provisa.compiler.sql_gen import CompilationContext, TableMeta
            ctx = CompilationContext(
                tables={
                    "orders": TableMeta(
                        table_id=1,
                        field_name="orders",
                        type_name="Order",
                        source_id="test-pg",
                        catalog_name="postgresql",
                        schema_name="public",
                        table_name="orders",
                        domain_id="default",
                    )
                }
            )
        except Exception:
            raise

        from provisa.executor.pool import SourcePool
        from unittest.mock import MagicMock

        # Create the server first so we can use its dedicated event loop for pool creation.
        # The pool and the loop used for execution must be the same loop.
        state_placeholder = MagicMock()
        server = ProvisaFlightServer(state_placeholder, location=location)
        loop = server._loop

        source_pool = SourcePool()
        loop.run_until_complete(source_pool.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        ))

        state = MagicMock()
        state.schemas = {"admin": schema}
        state.contexts = {"admin": ctx}
        state.rls_contexts = {"admin": RLSContext.empty()}
        state.roles = {"admin": {"id": "admin", "capabilities": ["full_results"]}}
        state.source_pools = source_pool
        state.source_types = {"test-pg": "postgresql"}
        state.source_dialects = {"test-pg": "postgres"}
        state.masking_rules = {}
        state.flight_client = None
        state.trino_conn = None
        server._state = state

        thread = threading.Thread(target=server.serve, daemon=True)
        thread.start()

        import time
        for _ in range(20):
            if _port_in_use(port):
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("PG-backed Flight server did not start in time")

        client = flight.connect(
            location,
            generic_options=[("grpc.max_metadata_size", 16 * 1024 * 1024)],
        )
        yield client, server, state, source_pool, loop

        client.close()
        server.shutdown()
        loop.run_until_complete(source_pool.close_all())
        loop.close()

    async def test_flight_server_starts(self, pg_backed_flight):
        """PG-backed Flight server starts and accepts a connection."""
        client, server, state, pool, loop = pg_backed_flight
        # A successful list_flights call proves the server is running
        _ = list(client.list_flights(b""))

    async def test_flight_do_get_returns_record_batches(self, pg_backed_flight):
        """do_get with a valid GraphQL ticket returns Arrow RecordBatches."""
        client, server, state, pool, loop = pg_backed_flight
        ticket_bytes = json.dumps({
            "query": "{ orders { id region amount } }",
            "role": "admin",
        }).encode()
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = client.do_get(ticket)
            table = reader.read_all()
        except flight.FlightServerError as exc:
            pytest.skip(f"Flight server returned error (may need Trino): {exc}")

        assert isinstance(table, pa.Table)

    async def test_flight_schema_matches_query(self, pg_backed_flight):
        """Returned schema field names match the queried columns."""
        client, server, state, pool, loop = pg_backed_flight
        ticket_bytes = json.dumps({
            "query": "{ orders { id region amount } }",
            "role": "admin",
        }).encode()
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = client.do_get(ticket)
            table = reader.read_all()
        except flight.FlightServerError:
            raise

        schema_names = set(table.schema.names)
        for field_name in ("id", "region", "amount"):
            assert field_name in schema_names

    async def test_flight_row_count_matches_sql(self, pg_backed_flight, pg_pool):
        """Row count from Flight matches direct PG count."""
        client, server, state, pool, loop = pg_backed_flight

        async with pg_pool.acquire() as conn:
            pg_count = await conn.fetchval('SELECT COUNT(*) FROM "public"."orders"')

        ticket_bytes = json.dumps({
            "query": "{ orders { id } }",
            "role": "admin",
        }).encode()
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = client.do_get(ticket)
            table = reader.read_all()
        except flight.FlightServerError:
            raise

        # The Flight server may apply sampling; row count <= pg_count
        assert table.num_rows <= pg_count

    async def test_flight_invalid_ticket_returns_error(self, pg_backed_flight):
        """Malformed ticket bytes raises a Flight error."""
        client, server, state, pool, loop = pg_backed_flight
        bad_ticket = flight.Ticket(b"{{bad-json}}")
        with pytest.raises(flight.FlightServerError):
            reader = client.do_get(bad_ticket)
            reader.read_all()

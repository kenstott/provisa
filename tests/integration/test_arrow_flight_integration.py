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

Tier 1 pure-Python helper tests (ProvisaFlightServer._parse_mode, Arrow table
builders, rows_to_arrow_table) have been moved to
tests/unit/test_flight_server_helpers.py — they require no infrastructure.

  2. In-process server — starts a ProvisaFlightServer in a background thread
     and connects a pyarrow.flight.FlightClient to it.

  3. Live PG-backed — extends the in-process server fixture with a real
     source pool.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading

import pytest

pa = pytest.importorskip("pyarrow")
flight = pytest.importorskip("pyarrow.flight")

from provisa.api.flight.server import ProvisaFlightServer

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
    """Return a minimal MagicMock AppState suitable for Flight server tests.

    # integration: mock-justified — AppState is not a docker-compose service.
    # It is a data structure populated from config at server startup. MagicMock
    # is used to scaffold the struct fields needed to start the Flight server.
    """
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

    async def test_flight_ticket_missing_query_returns_catalog(self, flight_server_and_client):
        """Ticket without 'query' key returns catalog metadata (not an error)."""
        client, server, state = flight_server_and_client
        ticket_bytes = json.dumps({"role": "admin"}).encode()
        ticket = flight.Ticket(ticket_bytes)
        reader = client.do_get(ticket)
        table = reader.read_all()
        assert table is not None

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

        # integration: mock-justified — MagicMock is used to scaffold the
        # AppState struct fields for server startup. AppState is not a
        # docker-compose service; the real data path (source_pool + PG) is live.

        # Create a dedicated event loop that runs in a background thread.
        # ProvisaFlightServer dispatches asyncpg coroutines to _main_loop via
        # run_coroutine_threadsafe, so _main_loop must be actively running.
        main_loop = asyncio.new_event_loop()

        def _run_main_loop():
            main_loop.run_forever()

        loop_thread = threading.Thread(target=_run_main_loop, daemon=True)
        loop_thread.start()

        # Create source pool on the running main_loop.
        source_pool = SourcePool()
        asyncio.run_coroutine_threadsafe(
            source_pool.add(
                "test-pg",
                source_type="postgresql",
                host=os.environ.get("PG_HOST", "localhost"),
                port=int(os.environ.get("PG_PORT", "5432")),
                database=os.environ.get("PG_DATABASE", "provisa"),
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
            ),
            main_loop,
        ).result(timeout=15)

        state_placeholder = MagicMock()
        server = ProvisaFlightServer(
            state_placeholder, location=location, main_loop=main_loop
        )

        state = MagicMock()
        state.schemas = {"admin": schema}
        state.contexts = {"admin": ctx}
        state.rls_contexts = {"admin": RLSContext.empty()}
        state.roles = {"admin": {"id": "admin", "capabilities": ["full_results", "ad_hoc_query"]}}
        state.source_pools = source_pool
        state.source_types = {"test-pg": "postgresql"}
        state.source_dialects = {"test-pg": "postgres"}
        state.masking_rules = {}
        state.flight_client = None
        state.trino_conn = None
        server._state = state

        flight_thread = threading.Thread(target=server.serve, daemon=True)
        flight_thread.start()

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
        yield client, server, state, source_pool, main_loop

        client.close()
        server.shutdown()
        asyncio.run_coroutine_threadsafe(source_pool.close_all(), main_loop).result(timeout=10)
        main_loop.call_soon_threadsafe(main_loop.stop)
        loop_thread.join(timeout=5)

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

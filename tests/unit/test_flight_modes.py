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
    _physical_type_to_arrow,
)
from provisa.api.flight.server import ProvisaFlightServer
from provisa.api.flight.server import _parse_limit_value


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class FakeState:
    """Minimal AppState substitute for testing."""

    def __init__(self):
        self.org_id = "default"
        self.schemas = {}
        self.contexts = {}
        self.rls_contexts = {}
        self.roles = {}
        self.source_pools = None
        self.source_types = {}
        self.source_dialects = {}
        self.tenant_db = None
        self.model_db = self.tenant_db
        self.engine_conn = None
        self.flight_client = None
        self.rate_limiter: object | None = None
        self.flight_global_cap: int | None = None


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
        assert _physical_type_to_arrow("varchar") == pa.utf8()

    def test_integer(self):
        assert _physical_type_to_arrow("integer") == pa.int32()

    def test_bigint(self):
        assert _physical_type_to_arrow("bigint") == pa.int64()

    def test_boolean(self):
        assert _physical_type_to_arrow("boolean") == pa.bool_()

    def test_double(self):
        assert _physical_type_to_arrow("double") == pa.float64()

    def test_decimal_parameterized(self):
        assert _physical_type_to_arrow("decimal(10,2)") == pa.float64()

    def test_timestamp(self):
        assert _physical_type_to_arrow("timestamp") == pa.timestamp("us")

    def test_unknown_raises(self):
        with pytest.raises(KeyError, match="Unmapped engine type"):
            _physical_type_to_arrow("unknown_fancy_type")


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

    def test_one_endpoint_with_its_ticket_and_no_location(self):
        """An empty location list: redeem on the service that issued it (the advertised port,
        which every worker of a launch accepts on)."""
        table = _make_table()
        info = catalog_table_to_flight_info(table)
        assert len(info.endpoints) == 1
        assert list(info.endpoints[0].locations) == []
        assert json.loads(info.endpoints[0].ticket.ticket.decode()) == {
            "domain": "sales",
            "table": "orders",
        }

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


# ---------------------------------------------------------------------------
# REQ-1905: server-wide Flight concurrency cap
# ---------------------------------------------------------------------------


class _FakeLimiter:
    """Records acquire/release calls; rejects a configured set of keys."""

    def __init__(self, reject_keys=()):
        self._reject_keys = set(reject_keys)
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, key: str, limit: int) -> bool:
        if key in self._reject_keys:
            return False
        self.acquired.append(key)
        return True

    async def release(self, key: str) -> None:
        self.released.append(key)


class TestGlobalFlightConcurrencyCap:
    """The server-wide limit (REQ-1905) is this worker's own stream slots — independent of the
    per-role cap (REQ-369's max_flight_streams, a rate-limiter gauge). Either one alone must be
    able to refuse a request regardless of the other's state. The role's quota is checked at the
    top of do_get and rejects at once; the server-wide slot is taken where execution reaches the
    engine or a source, and is waited for (tests/unit/test_flight_stream_wait.py)."""

    @pytest.fixture(autouse=True)
    def _fresh_slots(self, monkeypatch):
        from provisa.api.flight import stream_slots
        from provisa.core import settings_registry

        monkeypatch.setattr(stream_slots, "_slots", None)
        # Flight's own request timeout (REQ-1905), short: one test waits a slot out.
        monkeypatch.setattr(
            settings_registry,
            "_config",
            {"server": {"limits": {"request_timeouts": {"flight": 0.3}}}},
        )

    def _server(self, state, monkeypatch):
        server = ProvisaFlightServer.__new__(ProvisaFlightServer)
        server._state = state
        monkeypatch.setattr(server, "_execute_query", lambda request: "ok")
        return server

    def test_global_cap_refuses_even_with_no_role_cap_configured(self, monkeypatch):
        """A role with no per-role rate_limit configured is still gated by the global limit."""
        import threading

        state = FakeState()
        state.roles = {"analyst": {}}  # no rate_limit at all
        state.rate_limiter = _FakeLimiter()
        state.flight_global_cap = 1

        server = self._server(state, monkeypatch)
        release = threading.Event()

        def _execute(_request):
            # What every execution path does on a cache miss: wait for a stream slot, then run.
            release_slot = server._acquire_stream_slot()
            try:
                release.wait(10)
                return "ok"
            finally:
                release_slot()

        monkeypatch.setattr(server, "_execute_query", _execute)
        ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())
        holder = threading.Thread(target=lambda: server.do_get(None, ticket))
        holder.start()
        try:
            import time

            time.sleep(0.1)  # the holder has the only slot; this request waits out its budget
            with pytest.raises(flight.FlightServerError, match="server-wide"):
                server.do_get(None, ticket)
        finally:
            release.set()
            holder.join(timeout=10)

    def test_global_cap_has_room_role_cap_still_enforced(self, monkeypatch):
        """The global gate admitting a request does not bypass the existing per-role gate."""
        state = FakeState()
        state.roles = {"analyst": {"rate_limit": {"max_flight_streams": 1}}}
        state.rate_limiter = _FakeLimiter(reject_keys={"rl:flight:analyst"})
        state.flight_global_cap = 1

        server = self._server(state, monkeypatch)
        ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())

        with pytest.raises(
            flight.FlightServerError, match="max concurrent Arrow Flight streams reached$"
        ):
            server.do_get(None, ticket)

        # The one global slot was given back even though the per-role check failed after it: a
        # role with no cap is admitted at once rather than waiting out its budget.
        state.roles = {"analyst": {}}
        assert server.do_get(None, ticket) == "ok"
        assert state.rate_limiter.acquired == []

    def test_both_caps_have_room_query_executes_and_both_slots_released(self, monkeypatch):
        state = FakeState()
        state.roles = {"analyst": {"rate_limit": {"max_flight_streams": 4}}}
        state.rate_limiter = _FakeLimiter()
        state.flight_global_cap = 1

        server = self._server(state, monkeypatch)
        ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())

        assert server.do_get(None, ticket) == "ok"
        assert server.do_get(None, ticket) == "ok"  # the single global slot was released

        # The server-wide limit is not a rate-limiter gauge any more: only the role's key is.
        assert state.rate_limiter.acquired == ["rl:flight:analyst", "rl:flight:analyst"]
        assert state.rate_limiter.released == ["rl:flight:analyst", "rl:flight:analyst"]

    def test_no_global_cap_configured_skips_the_gate(self, monkeypatch):
        """flight_global_cap unset/0 (e.g. NoopRateLimiter deployments) never calls acquire."""
        state = FakeState()
        state.roles = {"analyst": {}}
        state.rate_limiter = _FakeLimiter()
        state.flight_global_cap = None

        server = self._server(state, monkeypatch)
        ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())

        result = server.do_get(None, ticket)

        assert result == "ok"
        assert state.rate_limiter.acquired == []

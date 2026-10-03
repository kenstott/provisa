# Copyright (c) 2026 Kenneth Stott
# Canary: d6658e40-4c85-4d92-b1f2-75faf0f54b2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for ProvisaFlightServer pure-Python helpers and rows→Arrow conversion.

Moved from tests/integration/test_arrow_flight_integration.py Tier 1 section.
These tests require no running infrastructure.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pa = pytest.importorskip("pyarrow")

from provisa.api.flight import server as flight_server  # noqa: E402
from provisa.api.flight.server import ProvisaFlightServer  # noqa: E402
from provisa.cache.store import NoopCacheStore  # noqa: E402
from provisa.executor.formats.arrow import rows_to_arrow_table  # noqa: E402
from provisa.executor.result import QueryResult  # noqa: E402
from provisa.compiler.sql_gen import ColumnRef  # noqa: E402
from provisa.pgwire import _pipeline  # noqa: E402
from provisa.transpiler.router import Route  # noqa: E402

# asyncio_mode = "auto" (pyproject.toml) already collects the async def tests below;
# an explicit pytestmark here would double-mark the sync test classes too.


class TestFlightServerBuildCatalogTable:
    """Unit tests for the Arrow table builders (no server needed)."""

    async def test_build_catalog_table_produces_correct_schema(self):
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
        from provisa.executor.formats.arrow import _column_names

        columns = [
            ColumnRef(alias=None, column="name", field_name="name", nested_in="customer"),
        ]
        names = _column_names(columns)
        assert names == ["customer.name"]

    async def test_decimal_converted_to_float(self):
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


class TestFlightSqlDispatchHopCount:
    """REQ-1887: _do_get_sql_governed collapses sequential _run_on_loop dispatches with no real
    ordering dependency between them into fewer, folded coroutines. These tests spy on
    ``_run_on_loop`` and assert the post-fix call count directly, rather than just asserting the
    request still succeeds — that's the only way to prove the hop count actually dropped."""

    @staticmethod
    def _server() -> ProvisaFlightServer:
        """A server instance without binding a port — __init__ would open a listener."""
        srv = ProvisaFlightServer.__new__(ProvisaFlightServer)
        srv._state = SimpleNamespace(
            federation_engine=None,
            source_pools=None,
            roles={},
            response_cache_store=NoopCacheStore(),
        )  # pyright: ignore[reportAttributeAccessIssue]
        return srv

    @staticmethod
    def _counting_run_on_loop(monkeypatch, srv) -> list:
        """Replace ``srv._run_on_loop`` with a spy that still runs the coroutine (via a fresh
        event loop per call — nothing here depends on a shared loop) so downstream code sees a
        real result, while recording how many times the dispatch boundary was crossed."""
        import asyncio

        calls: list = []

        def _fake(coro, *, timeout=None):  # noqa: ARG001  # pyright: ignore[reportUnusedVariable]  # timeout mirrors the real signature
            calls.append(coro)
            return asyncio.run(coro)

        monkeypatch.setattr(srv, "_run_on_loop", _fake)
        return calls

    def test_direct_route_function_invocation_is_one_hop(self, monkeypatch):
        """A `SELECT fn(...)` ticket short-circuits through govern_batch_final_plan_with_fn:
        ONE dispatch (function check + governance folded), where it used to be two separate
        _run_on_loop calls (maybe_invoke_registered_function, then govern_batch_final_plan)."""
        srv = self._server()
        calls = self._counting_run_on_loop(monkeypatch, srv)

        fn_result = QueryResult(rows=[(1,)], column_names=["id"])
        monkeypatch.setattr(
            _pipeline, "govern_batch_final_plan_with_fn", AsyncMock(return_value=fn_result)
        )

        stream = srv._do_get_sql_governed({"query": "SELECT tracked_fn(1)", "role": "org_admin"})

        assert len(calls) == 1  # was 2 before REQ-1887
        assert isinstance(stream, pa.flight.RecordBatchStream)

    def test_direct_route_native_execute_is_three_hops(self, monkeypatch):
        """A governed DIRECT-route plan that isn't a registered-function call: govern (1, folds
        the former function-invocation hop) + execute_native (1) + finalize_audit (1) = 3 hops,
        where it used to be 4 (a separate function-invocation hop before governance)."""
        srv = self._server()
        calls = self._counting_run_on_loop(monkeypatch, srv)

        plan = SimpleNamespace(
            warnings=[],  # REQ-1350: nothing to say
            route=Route.DIRECT,
            source_id="src1",
            sql="SELECT 1",
            exec_params=None,
            stamp="governed",
            response_cacheable=False,  # REQ-1897: the cache read folds into execute's dispatch
            cache_opt_in=False,
            cache_hit=None,  # as _Plan: not answered before routing
            cache_missed=(),
            live_caps=(),  # REQ-1909: the capped live sources the pipeline binds at mint (none here)
            live_caps_org=None,
            tier_caps=None,
            tier_plan=None,
        )
        monkeypatch.setattr(
            _pipeline, "govern_batch_final_plan_with_fn", AsyncMock(return_value=plan)
        )
        monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _p: None)
        monkeypatch.setattr(_pipeline, "finalize_audit", AsyncMock(return_value=None))

        source_pools = MagicMock()
        source_pools.has.return_value = False  # forces the non-streaming execute_native branch
        srv._state.source_pools = source_pools
        from provisa.executor.result import QueryResult

        result = QueryResult(rows=[(1,)], column_names=["id"])  # execute_native's real type
        srv._state.federation_engine = SimpleNamespace(
            execute_native=AsyncMock(return_value=result)
        )

        stream = srv._do_get_sql_governed({"query": "SELECT id FROM t", "role": "org_admin"})

        assert len(calls) == 3  # was 4 before REQ-1887
        assert isinstance(stream, pa.flight.RecordBatchStream)

    def test_engine_route_residency_is_three_hops(self, monkeypatch):
        """A governed ENGINE-route plan: govern (1) + residency (1, folds ensure_rows_resident +
        pushdown_row_materialize + ensure_resident) + finalize_audit (1) = 3 hops, where it used
        to be 6 (function-invocation + govern + 3 separate residency calls + finalize_audit)."""
        srv = self._server()
        calls = self._counting_run_on_loop(monkeypatch, srv)

        plan = SimpleNamespace(
            warnings=[],  # REQ-1350: nothing to say
            route=Route.ENGINE,
            physical_sql="SELECT 1",
            sources=["src1"],
            pk_bounds=[],
            exec_params=None,
            stamp="governed",
            audit_deferred=None,  # as _Plan: no audit record held back for the drain
            live_caps=(),  # REQ-1909: the capped live sources the pipeline binds at mint (none here)
            live_caps_org=None,
            tier_caps=None,
            tier_plan=None,
        )
        monkeypatch.setattr(
            _pipeline, "govern_batch_final_plan_with_fn", AsyncMock(return_value=plan)
        )
        monkeypatch.setattr(_pipeline, "require_governed_plan", lambda _p: None)
        monkeypatch.setattr(_pipeline, "finalize_audit", AsyncMock(return_value=None))
        monkeypatch.setattr(
            flight_server, "_prepare_engine_residency", AsyncMock(return_value=None)
        )
        # REQ-1897: a cache MISS is checked inside the SAME residency dispatch (no extra hop), and
        # this plan has nothing to write through.
        monkeypatch.setattr(_pipeline, "check_response_cache_arrow", AsyncMock(return_value=None))
        monkeypatch.setattr(_pipeline, "response_cache_tee", lambda *_a, **_k: None)

        empty_gen = iter(())
        srv._state.federation_engine = SimpleNamespace(
            execute_engine_stream=MagicMock(return_value=(pa.schema([]), empty_gen)),
        )

        stream = srv._do_get_sql_governed({"query": "SELECT id FROM t", "role": "org_admin"})

        assert len(calls) == 3  # was 6 before REQ-1887
        assert isinstance(stream, pa.flight.GeneratorStream)


class TestFlightErrorTruncation:
    """A FlightServerError message is propagated as gRPC trailing metadata, which grpc rejects
    outright above its default 16KB max_metadata_size (surfacing as an opaque RESOURCE_EXHAUSTED
    error instead of the real message). ``_flight_error`` caps the message well under that limit."""

    def test_short_message_passes_through_unmodified(self):
        err = flight_server._flight_error("boom")
        assert str(err) == "boom"

    def test_long_message_is_truncated_with_marker(self):
        msg = "x" * 20000
        err = flight_server._flight_error(msg)
        text = str(err)
        assert len(text) <= flight_server._FLIGHT_ERROR_MAX_LEN + len("...(truncated)")
        assert text.endswith("...(truncated)")

    def test_cause_is_chained(self):
        cause = ValueError("original")
        err = flight_server._flight_error("wrapped", cause)
        assert err.__cause__ is cause

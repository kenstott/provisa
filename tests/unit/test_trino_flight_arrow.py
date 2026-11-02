# Copyright (c) 2026 Kenneth Stott
# Canary: 536ac792-e309-4e7a-a70b-126bdc6cfb35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: Trino reads as Arrow through the Zaychik proxy, one test class per gap the proxy had
(docs/arch/fetch-paths.md §4).

The proxy's side is tested in Java (zaychik/trino-statement-protocol.patch). Here a loopback
Flight server stands in for it and speaks what the patched proxy speaks -- field metadata, the
context call header, error trailers -- to the real ADBC Flight SQL driver, so what is tested is
what Provisa reads off the wire. Expected row values are the Trino Python client's own for the
same protocol value (trino.mapper), the reader the Flight path replaces.
"""

from __future__ import annotations

import base64
import json
import math
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.flight as fl
import pytest

import provisa.executor.trino as trino_policy
import provisa.executor.trino_flight as trf
from provisa.executor.errors import FederationError, SystemCatalogUnavailable
from provisa.executor.result import ArrowResult, ArrowStreamResult

# -- the proxy's field metadata ------------------------------------------------------------------


def _long(v: int) -> dict:
    return {"kind": "LONG", "value": v}


def _type(raw: str, *args: dict) -> dict:
    return {"rawType": raw, "arguments": list(args)}


def _named(name: str | None, sig: dict) -> dict:
    value: dict[str, Any] = {"typeSignature": sig}
    if name is not None:
        value["fieldName"] = {"name": name}
    return {"kind": "NAMED_TYPE", "value": value}


def _field(name: str, arrow: pa.DataType, trino_type: str, sig: dict, encoding: str) -> pa.Field:
    return pa.field(
        name,
        arrow,
        metadata={
            "trino.type": trino_type,
            "trino.type_signature": json.dumps(sig),
            "trino.encoding": encoding,
        },
    )


_VARCHAR = _type("varchar", _long(2147483647))

#: (Trino type, signature, Arrow type, encoding, the value as the proxy writes it, the value as
#: the Trino protocol carries it -- what the Trino Python client maps).
_CORPUS: list[tuple[str, dict, pa.DataType, str, Any, Any]] = [
    ("boolean", _type("boolean"), pa.bool_(), "arrow", True, True),
    ("tinyint", _type("tinyint"), pa.int8(), "arrow", -7, -7),
    ("smallint", _type("smallint"), pa.int16(), "arrow", 300, 300),
    ("integer", _type("integer"), pa.int32(), "arrow", 70000, 70000),
    ("bigint", _type("bigint"), pa.int64(), "arrow", 2**40, 2**40),
    ("real", _type("real"), pa.float32(), "arrow", 0.1, 0.1),
    ("double", _type("double"), pa.float64(), "arrow", 1 / 3, 1 / 3),
    (
        "decimal(10,2)",
        _type("decimal", _long(10), _long(2)),
        pa.decimal128(10, 2),
        "arrow",
        Decimal("1.50"),
        "1.50",
    ),
    ("varchar", _VARCHAR, pa.string(), "arrow", "héllo", "héllo"),
    ("char(3)", _type("char", _long(3)), pa.string(), "arrow", "ab ", "ab "),
    ("varbinary", _type("varbinary"), pa.binary(), "arrow", b"\x01\x02", "AQI="),
    ("date", _type("date"), pa.date32(), "arrow", date(1000, 1, 1), "1000-01-01"),
    (
        "time(6)",
        _type("time", _long(6)),
        pa.time64("us"),
        "arrow",
        time(1, 2, 3, 123456),
        "01:02:03.123456",
    ),
    (
        "timestamp(6)",
        _type("timestamp", _long(6)),
        pa.timestamp("us"),
        "arrow",
        datetime(2024, 1, 2, 3, 4, 5, 123456),
        "2024-01-02 03:04:05.123456",
    ),
    (
        "timestamp(3) with time zone",
        _type("timestamp with time zone", _long(3)),
        pa.timestamp("us", tz="UTC"),
        "arrow",
        datetime(2024, 1, 2, 1, 4, 5, 123000, tzinfo=timezone.utc),
        "2024-01-02 03:04:05.123 +02:00",
    ),
    (
        "timestamp(6) with time zone",
        _type("timestamp with time zone", _long(6)),
        pa.timestamp("us", tz="UTC"),
        "arrow",
        datetime(2024, 3, 10, 6, 30, tzinfo=timezone.utc),
        "2024-03-10 01:30:00.000000 America/New_York",
    ),
    (
        "time(3) with time zone",
        _type("time with time zone", _long(3)),
        pa.string(),
        "text",
        "03:04:05.123+02:00",
        "03:04:05.123+02:00",
    ),
    ("json", _type("json"), pa.string(), "text", '{"a":1}', '{"a":1}'),
    (
        "uuid",
        _type("uuid"),
        pa.string(),
        "text",
        "12151fd2-7586-11e9-8f9e-2a86e4085a59",
        "12151fd2-7586-11e9-8f9e-2a86e4085a59",
    ),
    ("ipaddress", _type("ipaddress"), pa.string(), "text", "10.0.0.1", "10.0.0.1"),
    (
        "interval day to second",
        _type("interval day to second"),
        pa.string(),
        "text",
        "1 02:03:04.005",
        "1 02:03:04.005",
    ),
    ("interval year to month", _type("interval year to month"), pa.string(), "text", "1-2", "1-2"),
    (
        "array(integer)",
        _type("array", {"kind": "TYPE", "value": _type("integer")}),
        pa.string(),
        "json",
        "[1,null,3]",
        [1, None, 3],
    ),
    (
        "map(varchar, double)",
        _type(
            "map", {"kind": "TYPE", "value": _VARCHAR}, {"kind": "TYPE", "value": _type("double")}
        ),
        pa.string(),
        "json",
        '{"a":0.5,"b":"NaN"}',
        {"a": 0.5, "b": "NaN"},
    ),
    (
        "row(a bigint, varbinary)",
        _type("row", _named("a", _type("bigint")), _named(None, _type("varbinary"))),
        pa.string(),
        "json",
        '[7,"AQID"]',
        [7, "AQID"],
    ),
    (
        "array(timestamp(9))",
        _type("array", {"kind": "TYPE", "value": _type("timestamp", _long(9))}),
        pa.string(),
        "json",
        '["2024-01-02 03:04:05.123456500"]',
        ["2024-01-02 03:04:05.123456500"],
    ),
    ("unknown", _type("unknown"), pa.null(), "arrow", None, None),
]


def _corpus_table() -> pa.Table:
    fields = [_field(f"c{i}", c[2], c[0], c[1], c[3]) for i, c in enumerate(_CORPUS)]
    arrays = [pa.array([c[4], None], type=c[2]) for c in _CORPUS]
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _trino_client_values() -> list[Any]:
    from trino.mapper import RowMapperFactory

    mapper = RowMapperFactory().create(
        columns=[{"typeSignature": c[1]} for c in _CORPUS], legacy_primitive_types=False
    )
    return mapper.map([[c[5] for c in _CORPUS]])[0]


def _same(ours: Any, theirs: Any) -> bool:
    if isinstance(theirs, float) and isinstance(ours, float):
        if math.isnan(theirs):
            return math.isnan(ours)
        return ours == theirs or abs(ours - theirs) <= 1e-14 * max(abs(ours), abs(theirs))
    if isinstance(theirs, dict):
        return ours.keys() == theirs.keys() and all(_same(ours[k], theirs[k]) for k in theirs)
    if isinstance(theirs, (list, tuple)):
        return len(ours) == len(theirs) and all(_same(a, b) for a, b in zip(ours, theirs))
    return type(ours) is type(theirs) and ours == theirs


# -- a stand-in for the patched proxy ---------------------------------------------------------


class _Calls(fl.ServerMiddleware):
    def __init__(self, headers: dict) -> None:
        self.headers = headers
        self.trailers: dict[str, Any] = {}

    def sending_headers(self) -> dict:
        return self.trailers


class _CallsFactory(fl.ServerMiddlewareFactory):
    def __init__(self) -> None:
        self.contexts: list[dict] = []

    def start_call(self, info, headers):
        if str(info.method) == "FlightMethod.GET_FLIGHT_INFO":
            for value in headers.get(trf.CONTEXT_HEADER, []):
                self.contexts.append(json.loads(base64.b64decode(value)))
        return _Calls(headers)


class _FakeZaychik(fl.FlightServerBase):
    """Answers every statement with ``table``, or with the failures queued in ``failures``
    (each a dict of trailers, or ``"unavailable"``), one per statement, first."""

    def __init__(self, table: pa.Table, calls: _CallsFactory) -> None:
        super().__init__("grpc://127.0.0.1:0", middleware={"calls": calls})
        self.table = table
        self.failures: list[Any] = []
        self.statements = 0

    def get_flight_info(self, context, descriptor):
        if b"CommandStatementQuery" not in descriptor.command:
            # The driver's own probes at connect (SqlInfo): the proxy's answers are not under test.
            raise fl.FlightUnimplementedError("not a statement")
        self.statements += 1
        if self.failures:
            failure = self.failures.pop(0)
            if failure == "unavailable":
                context.get_middleware("calls").trailers = {
                    "x-trino-error-kind": "connection",
                    "x-trino-error-message-bin": b"Failed to connect to trino:8080",
                }
                raise fl.FlightUnavailableError("Failed to connect to trino:8080")
            context.get_middleware("calls").trailers = failure
            raise fl.FlightServerError(failure["x-trino-error-message-bin"].decode())
        endpoint = fl.FlightEndpoint(b"rows", [])
        return fl.FlightInfo(self.table.schema, descriptor, [endpoint], -1, -1)

    def do_get(self, context, ticket):
        return fl.RecordBatchStream(self.table)


def _connect(port: int):
    """The ADBC Flight SQL connection create_flight_connection builds, without its basic-auth
    handshake (the stand-in has no authentication; the read path does not depend on it)."""
    import adbc_driver_flightsql.dbapi as flight_sql

    return flight_sql.connect(uri=f"grpc://127.0.0.1:{port}", autocommit=True)


@pytest.fixture
def zaychik():
    calls = _CallsFactory()
    server = _FakeZaychik(_corpus_table(), calls)
    threading.Thread(target=server.serve, daemon=True).start()
    conn = _connect(server.port)
    server.calls = calls
    try:
        yield server, conn
    finally:
        conn.close()
        server.shutdown()


def _trino_error(name: str, error_type: str, message: str, code: int = 1) -> dict:
    return {
        "x-trino-error-kind": "query",
        "x-trino-error-name": name,
        "x-trino-error-type": error_type,
        "x-trino-error-code": str(code),
        "x-trino-query-id": "20261102_000000_00001_aaaaa",
        "x-trino-error-message-bin": message.encode("utf-8"),
    }


_CONTEXT = trf.TrinoContext(
    source="provisa/org-1",
    catalog="system",
    schema="org_1",
    properties={"query_max_execution_time": "120s", "retry_policy": "TASK"},
)


@pytest.fixture
def no_wait(monkeypatch):
    monkeypatch.setattr(trino_policy, "_retry_budget", lambda: 30.0)
    monkeypatch.setattr(trino_policy, "_backoff_secs", lambda attempt, cap=30.0: 0.0)


# -- gaps 1, 2 and 4: values and types ---------------------------------------------------------


class TestValuesAreTheTrinoClients:
    """Gap 1 (microseconds), gap 2 (types JDBC could not map), gap 4 (Trino's type names)."""

    def test_every_type_reads_as_the_trino_client_reads_it(self):
        ours = trf.trino_rows(_corpus_table())
        theirs = _trino_client_values()
        for i, (trino_type, *_rest) in enumerate(_CORPUS):
            assert _same(ours[0][i], theirs[i]), (trino_type, ours[0][i], theirs[i])
            assert ours[1][i] is None, trino_type

    def test_real_is_the_shortest_decimal_not_the_widened_float(self):
        (row,) = trf.trino_rows(
            pa.Table.from_arrays(
                [pa.array([0.1], pa.float32())],
                schema=pa.schema([_field("r", pa.float32(), "real", _type("real"), "arrow")]),
            )
        )
        assert row == (0.1,)

    def test_microseconds_survive(self):
        (row,) = trf.trino_rows(_corpus_table().slice(0, 1))
        index = [c[0] for c in _CORPUS].index("timestamp(6)")
        assert row[index].microsecond == 123456

    def test_an_instant_is_at_utc(self):
        (row,) = trf.trino_rows(_corpus_table().slice(0, 1))
        index = [c[0] for c in _CORPUS].index("timestamp(6) with time zone")
        assert row[index].tzinfo is timezone.utc
        assert row[index] == datetime(2024, 3, 10, 1, 30, tzinfo=ZoneInfo("America/New_York"))

    def test_structural_values_are_the_trino_clients_types(self):
        (row,) = trf.trino_rows(_corpus_table().slice(0, 1))
        names = [c[0] for c in _CORPUS]
        assert row[names.index("row(a bigint, varbinary)")] == (7, b"\x01\x02\x03")
        assert row[names.index("row(a bigint, varbinary)")].a == 7
        assert row[names.index("uuid")] == uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59")
        assert row[names.index("interval day to second")] == timedelta(
            days=1, hours=2, minutes=3, seconds=4, milliseconds=5
        )
        assert row[names.index("array(timestamp(9))")] == [datetime(2024, 1, 2, 3, 4, 5, 123456)]

    def test_column_types_are_trinos_names(self):
        assert trf.column_types(_corpus_table().schema) == [c[0] for c in _CORPUS]

    def test_read_through_the_driver(self, zaychik):
        server, conn = zaychik
        result = trf.execute_trino_flight(conn, "SELECT *", context=_CONTEXT)
        assert isinstance(result, ArrowResult)
        assert result.column_types == [c[0] for c in _CORPUS]
        assert _same(result.rows, trf.trino_rows(_corpus_table()))
        assert next(iter(result.arrow_batches())).num_rows == 2

    def test_streamed_read_through_the_driver(self, zaychik):
        server, conn = zaychik
        result = trf.execute_trino_flight_sync(conn, "SELECT *", context=_CONTEXT)
        assert isinstance(result, ArrowStreamResult)
        assert result.column_types == [c[0] for c in _CORPUS]
        assert _same(result.rows, trf.trino_rows(_corpus_table()))


# -- gap 3: session properties ------------------------------------------------------------------


class TestSessionReachesTrino:
    def test_header_carries_source_catalog_schema_and_properties(self):
        decoded = json.loads(base64.b64decode(_CONTEXT.header()))
        assert decoded == {
            "source": "provisa/org-1",
            "catalog": "system",
            "schema": "org_1",
            "properties": {"query_max_execution_time": "120s", "retry_policy": "TASK"},
        }

    def test_the_proxy_receives_the_context_with_the_statement(self, zaychik):
        server, conn = zaychik
        trf.execute_trino_flight(conn, "SELECT *", context=_CONTEXT)
        assert server.calls.contexts == [json.loads(base64.b64decode(_CONTEXT.header()))]

    def test_backend_session_merges_fte_hints_caller_hints_and_the_timeout(self, monkeypatch):
        from provisa.federation.backend import TrinoBackend
        from provisa.federation.engine import build_trino_engine

        monkeypatch.setattr(trino_policy, "_trino_query_timeout", lambda: 120)
        backend = TrinoBackend(build_trino_engine())
        state = SimpleNamespace(
            engine_conn_kwargs={"source": "provisa/org-1", "catalog": "system", "schema": "o1"},
            engine_session_hints={"retry_policy": "TASK", "join_distribution_type": "AUTOMATIC"},
        )
        context = backend._flight_context(state, {"join_distribution_type": "BROADCAST"}, None)
        assert context == trf.TrinoContext(
            source="provisa/org-1",
            catalog="system",
            schema="o1",
            properties={
                "retry_policy": "TASK",
                "join_distribution_type": "BROADCAST",
                "query_max_execution_time": "120s",
            },
        )
        # A caller's own timeout is kept.
        assert (
            backend._flight_context(state, {"query_max_execution_time": "5s"}, None).properties[
                "query_max_execution_time"
            ]
            == "5s"
        )


# -- gap 5: errors ------------------------------------------------------------------------------


class TestErrorsAreTrinos:
    def test_a_failed_statement_is_a_federation_error_by_trinos_name(self, zaychik):
        server, conn = zaychik
        server.failures.append(
            _trino_error("SYNTAX_ERROR", "USER_ERROR", "line 1:8: mismatched input 'FROMM'")
        )
        with pytest.raises(FederationError) as raised:
            trf.execute_trino_flight(conn, "SELECT x FROMM t", context=_CONTEXT)
        err = raised.value
        assert (err.error_name, err.error_type) == ("SYNTAX_ERROR", "USER_ERROR")
        assert err.message == "line 1:8: mismatched input 'FROMM'"
        assert err.query_id == "20261102_000000_00001_aaaaa"
        assert server.statements == 1  # a user error is not retried

    def test_a_missing_system_catalog_is_told_apart(self, zaychik, no_wait, monkeypatch):
        monkeypatch.setattr(trino_policy, "_retry_budget", lambda: 0.0)
        server, conn = zaychik
        server.failures.append(
            _trino_error(
                "CATALOG_NOT_FOUND", "USER_ERROR", "line 1:15: Catalog 'provisa_admin' not found"
            )
        )
        with pytest.raises(SystemCatalogUnavailable):
            trf.execute_trino_flight(conn, "SELECT 1", context=_CONTEXT)

    def test_coordinator_loss_is_retried(self, zaychik, no_wait):
        server, conn = zaychik
        server.failures.append(
            _trino_error(
                "SERVER_STARTING_UP", "INTERNAL_ERROR", "Trino server is still initializing"
            )
        )
        server.failures.append("unavailable")
        result = trf.execute_trino_flight(conn, "SELECT *", context=_CONTEXT)
        assert server.statements == 3
        assert _same(result.rows, trf.trino_rows(_corpus_table()))

    def test_memory_errors_are_never_retried(self, zaychik, no_wait):
        server, conn = zaychik
        server.failures.append(
            _trino_error(
                "EXCEEDED_LOCAL_MEMORY_LIMIT",
                "INSUFFICIENT_RESOURCES",
                "Query exceeded per-node memory limit of 1GB",
            )
        )
        with pytest.raises(MemoryError, match="Query exceeded Trino memory limit"):
            trf.execute_trino_flight(conn, "SELECT *", context=_CONTEXT)
        assert server.statements == 1

    def test_an_unreachable_coordinator_is_a_connection_error(self, zaychik, monkeypatch):
        monkeypatch.setattr(trino_policy, "_retry_budget", lambda: 0.0)
        server, conn = zaychik
        server.failures.append("unavailable")
        with pytest.raises(trf.EngineUnreachable) as raised:
            trf.execute_trino_flight(conn, "SELECT *", context=_CONTEXT)
        from provisa.federation import trino_lifecycle

        assert trino_lifecycle.classify_error(raised.value) == "connection"
        assert trino_policy.is_connection_error(raised.value)
        assert str(raised.value) == "Failed to connect to trino:8080"

    def test_an_unreachable_proxy_is_a_connection_error(self, zaychik, monkeypatch):
        monkeypatch.setattr(trino_policy, "_retry_budget", lambda: 0.0)
        server, conn = zaychik
        server.shutdown()  # the proxy goes away under a connection made while it was up
        with pytest.raises(trf.EngineUnreachable):
            trf.execute_trino_flight(conn, "SELECT 1", context=_CONTEXT)


# -- gap 6: parameters ---------------------------------------------------------------------------


_PARAMS: list[Any] = [
    None,
    True,
    False,
    0,
    -(2**40),
    1.5,
    float("inf"),
    float("-inf"),
    float("nan"),
    "o'brien",
    b"\x00\xff",
    date(2024, 1, 2),
    datetime(2024, 1, 2, 3, 4, 5, 6),
    datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=ZoneInfo("America/New_York")),
    datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    time(3, 4, 5, 6),
    time(3, 4, 5, 6, tzinfo=timezone(timedelta(hours=-8))),
    Decimal("12345.678900"),
    Decimal("1E-7"),
    uuid.UUID("12151fd2-7586-11e9-8f9e-2a86e4085a59"),
    [1, "a", None],
    (1, "a"),
    {"k": 1.5},
]


class TestParametersBindAsTheTrinoClientBindsThem:
    @pytest.mark.parametrize("param", _PARAMS, ids=repr)
    def test_each_literal_is_the_trino_clients(self, param):
        from trino.dbapi import Cursor

        class _Formatter:
            def _format_prepared_param(self, p):
                return Cursor._format_prepared_param(self, p)  # type: ignore[arg-type]

        assert trf.render_param(param) == _Formatter()._format_prepared_param(param)

    def test_dates_decimals_and_bytes_are_typed_literals(self):
        assert trf.render_param(date(2024, 1, 2)) == "DATE '2024-01-02'"
        assert trf.render_param(Decimal("1E-7")) == "DECIMAL '0.0000001'"
        assert trf.render_param(b"\x00\xff") == "X'00ff'"
        assert trf.render_param(datetime(2024, 1, 2, 3, 4, 5)) == (
            "TIMESTAMP '2024-01-02 03:04:05.000000'"
        )

    def test_values_are_bound_by_execute_immediate_in_placeholder_order(self):
        assert trf.bound_statement(
            "SELECT * FROM t WHERE b = $2 AND a = $1 OR c = $2", [1, "x"]
        ) == ("EXECUTE IMMEDIATE 'SELECT * FROM t WHERE b = ? AND a = ? OR c = ?' USING 'x',1,'x'")

    def test_embedded_params_comment_supplies_the_values(self):
        sql = "-- provisa-params: $1='2024-01-02' $2=7\nSELECT * FROM t WHERE d = $1 AND n = $2"
        assert trf.bound_statement(sql, None) == (
            "EXECUTE IMMEDIATE 'SELECT * FROM t WHERE d = ? AND n = ?' USING '2024-01-02',7"
        )

    def test_an_unsupported_value_is_refused(self):
        with pytest.raises(TypeError, match="not supported"):
            trf.render_param(object())


# -- the backend reads through the proxy only ---------------------------------------------------


class TestBackendReadsThroughTheProxy:
    def _backend(self):
        from provisa.federation.backend import TrinoBackend
        from provisa.federation.engine import build_trino_engine

        return TrinoBackend(build_trino_engine())

    def _state(self, **overrides):
        state = SimpleNamespace(
            engine_conn=None,
            engine_conn_kwargs={"source": "provisa/org-1", "catalog": "system", "schema": "o1"},
            engine_session_hints={},
            active_engine_endpoint=None,
            active_isolated_org=None,
            flight_client=object(),
        )
        for k, v in overrides.items():
            setattr(state, k, v)
        return state

    def test_an_org_on_its_own_coordinator_has_no_transport(self):
        backend = self._backend()
        with pytest.raises(RuntimeError, match="no Arrow Flight transport"):
            backend.execute_sync(
                self._state(active_engine_endpoint=("warehouse", 8080)), "SELECT 1"
            )
        with pytest.raises(RuntimeError, match="no Arrow Flight transport"):
            backend.execute_sync(self._state(active_isolated_org="org-9"), "SELECT 1")

    def test_an_unbound_terminal_is_refused(self):
        backend = self._backend()
        with pytest.raises(RuntimeError, match="connection not available"):
            backend.execute_sync(self._state(engine_conn_kwargs=None), "SELECT 1")

    @pytest.mark.asyncio
    async def test_execute_reads_through_the_proxy(self, zaychik, monkeypatch):
        monkeypatch.setattr(trino_policy, "_trino_query_timeout", lambda: 120)
        server, conn = zaychik
        backend = self._backend()
        result = await backend.execute(self._state(flight_client=conn), "SELECT *")
        assert isinstance(result, ArrowResult)
        assert _same(result.rows, trf.trino_rows(_corpus_table()))
        (context,) = server.calls.contexts
        assert context["source"] == "provisa/org-1"
        assert context["properties"] == {"query_max_execution_time": "120s"}

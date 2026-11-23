# Copyright (c) 2026 Kenneth Stott
# Canary: b7b6b243-e265-4b22-9cb2-f5753c486a7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark data source's pyspark-free parts (REQ-2270): its options, the Spark type of each
declared Arrow type, the Flight SQL statement it sends, and its calls against Provisa's own Flight
server on a loopback port (the governed pipeline stubbed to show what it is asked, as in
test_flightsql_statement). The read through a real Spark is tests/integration/test_spark_reads_e2e.py.
"""

# Requirements: REQ-2270

from __future__ import annotations

import sys
import types
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as fl
import pytest

# The package is its own distribution (provisa-pyspark/), not installed into Provisa's venv.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "provisa-pyspark"))

from provisa_pyspark import flight  # noqa: E402
from provisa_pyspark.options import OptionError, ReadOptions, table_parts  # noqa: E402
from provisa_pyspark.spark_types import (  # noqa: E402
    UnmappedType,
    spark_ddl,
    spark_simple,
    spark_type,
)

from provisa.api.flight import flightsql  # noqa: E402
from provisa.api.flight.server import ProvisaFlightServer  # noqa: E402

_URI = "grpc://127.0.0.1:8815"


# --- options ----------------------------------------------------------------------------------


def test_load_path_names_the_table_and_the_credential_rides_the_headers():
    read = ReadOptions.parse(
        {"uri": _URI, "token": "pat-1", "role": "clerk", "path": "sales.orders"}
    )
    assert read.table == ("sales", "orders")
    assert read.statement() == 'SELECT * FROM "sales"."orders"'
    assert read.headers() == [(b"authorization", b"Bearer pat-1"), (b"x-provisa-role", b"clerk")]


def test_the_table_option_names_the_table_too():
    assert ReadOptions.parse({"uri": _URI, "table": "orders"}).table == ("orders",)


def test_option_names_are_case_insensitive_as_spark_options_are():
    read = ReadOptions.parse({"URI": _URI, "Token": "t", "PATH": "orders"})
    assert (read.uri, read.token) == (_URI, "t")


def test_without_a_token_no_credential_is_sent():
    assert ReadOptions.parse({"uri": _URI, "path": "orders"}).headers() == []


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"path": "orders"}, "uri is required"),
        ({"uri": "http://h:8815", "path": "orders"}, "not a Flight endpoint"),
        ({"uri": _URI}, "no table is named"),
        ({"uri": _URI, "path": "a", "table": "b"}, "named twice"),
        ({"uri": _URI, "path": "orders", "tokn": "t"}, "unknown option"),
        ({"uri": _URI, "path": "orders", "token": " "}, "option token is empty"),
        ({"uri": _URI, "path": "orders", "role": ""}, "option role is empty"),
    ],
)
def test_options_that_name_no_one_read_are_refused_by_name(options, message):
    with pytest.raises(OptionError, match=message):
        ReadOptions.parse(options)


@pytest.mark.parametrize(
    ("name", "parts"),
    [
        ("orders", ("orders",)),
        ("sales.orders", ("sales", "orders")),
        ('"sales.eu".orders', ("sales.eu", "orders")),
        ('sales."say ""hi"""', ("sales", 'say "hi"')),
    ],
)
def test_a_double_quoted_part_keeps_its_dots_and_quotes(name, parts):
    assert table_parts(name) == parts


@pytest.mark.parametrize("name", ["sales..orders", "sales.", '"open', 'a"b', '"a"b'])
def test_a_malformed_table_name_is_refused(name):
    with pytest.raises(OptionError):
        table_parts(name)


def test_a_quote_in_a_part_is_doubled_in_the_statement():
    read = ReadOptions.parse({"uri": _URI, "path": 'sales."o""rders"'})
    assert read.statement() == 'SELECT * FROM "sales"."o""rders"'


# --- schema mapping ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("arrow", "simple"),
    [
        (pa.bool_(), "boolean"),
        (pa.int8(), "tinyint"),
        (pa.int16(), "smallint"),
        (pa.int32(), "int"),
        (pa.int64(), "bigint"),
        (pa.uint8(), "smallint"),
        (pa.uint16(), "int"),
        (pa.uint32(), "bigint"),
        (pa.uint64(), "decimal(20,0)"),
        (pa.float32(), "float"),
        (pa.float64(), "double"),
        (pa.decimal128(10, 2), "decimal(10,2)"),
        (pa.decimal128(38, 0), "decimal(38,0)"),
        (pa.string(), "string"),
        (pa.large_string(), "string"),
        (pa.string_view(), "string"),
        (pa.binary(), "binary"),
        (pa.large_binary(), "binary"),
        (pa.date32(), "date"),
        (pa.timestamp("us", tz="UTC"), "timestamp"),
        (pa.timestamp("ns", tz="America/New_York"), "timestamp"),
        (pa.timestamp("us"), "timestamp_ntz"),
        (pa.time64("us"), "time(6)"),
        (pa.duration("us"), "interval day to second"),
        (pa.null(), "void"),
        (pa.list_(pa.int32()), "array<int>"),
        (pa.large_list(pa.string()), "array<string>"),
        (pa.map_(pa.string(), pa.int64()), "map<string,bigint>"),
        (pa.struct([("a", pa.int32()), ("b", pa.string())]), "struct<a:int,b:string>"),
    ],
)
def test_each_declared_type_is_the_spark_type_holding_it_exactly(arrow, simple):
    assert spark_type(arrow, "c").simple == simple


def test_the_ddl_quotes_every_column_name_and_struct_member():
    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("odd `name`", pa.decimal128(10, 2)),
            ("s", pa.struct([("x y", pa.int32())])),
        ]
    )
    assert spark_ddl(schema) == "`id` int, `odd ``name``` decimal(10,2), `s` struct<`x y`:int>"
    assert spark_simple(schema) == [
        ("id", "int"),
        ("odd `name`", "decimal(10,2)"),
        ("s", "struct<x y:int>"),
    ]


@pytest.mark.parametrize(
    "arrow",
    [pa.decimal256(40, 2), pa.list_(pa.decimal256(50, 0)), pa.dictionary(pa.int32(), pa.string())],
)
def test_a_type_no_spark_type_holds_is_refused_naming_its_column(arrow):
    with pytest.raises(UnmappedType, match="'amount'"):
        spark_type(arrow, "amount")


# --- batches in Spark's layout ----------------------------------------------------------------

_DECLARED = pa.schema(
    [("id", pa.int32()), ("region", pa.string()), ("amount", pa.decimal128(10, 2))]
)
_TARGET = pa.schema([("id", pa.int32()), ("region", pa.string()), ("amount", pa.decimal128(10, 2))])


def test_a_batch_in_another_layout_of_the_declared_types_is_cast_to_spark_s():
    batch = pa.record_batch(
        [
            pa.array([1], pa.int32()),
            pa.array(["east"], pa.large_string()),
            pa.array([Decimal("1.50")], pa.decimal128(10, 2)),
        ],
        names=["id", "region", "amount"],
    )
    out = flight.conformed(batch, _DECLARED, _TARGET)
    assert out.schema.equals(_TARGET)
    assert out.to_pylist() == [{"id": 1, "region": "east", "amount": Decimal("1.50")}]


def test_rows_of_another_type_than_declared_are_refused_by_column():
    batch = pa.record_batch(
        [pa.array([1], pa.int32()), pa.array(["east"]), pa.array([1.5], pa.float64())],
        names=["id", "region", "amount"],
    )
    with pytest.raises(flight.SchemaDrift, match="'amount' arrived as double"):
        flight.conformed(batch, _DECLARED, _TARGET)


def test_rows_with_other_columns_than_declared_are_refused():
    batch = pa.record_batch([pa.array([1], pa.int32())], names=["id"])
    with pytest.raises(flight.SchemaDrift, match="columns"):
        flight.conformed(batch, _DECLARED, _TARGET)


def test_a_cast_that_would_lose_a_value_raises_rather_than_truncate():
    declared = pa.schema([("at", pa.timestamp("ns"))])
    target = pa.schema([("at", pa.timestamp("us"))])
    batch = pa.record_batch([pa.array([1_000_000_001], pa.timestamp("ns"))], names=["at"])
    with pytest.raises(pa.ArrowInvalid):
        flight.conformed(batch, declared, target)


# --- the Flight SQL statement and calls -------------------------------------------------------


def test_the_statement_command_is_the_flight_sql_message_provisa_reads():
    query = 'SELECT * FROM "sales"."orders" -- é ' + "x" * 300  # a length past one varint byte
    command = flightsql.unpack(flight.statement_command(query))
    assert isinstance(command, flightsql.pb.CommandStatementQuery)
    assert command.query == query


class _State:
    auth_config = None
    auth_middleware_active = False
    multitenancy = False
    org_id = "default"
    settings_overrides: dict = {"derived_roles": []}


@pytest.fixture
def served(monkeypatch):
    """Provisa's Flight server on a loopback port, its governed pipeline and its Describe stubbed:
    each records the caller and statement it was asked for."""
    asked: list[tuple[str, dict]] = []
    declared = pa.schema([("id", pa.int32()), ("region", pa.string())])

    def _pipeline(self, request, ticket):  # noqa: ARG001  # the native ticket's signature
        asked.append(("rows", dict(request)))
        return fl.RecordBatchStream(
            pa.table(
                {"id": pa.array([1], pa.int32()), "region": pa.array(["east"], pa.large_string())}
            )
        )

    def _describe(self, context, query):
        asked.append(("schema", self._caller_request(context, query)))
        return declared

    monkeypatch.setattr(ProvisaFlightServer, "_do_get_on_loop", _pipeline)
    monkeypatch.setattr(ProvisaFlightServer, "_described_schema", _describe)
    server = ProvisaFlightServer(_State(), location="grpc://127.0.0.1:0")
    try:
        yield types.SimpleNamespace(
            read=ReadOptions.parse(
                {
                    "uri": f"grpc://127.0.0.1:{server.port}",
                    "token": "pat-1",
                    "role": "clerk",
                    "path": "sales.orders",
                }
            ),
            asked=asked,
            declared=declared,
        )
    finally:
        server.shutdown()


_CALLER = {"query": 'SELECT * FROM "sales"."orders"', "token": "pat-1", "role": "clerk"}


def test_the_declared_schema_is_described_for_the_caller(served):
    assert flight.declared_schema(served.read).equals(served.declared)
    assert served.asked == [("schema", _CALLER)]


def test_each_endpoint_is_read_as_the_caller(served):
    (endpoint,) = flight.endpoints(served.read)
    assert endpoint.uri == served.read.uri
    rows = [b.to_pylist() for b in flight.batches(served.read, endpoint)]
    assert rows == [[{"id": 1, "region": "east"}]]
    assert served.asked == [("rows", _CALLER)]


def test_an_endpoint_naming_another_server_is_read_there():
    elsewhere = fl.FlightEndpoint(b"t", [fl.Location("grpc+tls://other:8815")])  # pyright: ignore[reportPrivateImportUsage]
    reuse = fl.FlightEndpoint(b"t", [fl.Location("arrow-flight-reuse-connection://?")])  # pyright: ignore[reportPrivateImportUsage]
    read = ReadOptions.parse({"uri": _URI, "path": "orders"})
    assert flight._endpoint_uri(read, elsewhere) == "grpc+tls://other:8815"  # noqa: SLF001
    assert flight._endpoint_uri(read, reuse) == _URI  # noqa: SLF001

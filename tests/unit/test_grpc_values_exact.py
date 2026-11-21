# Copyright (c) 2026 Kenneth Stott
# Canary: 7d0c5e2a-41b9-4f3e-9a6d-c2f81b5e0d93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1965 (amended 2026-11-02): gRPC writes every value exactly. A result is served through the
servicer's own query path -- its Arrow batches through the Rust encoder (REQ-2194) -- into a
compiled service's real messages, and read back off the wire: an array, struct or map in a string
field is JSON, never a Python repr; bytes are never a Python ``b'...'`` literal; an interval is
ISO 8601 in a bytes field as in a string field; PostgreSQL's infinities say "infinity" /
"-infinity" in a string field and are the Timestamp's maximum and minimum in a Timestamp field; a
value beyond a float or double field's range is refused by name (J502b: a numeric past a double's
range too), and the refusal reaches the client as INVALID_ARGUMENT on every route."""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pyarrow as pa
import pytest

from provisa.core.ir_types import infinite_date, infinite_timestamp
from provisa.executor.result import ArrowResult, QueryResult
from provisa.grpc.descriptor import OrgDescriptor
from provisa.grpc.server import ProvisaServicer, _nodes_field
from tests.unit.test_grpc_server import _make_state

_SERVICE = OrgDescriptor("""syntax = "proto3";
package provisa.v1;
import "google/protobuf/timestamp.proto";
message Customers { optional int64 id = 1; optional string tags = 2; repeated Orders orders = 3; }
message Orders {
  optional int64 id = 1;
  optional string payload = 2;
  optional bytes blob = 3;
  optional float ratio = 4;
  optional double price = 5;
  optional google.protobuf.Timestamp created = 6;
  optional string day = 7;
  optional string seen = 8;
  Customers customers = 9;
}
message OrdersRequest { int32 limit = 1; }
message CustomersGroupByRow { string group_key = 1; repeated Customers nodes = 3; }
message OrdersGroupByRow { string group_key = 1; repeated Orders nodes = 3; }
service ProvisaService { rpc QueryOrders (OrdersRequest) returns (stream Orders); }
""")

_FLT_MAX = 3.4028234663852886e38


class _Aborted(Exception):
    """What ``context.abort`` raises, as the server's does: the RPC ends with this status."""

    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(code, details)
        self.code, self.details = code, details


async def _answered(result, *, cached: bool = False) -> list:
    """``result`` served by QueryOrders -- from the API route's terminal, or (``cached``) as a
    response-cache hit -- each message read back off the wire."""
    from provisa.transpiler.router import Route

    servicer = ProvisaServicer(_make_state(), _SERVICE)
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata.return_value = [("x-provisa-role", "admin")]

    def abort(code, details):
        raise _Aborted(code, details)

    context.abort.side_effect = abort
    request = MagicMock()
    request.limit = 0
    plan = SimpleNamespace(
        route=Route.CACHE,
        source_id=None,
        cache_hit=("hit",) if cached else None,
        warnings=[],
        materialize=None,
    )
    (column,) = result.column_names
    with (
        patch(
            "provisa.grpc.query_ir.grpc_table_to_graphql",
            return_value=(f"{{ orders {{ {column} }} }}", {}),
        ),
        patch(
            "provisa.api.generated_plan.compile_generated_graphql",
            return_value=[SimpleNamespace(sql=f"SELECT {column} FROM orders", params=[])],
        ),
        patch(
            "provisa.pgwire._pipeline._govern_and_route_compiled",
            new_callable=AsyncMock,
            return_value=plan,
        ),
        patch(
            "provisa.pgwire._pipeline._execute_plan", new_callable=AsyncMock, return_value=result
        ),
        patch(
            "provisa.pgwire._pipeline.cached_result", new_callable=AsyncMock, return_value=result
        ),
    ):
        sent = [m async for m in servicer._handle_query(request, context, "Orders")]
    return [_SERVICE.Orders.FromString(m) for m in sent]


async def _served(column: str, *values: object, declared: str | None = None) -> list:
    """``values`` of ``column`` as a row engine returns them: its rows reach the encoder as Arrow
    at the one fetch boundary (ir_arrow.row_batch), typed by the engine's ``declared`` name."""
    types = None if declared is None else [declared]
    return await _answered(
        QueryResult(rows=[(v,) for v in values], column_names=[column], column_types=types)
    )


async def _served_arrow(column: str, array: pa.Array) -> list:
    """``array`` as an Arrow engine hands it over: its batches straight to the encoder."""
    table = pa.table({column: array})
    return await _answered(ArrowResult(table, to_rows=lambda t: list(zip(*t.to_pydict().values()))))


# --- 1: an array, a struct or a map in a string field is JSON ------------------------------------


@pytest.mark.asyncio
async def test_an_array_in_a_string_field_is_json_not_a_python_repr():
    (row,) = await _served_arrow("payload", pa.array([["a", None, "b"]]))
    assert row.payload == '["a",null,"b"]'
    (row,) = await _served_arrow("payload", pa.array([[True, None]]))
    assert row.payload == "[true,null]"


@pytest.mark.asyncio
async def test_a_json_columns_text_is_sent_as_the_source_wrote_it():
    (row,) = await _served("payload", ["a", None, True], declared="JSON")
    assert json.loads(row.payload) == ["a", None, True]


@pytest.mark.asyncio
async def test_a_struct_in_a_string_field_is_a_json_object():
    (row,) = await _served("payload", {"k": 1.5, "on": date(2026, 1, 2), "n": None})
    assert row.payload == '{"k":1.5,"on":"2026-01-02","n":null}'


@pytest.mark.asyncio
async def test_a_map_in_a_string_field_is_a_json_object():
    # Arrow holds a map as its (key, value) entries.
    entries = pa.array([[("a", 1), ("b", 2)]], pa.map_(pa.string(), pa.int64()))
    (row,) = await _served_arrow("payload", entries)
    assert row.payload == '{"a":1,"b":2}'


@pytest.mark.asyncio
async def test_values_inside_json_are_written_as_every_surface_writes_them():
    kinds = pa.struct(
        [
            ("d", pa.decimal128(5, 2)),  # exact: a decimal element is a JSON string (REQ-1965)
            ("b", pa.binary()),
            ("span", pa.duration("us")),
            ("at", pa.timestamp("us", "UTC")),
            ("l", pa.list_(pa.list_(pa.int64()))),
        ]
    )
    value = {
        "d": Decimal("1.10"),
        "b": b"\x00\xff",
        "span": timedelta(days=1, seconds=5),
        "at": datetime(2026, 1, 2, 3, 4, 5, 600000, tzinfo=timezone.utc),
        "l": [[1, 2], []],
    }
    (row,) = await _served_arrow("payload", pa.array([value], kinds))
    assert row.payload == (
        '{"d":"1.10","b":"\\\\x00ff","span":"P1DT5S","at":"2026-01-02T03:04:05.6+00:00",'
        '"l":[[1,2],[]]}'
    )


@pytest.mark.asyncio
async def test_a_nan_or_infinite_number_inside_json_is_refused_by_name():
    with pytest.raises(_Aborted) as refused:
        await _served_arrow("payload", pa.array([[1.0, float("nan")]]))
    assert refused.value.code == grpc.StatusCode.INVALID_ARGUMENT
    assert '"payload"' in refused.value.details
    with pytest.raises(_Aborted, match="payload"):
        await _served_arrow("payload", pa.array([{"x": float("inf")}]))


# --- 2: bytes as their text, an interval as ISO 8601 ---------------------------------------------


@pytest.mark.asyncio
async def test_bytes_in_a_string_field_are_postgres_hex_not_a_python_literal():
    (row,) = await _served("payload", b"ab")
    assert row.payload == "\\x6162"


@pytest.mark.asyncio
async def test_an_interval_in_a_bytes_field_is_iso_8601():
    (row,) = await _served("blob", timedelta(days=1, seconds=5))
    assert row.blob == b"P1DT5S"


@pytest.mark.asyncio
async def test_bytes_in_a_bytes_field_are_the_bytes_themselves():
    (row,) = await _served("blob", b"\x00\xff\x80")
    assert row.blob == b"\x00\xff\x80"


# --- 3: PostgreSQL's infinities ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_infinite_date_in_a_string_field_says_infinity():
    up, down = await _served("day", infinite_date(1), infinite_date(-1))
    assert (up.day, down.day) == ("infinity", "-infinity")


@pytest.mark.asyncio
async def test_an_infinite_timestamp_in_a_string_field_says_infinity():
    up, down = await _served(
        "seen",
        infinite_timestamp(1, aware=True),
        infinite_timestamp(-1, aware=True),
        declared="TIMESTAMPTZ",
    )
    (naive,) = await _served("seen", infinite_timestamp(1, aware=False), declared="TIMESTAMP")
    assert (up.seen, down.seen, naive.seen) == ("infinity", "-infinity", "infinity")


@pytest.mark.asyncio
async def test_an_infinite_timestamp_in_a_timestamp_field_is_its_maximum_and_minimum():
    """Maintainer 2026-11-02: Timestamp has no infinity; +infinity is 9999-12-31T23:59:59.999999Z
    and -infinity 0001-01-01T00:00:00Z, aware or naive."""
    rows = await _served(
        "created",
        infinite_timestamp(1, aware=True),
        infinite_timestamp(-1, aware=True),
        declared="TIMESTAMPTZ",
    ) + await _served(
        "created",
        infinite_timestamp(1, aware=False),
        infinite_timestamp(-1, aware=False),
        declared="TIMESTAMP",
    )
    written = [r.created.ToJsonString() for r in rows]
    assert written == [
        "9999-12-31T23:59:59.999999Z",
        "0001-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999Z",
        "0001-01-01T00:00:00Z",
    ]
    assert [(r.created.seconds, r.created.nanos) for r in rows] == [
        (253402300799, 999999000),
        (-62135596800, 0),
        (253402300799, 999999000),
        (-62135596800, 0),
    ]


# --- 5: a float or double field refuses a value beyond its range --------------------------------


@pytest.mark.asyncio
async def test_a_value_beyond_a_float_fields_range_is_refused_by_name():
    for value in (1e39, Decimal("-4e38")):
        with pytest.raises(_Aborted) as refused:
            await _served("ratio", value)
        assert refused.value.code == grpc.StatusCode.INVALID_ARGUMENT
        assert re.search(r'"ratio".*out of range for type real', refused.value.details)


@pytest.mark.asyncio
async def test_a_float_field_keeps_its_largest_value_and_postgres_infinities():
    top, near, up, down = await _served("ratio", _FLT_MAX, 3.0e38, math.inf, -math.inf)
    assert math.isclose(top.ratio, _FLT_MAX, rel_tol=1e-14)
    assert math.isclose(near.ratio, 3.0e38, rel_tol=1e-7)  # float32's own precision
    assert (up.ratio, down.ratio) == (math.inf, -math.inf)


@pytest.mark.asyncio
async def test_a_double_field_is_not_bound_by_a_float_fields_range():
    (row,) = await _served("price", 1e39)
    assert math.isclose(row.price, 1e39, rel_tol=1e-14)


@pytest.mark.asyncio
async def test_a_numeric_beyond_a_double_fields_range_is_refused_by_name():
    """J502b: a numeric past the double range, in a double field, is refused naming the column,
    as PostgreSQL refuses it -- never sent as an infinity."""
    for value in (Decimal("1E+400"), Decimal("-1.5E+309")):
        with pytest.raises(_Aborted) as refused:
            await _served("price", value, declared="NUMERIC")
        assert refused.value.code == grpc.StatusCode.INVALID_ARGUMENT
        assert re.search(
            r'column "price": value .* is out of range for type double precision',
            refused.value.details,
        )
    (row,) = await _served("price", Decimal("1.5E+308"), declared="NUMERIC")
    assert math.isclose(row.price, 1.5e308, rel_tol=1e-14)
    up, down = await _served("price", Decimal("Infinity"), Decimal("-Infinity"), declared="NUMERIC")
    assert (up.price, down.price) == (math.inf, -math.inf)  # PostgreSQL's own infinities


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True], ids=["api-route", "cache-hit"])
async def test_a_refusal_on_a_buffered_route_is_invalid_argument_not_unknown(cached):
    """J502b: a value the encoder refuses on the API route or a response-cache hit ends the RPC
    with INVALID_ARGUMENT and the refusal's text, as on the streamed routes, never an opaque
    UNKNOWN."""
    result = QueryResult(
        rows=[(Decimal("1E+400"),)], column_names=["price"], column_types=["NUMERIC"]
    )
    with pytest.raises(_Aborted) as refused:
        await _answered(result, cached=cached)
    assert refused.value.code == grpc.StatusCode.INVALID_ARGUMENT
    assert '"price"' in refused.value.details


# --- 4: a group's nodes --------------------------------------------------------------------------


def _node(type_name: str, node: dict):
    group = getattr(_SERVICE, f"{type_name}GroupByRow")
    sent = group.FromString(_nodes_field(group, getattr(_SERVICE, type_name), [node]))
    (built,) = sent.nodes
    return built


def test_a_nodes_relations_are_written_by_the_encoder():
    """A node is the JSON its group's nodes column holds (bytes as their hex text)."""
    node = {
        "id": 1,
        "tags": ["x", "y"],
        "orders": [{"id": 7, "payload": {"k": [1]}, "blob": "\\x01"}, {"id": 8}],
    }
    sent = _node("Customers", node)
    assert sent.tags == '["x","y"]'
    assert [o.id for o in sent.orders] == [7, 8]
    assert sent.orders[0].payload == '{"k":[1]}'
    assert sent.orders[0].blob == b"\\x01"


def test_a_nodes_many_to_one_relation_is_written_by_the_encoder():
    sent = _node("Orders", {"id": 7, "customers": {"id": 1, "tags": "t"}})
    assert (sent.customers.id, sent.customers.tags) == (1, "t")

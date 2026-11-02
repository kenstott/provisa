# Copyright (c) 2026 Kenneth Stott
# Canary: 7d0c5e2a-41b9-4f3e-9a6d-c2f81b5e0d93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1965 (amended 2026-11-02): gRPC writes every value exactly. A row is served through the
servicer's own query path into a compiled service's real messages, and read back off the wire:
an array, struct or map in a string field is JSON, never a Python repr; bytes are never a Python
``b'...'`` literal; an interval is ISO 8601 in a bytes field as in a string field; PostgreSQL's
infinities say "infinity" / "-infinity" in a string field and are the Timestamp's maximum and
minimum in a Timestamp field; a value beyond a float field's range is refused by name."""

from __future__ import annotations

import math
import warnings
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import grpc.aio
import pytest

from provisa.core.ir_types import infinite_date, infinite_timestamp
from provisa.grpc.descriptor import OrgDescriptor
from provisa.grpc.server import ProvisaServicer
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
service ProvisaService { rpc QueryOrders (OrdersRequest) returns (stream Orders); }
""")

_FLT_MAX = 3.4028234663852886e38


async def _served(column: str, *values: object) -> list:
    """``values`` of ``column`` served by QueryOrders, each row read back off the wire."""
    from provisa.transpiler.router import Route

    servicer = ProvisaServicer(_make_state(), _SERVICE)
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata.return_value = [("x-provisa-role", "admin")]
    request = MagicMock()
    request.limit = 0
    plan = SimpleNamespace(
        route=Route.CACHE, source_id=None, cache_hit=None, warnings=[], materialize=None
    )
    result = SimpleNamespace(column_names=[column], rows=[[v] for v in values])
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
            "provisa.pgwire._pipeline._execute_plan",
            new_callable=AsyncMock,
            return_value=result,
        ),
    ):
        sent = [m async for m in servicer._handle_query(request, context, "Orders", "orders")]
    return [_SERVICE.Orders.FromString(m.SerializeToString()) for m in sent]


# --- 1: an array, a struct or a map in a string field is JSON ------------------------------------


@pytest.mark.asyncio
async def test_an_array_in_a_string_field_is_json_not_a_python_repr():
    (row,) = await _served("payload", ["a", None, True])
    assert row.payload == '["a",null,true]'


@pytest.mark.asyncio
async def test_a_struct_in_a_string_field_is_a_json_object():
    (row,) = await _served("payload", {"k": 1.5, "on": date(2026, 1, 2), "n": None})
    assert row.payload == '{"k":1.5,"on":"2026-01-02","n":null}'


@pytest.mark.asyncio
async def test_a_map_in_a_string_field_is_a_json_object():
    # Arrow hands a map over as its (key, value) entries.
    (row,) = await _served("payload", [("a", 1), ("b", 2)])
    assert row.payload == '{"a":1,"b":2}'


@pytest.mark.asyncio
async def test_values_inside_json_are_written_as_every_surface_writes_them():
    (row,) = await _served(
        "payload",
        [
            Decimal("1.10"),  # exact: a decimal element is a JSON string (REQ-1965)
            b"\x00\xff",
            timedelta(days=1, seconds=5),
            datetime(2026, 1, 2, 3, 4, 5, 600000, tzinfo=timezone.utc),
            infinite_date(1),
            [[1, 2], []],
        ],
    )
    assert row.payload == (
        '["1.10","\\\\x00ff","P1DT5S","2026-01-02T03:04:05.6+00:00","infinity",[[1,2],[]]]'
    )


@pytest.mark.asyncio
async def test_a_nan_or_infinite_number_inside_json_is_refused_by_name():
    with pytest.raises(ValueError, match="payload"):
        await _served("payload", [1.0, float("nan")])
    with pytest.raises(ValueError, match="payload"):
        await _served("payload", {"x": float("inf")})


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
    up, down, naive = await _served(
        "seen",
        infinite_timestamp(1, aware=True),
        infinite_timestamp(-1, aware=True),
        infinite_timestamp(1, aware=False),
    )
    assert (up.seen, down.seen, naive.seen) == ("infinity", "-infinity", "infinity")


@pytest.mark.asyncio
async def test_an_infinite_timestamp_in_a_timestamp_field_is_its_maximum_and_minimum():
    """Maintainer 2026-11-02: Timestamp has no infinity; +infinity is 9999-12-31T23:59:59.999999Z
    and -infinity 0001-01-01T00:00:00Z, aware or naive."""
    rows = await _served(
        "created",
        infinite_timestamp(1, aware=True),
        infinite_timestamp(-1, aware=True),
        infinite_timestamp(1, aware=False),
        infinite_timestamp(-1, aware=False),
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


# --- 5: a float field refuses a value beyond its range -------------------------------------------


@pytest.mark.asyncio
async def test_a_value_beyond_a_float_fields_range_is_refused_by_name():
    with pytest.raises(ValueError, match=r'"ratio".*out of range for type real'):
        await _served("ratio", 1e39)
    with pytest.raises(ValueError, match=r'"ratio".*out of range for type real'):
        await _served("ratio", Decimal("-4e38"))


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


# --- 4: a node's relations, with no deprecated protobuf API --------------------------------------


def test_a_nodes_relations_are_built_without_a_deprecation_warning():
    servicer = ProvisaServicer(MagicMock(), _SERVICE)
    node = {
        "id": 1,
        "tags": ["x", "y"],
        "orders": [{"id": 7, "payload": {"k": [1]}, "blob": b"\x01"}, {"id": 8}],
    }
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        msg = servicer._dict_row_to_message(_SERVICE.Customers, node)
    sent = _SERVICE.Customers.FromString(msg.SerializeToString())
    assert sent.tags == '["x","y"]'
    assert [o.id for o in sent.orders] == [7, 8]
    assert sent.orders[0].payload == '{"k":[1]}'
    assert sent.orders[0].blob == b"\x01"


def test_a_nodes_many_to_one_relation_is_built_without_a_deprecation_warning():
    servicer = ProvisaServicer(MagicMock(), _SERVICE)
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        msg = servicer._dict_row_to_message(
            _SERVICE.Orders, {"id": 7, "customers": {"id": 1, "tags": "t"}}
        )
    sent = _SERVICE.Orders.FromString(msg.SerializeToString())
    assert (sent.customers.id, sent.customers.tags) == (1, "t")

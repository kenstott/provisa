# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f49be-19f3-4ec4-a18b-05165ef8e3b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Python gRPC value path the Rust encoder replaced (REQ-2194), frozen as the differential's
reference: provisa/grpc/server.py's ``_json_value``, ``_text``, ``_float32``, ``_proto_value`` and
``ProvisaServicer._dict_row_to_message`` as they stood on main at 3bf137393b (J502, REQ-1965 as
amended 2026-11-02), copied unchanged except that ``_dict_row_to_message`` takes the service it
reads relations from as ``pb2``. Nothing in the product imports this module."""

# ruff: noqa

from __future__ import annotations

import json
import math
import struct
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, cast

from dateutil.relativedelta import relativedelta
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message_factory import GetMessageClass

if TYPE_CHECKING:
    from google.protobuf.timestamp_pb2 import Timestamp

from provisa.core.ir_types import bytea_hex, infinity_of, iso8601_duration
from provisa.executor.serialize import temporal_text


def _json_value(field, value):
    """A value inside an array, struct or map as JSON holds it (REQ-1965): a decimal as its exact
    text, a date or time as PostgreSQL's json writes it, an infinity, an interval and bytes as
    their text on every surface. Arrow hands a map over as its (key, value) entries."""
    if isinstance(value, dict):
        return {str(k): _json_value(field, v) for k, v in value.items()}
    if (
        isinstance(value, list)
        and value
        and all(isinstance(item, tuple) and len(item) == 2 for item in value)
    ):
        return {str(k): _json_value(field, v) for k, v in value}
    if isinstance(value, (list, tuple)):
        return [_json_value(field, item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f'column "{field.name}": {value} is not a number JSON can hold')
        return value
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat") and not infinity_of(value):
        return temporal_text(value)
    return _text(field, value)


def _text(field, value) -> str:
    """A value as the text a string field carries: an array, struct or map as JSON (REQ-1965),
    PostgreSQL's infinities as "infinity" / "-infinity" as GraphQL and pgwire write them, an
    interval as ISO 8601 and bytes as PostgreSQL's hex, as on every surface."""
    if isinstance(value, str):
        return value
    sign = infinity_of(value)
    if sign:
        return "infinity" if sign > 0 else "-infinity"
    if isinstance(value, (timedelta, relativedelta)):
        return iso8601_duration(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytea_hex(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(_json_value(field, value), ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _float32(field, number: float) -> float:
    """``number`` for a float field, refused by name when it is beyond the field's range, as
    PostgreSQL refuses a real out of range (protobuf would send it as an infinity)."""
    if math.isfinite(number):
        try:
            struct.pack("<f", number)
        except OverflowError:
            raise ValueError(
                f'column "{field.name}": value {number!r} is out of range for type real'
            ) from None
    return number


def _proto_value(field, value):
    """Adapt a driver row value to the proto field's wire type.

    Protobuf assignment is exact-typed: a ``datetime`` into a ``string`` field, or into a
    ``google.protobuf.Timestamp`` field, raises ``TypeError: bad argument type for built-in
    operation`` — the whole RPC dies with StatusCode.UNKNOWN. The driver's Python type is decided by
    the PHYSICAL column (asyncpg hands back ``datetime`` for TIMESTAMP, ``Decimal`` for NUMERIC)
    while the proto field type comes from the REGISTERED column type, so the two disagree by design
    on any column whose registration widens or narrows the physical type. Every other transport
    normalizes at its own serialization boundary (serialize_rows / Arrow / the pgwire encoders);
    this is gRPC's.

    REQ-1884: date/datetime and FieldDescriptor are module-level imports, not per-call — this
    function runs once per (row, column) on the ENGINE-route streaming path (millions of times
    for a large scan), and a per-call ``import`` statement still pays a sys.modules lookup +
    attribute bind every time even when the module is already loaded.
    """
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        if field.message_type.full_name == "google.protobuf.Timestamp":
            # The class of THIS field's Timestamp: an organisation's service is compiled into a
            # pool of its own, and its descriptor's `_concrete_class` fails under upb ("error
            # return without exception set") when that pool was built before this module loaded.
            ts = cast("Timestamp", GetMessageClass(field.message_type)())
            ts.FromDatetime(value if isinstance(value, datetime) else datetime.fromisoformat(value))
            return ts
        return value
    if field.type == FieldDescriptor.TYPE_STRING:
        return _text(field, value)
    if field.type == FieldDescriptor.TYPE_FLOAT:
        return _float32(field, float(value))
    if field.type == FieldDescriptor.TYPE_DOUBLE:
        return float(value)
    if field.type in (
        FieldDescriptor.TYPE_INT32,
        FieldDescriptor.TYPE_INT64,
        FieldDescriptor.TYPE_UINT32,
        FieldDescriptor.TYPE_UINT64,
    ):
        return int(value.toordinal()) if isinstance(value, (date, datetime)) else int(value)
    if field.type == FieldDescriptor.TYPE_BOOL:
        return bool(value)
    if field.type == FieldDescriptor.TYPE_BYTES:
        return value if isinstance(value, bytes) else _text(field, value).encode()
    return value


def _dict_row_to_message(pb2, msg_cls, d: dict):
    """Build a ``{Type}`` proto message from a node's nested dict (REQ-1405) — the same
    nested-dict shape a group's nodes column holds for every other transport,
    just typed instead of left as JSON. Relation columns (``_build_rel_json_expr``) surface as
    a dict (many-to-one) or list-of-dicts (one-to-many) value keyed by the relation field name;
    every other value is a scalar assigned via the shared ``_proto_value`` adapter."""
    descriptor = msg_cls.DESCRIPTOR
    kwargs = {}
    nested: dict = {}
    for key, val in d.items():
        if val is None:
            continue
        field = descriptor.fields_by_name.get(key)
        if field is None:
            continue
        if (
            field.message_type is not None
            and field.message_type.full_name != "google.protobuf.Timestamp"
        ):
            nested[key] = val
        else:
            kwargs[key] = _proto_value(field, val)
    msg = msg_cls(**kwargs)
    for key, val in nested.items():
        field = descriptor.fields_by_name[key]
        sub_cls = getattr(pb2, field.message_type.name, None)
        if sub_cls is None:
            continue
        if field.is_repeated:
            items = val if isinstance(val, list) else [val]
            getattr(msg, key).extend(
                _dict_row_to_message(pb2, sub_cls, item) for item in items if isinstance(item, dict)
            )
        elif isinstance(val, dict):
            getattr(msg, key).CopyFrom(_dict_row_to_message(pb2, sub_cls, val))
    return msg

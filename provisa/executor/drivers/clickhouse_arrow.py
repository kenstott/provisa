# Copyright (c) 2026 Kenneth Stott
# Canary: b4aa70ea-073c-48bb-b6ec-11e353910d57
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A ClickHouse DIRECT result read as Arrow, holding the values its row read gives (REQ-2194).

ClickHouse's Arrow output writes some types as something else, and names no ClickHouse type:

- ``IPv4`` and ``DateTime`` are ``uint32`` (an address, seconds since the epoch), ``IPv6`` and
  ``Int128``/``UInt128`` 16 bytes, ``Int256``/``UInt256`` 32 bytes, ``Enum8``/``Enum16`` their codes;
- ``Date`` is ``uint16`` days before ClickHouse 26 and ``date32`` from it;
- a ``DateTime64`` with no zone of its own is labelled with the session's zone;
- ``UUID`` is the ``arrow.uuid`` extension (ClickHouse 26 and later; earlier servers refuse it).

So the result's ClickHouse types are read from ``DESCRIBE (query)`` whenever the Arrow schema holds
one of those Arrow types, and each such column becomes what clickhouse-connect's row read gives,
as the IR holds it, by Arrow compute and numpy over whole columns (no per-row Python):

- ``IPv4``, ``IPv6``: the address text PostgreSQL's inet_out writes (``ir_arrow.text_of``);
- ``UUID``: its canonical text;
- ``DateTime``, ``DateTime64``: timestamps in the unit of the server's type (``DateTime`` and
  ``DateTime64(0)`` seconds, ``DateTime64(1..3)`` milliseconds, ``(4..6)`` microseconds,
  ``(7..9)`` nanoseconds: the unit ClickHouse's own Arrow output gives a ``DateTime64``), in the
  zone the row read applies (``QueryContext.active_tz``: the column's own zone, else the
  server's), naive for UTC;
- ``Date`` as ``uint16``: ``date32``;
- ``Enum8``, ``Enum16``: the label;
- ``Int128``, ``UInt128``, ``Int256``, ``UInt256``: ``decimal256(76, 0)``;
- each inside ``Nullable``, ``LowCardinality``, ``Array``, ``Map`` and ``Tuple``.

Rows built from the table (``to_rows``) hold the row read's Python values: an ``IPv4Address`` or
``IPv6Address``, a ``uuid.UUID``, an ``int`` of a wide integer, a ``dict`` of a ``Map``, a
``tuple`` of a ``Tuple``, a ``datetime`` (to the microsecond, a finer value floored, as the row
read floors it).
"""

# Requirements: REQ-2194

from __future__ import annotations

import ipaddress
import uuid
from collections.abc import Callable, Sequence
from datetime import timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from provisa.core.ir_arrow import column_values

# A ClickHouse type as clickhouse-connect's registry parses it (base_type, nullable, element_type,
# key_type/value_type, element_types, tzinfo, type_def).
CHType = Any
# The zone the row read gives a DateTime column of this declared zone (None: the server's).
ZoneOf = Callable[[tzinfo | None], tzinfo | None]


def _call(name: str, *args: Any, value_set: pa.Array | None = None) -> Any:  # noqa: ANN401 — an Arrow array or scalar
    """An Arrow compute function by name (pyarrow's stubs name none of its generated functions)."""
    if value_set is not None:
        return pc.call_function(name, list(args), pc.SetLookupOptions(value_set))
    return pc.call_function(name, list(args))


# Pinned so a server profile cannot change the Arrow form the conversion reads: LowCardinality as
# its values (not a dictionary), FixedString as fixed-size binary (the row read's bytes).
ARROW_SETTINGS = {
    "output_format_arrow_low_cardinality_as_dictionary": "0",
    "output_format_arrow_fixed_string_as_fixed_byte_array": "1",
}

_WIDE_INTS = {"Int128": True, "Int256": True, "UInt128": False, "UInt256": False}
_DATETIMES = frozenset({"DateTime", "DateTime64"})


def needs_types(schema: pa.Schema) -> bool:
    """Whether a column of ``schema`` is an Arrow type that stands for more than one ClickHouse
    type, so its values can be read only with the ClickHouse type ``DESCRIBE`` gives."""
    return any(_ambiguous(field.type) for field in schema)


def _ambiguous(t: pa.DataType) -> bool:
    if isinstance(t, pa.ExtensionType):
        return True
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return _ambiguous(t.value_type)
    if pa.types.is_map(t) or pa.types.is_struct(t):
        return True  # the row read gives a Map as a dict and a Tuple as a tuple
    return (
        pa.types.is_uint16(t)
        or pa.types.is_uint32(t)
        or pa.types.is_int8(t)
        or pa.types.is_int16(t)
        or pa.types.is_fixed_size_binary(t)
        or pa.types.is_timestamp(t)
    )


def _check_width(types: Sequence[CHType], width: int) -> None:
    if len(types) != width:
        raise ValueError(f"DESCRIBE named {len(types)} columns for an Arrow result of {width}")


def row_values_table(table: pa.Table, types: Sequence[CHType], zone_of: ZoneOf) -> pa.Table:
    """``table`` with each column holding what the row read gives of its ClickHouse type."""
    _check_width(types, table.num_columns)
    columns = [
        pa.chunked_array(
            [_convert(chunk, t, zone_of) for chunk in column.chunks],
            type=_converted_type(column.type, t, zone_of),
        )
        for column, t in zip(table.columns, types, strict=True)
    ]
    return pa.Table.from_arrays(columns, names=table.column_names)


def row_values_schema(schema: pa.Schema, types: Sequence[CHType], zone_of: ZoneOf) -> pa.Schema:
    """The schema ``row_values_table`` gives a table of ``schema``: a stream's, known before its
    first batch."""
    _check_width(types, len(schema))
    return pa.schema(
        [
            pa.field(field.name, _converted_type(field.type, t, zone_of))
            for field, t in zip(schema, types, strict=True)
        ]
    )


def row_values_batch(
    batch: pa.RecordBatch, types: Sequence[CHType], zone_of: ZoneOf, schema: pa.Schema
) -> pa.RecordBatch:
    """One record batch of a stream converted as ``row_values_table`` converts a whole result;
    ``schema`` is the stream's ``row_values_schema``."""
    _check_width(types, batch.num_columns)
    return pa.RecordBatch.from_arrays(
        [_convert(column, t, zone_of) for column, t in zip(batch.columns, types, strict=True)],
        schema=schema,
    )


def _converted_type(arrow_type: pa.DataType, t: CHType, zone_of: ZoneOf) -> pa.DataType:
    return _convert(_empty(arrow_type), t, zone_of).type


def _empty(t: pa.DataType) -> pa.Array:
    """An empty array of ``t``, an extension type or one nesting it among them
    (``pa.array([], t)`` builds none of those)."""
    if isinstance(t, pa.ExtensionType):
        return t.wrap_array(_empty(t.storage_type))
    no_offsets = pa.array([0], pa.int32())
    if pa.types.is_map(t):
        return pa.MapArray.from_arrays(no_offsets, _empty(t.key_type), _empty(t.item_type), type=t)
    if pa.types.is_list(t):
        return pa.ListArray.from_arrays(no_offsets, _empty(t.value_type), type=t)
    if pa.types.is_struct(t):
        fields = [t.field(i) for i in range(t.num_fields)]
        return pa.StructArray.from_arrays([_empty(f.type) for f in fields], fields=fields)
    return pa.array([], t)


def _convert(arr: pa.Array, t: CHType, zone_of: ZoneOf) -> pa.Array:  # noqa: C901 — one branch per ClickHouse type
    base = t.base_type
    if base == "Array":
        values = _convert(arr.values, t.element_type, zone_of)
        return _relist(arr, values)
    if base == "Map":
        keys = _convert(arr.keys, t.key_type, zone_of)
        items = _convert(arr.items, t.value_type, zone_of)
        return pa.MapArray.from_arrays(
            arr.offsets, keys, items, mask=arr.is_null() if arr.null_count else None
        )
    if base == "Tuple":
        fields = [_convert(arr.field(i), et, zone_of) for i, et in enumerate(t.element_types)]
        names = [arr.type.field(i).name for i in range(arr.type.num_fields)]
        return pa.StructArray.from_arrays(
            fields, names=names, mask=arr.is_null() if arr.null_count else None
        )
    if isinstance(arr.type, pa.ExtensionType):
        arr = arr.storage
    if base == "IPv4":
        return ipv4_text(arr)
    if base == "IPv6":
        return ipv6_text(arr)
    if base == "UUID":
        return uuid_text(arr)
    if base in _DATETIMES:
        return _timestamps(arr, _unit_of(t), zone_of(t.tzinfo))
    if base == "Date" and pa.types.is_uint16(arr.type):
        return arr.cast(pa.int32()).cast(pa.date32())
    if base in ("Enum8", "Enum16"):
        codes = pa.array(list(t.type_def.values), arr.type)
        return pc.take(
            pa.array(list(t.type_def.keys), pa.string()), _call("index_in", arr, value_set=codes)
        )
    if base in _WIDE_INTS:
        return _wide_ints(arr, signed=_WIDE_INTS[base])
    return arr


def _relist(arr: pa.Array, values: pa.Array) -> pa.Array:
    """A list array of ``arr``'s shape holding ``values`` (``arr.values`` converted)."""
    offsets = arr.offsets
    if arr.offset or offsets[0].as_py():
        # arr.values is the whole child; rebase to the slice the offsets name.
        start = offsets[0].as_py()
        values = values.slice(start, offsets[-1].as_py() - start)
        offsets = _call("subtract", offsets, pa.scalar(start, offsets.type))
    return pa.ListArray.from_arrays(offsets, values, mask=arr.is_null() if arr.null_count else None)


# -- Addresses and UUIDs -------------------------------------------------------------------------


def _fixed_bytes(arr: pa.Array) -> np.ndarray:
    """A fixed_size_binary column's bytes as an (N, width) uint8 matrix (a null row's bytes too)."""
    width = arr.type.byte_width
    data = np.frombuffer(arr.buffers()[1], dtype=np.uint8)
    return data[arr.offset * width : (arr.offset + len(arr)) * width].reshape(len(arr), width)


def _uint32s(arr: pa.Array) -> np.ndarray:
    data = np.frombuffer(arr.buffers()[1], dtype=np.uint32)
    return data[arr.offset : arr.offset + len(arr)]


def _validity(arr: pa.Array) -> pa.Buffer | None:
    """``arr``'s validity as a bitmap at offset zero (None when no value is null)."""
    if not arr.null_count:
        return None
    return arr.is_valid().buffers()[1]


def _text_of(arr: pa.Array, chars: np.ndarray, keep: np.ndarray) -> pa.Array:
    """A string column of ``arr``'s length and nulls: row i is ``chars[i][keep[i]]``."""
    lengths = keep.sum(axis=1, dtype=np.int64)
    offsets = np.zeros(len(arr) + 1, dtype=np.int32)
    np.cumsum(lengths, out=offsets[1:])
    data = np.ascontiguousarray(chars[keep], dtype=np.uint8)
    return pa.Array.from_buffers(
        pa.string(),
        len(arr),
        [_validity(arr), pa.py_buffer(offsets), pa.py_buffer(data)],
        null_count=arr.null_count,
    )


_HEX = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)
_DIGITS = np.array(
    [[ord(c) for c in f"{n:<3}"] for n in range(256)], dtype=np.uint8
)  # each byte's decimal digits, left aligned
_DIGIT_COUNT = np.array([len(str(n)) for n in range(256)], dtype=np.int64)
_COLON, _DOT, _DASH = ord(":"), ord("."), ord("-")


def _dotted(octets: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Dotted-quad characters of (N, 4) octets: a (N, 15) matrix and the mask of its kept cells."""
    n = len(octets)
    chars = np.full((n, 15), _DOT, dtype=np.uint8)
    keep = np.zeros((n, 15), dtype=bool)
    for j in range(4):
        col = 4 * j
        chars[:, col : col + 3] = _DIGITS[octets[:, j]]
        keep[:, col : col + 3] = np.arange(3) < _DIGIT_COUNT[octets[:, j]][:, None]
        if j < 3:
            keep[:, col + 3] = True
    return chars, keep


def ipv4_text(arr: pa.Array) -> pa.Array:
    """A ClickHouse IPv4 column (uint32) as its dotted-quad text."""
    values = _uint32s(arr)
    octets = np.stack([(values >> s) & 0xFF for s in (24, 16, 8, 0)], axis=1).astype(np.intp)
    chars, keep = _dotted(octets)
    return _text_of(arr, chars, keep)


def ipv6_text(arr: pa.Array) -> pa.Array:
    """A ClickHouse IPv6 column (16 bytes, network order) as the text inet_out writes
    (``ir_arrow._pg_address``): RFC 5952 hextets with the first longest run of two or more zero
    hextets as ``::``, except an address whose first 80 bits are zero and next 16 zero or ffff
    (six zero hextets at its start, or five then ffff), which ends in a dotted quad."""
    raw = _fixed_bytes(arr)
    n = len(arr)
    words = raw.reshape(n, 8, 2).astype(np.uint16)
    words = (words[:, :, 0] << 8) | words[:, :, 1]
    zero = words == 0
    run = np.zeros((n, 8), dtype=np.int64)  # the zero run ending at each hextet
    for k in range(8):
        run[:, k] = (run[:, k - 1] + 1 if k else 1) * zero[:, k]
    best_len = run.max(axis=1)
    best_end = run.argmax(axis=1)  # the first longest run ends first
    best_start = best_end - best_len + 1
    squeeze = best_len >= 2
    k = np.arange(8)
    in_run = squeeze[:, None] & (k >= best_start[:, None]) & (k <= best_end[:, None])

    # Slots: a leading ':', then per hextet a ':' (from the second) and four hex digits, then a
    # trailing ':'.
    nibbles = np.stack([(words >> s) & 0xF for s in (12, 8, 4, 0)], axis=2)
    significant = np.maximum.accumulate(nibbles != 0, axis=2)
    significant[:, :, 3] = True
    chars = np.full((n, 41), _COLON, dtype=np.uint8)
    keep = np.zeros((n, 41), dtype=bool)
    keep[:, 0] = squeeze & (best_start == 0)
    for i in range(8):
        if i:  # the ':' before hextet i; inside the run only the first is kept
            keep[:, 5 * i] = ~(in_run[:, i] & (best_start < i))
        chars[:, 5 * i + 1 : 5 * i + 5] = _HEX[nibbles[:, i]]
        keep[:, 5 * i + 1 : 5 * i + 5] = significant[:, i] & ~in_run[:, i][:, None]
    keep[:, 40] = squeeze & (best_end == 7)
    text = _text_of(arr, chars, keep)

    mapped = (best_start == 0) & ((best_len == 6) | ((best_len == 5) & (words[:, 5] == 0xFFFF)))
    if not mapped.any():
        return text
    tail_chars, tail_keep = _dotted(raw[:, 12:16].astype(np.intp))
    head = np.where(best_len == 6, 2, 7)  # "::" or "::ffff:"
    head_chars = np.broadcast_to(np.frombuffer(b"::ffff:", dtype=np.uint8), (n, 7))
    head_keep = np.arange(7) < head[:, None]
    dotted = _text_of(
        arr,
        np.concatenate([head_chars, tail_chars], axis=1),
        np.concatenate([head_keep, tail_keep], axis=1),
    )
    return _call("if_else", pa.array(mapped), dotted, text)


_UUID_LAYOUT = (4, 2, 2, 2, 6)  # bytes per dash-separated group


def uuid_text(arr: pa.Array) -> pa.Array:
    """A UUID column (16 bytes, RFC 4122 order) as its canonical lowercase text."""
    raw = _fixed_bytes(arr)
    n = len(arr)
    hexes = np.empty((n, 16, 2), dtype=np.uint8)
    hexes[:, :, 0] = _HEX[raw >> 4]
    hexes[:, :, 1] = _HEX[raw & 0xF]
    groups, at = [], 0
    for size in _UUID_LAYOUT:
        if groups:
            groups.append(np.full((n, 1), _DASH, dtype=np.uint8))
        groups.append(hexes[:, at : at + size].reshape(n, 2 * size))
        at += size
    chars = np.concatenate(groups, axis=1)
    return _text_of(arr, chars, np.ones((n, 36), dtype=bool))


# -- Timestamps and integers ---------------------------------------------------------------------


_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


def _unit_of(t: CHType) -> str:
    """The Arrow unit of a ClickHouse ``DateTime`` (seconds) or ``DateTime64(scale)``: the unit
    ClickHouse's Arrow output writes that scale in."""
    if t.base_type == "DateTime":
        return "s"
    scale = t.scale
    if scale == 0:
        return "s"
    if scale <= 3:
        return "ms"
    if scale <= 6:
        return "us"
    return "ns"


def _rescaled(ticks: pa.Array, from_unit: str, to_unit: str) -> pa.Array:
    """Epoch ``ticks`` (int64) of ``from_unit`` in ``to_unit``, a finer value floored."""
    have, want = _PER_SECOND[from_unit], _PER_SECOND[to_unit]
    if have == want:
        return ticks
    if have < want:
        return _call("multiply", ticks, pa.scalar(want // have, pa.int64()))
    divisor = pa.scalar(have // want, pa.int64())
    whole = _call("divide", ticks, divisor)  # toward zero
    over = _call("greater", _call("multiply", whole, divisor), ticks)
    return _call("subtract", whole, over.cast(pa.int64()))


def _timestamps(arr: pa.Array, unit: str, zone: tzinfo | None) -> pa.Array:
    """A DateTime (uint32 seconds) or DateTime64 (an Arrow timestamp) column as a timestamp of
    ``unit`` (the server type's, ``_unit_of``), in ``zone`` (naive when None)."""
    from_unit = "s" if pa.types.is_uint32(arr.type) else arr.type.unit
    ticks = _rescaled(arr.cast(pa.int64()), from_unit, unit)
    return ticks.cast(pa.timestamp(unit, tz=None if zone is None else zone_name(zone)))


def _micros(arr: pa.Array, t: CHType) -> pa.Array:
    """A converted column with each DateTime in it (nested too) as microseconds, a finer value
    floored: the precision of the row read's ``datetime``."""
    base = t.base_type
    if base == "Array":
        return _relist(arr, _micros(arr.values, t.element_type))
    if base == "Map":
        return pa.MapArray.from_arrays(
            arr.offsets,
            _micros(arr.keys, t.key_type),
            _micros(arr.items, t.value_type),
            mask=arr.is_null() if arr.null_count else None,
        )
    if base == "Tuple":
        fields = [_micros(arr.field(i), et) for i, et in enumerate(t.element_types)]
        names = [arr.type.field(i).name for i in range(arr.type.num_fields)]
        return pa.StructArray.from_arrays(
            fields, names=names, mask=arr.is_null() if arr.null_count else None
        )
    if base in _DATETIMES and arr.type.unit != "us":
        micros = _rescaled(arr.cast(pa.int64()), arr.type.unit, "us")
        return micros.cast(pa.timestamp("us", tz=arr.type.tz))
    return arr


def _holds_datetime(t: CHType) -> bool:
    base = t.base_type
    if base == "Array":
        return _holds_datetime(t.element_type)
    if base == "Map":
        return _holds_datetime(t.key_type) or _holds_datetime(t.value_type)
    if base == "Tuple":
        return any(_holds_datetime(et) for et in t.element_types)
    return base in _DATETIMES


def _column_micros(column: Any, t: CHType) -> Any:  # noqa: ANN401 — an Arrow Array or ChunkedArray
    if isinstance(column, pa.ChunkedArray):
        return pa.chunked_array(
            [_micros(chunk, t) for chunk in column.chunks],
            type=_micros(_empty(column.type), t).type,
        )
    return _micros(column, t)


def zone_name(zone: tzinfo) -> str:
    """The Arrow name of the zone clickhouse-connect gives a column: an IANA key, or a fixed
    offset as ``+HH:MM`` (``ir_arrow._zone`` reads both)."""
    key = getattr(zone, "key", None) or getattr(zone, "zone", None)
    if isinstance(key, str):
        return key
    if isinstance(zone, timezone):
        offset = zone.utcoffset(None)
        if offset == timedelta(0):
            return "UTC"
        sign = "-" if offset < timedelta(0) else "+"
        minutes = abs(offset) // timedelta(minutes=1)
        return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"
    raise TypeError(f"no Arrow zone name for {zone!r}")


def _wide_ints(arr: pa.Array, *, signed: bool) -> pa.Array:
    """A 128- or 256-bit integer column (little-endian bytes) as decimal256(76, 0): the same
    two's-complement bits, widened to 32 bytes."""
    raw = _fixed_bytes(arr)
    n, width = raw.shape
    if not signed and width == 32 and (raw[:, 31] & 0x80).any():
        # decimal256 is signed 256 bits: a UInt256 at or above 2**255 has no Arrow value.
        raise ValueError("a ClickHouse UInt256 value at or above 2**255 has no Arrow decimal256")
    wide = np.zeros((n, 32), dtype=np.uint8)
    wide[:, :width] = raw
    if signed and width < 32:
        wide[:, width:] = np.where(raw[:, width - 1 : width] & 0x80, 0xFF, 0)
    return pa.Array.from_buffers(
        pa.decimal256(76, 0),
        n,
        [_validity(arr), pa.py_buffer(np.ascontiguousarray(wide))],
        null_count=arr.null_count,
    )


# -- Rows ----------------------------------------------------------------------------------------


def row_builder(types: Sequence[CHType]) -> Callable[[Any], list[tuple]]:
    """``to_rows`` of a converted table: each value as clickhouse-connect's row read gives it."""
    restores = [_restore_of(t) for t in types]
    datetimes = [_holds_datetime(t) for t in types]

    def to_rows(data: Any) -> list[tuple]:  # noqa: ANN401 — a pyarrow Table or RecordBatch
        columns = []
        for column, t, restore, timed in zip(data.columns, types, restores, datetimes, strict=True):
            values = column_values(_column_micros(column, t) if timed else column)
            columns.append(values if restore is None else [restore(v) for v in values])
        return list(zip(*columns))

    return to_rows


Restore = Callable[[Any], Any]


def _restore_of(t: CHType) -> Restore | None:  # noqa: C901 — one branch per ClickHouse type
    """The Python value the row read gives of a converted cell, or None where it is the cell."""
    base = t.base_type
    if base == "IPv4":
        return _nullable(ipaddress.IPv4Address)
    if base == "IPv6":
        return _nullable(ipaddress.IPv6Address)
    if base == "UUID":
        return _nullable(uuid.UUID)
    if base in _WIDE_INTS:
        return _nullable(_int_of)
    if base == "Array":
        element = _restore_of(t.element_type)
        if element is None:
            return None
        return _nullable(lambda v: [element(e) for e in v])
    if base == "Map":
        key = _restore_of(t.key_type) or _same
        item = _restore_of(t.value_type) or _same
        return _nullable(lambda v: {key(k): item(i) for k, i in v})
    if base == "Tuple":
        elements = [_restore_of(et) or _same for et in t.element_types]
        return _nullable(lambda v: tuple(r(e) for r, e in zip(elements, v.values(), strict=True)))
    return None


def _int_of(value: Decimal) -> int:
    return int(value)


def _same(value: Any) -> Any:  # noqa: ANN401 — any cell
    return value


def _nullable(restore: Restore) -> Restore:
    return lambda v: None if v is None else restore(v)

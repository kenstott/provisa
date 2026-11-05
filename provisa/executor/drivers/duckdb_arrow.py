# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c0a9e-58d2-4b17-9e41-c2a7d05b8e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB results read as Arrow, and the rows a row consumer is given from them (REQ-2194).

DuckDB's reads are taken from its Arrow reader, never from its DB-API rows: the engine hands
the result on as Arrow, and a consumer that still takes Python rows has them built from the
batches as it reads them (``arrow_rows``).

The reading cursor exports Arrow losslessly (:data:`LOSSLESS_ARROW`). DuckDB's default export
drops a ``TIMETZ`` value's offset and carries a UUID as its text; the lossless export keeps both,
and carries the types Arrow has no match for (``HUGEINT``, ``UHUGEINT``, ``VARINT``, ``BIT``,
``TIMETZ``) as DuckDB's own bytes, decoded here. The rows ``arrow_rows`` builds are the values
DuckDB's own Python rows hold, at every depth, with two exceptions DuckDB's rows get wrong and its
Arrow export does not: an interval keeps its months (``1 year 2 months`` is not 420 days) and an
infinite date or timestamp is the IR infinity, not the last date Python holds
(``ir_arrow.column_values``). A ``TIMESTAMP_NS`` or ``TIME_NS`` is read at microseconds,
truncated, as DuckDB's rows read it, and a ``TIME`` or ``TIMETZ`` at ``24:00:00``, which
PostgreSQL holds and Python's ``time`` does not, is the text DuckDB's rows hold for it.

An error the engine meets while its reader produces a later batch is raised then, past the point
where the statement was run, as an ``OSError``; ``answered_batches`` raises it there as PostgreSQL's
own error, or as the exception DuckDB raises for the same error met as the statement is run.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import time, timedelta, timezone
from typing import Any, Callable

import duckdb
import pyarrow as pa

from provisa.core.ir_arrow import column_values
from provisa.core.pg_error import from_engine
from provisa.core.ir_types import infinite_date, infinite_timestamp, interval_value

# Requirements: REQ-2194

#: Run on the reading cursor (session-scoped: the shared connection and its other cursors, the
#: Flight transport's among them, keep DuckDB's default export).
LOSSLESS_ARROW = "SET SESSION arrow_lossless_conversion = true"

_Fix = Callable[[Any], Any]

_DATE_INFINITY = 2**31 - 1
_TIMESTAMP_INFINITY = 2**63 - 1
# DuckDB's dtime_tz_t: the time's microseconds in the high 40 bits, and the offset in the low 24
# bits stored as MAX_OFFSET - offset_seconds.
_TIMETZ_MAX_OFFSET = 16 * 60 * 60 - 1
_DAY_SECONDS = 24 * 60 * 60
_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}
#: The time PostgreSQL holds and Python's ``time`` does not, as DuckDB's rows hold it.
_END_OF_DAY = "24:00:00"


#: DuckDB's exceptions by the kind its message opens with, spaced and cased as there ("Out of Range
#: Error: ..." is an OutOfRangeException).
_ENGINE_ERRORS: dict[str, type[duckdb.Error]] = {
    name.removesuffix("Exception").lower(): cls
    for name, cls in vars(duckdb).items()
    if name.endswith("Exception") and isinstance(cls, type) and issubclass(cls, duckdb.Error)
}


def _engine_error(error: OSError) -> Exception:
    """The exception DuckDB raises for the error its Arrow reader raised as ``error``; ``error``
    itself where its message names no kind DuckDB has an exception for."""
    message = str(error)
    kind, sep, _ = message.partition(" Error: ")
    raised = _ENGINE_ERRORS.get(kind.replace(" ", "").lower()) if sep else None
    return error if raised is None else raised(message)


def answered_batches(reader: Any) -> Iterator[pa.RecordBatch]:  # noqa: ANN401 — a reader
    """The batches of DuckDB's Arrow ``reader``. DuckDB produces them as they are read, so an
    error the engine meets in a later batch is raised here, as an ``OSError`` carrying its
    message: PostgreSQL's own (raised by ``provisa.core.pg_error.raising``) is raised as that
    PostgreSQL error, and any other as the exception DuckDB raises for it when it is met as the
    statement is run, so the client is answered with its SQLSTATE either way."""
    batches = iter(reader)
    while True:
        try:
            batch = next(batches)
        except StopIteration:
            return
        except OSError as error:
            answered = from_engine(error)
            raise (_engine_error(error) if answered is None else answered) from error
        yield batch


def _hugeint(raw: bytes) -> int:
    return int.from_bytes(raw, "little", signed=True)


def _uhugeint(raw: bytes) -> int:
    return int.from_bytes(raw, "little", signed=False)


def _varint(raw: bytes) -> str:
    # A three-byte header (sign in its top bit, set for a non-negative value) then the magnitude,
    # big-endian; a negative value has every byte inverted. DuckDB's rows read a VARINT as text.
    if raw[0] & 0x80:
        return str(int.from_bytes(raw[3:], "big"))
    return str(-int.from_bytes(bytes(b ^ 0xFF for b in raw[3:]), "big"))


def _bit(raw: bytes) -> str:
    # The first byte counts the padding bits that open the bit string; DuckDB's rows read it as
    # its '0'/'1' text.
    bits = "".join(f"{b:08b}" for b in raw[1:])
    return bits[raw[0] :]


def _clock(seconds: int, micro: int, tzinfo: timezone | None = None) -> time:
    minutes, second = divmod(seconds, 60)
    hour, minute = divmod(minutes, 60)
    return time(hour, minute, second, micro, tzinfo=tzinfo)


def _offset_text(offset: int) -> str:
    # DuckDB's spelling of a zone offset: its hours, then its minutes and seconds where not zero.
    sign = "-" if offset < 0 else "+"
    minutes, second = divmod(abs(offset), 60)
    hour, minute = divmod(minutes, 60)
    text = f"{sign}{hour:02d}"
    if minute or second:
        text += f":{minute:02d}"
    if second:
        text += f":{second:02d}"
    return text


def _timetz(raw: bytes) -> time | str:
    packed = int.from_bytes(raw, "little", signed=False)
    micros = packed >> 24
    offset = _TIMETZ_MAX_OFFSET - (packed & 0xFFFFFF)
    seconds, micro = divmod(micros, 1_000_000)
    if seconds >= _DAY_SECONDS:
        return _END_OF_DAY + _offset_text(offset)
    return _clock(seconds, micro, timezone(timedelta(seconds=offset)))


def _time(unit: str) -> _Fix:
    """A time from its stored count of ``unit``: at microseconds, truncated, as DuckDB's rows read
    it, and ``24:00:00`` as its text (``to_pylist`` reads that as midnight)."""
    per_second = _PER_SECOND[unit]

    def fix(stored: int) -> time | str:
        seconds, part = divmod(stored, per_second)
        if seconds >= _DAY_SECONDS:
            return _END_OF_DAY
        return _clock(seconds, part * 1_000_000 // per_second)

    return fix


_OPAQUE: dict[str, _Fix] = {
    "hugeint": _hugeint,
    "uhugeint": _uhugeint,
    "bignum": _varint,
    "varint": _varint,
    "bit": _bit,
    "time_tz": _timetz,
}


def _read_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every nanosecond timestamp read at microseconds, as DuckDB's rows read one."""
    if pa.types.is_timestamp(t) and t.unit == "ns":
        return pa.timestamp("us", t.tz)
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_read_type(t.value_type)))
    if pa.types.is_large_list(t):
        return pa.large_list(t.value_field.with_type(_read_type(t.value_type)))
    if pa.types.is_fixed_size_list(t):
        return pa.list_(t.value_field.with_type(_read_type(t.value_type)), t.list_size)
    if pa.types.is_struct(t):
        return pa.struct([f.with_type(_read_type(f.type)) for f in t])
    if pa.types.is_map(t):
        return pa.map_(
            t.key_field.with_type(_read_type(t.key_type)),
            t.item_field.with_type(_read_type(t.item_type)),
        )
    return t


def _nested_temporal(t: pa.DataType) -> _Fix:
    """A nested date or timestamp read from its stored integer, so an infinite one is the IR
    infinity rather than a value Python cannot hold, and one in a zone is in that zone as a
    top-level one is (ir_arrow.column_values: zoneinfo, never pytz)."""
    if pa.types.is_date32(t):
        end, infinite = _DATE_INFINITY, infinite_date
    else:
        aware = t.tz is not None
        end = _TIMESTAMP_INFINITY

        def infinite(sign: int) -> Any:
            return infinite_timestamp(sign, aware=aware)

    def fix(stored: int) -> Any:
        if stored == end:
            return infinite(1)
        if stored == -end:
            return infinite(-1)
        if pa.types.is_timestamp(t) and t.tz is not None:
            return column_values(pa.array([stored], pa.int64()).cast(t))[0]
        return (
            pa.scalar(stored, pa.int64() if not pa.types.is_date32(t) else pa.int32())
            .cast(t)
            .as_py()
        )

    return fix


def _storage(t: pa.DataType) -> pa.DataType:
    """The type a column is read as before its values are fixed: nested dates and timestamps, and
    times at any depth, as their stored integers (see :func:`_nested_temporal`, :func:`_time`)."""
    if pa.types.is_date32(t) or pa.types.is_time32(t):
        return pa.int32()
    if pa.types.is_timestamp(t) or pa.types.is_time64(t):
        return pa.int64()
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_storage(t.value_type)))
    if pa.types.is_large_list(t):
        return pa.large_list(t.value_field.with_type(_storage(t.value_type)))
    if pa.types.is_fixed_size_list(t):
        return pa.list_(t.value_field.with_type(_storage(t.value_type)), t.list_size)
    if pa.types.is_struct(t):
        return pa.struct([f.with_type(_storage(f.type)) for f in t])
    if pa.types.is_map(t):
        return pa.map_(
            t.key_field.with_type(_storage(t.key_type)),
            t.item_field.with_type(_storage(t.item_type)),
        )
    return t


def _fix(t: pa.DataType) -> _Fix | None:
    """How a value of ``t``, as ``to_pylist`` gives it (after :func:`_storage`), becomes the
    value DuckDB's rows hold; None where it already is."""
    if isinstance(t, pa.OpaqueType) and t.vendor_name == "DuckDB" and t.type_name in _OPAQUE:
        return _OPAQUE[t.type_name]
    if pa.types.is_interval(t):
        return lambda v: interval_value(v.months, v.days, v.nanoseconds // 1000)
    if pa.types.is_date32(t) or pa.types.is_timestamp(t):
        return _nested_temporal(t)
    if pa.types.is_time(t):
        return _time(t.unit)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        item = _fix(t.value_type)
        if item is None:
            return None
        return lambda v: [None if x is None else item(x) for x in v]
    if pa.types.is_fixed_size_list(t):
        # DuckDB's rows read an ARRAY as a tuple.
        item = _fix(t.value_type)
        if item is None:
            return tuple
        return lambda v: tuple(None if x is None else item(x) for x in v)
    if pa.types.is_struct(t):
        fields = [(f.name, _fix(f.type)) for f in t]
        if all(f is None for _, f in fields):
            return None
        return lambda v: {
            name: (v[name] if f is None or v[name] is None else f(v[name])) for name, f in fields
        }
    if pa.types.is_map(t):
        # DuckDB's rows read a MAP as a dict.
        key, item = _fix(t.key_type), _fix(t.item_type)
        return lambda v: {
            (k if key is None else key(k)): (x if item is None or x is None else item(x))
            for k, x in v
        }
    return None


def _plain(column: pa.Array | pa.ChunkedArray) -> list[Any]:
    """A column that needs no fixing, as Python values. Through numpy where that gives the same
    values ``to_pylist`` does, five to ten times faster: text and decimals at any time (a null is
    None either way), numbers and booleans when the column holds no null (numpy would read one as
    NaN)."""
    t = column.type
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_decimal(t):
        return column.to_numpy(zero_copy_only=False).tolist()
    numeric = pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_boolean(t)
    if numeric and column.null_count == 0:
        return column.to_numpy(zero_copy_only=False).tolist()
    return column.to_pylist()


def _column(column: pa.Array | pa.ChunkedArray) -> list[Any]:
    read = _read_type(column.type)
    if read != column.type:
        column = column.cast(read, safe=False)
    t = column.type
    if pa.types.is_interval(t) or pa.types.is_date32(t) or pa.types.is_timestamp(t):
        return column_values(column)
    fix = _fix(t)
    if fix is None:
        return _plain(column)
    storage = _storage(t)
    values = (column if storage == t else column.cast(storage)).to_pylist()
    return [None if v is None else fix(v) for v in values]


def arrow_rows(data: pa.RecordBatch | pa.Table) -> list[tuple]:
    """The rows of a DuckDB Arrow batch or table, each value as DuckDB's own rows hold it (see the
    module docstring for the two it gets wrong and this does not)."""
    return list(zip(*(_column(c) for c in data.columns)))

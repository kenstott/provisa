# Copyright (c) 2026 Kenneth Stott
# Canary: 236d6c0a-c95b-4b20-8d0d-a8eb74ba6e6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Canonical IR types as Arrow types (REQ-846, REQ-1915).

A table's declared columns carry IR types (``provisa.core.ir_types``). When rows that arrive as
Python values have to travel as Arrow record batches — a staged Parquet batch, a replication
batch — this is the one place the Arrow type of each IR type is decided, so an all-NULL column
is typed by its declaration and never inferred.

``numeric`` has no declared precision in the IR. The default carries it as text, the exact
digits the source gave, which every store parses back without loss; a caller that lands into a
fixed decimal type passes that type instead.
"""

from __future__ import annotations

import ipaddress
import json
import math
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, localcontext
from typing import Any

import pyarrow as pa
from dateutil.relativedelta import relativedelta

from provisa.core.ir_types import (
    infinite_date,
    infinite_timestamp,
    interval_parts,
    interval_value,
    to_ir,
)

# Canonical IR name → the pyarrow factory name, or one of the two spellings decided below.
_IR_TO_ARROW: dict[str, str] = {
    "smallint": "int16",
    "integer": "int32",
    "bigint": "int64",
    "text": "string",
    "boolean": "bool_",
    "float": "float32",
    "double": "float64",
    "numeric": "decimal",
    "date": "date32",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",  # REQ-2050: an instant, in UTC with its zone marked
    "time": "string",
    "interval": "string",  # its text form: months, days and microseconds, which a store parses
    "uuid": "string",
    "bytea": "binary",
    "json": "string",
    "vector": "vector",  # REQ-424: an embedding, a list of float32
}


def arrow_type(ir_type: str, *, decimal: pa.DataType | None = None) -> pa.DataType:
    """The Arrow type of a canonical IR type. Raises on an unknown type (never a silent widen).
    ``decimal`` is the type a ``numeric`` column takes; None carries it as text."""
    spelling = _IR_TO_ARROW.get(to_ir(ir_type))
    if spelling is None:
        raise ValueError(f"no Arrow type for IR type {ir_type!r}")
    if spelling == "decimal":
        return decimal if decimal is not None else pa.string()
    if spelling == "timestamp":
        return pa.timestamp("us")
    if spelling == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if spelling == "vector":
        return pa.list_(pa.float32())
    return getattr(pa, spelling)()


def arrow_schema(
    columns: list[tuple[str, str]], *, decimal: pa.DataType | None = None
) -> pa.Schema:
    """The Arrow schema of ``columns`` ((name, IR type) pairs), in their order."""
    return pa.schema([(name, arrow_type(ir, decimal=decimal)) for name, ir in columns])


def arrow_value(value: Any, ir_type: str, *, decimal: pa.DataType | None = None) -> Any:
    """A row value as its column's Arrow type accepts it: JSON as its text; ``numeric`` as a
    Decimal for a decimal column, else as its text; ISO-8601 text in a date or timestamp column
    as that date or (UTC) timestamp."""
    if value is None:
        return None
    canonical = to_ir(ir_type)
    if canonical == "json" and not isinstance(value, str):
        return json.dumps(value)
    if canonical == "numeric":
        return Decimal(str(value)) if decimal is not None else str(value)
    if canonical == "interval" and isinstance(value, (timedelta, relativedelta)):
        months, days, micros = interval_parts(value)
        return f"{months} months {days} days {micros} microseconds"
    if canonical in ("time", "uuid", "interval") and not isinstance(value, str):
        return str(value)
    # A JSON source (Elasticsearch) gives a date as its ISO-8601 text; Arrow does not parse text.
    # A string that is not ISO-8601 raises ValueError here.
    if canonical == "timestamp" and isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    if canonical == "timestamptz":
        # REQ-2050: an instant. Text is ISO-8601; a value with no zone is UTC, as the readers
        # that normalize a zoned source (Oracle, SQL Server) hand it on.
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        if isinstance(parsed, datetime):
            return (
                parsed.replace(tzinfo=timezone.utc)
                if parsed.tzinfo is None
                else parsed.astimezone(timezone.utc)
            )
        return parsed
    if canonical == "date" and isinstance(value, str):
        return date.fromisoformat(value)
    # REQ-424: a source that holds an embedding as its JSON text gives the text.
    if canonical == "vector" and isinstance(value, str):
        return json.loads(value)
    return value


# DuckDB stores PostgreSQL's infinity and -infinity at the ends of the storage integer's range
# but one: a date as its day count, a timestamp as its microseconds.
_DATE_INFINITY = 2**31 - 1
_TIMESTAMP_INFINITY = 2**63 - 1


def column_values(column: pa.Array | pa.ChunkedArray) -> list[Any]:
    """An Arrow column's values as row values: ``to_pylist``, an interval column's each the IR
    interval it is (ir_types.interval_value) rather than Arrow's own ``MonthDayNano``, and an
    infinite date or timestamp the IR infinity it is (ir_types.infinite_date,
    infinite_timestamp), which ``to_pylist`` cannot hold."""
    if pa.types.is_interval(column.type):
        return [
            None if v is None else interval_value(v.months, v.days, v.nanoseconds // 1000)
            for v in column.to_pylist()
        ]
    if pa.types.is_date32(column.type):
        return _with_infinities(column, pa.int32(), _DATE_INFINITY, infinite_date)
    if pa.types.is_timestamp(column.type) and column.type.unit == "us":
        aware = column.type.tz is not None
        return _with_infinities(
            column,
            pa.int64(),
            _TIMESTAMP_INFINITY,
            lambda sign: infinite_timestamp(sign, aware=aware),
        )
    return column.to_pylist()


def _with_infinities(
    column: pa.Array | pa.ChunkedArray, storage: pa.DataType, end: int, infinite: Any
) -> list[Any]:
    import pyarrow.compute as pc

    # call_function: the comparison and selection kernels are generated at import, so a type
    # checker sees no pc.equal or pc.if_else (as scheduler/jobs.py reads them).
    raw = column.cast(storage)
    positive = pc.fill_null(pc.call_function("equal", [raw, pa.scalar(end, storage)]), False)
    negative = pc.fill_null(pc.call_function("equal", [raw, pa.scalar(-end, storage)]), False)
    infinite_at = pc.call_function("or", [positive, negative])
    if not pc.call_function("any", [infinite_at]).as_py():
        return column.to_pylist()
    finite = pc.call_function("if_else", [infinite_at, pa.scalar(None, column.type), column])
    return [
        infinite(1) if up else infinite(-1) if down else value
        for value, up, down in zip(
            finite.to_pylist(), positive.to_pylist(), negative.to_pylist(), strict=True
        )
    ]


def rows_to_batch(
    rows: list[dict],
    columns: list[tuple[str, str]],
    schema: pa.Schema,
    *,
    decimal: pa.DataType | None = None,
) -> pa.RecordBatch:
    """One record batch of ``rows`` (dicts keyed by column name) typed by ``schema``. A column a
    row does not carry is NULL in it."""
    arrays = [
        pa.array(
            [arrow_value(r.get(name), ir, decimal=decimal) for r in rows], type=schema.field(i).type
        )
        for i, (name, ir) in enumerate(columns)
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


_EPOCH_DATE = date(1970, 1, 1)
_EPOCH = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_JSON_TYPES = frozenset({"JSON", "JSONB"})


def row_batch(  # REQ-2194
    rows: Sequence[Any], names: Sequence[str], declared: Sequence[str | None] | None = None
) -> pa.RecordBatch:
    """Rows a row-returning engine fetched, as one Arrow record batch: the fetch boundary of every
    engine that has no columnar read yet (docs/arch/fetch-paths.md), so the encoders read Arrow
    from every engine. This is an interim cost -- each value passes through Python once more --
    until each engine is columnar.

    Each column holds its values as :func:`column_values` reads them back: the same Python value,
    of the same type (a Decimal with its own exponent, an aware datetime with its own offset), so
    what the encoder writes of the cell is what it writes of the value. Where Arrow cannot hold a
    column's values as they are, it holds what PostgreSQL writes of them (``declared``, the
    engine's type name for the column, says which type that is): a numeric of mixed scales or
    NaN as numeric_out's text (as a money or a float the numeric it is, at its widest scale), a
    time with a zone of differing offsets as timetz_out's text, an address as inet_out's or
    cidr_out's, a JSON document of varying keys as its JSON text, anything else as its str().
    Aware datetimes of differing offsets are held as instants in UTC."""
    width = len(names)
    types = list(declared) if declared else [None] * width
    arrays = [_column([row[i] for row in rows], types[i]) for i in range(width)]
    return pa.RecordBatch.from_arrays(arrays, names=list(names))


_FLOAT_TYPES = frozenset({"DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT4", "FLOAT8", "REAL"})
_ADDRESSES = (
    ipaddress.IPv4Address,
    ipaddress.IPv6Address,
    ipaddress.IPv4Interface,
    ipaddress.IPv6Interface,
    ipaddress.IPv4Network,
    ipaddress.IPv6Network,
)


def _declared(declared: str | None) -> str:
    return "" if declared is None else str(declared).strip().upper()


def _column(values: list[Any], declared: str | None) -> pa.Array:
    values = [bytes(v) if isinstance(v, (memoryview, bytearray)) else v for v in values]
    present = [v for v in values if v is not None]
    if not present:
        return pa.nulls(len(values))
    if any(getattr(v, "pg_infinity", 0) for v in present):
        held = _with_sentinels(values, present)
        if held is not None:
            return held
    if any(isinstance(v, relativedelta) for v in present):
        held = _intervals(values)
        if held is not None:
            return held
    if all(isinstance(v, time) and v.tzinfo is not None for v in present):
        held = _clocks(values)
        if held is not None:
            return held
    if all(isinstance(v, Decimal) for v in present):
        held = _decimals(values, present, _declared(declared))
        if held is not None:
            return held
    array = _inferred(values)
    if array is not None and _same_values(column_values(array), values):
        return array
    if all(isinstance(v, datetime) and v.tzinfo is not None for v in present):
        array = _instants(values)
        if array is not None:
            return array
    kind = _declared(declared)
    texts = [None if v is None else _text(v, kind) for v in values]
    return _inferred(texts) or pa.array([None if v is None else str(v) for v in values])


def _inferred(values: list[Any]) -> pa.Array | None:
    try:
        return pa.array(values)
    except (
        pa.ArrowInvalid,
        pa.ArrowTypeError,
        pa.ArrowNotImplementedError,
        OverflowError,
        TypeError,
        ValueError,
    ):
        return None


def _decimals(values: list[Any], present: list[Decimal], kind: str) -> pa.Array | None:
    """A numeric column: exact at the one scale its values share; else, for a money or a float,
    exact at its widest scale; else numeric_out's text of each."""
    finite = all(v.is_finite() for v in present)
    exponents = {v.as_tuple().exponent for v in present} if finite else set()
    if finite and (len(exponents) == 1 or kind == "MONEY" or kind in _FLOAT_TYPES):
        scale = max(0, -min(int(e) for e in exponents))
        if kind in _FLOAT_TYPES:
            return pa.array([None if v is None else float(v) for v in values], pa.float64())
        if len(exponents) == 1:
            held = values
        else:
            quantum = Decimal(1).scaleb(-scale)
            with localcontext() as wide:
                wide.prec = 1000  # quantize refuses a result past the context's 28 digits
                held = [None if v is None else v.quantize(quantum) for v in values]
        # J518 (J502b): a coefficient with a positive exponent (4E+38) has that many more
        # integer digits, as numeric_out writes it; past decimal256 it is numeric_out's text
        digits = max(
            len(v.as_tuple().digits) + max(0, int(v.as_tuple().exponent))
            for v in held
            if v is not None
        )
        precision = max(digits, scale, 1)
        if precision <= 76:
            kind_of = pa.decimal128 if precision <= 38 else pa.decimal256
            return pa.array(held, kind_of(max(precision, scale), scale))
    return pa.array([None if v is None else _numeric_text(v) for v in values], pa.string())


def _numeric_text(value: Decimal) -> str:
    """numeric_out: the plain digits, NaN and the infinities by name, no negative zero."""
    if value.is_nan():
        return "NaN"
    if value.is_infinite():
        return "Infinity" if value > 0 else "-Infinity"
    return format(value.copy_abs() if value.is_zero() else value, "f")


def _clocks(values: list[Any]) -> pa.Array | None:
    """A timetz column of one offset, as Arrow times of day on 1970-01-01 in that offset."""
    offsets = {v.utcoffset() for v in values if v is not None}
    if len(offsets) != 1:
        return None
    (offset,) = offsets
    if offset is None or offset.seconds % 60 or offset.microseconds:
        return None
    zone = timezone(offset)
    stamps = [
        None if v is None else datetime.combine(_EPOCH_DATE, v.replace(tzinfo=None), tzinfo=zone)
        for v in values
    ]
    try:
        return pa.array(stamps)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        return None


def _offset_text(offset: timedelta) -> str:
    """The offset as PostgreSQL writes it: +HH, then :MM and :SS only where not zero."""
    seconds = int(offset.total_seconds())
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}"
    if minutes or secs:
        text += f":{minutes:02d}"
    if secs:
        text += f":{secs:02d}"
    return text


def _timetz_text(value: time) -> str:
    clock = value.replace(tzinfo=None).isoformat()
    offset = value.utcoffset()
    return clock if offset is None else clock + _offset_text(offset)


def _pg_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str:
    """inet_net_ntop: an IPv6 address whose first 80 bits are zero and next 16 zero or ffff (its
    longest run of zero words, at its start, six long or five then ffff) ends in dotted quad."""
    if ip.version == 4:
        return str(ip)
    words = [int.from_bytes(ip.packed[k : k + 2], "big") for k in range(0, 16, 2)]
    best_base, best_len, cur_base, cur_len = -1, 0, -1, 0
    for k, word in enumerate([*words, 1]):
        if word == 0 and k < 8:
            cur_base, cur_len = (k, 1) if cur_base == -1 else (cur_base, cur_len + 1)
            continue
        if cur_base != -1 and (best_base == -1 or cur_len > best_len):
            best_base, best_len = cur_base, cur_len
        cur_base, cur_len = -1, 0
    if best_base != 0 or not (best_len == 6 or (best_len == 5 and words[5] == 0xFFFF)):
        return str(ip)
    head = "::" if best_len == 6 else "::ffff:"
    return head + str(ipaddress.IPv4Address(ip.packed[12:]))


def _address_text(value: Any, kind: str) -> str:
    """inet_out (an address with its whole prefix written without it) or cidr_out."""
    if isinstance(value, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
        ip, prefix = value.network_address, value.prefixlen
    elif isinstance(value, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)):
        ip, prefix = value.ip, value.network.prefixlen
    else:
        ip, prefix = value, value.max_prefixlen
    if kind != "CIDR" and prefix == ip.max_prefixlen:
        return _pg_address(ip)
    return f"{_pg_address(ip)}/{prefix}"


def _text(value: Any, kind: str) -> Any:
    """What PostgreSQL writes of a value Arrow cannot hold as it is (a list: each element's)."""
    if isinstance(value, str):
        return value
    if kind in _JSON_TYPES:
        return json.dumps(value)
    if isinstance(value, list):
        return [None if v is None else _text(v, "") for v in value]
    if isinstance(value, Decimal):
        return _numeric_text(value)
    if isinstance(value, time) and value.tzinfo is not None:
        return _timetz_text(value)
    if isinstance(value, _ADDRESSES):
        return _address_text(value, kind)
    return str(value)


def _instants(values: list[Any]) -> pa.Array | None:
    try:
        return pa.array(
            [None if v is None else v.astimezone(timezone.utc) for v in values],
            pa.timestamp("us", tz="UTC"),
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
        return None


def _with_sentinels(values: list[Any], present: list[Any]) -> pa.Array | None:
    """A date or timestamp column holding PostgreSQL's infinities, as the sentinels
    :func:`column_values` reads back as them."""
    if all(isinstance(v, datetime) for v in present):
        aware = {v.tzinfo is not None for v in present}
        if len(aware) != 1:
            return None
        zoned = aware.pop()
        epoch = _EPOCH_UTC if zoned else _EPOCH

        def micros(v: datetime) -> int:
            sign = getattr(v, "pg_infinity", 0)
            if sign:
                return sign * _TIMESTAMP_INFINITY
            delta = v - epoch
            return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds

        raw = pa.array([None if v is None else micros(v) for v in values], pa.int64())
        return raw.cast(pa.timestamp("us", tz="UTC" if zoned else None))
    if all(isinstance(v, date) and not isinstance(v, datetime) for v in present):

        def days(v: date) -> int:
            sign = getattr(v, "pg_infinity", 0)
            return sign * _DATE_INFINITY if sign else (v - _EPOCH_DATE).days

        return pa.array([None if v is None else days(v) for v in values], pa.int32()).cast(
            pa.date32()
        )
    return None


def _intervals(values: list[Any]) -> pa.Array | None:
    """An interval column some of whose values have months, as Arrow's month-day-nano."""
    if not all(v is None or isinstance(v, (timedelta, relativedelta)) for v in values):
        return None
    parts = [None if v is None else interval_parts(v) for v in values]
    array = pa.array(
        [None if p is None else (p[0], p[1], p[2] * 1000) for p in parts],
        pa.month_day_nano_interval(),
    )
    return array if _same_values(column_values(array), values) else None


def _same_values(back: list[Any], values: list[Any]) -> bool:
    return len(back) == len(values) and all(_same(b, v) for b, v in zip(back, values, strict=True))


def _same(back: Any, value: Any) -> bool:
    """Whether ``back`` is ``value`` to every writer of it: equal, of the same type, a Decimal
    with the same digits and exponent, a datetime or time at the same offset, a float of the same
    bits."""
    if value is None or back is None:
        return value is None and back is None
    if isinstance(value, tuple) and isinstance(back, list):
        # Arrow holds a tuple (a point's coordinates, a record) as a list: its writers take either.
        return len(back) == len(value) and all(
            _same(b, v) for b, v in zip(back, value, strict=True)
        )
    if type(back) is not type(value):
        return False
    if isinstance(value, Decimal):
        # numeric has no negative zero: -0.00 is 0.00 to PostgreSQL
        b, v = back.as_tuple(), value.as_tuple()
        return (b.digits, b.exponent) == (v.digits, v.exponent) and (
            back.is_zero() or b.sign == v.sign
        )
    if isinstance(value, float):
        return (back != back and value != value) or (
            back == value and math.copysign(1.0, back) == math.copysign(1.0, value)
        )
    if isinstance(value, (datetime, time)):
        return back == value and back.utcoffset() == value.utcoffset()
    if isinstance(value, (list, tuple)):
        return len(back) == len(value) and all(
            _same(b, v) for b, v in zip(back, value, strict=True)
        )
    if isinstance(value, dict):
        return list(back) == list(value) and all(_same(back[k], value[k]) for k in value)
    return bool(back == value)

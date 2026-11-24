# Copyright (c) 2026 Kenneth Stott
# Canary: 6e1b9f47-3d02-4c85-a7e6-0f9d2c58b314
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A numeric column's precision and scale, as the model records them (REQ-1961).

The model records a decision: a numeric column either declares its precision and scale, and is
then exact everywhere it is read, or declares none and is floating point everywhere. The
declaration lives in the column's stored type, in one spelling — ``numeric(p,s)`` — written by
:func:`numeric_type` and read by :func:`numeric_size`. Nothing else writes or parses it.
"""

# Requirements: REQ-1961

from __future__ import annotations

import re
from decimal import Decimal

import pyarrow as pa

_SIZED = re.compile(r"^\s*(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.IGNORECASE)
_DECIMAL128_MAX_PRECISION = 38
_DECIMAL256_MAX_PRECISION = 76


class NumericSizeInvalid(ValueError):
    """A precision and scale that no numeric column can have."""

    def __init__(self, precision: object, scale: object) -> None:
        self.precision = precision
        self.scale = scale
        super().__init__(
            f"numeric({precision},{scale}) is not a numeric type: precision is 1 to "
            f"{_DECIMAL256_MAX_PRECISION} and scale is 0 to the precision"
        )


def require_numeric_size(precision: object, scale: object) -> tuple[int, int]:
    """(precision, scale) when they size a numeric column, else :class:`NumericSizeInvalid`."""
    if (
        isinstance(precision, bool)
        or isinstance(scale, bool)
        or not isinstance(precision, int)
        or not isinstance(scale, int)
        or not 0 < precision <= _DECIMAL256_MAX_PRECISION
        or not 0 <= scale <= precision
    ):
        raise NumericSizeInvalid(precision, scale)
    return precision, scale


def numeric_type(precision: int, scale: int) -> str:
    """The stored type of a numeric column that declares ``precision`` and ``scale``."""
    precision, scale = require_numeric_size(precision, scale)
    return f"numeric({precision},{scale})"


def numeric_size(data_type: str | None) -> tuple[int, int] | None:
    """The (precision, scale) a stored type declares, or None when it declares none — a bare
    ``numeric``, or a type that is not numeric at all."""
    sized = _SIZED.match(data_type) if data_type else None
    if sized is None:
        return None
    return require_numeric_size(int(sized[1]), int(sized[2]))


def numeric_arrow_type(data_type: str | None) -> pa.DataType:
    """The Arrow type a numeric column is read as, and listed under: exact where the model
    declares its size, floating point where it does not."""
    size = numeric_size(data_type)
    if size is None:
        return pa.float64()
    precision, scale = size
    if precision <= _DECIMAL128_MAX_PRECISION:
        return pa.decimal128(precision, scale)
    return pa.decimal256(precision, scale)


# --- the engine edge -----------------------------------------------------------------------------

# ClickHouse's sized names fix the precision: Decimal64(4) is 18 digits, 4 of them after the point.
_CLICKHOUSE_SIZED = {"decimal32": 9, "decimal64": 18, "decimal128": 38, "decimal256": 76}
# Oracle's (and Snowflake's) NUMBER is a decimal: NUMBER(10,2) is stored as numeric(10,2).
DECIMAL_NAMES = ("decimal", "numeric", "dec", "number", *_CLICKHOUSE_SIZED)
_WRAPPED = re.compile(r"^\s*(?:nullable|lowcardinality)\s*\((.*)\)\s*$", re.IGNORECASE)
_REPORTED = re.compile(r"^\s*([a-z0-9]+)\s*(?:\((.*)\))?\s*$", re.IGNORECASE)


class ReportedNumericMalformed(ValueError):
    """An engine or source reported a decimal type whose size cannot be read."""

    def __init__(self, reported: str) -> None:
        self.reported = reported
        super().__init__(f"the reported type {reported!r} is a decimal whose size cannot be read")


def numeric_size_of(reported_type: str) -> tuple[int, int] | None:
    """The (precision, scale) an engine or source REPORTS for a column, read from the type as it
    prints it — the only report some engines give (Trino's ``decimal(10,2)``, DuckDB's
    ``DECIMAL(10,2)``, ClickHouse's ``Decimal(10, 2)`` and ``Decimal64(4)``, Postgres'
    ``numeric(10,2)``).

    None for a decimal reported with no size, and for a type that is not a decimal. A decimal
    whose size is there but unreadable raises :class:`ReportedNumericMalformed`: it is not an
    undeclared numeric.

    Called only where a type is taken from an engine or source, at registration and
    introspection; what is stored is :func:`numeric_type`'s one spelling, and everything after
    reads that through :func:`numeric_size`.
    """
    text = reported_type
    while (wrapped := _WRAPPED.match(text)) is not None:
        text = wrapped[1]
    named = _REPORTED.match(text)
    if named is None or named[1].lower() not in DECIMAL_NAMES:
        return None
    name, arguments = named[1].lower(), named[2]
    if arguments is None:
        if name in _CLICKHOUSE_SIZED:
            raise ReportedNumericMalformed(reported_type)  # these never print without a scale
        return None
    parts = [part.strip() for part in arguments.split(",")]
    if not all(part.isdigit() for part in parts):
        raise ReportedNumericMalformed(reported_type)
    numbers = [int(part) for part in parts]
    if name in _CLICKHOUSE_SIZED and len(numbers) == 1:
        size = (_CLICKHOUSE_SIZED[name], numbers[0])
    elif name not in _CLICKHOUSE_SIZED and len(numbers) == 1:
        size = (numbers[0], 0)  # decimal(10): no digits after the point
    elif name not in _CLICKHOUSE_SIZED and len(numbers) == 2:  # noqa: PLR2004
        size = (numbers[0], numbers[1])
    else:
        raise ReportedNumericMalformed(reported_type)
    try:
        return require_numeric_size(*size)
    except NumericSizeInvalid as invalid:
        raise ReportedNumericMalformed(reported_type) from invalid


def stored_type_of(reported_type: str) -> str:
    """The type to STORE for a column an engine or source reports as ``reported_type``: a sized
    decimal in the one canonical spelling, anything else as reported."""
    size = numeric_size_of(reported_type)
    return reported_type if size is None else numeric_type(*size)


def decimal_text(value: Decimal) -> str:
    """A DECIMAL as the JSON string of its exact digits (REQ-1867, REQ-1961): never a float,
    which loses digits past about 17 and cannot hold a value past 2^64. Positional, never an
    exponent: ``Decimal("1E+3")`` is ``"1000"``, ``Decimal("0E-7")`` is ``"0.0000000"``."""
    return format(value, "f")

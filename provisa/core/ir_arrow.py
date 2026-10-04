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

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa

from provisa.core.ir_types import to_ir

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
    "time": "string",
    "interval": "string",  # its text form: days, seconds and microseconds, which a store parses
    "uuid": "string",
    "bytea": "binary",
    "json": "string",
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
    if canonical == "interval" and isinstance(value, timedelta):
        return f"{value.days} days {value.seconds} seconds {value.microseconds} microseconds"
    if canonical in ("time", "uuid", "interval") and not isinstance(value, str):
        return str(value)
    # A JSON source (Elasticsearch) gives a date as its ISO-8601 text; Arrow does not parse text.
    # A string that is not ISO-8601 raises ValueError here.
    if canonical == "timestamp" and isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    if canonical == "date" and isinstance(value, str):
        return date.fromisoformat(value)
    return value


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

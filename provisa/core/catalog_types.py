# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f0d94-6b3e-4a81-9d5c-e1a48b7f3062
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Arrow type a registered column is listed under in the catalog (REQ-143, REQ-128).

One mapper, used wherever the catalog is listed: Arrow Flight, ``/data/catalog``, the MCP catalog
tools and the JDBC driver's metadata.
"""

from __future__ import annotations

import pyarrow as pa

from provisa.core.ir_types import to_ir

# the engine type -> Arrow type mapping
_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "boolean": pa.bool_(),
    "tinyint": pa.int8(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "real": pa.float32(),
    "double": pa.float64(),
    "double precision": pa.float64(),
    "float": pa.float32(),
    "decimal": pa.float64(),
    "numeric": pa.float64(),
    "varchar": pa.utf8(),
    "char": pa.utf8(),
    "text": pa.utf8(),
    "varbinary": pa.binary(),
    "bytea": pa.binary(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    # Arrow has no zoned time: a read delivers the time of day.
    "time with time zone": pa.time64("us"),
    "timetz": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
    "interval": pa.utf8(),
    "json": pa.utf8(),
    "jsonb": pa.utf8(),
    "uuid": pa.utf8(),
}


# Canonical IR type (core/ir_types.py) -> the Arrow type the catalog lists it under.
_IR_ARROW: dict[str, pa.DataType] = {
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "numeric": pa.float64(),
    "boolean": pa.bool_(),
    "text": pa.utf8(),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "timestamp": pa.timestamp("us"),
    "interval": pa.utf8(),
    "uuid": pa.utf8(),
    "bytea": pa.binary(),
    "json": pa.utf8(),
}


# How a natively nested type is spelled where one is still met in a stored type. Semantic SQL has
# no array, struct or map type (REQ-1965): nested data is a JSON column, and JSON is what every
# SQL catalog lists it as. Registration stores ``json`` for such a column; a spelling below is
# listed the same way, never as a list, a map or a struct.
_NESTED = ("array", "map", "row", "struct", "list")


def _is_nested(text: str) -> bool:
    return text.endswith("[]") or text.partition("(")[0].strip() in _NESTED


def _physical_type_to_arrow(column_type: str) -> pa.DataType:
    """The Arrow type a registered column type is listed under.

    A nested type is JSON (REQ-1965), listed as the catalog lists any ``json`` column. A scalar
    is named by the catalog's own table or by the product's one type vocabulary. A type neither
    names raises ``KeyError``: the caller lists the column as text and says so.
    """
    text = column_type.strip().lower()
    if _is_nested(text):
        return _ARROW_TYPE_MAP["json"]
    base = text.partition("(")[0].strip()
    # Parameterized scalars: decimal(10,2) -> decimal, varchar(20) -> varchar.
    if base in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[base]
    # Any other spelling an engine or a source reports (``character varying``, ``int4``,
    # ``timestamp without time zone``): the product's one type vocabulary names it.
    try:
        canonical = to_ir(text)
    except ValueError:
        canonical = None
    if canonical in _IR_ARROW:
        return _IR_ARROW[canonical]
    raise KeyError(f"Unmapped engine type: {column_type!r}")

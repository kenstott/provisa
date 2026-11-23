# Copyright (c) 2026 Kenneth Stott
# Canary: 5aa372fb-ef5e-4449-b53d-f0110951666d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark type of each Arrow type Provisa declares a column as (REQ-2270).

A table's declared types arrive as an Arrow schema (Flight SQL GetSchema). Each column is given the
Spark type that holds its values exactly; a type Spark has no exact equivalent for is refused by
column name, never widened or read as a string. Nothing here imports pyspark: the mapping is the
Spark type's own spelling, both as DDL (what ``DataSource.schema`` answers) and as Spark's
``simpleString`` (what the schema Spark hands back to the reader is compared with).
"""

# Requirements: REQ-2270

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

#: Spark's largest decimal precision.
_MAX_DECIMAL_PRECISION = 38


class UnmappedType(ValueError):
    """An Arrow type no Spark type holds exactly."""


@dataclass(frozen=True)
class SparkType:
    """A Spark type as DDL (column names quoted) and as Spark's ``simpleString`` (unquoted)."""

    ddl: str
    simple: str


def _quoted(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _scalar(arrow: pa.DataType) -> str | None:
    """The Spark spelling of a scalar Arrow type, or None when it is not a scalar this maps."""
    if pa.types.is_boolean(arrow):
        return "boolean"
    if pa.types.is_int8(arrow):
        return "tinyint"
    if pa.types.is_int16(arrow) or pa.types.is_uint8(arrow):
        return "smallint"
    if pa.types.is_int32(arrow) or pa.types.is_uint16(arrow):
        return "int"
    if pa.types.is_int64(arrow) or pa.types.is_uint32(arrow):
        return "bigint"
    if pa.types.is_uint64(arrow):
        return "decimal(20,0)"
    if pa.types.is_float16(arrow) or pa.types.is_float32(arrow):
        return "float"
    if pa.types.is_float64(arrow):
        return "double"
    if pa.types.is_decimal(arrow):
        if arrow.precision > _MAX_DECIMAL_PRECISION:
            return None
        return f"decimal({arrow.precision},{arrow.scale})"
    if (
        pa.types.is_string(arrow)
        or pa.types.is_large_string(arrow)
        or pa.types.is_string_view(arrow)
    ):
        return "string"
    if (
        pa.types.is_binary(arrow)
        or pa.types.is_large_binary(arrow)
        or pa.types.is_binary_view(arrow)
        or pa.types.is_fixed_size_binary(arrow)
    ):
        return "binary"
    if pa.types.is_date(arrow):
        return "date"
    if pa.types.is_timestamp(arrow):
        return "timestamp" if arrow.tz is not None else "timestamp_ntz"
    if pa.types.is_time(arrow):
        return "time(6)"
    if pa.types.is_duration(arrow):
        return "interval day to second"
    if pa.types.is_null(arrow):
        return "void"
    return None


def spark_type(arrow: pa.DataType, column: str) -> SparkType:
    """The Spark type holding ``arrow``'s values exactly; :class:`UnmappedType` naming ``column``
    when there is none."""
    scalar = _scalar(arrow)
    if scalar is not None:
        return SparkType(scalar, scalar)
    if (
        pa.types.is_list(arrow)
        or pa.types.is_large_list(arrow)
        or pa.types.is_fixed_size_list(arrow)
    ):
        element = spark_type(arrow.value_type, column)
        return SparkType(f"array<{element.ddl}>", f"array<{element.simple}>")
    if pa.types.is_map(arrow):
        key = spark_type(arrow.key_type, column)
        value = spark_type(arrow.item_type, column)
        return SparkType(f"map<{key.ddl},{value.ddl}>", f"map<{key.simple},{value.simple}>")
    if pa.types.is_struct(arrow):
        fields = [arrow.field(i) for i in range(arrow.num_fields)]
        members = [(f.name, spark_type(f.type, column)) for f in fields]
        ddl = ",".join(f"{_quoted(name)}:{t.ddl}" for name, t in members)
        simple = ",".join(f"{name}:{t.simple}" for name, t in members)
        return SparkType(f"struct<{ddl}>", f"struct<{simple}>")
    raise UnmappedType(f"column {column!r} is declared {arrow}, which no Spark type holds exactly")


def spark_columns(schema: pa.Schema) -> list[tuple[str, SparkType]]:
    """Each column of ``schema`` with its Spark type, in order."""
    return [(field.name, spark_type(field.type, field.name)) for field in schema]


def spark_ddl(schema: pa.Schema) -> str:
    """``schema`` as the DDL a Python data source's ``schema()`` answers."""
    return ", ".join(f"{_quoted(name)} {t.ddl}" for name, t in spark_columns(schema))


def spark_simple(schema: pa.Schema) -> list[tuple[str, str]]:
    """Each column of ``schema`` with its Spark type's ``simpleString``, as a ``StructType`` Spark
    parsed from :func:`spark_ddl` lists them."""
    return [(name, t.simple) for name, t in spark_columns(schema)]

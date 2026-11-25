# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0c8e3a-7b41-4f6e-9a2d-c18e64b07f95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Oracle results read as Arrow through python-oracledb's data-frame fetch (REQ-2194).

A statement's result is taken from ``Connection.fetch_df_all`` / ``fetch_df_batches``, never from
the driver's Python rows: python-oracledb builds the Arrow columns itself. The columns keep the
types the statement declares:

- ``NUMBER(p,s)`` is ``decimal128(p,s)`` (``fetch_decimals``), exact as the model reads a sized
  numeric (REQ-1961); a ``NUMBER`` with no size, a ``FLOAT`` and ``BINARY_DOUBLE`` are
  ``float64``, ``BINARY_FLOAT`` ``float32``.
- ``DATE`` is a timestamp in seconds; ``TIMESTAMP(p)`` in seconds, milliseconds, microseconds or
  nanoseconds by its fractional precision.
- ``TIMESTAMP WITH LOCAL TIME ZONE`` is a timestamp in UTC, the session's zone
  (zoned.oracle_connection); a ``TIMESTAMP WITH TIME ZONE`` is cast to it in the database
  (zoned.instants_statement), so it too is the instant it is.
- ``CLOB``, ``NCLOB``, ``LONG`` and the character types are strings, ``BLOB``, ``RAW`` and
  ``LONG RAW`` binary: a LOB is read as its value (python-oracledb reads LOBs of up to 1 GB).
- ``INTERVAL YEAR TO MONTH`` and ``INTERVAL DAY TO SECOND`` are ``month_day_nano`` intervals.

A type python-oracledb has no Arrow form for is cast in the database before the fetch, as a
time-zoned timestamp is (:func:`arrow_statement`): ``ROWID`` and ``UROWID`` are read as their
text, ``JSON`` as its text typed as JSON in the result schema (the canonical ``arrow.json``
extension, as the DuckDB engine's Arrow types a JSON column), and ``XMLTYPE`` as its text. An
object type, a ``BFILE`` and a nested cursor are refused by name before the statement runs
(:class:`OracleArrowUnsupported`): the read is never retried as Python rows.
"""

# Requirements: REQ-2194, REQ-1961, REQ-2050

from __future__ import annotations

from typing import Any

import oracledb  # pyright: ignore[reportMissingImports]
import pyarrow as pa

from provisa.core.ir_arrow import column_values
from provisa.core.ir_types import landing_form

#: The database types python-oracledb's data-frame fetch reads as Arrow.
_ARROW_READABLE = frozenset(
    {
        oracledb.DB_TYPE_BINARY_DOUBLE,
        oracledb.DB_TYPE_BINARY_FLOAT,
        oracledb.DB_TYPE_BLOB,
        oracledb.DB_TYPE_BOOLEAN,
        oracledb.DB_TYPE_CHAR,
        oracledb.DB_TYPE_CLOB,
        oracledb.DB_TYPE_DATE,
        oracledb.DB_TYPE_INTERVAL_DS,
        oracledb.DB_TYPE_INTERVAL_YM,
        oracledb.DB_TYPE_LONG,
        oracledb.DB_TYPE_LONG_RAW,
        oracledb.DB_TYPE_NCHAR,
        oracledb.DB_TYPE_NCLOB,
        oracledb.DB_TYPE_NUMBER,
        oracledb.DB_TYPE_NVARCHAR,
        oracledb.DB_TYPE_RAW,
        oracledb.DB_TYPE_TIMESTAMP,
        oracledb.DB_TYPE_TIMESTAMP_LTZ,
        oracledb.DB_TYPE_TIMESTAMP_TZ,
        oracledb.DB_TYPE_VARCHAR,
        oracledb.DB_TYPE_VECTOR,
    }
)

#: The database types python-oracledb's data-frame fetch has no Arrow form for, each as the
#: expression over ``{col}`` that reads it as one it has, in the database (REQ-2194).
_CAST_IN_DATABASE: dict[Any, str] = {
    # The instant it is, given in the session's zone, UTC (REQ-2050, zoned.oracle_connection).
    oracledb.DB_TYPE_TIMESTAMP_TZ: "CAST({col} AS TIMESTAMP WITH LOCAL TIME ZONE)",
    # A universal rowid is at most 4000 bytes; a physical one is 18 characters.
    oracledb.DB_TYPE_ROWID: "CAST({col} AS VARCHAR2(4000))",
    oracledb.DB_TYPE_UROWID: "CAST({col} AS VARCHAR2(4000))",
    oracledb.DB_TYPE_JSON: "JSON_SERIALIZE({col} RETURNING CLOB)",
    # Its text, as an Oracle xml column lands (ir_types.landing_form).
    oracledb.DB_TYPE_XMLTYPE: landing_form("xml", "oracle"),
}

#: The relation a statement with a cast column is read from, and its columns' names in it.
_CAST_FROM, _CAST_COLUMN = "provisa_read", "c{}"

#: The zone a local time zone value is given in: the session's (zoned.oracle_connection).
_SESSION_ZONE = "UTC"


class OracleArrowUnsupported(ValueError):
    """A result column of a type python-oracledb's Arrow fetch cannot read (REQ-2194)."""

    def __init__(self, column: str, type_name: str) -> None:
        self.column = column
        self.type_name = type_name
        super().__init__(
            f"Oracle column {column!r} is of type {type_name}, which python-oracledb cannot "
            f"read as Arrow; select it cast to a type it reads (for example its text)"
        )


def _require_arrow_readable(description: Any) -> None:  # noqa: ANN401 — oracledb FetchInfo list
    # The first column neither read as Arrow nor cast in the database to a type that is: an
    # object type, a BFILE, a nested cursor.
    for column in description:
        if column.type_code not in _ARROW_READABLE and column.type_code not in _CAST_IN_DATABASE:
            raise OracleArrowUnsupported(column.name.lower(), column.type_code.name)


def _cast_select(sql: str, description: Any) -> str:  # noqa: ANN401 — oracledb FetchInfo list
    # ``sql`` with each column of a type that has no Arrow form cast in the database to one that
    # has; ``sql`` itself when it has none. Its columns are named in a subquery factoring, so a
    # column named by an expression is cast as any other.
    casts = [_CAST_IN_DATABASE.get(d.type_code) for d in description]
    if not any(casts):
        return sql
    names = [_CAST_COLUMN.format(i) for i in range(len(description))]
    items = ", ".join(
        name if cast is None else f"{cast.format(col=name)} AS {name}"
        for name, cast in zip(names, casts)
    )
    return f"WITH {_CAST_FROM} ({', '.join(names)}) AS ({sql}) SELECT {items} FROM {_CAST_FROM}"


def arrow_statement(cursor: Any, sql: str) -> tuple[str, Any]:  # noqa: ANN401 — oracledb cursor
    """The statement to read ``sql`` as Arrow, and ``sql``'s described result (None for one that
    answers no rows): ``sql`` is parsed on ``cursor`` (its result's columns described, nothing
    run); a column of a type python-oracledb has no Arrow form for is cast in the database
    (``_CAST_IN_DATABASE``), and one that cannot be is refused by name
    (:class:`OracleArrowUnsupported`) before the statement runs."""
    cursor.parse(sql)
    described = cursor.description
    if not described:
        return sql, described
    _require_arrow_readable(described)
    return _cast_select(sql, described), described


def _local_zone_columns(description: Any) -> list[int]:  # noqa: ANN401 — oracledb FetchInfo list
    # A zoned column has been cast to a local time zone one (arrow_statement): its
    # described type is the statement's own, so both name a column read in the session's zone.
    zoned = (oracledb.DB_TYPE_TIMESTAMP_LTZ, oracledb.DB_TYPE_TIMESTAMP_TZ)
    return [i for i, d in enumerate(description) if d.type_code in zoned]


def arrow_table(frame: Any, names: list[str], description: Any) -> pa.Table:  # noqa: ANN401
    """python-oracledb's data frame ``frame`` as a pyarrow table (through the Arrow PyCapsule
    stream, no copy), its columns named ``names`` (the statement's, lower case), each local
    time zone column marked as the UTC instants it holds and each JSON column (read as its text,
    :func:`arrow_statement`) typed as JSON."""
    table = pa.table(frame)
    columns = list(table.columns)
    for i in _local_zone_columns(description):
        column = columns[i]
        # Its stored values are already the UTC instants: naming the zone reinterprets, never
        # shifts, them.
        columns[i] = column.cast(pa.timestamp(column.type.unit, tz=_SESSION_ZONE))
    for i, d in enumerate(description):
        if d.type_code is oracledb.DB_TYPE_JSON:
            # Its text is the JSON: typing it so reinterprets, never converts, the values.
            columns[i] = columns[i].cast(pa.json_(columns[i].type))
    return pa.Table.from_arrays(columns, names=names)


def _row_column(column: pa.ChunkedArray | pa.Array) -> list[Any]:
    kind = column.type
    if pa.types.is_timestamp(kind) and kind.unit == "ns":
        # Python's datetime holds microseconds: a TIMESTAMP(7..9) row value is truncated to them,
        # as python-oracledb's own rows read it. The Arrow column keeps its nanoseconds.
        column = column.cast(pa.timestamp("us", tz=kind.tz), safe=False)
    return column_values(column)


def oracle_rows(data: pa.RecordBatch | pa.Table) -> list[tuple]:
    """The rows of an Oracle Arrow batch or table, for a consumer that reads rows: built from the
    Arrow columns as that consumer reads them (ir_arrow.column_values)."""
    return list(zip(*(_row_column(c) for c in data.columns)))

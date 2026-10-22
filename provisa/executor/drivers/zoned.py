# Copyright (c) 2026 Kenneth Stott
# Canary: afb19364-5cb3-4486-8750-ffc2fb63389a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A zoned timestamp read as the instant it is (REQ-2050), from the two drivers that would lose
it: SQL Server's ``datetimeoffset`` (pyodbc has no conversion for it) and Oracle's ``TIMESTAMP
WITH TIME ZONE`` (python-oracledb hands it on with its offset dropped) and ``TIMESTAMP WITH LOCAL
TIME ZONE`` (read in the session's zone). Each comes back as an aware datetime in UTC.
python-oracledb (Thin mode as Thick) hands a TIMESTAMP WITH TIME ZONE on as its wall time with its
offset dropped, whatever type a fetch asks for, so the instant is taken in the database: a
statement with such a column is run again with the column cast to TIMESTAMP WITH LOCAL TIME ZONE
(:func:`execute_instants`), which the database gives in the session's zone, UTC. Oracle's
INTERVAL YEAR TO MONTH, which python-oracledb hands on as its own ``IntervalYM``, is read here too,
as the IR interval it is (ir_types.interval_value)."""

from __future__ import annotations

import struct
from datetime import datetime, timedelta, timezone
from typing import Any

from provisa.core.ir_types import interval_value

# The ODBC SQL type of SQL Server's datetimeoffset.
SQL_SS_TIMESTAMPOFFSET = -155
# year, month, day, hour, minute, second, fraction (nanoseconds), offset hours, offset minutes.
_DATETIMEOFFSET = struct.Struct("<6hI2h")


def datetimeoffset(raw: bytes | None) -> datetime | None:
    """A ``datetimeoffset`` as ODBC hands it on (its SQL_SS_TIMESTAMPOFFSET_STRUCT), in UTC."""
    if raw is None:
        return None
    year, month, day, hour, minute, second, nanos, tz_hours, tz_minutes = _DATETIMEOFFSET.unpack(
        raw
    )
    local = datetime(year, month, day, hour, minute, second, nanos // 1000)
    offset = timezone(timedelta(hours=tz_hours, minutes=tz_minutes))
    return local.replace(tzinfo=offset).astimezone(timezone.utc)


def mssql_connection(conn: Any) -> Any:
    """A pyodbc connection to SQL Server (or Fabric/Synapse) that reads a datetimeoffset as the
    instant it is."""
    conn.add_output_converter(SQL_SS_TIMESTAMPOFFSET, datetimeoffset)
    return conn


def _oracle_ltz(value: datetime | None) -> datetime | None:
    # The session's zone is UTC (oracle_connection), the zone a local time zone value is read in.
    return None if value is None else value.replace(tzinfo=timezone.utc)


def _oracle_ym(value: Any) -> Any:
    # An oracledb.IntervalYM: its years and months, as the IR interval of that many months.
    return None if value is None else interval_value(value.years * 12 + value.months, 0, 0)


def _oracle_output_type(cursor: Any, metadata: Any) -> Any:
    import oracledb  # pyright: ignore[reportMissingImports]

    if metadata.type_code is oracledb.DB_TYPE_INTERVAL_YM:
        return cursor.var(
            oracledb.DB_TYPE_INTERVAL_YM, arraysize=cursor.arraysize, outconverter=_oracle_ym
        )
    # A local time zone value is given in the session's zone, UTC (a zoned one is cast to it,
    # execute_instants).
    if metadata.type_code is oracledb.DB_TYPE_TIMESTAMP_LTZ:
        return cursor.var(
            oracledb.DB_TYPE_TIMESTAMP_LTZ, arraysize=cursor.arraysize, outconverter=_oracle_ltz
        )
    return None


#: The relation a statement with a zoned column is read from, and its columns' names in it.
_ZONED_FROM, _ZONED_COLUMN = "provisa_zoned", "c{}"


def _zoned_select(sql: str, description: Any) -> str | None:
    """``sql`` with each TIMESTAMP WITH TIME ZONE column of its result (``description``) cast to
    TIMESTAMP WITH LOCAL TIME ZONE, in the database; None when it has none. Its columns are named
    in a subquery factoring, so a column named by an expression is read as any other."""
    import oracledb  # pyright: ignore[reportMissingImports]

    zoned = [d.type_code is oracledb.DB_TYPE_TIMESTAMP_TZ for d in description]
    if not any(zoned):
        return None
    names = [_ZONED_COLUMN.format(i) for i in range(len(description))]
    items = ", ".join(
        f"CAST({name} AS TIMESTAMP WITH LOCAL TIME ZONE) AS {name}" if is_zoned else name
        for name, is_zoned in zip(names, zoned)
    )
    return f"WITH {_ZONED_FROM} ({', '.join(names)}) AS ({sql}) SELECT {items} FROM {_ZONED_FROM}"


def execute_instants(cursor: Any, sql: str, bound: Any, **kw: Any) -> list[str] | None:
    """Run ``sql`` on ``cursor`` so a TIMESTAMP WITH TIME ZONE column is read as its instant: run
    as given, and -- when its result has such a column -- once more with the column cast in the
    database (:func:`_zoned_select`). The result's column names as the statement gives them
    (lower case), or None for a statement that answers no rows."""
    cursor.execute(sql, bound, **kw)
    if not cursor.description:
        return None
    names = [d.name.lower() for d in cursor.description]
    cast = _zoned_select(sql, cursor.description)
    if cast is not None:
        cursor.execute(cast, bound, **kw)
    return names


def oracle_connection(conn: Any) -> Any:
    """A python-oracledb connection that reads a zoned timestamp as the instant it is: the
    session is in UTC, the zone a local time zone value is given in (a zoned one is cast to it by
    :func:`execute_instants`). A year-to-month interval is read as its months."""
    with conn.cursor() as cur:
        cur.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
    conn.outputtypehandler = _oracle_output_type
    return conn

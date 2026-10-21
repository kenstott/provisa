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
python-oracledb's Thin mode, the default, ignores the session's NLS formats, so a TIMESTAMP WITH
TIME ZONE fetched as text came without its offset: it is fetched as a local time zone value
instead, which the database gives in the session's zone, UTC. Oracle's
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
    # A zoned value is fetched as a local time zone one: the database gives the instant in the
    # session's zone (UTC), and nothing is lost in a conversion on this side.
    zoned = metadata.type_code
    if zoned is oracledb.DB_TYPE_TIMESTAMP_TZ or zoned is oracledb.DB_TYPE_TIMESTAMP_LTZ:
        return cursor.var(
            oracledb.DB_TYPE_TIMESTAMP_LTZ, arraysize=cursor.arraysize, outconverter=_oracle_ltz
        )
    return None


def oracle_connection(conn: Any) -> Any:
    """A python-oracledb connection that reads a zoned timestamp as the instant it is: the
    session is in UTC, and a TIMESTAMP WITH TIME ZONE or WITH LOCAL TIME ZONE is fetched as a
    local time zone value, which the session gives in UTC. A year-to-month interval is read as its
    months."""
    with conn.cursor() as cur:
        cur.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
    conn.outputtypehandler = _oracle_output_type
    return conn

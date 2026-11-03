# Copyright (c) 2026 Kenneth Stott
# Canary: acf7b95b-5f72-4563-a566-481f4b5edc68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow values on the pgwire path (REQ-2167, REQ-2194): an Arrow column read by
ir_arrow.column_values and written by the pgwire encoders answers what PostgreSQL 17 answers.

- A timestamptz in a named zone is written in that zone by zoneinfo's rules, which PostgreSQL's
  agree with, not pytz's (which pyarrow's to_pylist takes, and which has no daylight time after
  2037, no transitions before 1901 and offsets rounded to the minute).
- A zoned nanosecond value whose local time is outside pandas' range (1677 to 2262) is written.
- A UUID held as a plain fixed_size_binary(16) is written as the UUID."""

# Requirements: REQ-2167, REQ-2194

from __future__ import annotations

import datetime
import random
import struct
import uuid
from zoneinfo import ZoneInfo, available_timezones

import pyarrow as pa
import pytest
from buenavista.postgres import BVTYPE_TO_PGTYPE, BVType

from provisa.core.ir_arrow import column_values

_UTC = datetime.timezone.utc
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=_UTC)
_PG_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=_UTC)
_TEXT, _BINARY = BVTYPE_TO_PGTYPE[BVType.TIMESTAMPTZ][1], BVTYPE_TO_PGTYPE[BVType.TIMESTAMPTZ][2]

# Fixed instants, each zone's text checked at all of them: before 1901 (pytz keeps LMT there), its
# transitions' era, and after 2037 (pytz has no daylight time there), January and July for both
# hemispheres.
_SAMPLE = [
    datetime.datetime(*fields, tzinfo=_UTC)
    for fields in [
        (2, 6, 15, 12),
        (1800, 1, 15, 12),
        (1850, 7, 15, 3, 30, 15, 250),
        (1890, 1, 1),
        (1900, 7, 1, 23, 59, 59, 999999),
        (1920, 1, 15, 6),
        (1945, 7, 15, 18),
        (1970, 1, 1),
        (1999, 12, 31, 23, 59, 59),
        (2024, 7, 1, 12),
        (2037, 1, 15, 12),
        (2038, 7, 15, 12),
        (2050, 1, 15, 12),
        (2050, 7, 15, 12),
        (2099, 7, 15, 12),
        (2101, 1, 15, 12),
        (2400, 7, 15, 12),
        (9998, 7, 15, 12),
    ]
]


def _instants(zone: str) -> list[datetime.datetime]:
    rng = random.Random(zone)
    low = int((datetime.datetime(2, 1, 1, tzinfo=_UTC) - _EPOCH).total_seconds())
    high = int((datetime.datetime(9998, 12, 1, tzinfo=_UTC) - _EPOCH).total_seconds())
    drawn = [
        _EPOCH
        + datetime.timedelta(seconds=rng.randint(low, high), microseconds=rng.randrange(10**6))
        for _ in range(30)
    ]
    return _SAMPLE + drawn


def _offset(seconds: int) -> str:
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}"
    if minutes or secs:
        text += f":{minutes:02d}"
    return text + (f":{secs:02d}" if secs else "")


def _pg_text(instant: datetime.datetime, zone: datetime.tzinfo) -> str:
    """timestamptz_out of ``instant`` with TimeZone ``zone`` (ISO DateStyle)."""
    local = instant.astimezone(zone)
    text = f"{local.year:04d}-{local:%m-%d %H:%M:%S}"
    if local.microsecond:
        text += f".{local.microsecond:06d}".rstrip("0")
    offset = local.utcoffset()
    assert offset is not None
    return text + _offset(int(offset.total_seconds()))


def _micros(instant: datetime.datetime) -> int:
    delta = instant - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


def _pg_binary(instant: datetime.datetime) -> bytes:
    delta = instant - _PG_EPOCH
    return struct.pack("!q", (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds)


@pytest.mark.parametrize("zone", sorted(available_timezones()))
def test_a_timestamptz_in_a_named_zone_is_written_as_postgresql_writes_it(zone: str) -> None:
    instants = _instants(zone)
    tz = ZoneInfo(zone)
    for unit, per_micro in (("us", 1), ("ns", 1000)):
        held = [i for i in instants if unit == "us" or 1678 <= i.year <= 2261]
        column = pa.array([_micros(i) * per_micro for i in held] + [None], pa.int64()).cast(
            pa.timestamp(unit, zone)
        )
        values = column_values(column)
        assert values[-1] is None
        got = [(_TEXT(v), _BINARY(v)) for v in values[:-1]]
        assert got == [(_pg_text(i, tz), _pg_binary(i)) for i in held], (zone, unit)


@pytest.mark.parametrize(
    ("zone", "instant", "text"),
    [
        # pytz: -05 (no daylight time after 2037)
        (
            "America/New_York",
            datetime.datetime(2050, 7, 15, 12, tzinfo=_UTC),
            "2050-07-15 08:00:00-04",
        ),
        # pytz: -04:56 (local mean time rounded to the minute)
        (
            "America/New_York",
            datetime.datetime(1880, 1, 1, tzinfo=_UTC),
            "1879-12-31 19:03:58-04:56:02",
        ),
        (
            "Asia/Kolkata",
            datetime.datetime(1850, 1, 1, tzinfo=_UTC),
            "1850-01-01 05:53:28+05:53:28",
        ),
        (
            "Australia/Lord_Howe",
            datetime.datetime(2060, 1, 1, tzinfo=_UTC),
            "2060-01-01 11:00:00+11",
        ),
        ("Europe/London", datetime.datetime(2040, 7, 1, tzinfo=_UTC), "2040-07-01 01:00:00+01"),
    ],
)
def test_postgresql_text_of_named_zones_where_pytz_differs(
    zone: str, instant: datetime.datetime, text: str
) -> None:
    column = pa.array([_micros(instant)], pa.int64()).cast(pa.timestamp("us", zone))
    assert _TEXT(column_values(column)[0]) == text
    # A value from to_pylist itself, in pytz's zone, is written by zoneinfo's rules too.
    assert _TEXT(column.to_pylist()[0]) == text


@pytest.mark.parametrize(
    ("zone", "nanos", "text"),
    [
        # local time after pandas' last instant, 2262-04-11 23:47:16.854775807
        ("Asia/Tokyo", 2**63 - 1, "2262-04-12 08:47:16.854775+09"),
        # local time before pandas' first instant, 1677-09-21 00:12:43.145224193 (and int64's
        # minimum, which pandas reads as NaT)
        ("America/New_York", -(2**63) + 1, "1677-09-20 19:16:41.145224-04:56:02"),
        ("America/New_York", -(2**63), "1677-09-20 19:16:41.145224-04:56:02"),
        ("Pacific/Kiritimati", 2**63 - 1001, "2262-04-12 13:47:16.854774+14"),
        # ns floored to the microsecond, as a negative value too
        ("UTC", -1, "1969-12-31 23:59:59.999999+00"),
    ],
)
def test_a_zoned_nanosecond_value_outside_pandas_range_is_written(
    zone: str, nanos: int, text: str
) -> None:
    column = pa.array([nanos], pa.int64()).cast(pa.timestamp("ns", zone))
    value = column_values(column)[0]
    instant = _EPOCH + datetime.timedelta(microseconds=nanos // 1000)
    assert (_TEXT(value), _BINARY(value)) == (text, _pg_binary(instant))


_UUIDS = [
    uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    uuid.UUID("00000000-0000-0000-0000-000000000000"),
    uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
]


@pytest.mark.parametrize("extension", [False, True], ids=["fixed_size_binary", "arrow.uuid"])
def test_a_uuid_held_as_16_bytes_is_written_as_the_uuid(extension: bool) -> None:
    column = pa.array([u.bytes for u in _UUIDS] + [None], pa.binary(16))
    if extension:
        column = pa.ExtensionArray.from_storage(pa.uuid(), column)
    text, binary = BVTYPE_TO_PGTYPE[BVType.UUID][1], BVTYPE_TO_PGTYPE[BVType.UUID][2]
    values = column_values(column)
    assert values[-1] is None
    assert [text(v) for v in values[:-1]] == [str(u) for u in _UUIDS]
    assert [binary(v) for v in values[:-1]] == [u.bytes for u in _UUIDS]

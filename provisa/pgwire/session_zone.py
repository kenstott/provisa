# Copyright (c) 2026 Kenneth Stott
# Canary: 7c31e0a4-58d2-4b19-a6f7-c24e9d0b1f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The TimeZone a session is on (REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED). No engine is told it,
so what a form writes in UTC is right only for a session on UTC: a statement that calls one of those
forms from any other zone is refused by name.

A zone comes from the startup packet, SET, SET LOCAL or set_config, and SET is undone by ROLLBACK
as in PostgreSQL. Where the zone cannot be told it is ``UNKNOWN_ZONE``, which is not UTC.

What pgwire writes of the zone itself is PostgreSQL's: a timestamptz sent as text is written in it,
binary is the instant whatever the zone, and SHOW and ParameterStatus name it as PostgreSQL does. A
zone PostgreSQL does not know is refused where it is named, and a value that cannot be written in
the session's zone is refused rather than written in another."""

# Requirements: REQ-2171

from __future__ import annotations

import datetime
import functools
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from buenavista.core import BVType

from provisa.core.pg_error import PostgresError
from provisa.transpiler.pg_forms_type_io import zoned_call
from provisa.transpiler.pg_forms_types import REFUSED

#: A zone no statement of this session names: a set_config of a parameter, a ROLLBACK TO.
UNKNOWN_ZONE = "unknown"
_UTC = frozenset({"utc", "etc/utc", "gmt", "etc/gmt"})
_SET = re.compile(
    r"^\s*SET\s+(?:(?P<scope>SESSION|LOCAL)\s+)?\"?TIME\s*ZONE\b\"?\s*(?:TO\b|=)?\s*"
    r"(?P<value>.*?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_RESET = re.compile(
    r"^\s*(?:RESET\s+(?:\"?TIME\s*ZONE\b\"?|ALL)|DISCARD\s+ALL)\s*;?\s*$", re.IGNORECASE
)
_ROLLED_BACK = re.compile(r"^\s*(?:ROLLBACK|ABORT)\b", re.IGNORECASE)
_TO_SAVEPOINT = re.compile(r"^\s*ROLLBACK\s+(?:(?:WORK|TRANSACTION)\s+)?TO\b", re.IGNORECASE)
_SET_CONFIG = re.compile(r"\bset_config\"?\s*\(\s*(?P<name>'(?:[^']|'')*')?", re.IGNORECASE)
_OF_ZONE = re.compile(
    r"\s*(?:::\s*\w+\s*)?,\s*'(?P<value>(?:[^']|'')*)'\s*(?:::\s*\w+\s*)?,\s*(?P<local>\w+)\s*\)",
    re.IGNORECASE,
)
_HOURS = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
_INTERVAL = re.compile(r"^INTERVAL\b", re.IGNORECASE)
_INTERVAL_OFFSET = re.compile(
    r"^INTERVAL\s*'(?P<sign>[+-]?)(?P<hours>\d+)(?::(?P<minutes>\d+))?'"
    r"\s*(?:HOUR(?:\s+TO\s+MINUTE)?)?$",
    re.IGNORECASE,
)
_POSIX_NAME = r"(?:<[A-Za-z0-9+-]+>|[A-Za-z]{3,})"
_POSIX_OFFSET = r"[+-]?\d{1,3}(?::\d{1,2}(?::\d{1,2})?)?"
_POSIX_FIXED = re.compile(rf"^{_POSIX_NAME}(?P<offset>{_POSIX_OFFSET})$")
_POSIX_RULED = re.compile(
    rf"^{_POSIX_NAME}{_POSIX_OFFSET}{_POSIX_NAME}(?:{_POSIX_OFFSET})?(?:,.*)?$"
)
_DAY_SECONDS = 86400


@dataclass(frozen=True)
class Zone:
    """A zone as PostgreSQL reads it: the name SHOW writes, and what a value is written in. A POSIX
    zone with a daylight rule has a name and no ``tz``: the rule is PostgreSQL's to compute."""

    name: str | None
    tz: datetime.tzinfo | None


_SERVER = Zone("UTC", datetime.timezone.utc)


@functools.cache
def _zone_names() -> dict[str, str]:
    """Each zone by its name in lower case: PostgreSQL finds a zone in any case."""
    return {name.lower(): name for name in available_timezones()}


def _invalid(word: str) -> PostgresError:
    return PostgresError("22023", f'invalid value for parameter "TimeZone": "{word}"')


def _offset_zone(east: int) -> Zone:
    """A zone of ``east`` seconds from UTC, as pg_tzset_offset names it: <-05>+05."""
    hours, rest = divmod(abs(east), 3600)
    minutes, seconds = divmod(rest, 60)
    written = f"{hours:02d}" + (f":{minutes:02d}" if rest else "")
    written += f":{seconds:02d}" if seconds else ""
    ahead, behind = ("-", "+") if east < 0 else ("+", "-")
    return Zone(
        f"<{ahead}{written}>{behind}{written}",
        datetime.timezone(datetime.timedelta(seconds=east)),
    )


def _posix_east(offset: str) -> int:
    """POSIX's offset is hours WEST of UTC."""
    west = offset.startswith("-")
    parts = [int(part) for part in offset.lstrip("+-").split(":")] + [0, 0]
    seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
    return seconds if west else -seconds


def _fixed(word: str, east: int, name: str | None = None) -> Zone:
    if abs(east) >= _DAY_SECONDS:
        raise _invalid(word)
    zone = _offset_zone(east)
    return zone if name is None else Zone(name, zone.tz)


@functools.lru_cache(maxsize=1024)
def read_zone(word: str) -> Zone:
    """What PostgreSQL's check_timezone reads of a value of TimeZone: an interval, a number of hours
    east of UTC, a zone's name in any case, else a POSIX zone. Any other is refused, 22023."""
    if _INTERVAL.match(word):
        offset = _INTERVAL_OFFSET.match(word)
        if offset is None:
            return Zone(None, None)
        east = int(offset["hours"]) * 3600 + int(offset["minutes"] or 0) * 60
        return _fixed(word, -east if offset["sign"] == "-" else east)
    if _HOURS.match(word):
        return _fixed(word, round(float(word) * 3600))
    name = _zone_names().get(word.lower())
    if name is not None:
        return Zone(name, ZoneInfo(name))
    fixed = _POSIX_FIXED.match(word)
    if fixed is not None:
        return _fixed(word, _posix_east(fixed["offset"]), word.upper())
    if _POSIX_RULED.match(word):
        return Zone(word.upper(), None)
    raise _invalid(word)


def _unwritable(zone: str | None) -> PostgresError:
    return PostgresError(
        "0A000", f"a timestamptz cannot be written as text in the session TimeZone {zone}"
    )


def on_utc(zone: str | None) -> bool:
    """Whether a session on ``zone`` reads what is written in UTC as PostgreSQL would write it."""
    return zone is None or zone.lower() in _UTC


class SessionZone:
    """The zone in force, ``current``, and the session's own, which a SET LOCAL leaves alone. None
    is the server's, which is UTC."""

    def __init__(self, started: str | None = None) -> None:
        self.started = started
        self.current = started
        self._session = started
        self._at_begin = started

    def _set(self, value: str | None, local: bool, in_block: bool) -> None:
        if local and not in_block:
            return  # PostgreSQL warns, and nothing changes
        self.current = value
        if not local:
            self._session = value

    def _said(self, value: str) -> str | None:
        word = value.strip().strip("'\"")
        if word.upper() in ("DEFAULT", "LOCAL"):
            return self.started
        read_zone(word)
        return word

    def _in_force(self) -> Zone | None:
        if self.current is None:
            return _SERVER
        return None if self.current == UNKNOWN_ZONE else read_zone(self.current)

    def shown(self) -> str | None:
        """The name SHOW writes; None where the zone cannot be told."""
        zone = self._in_force()
        return None if zone is None else zone.name

    def named(self) -> str:
        """The name SHOW writes, refused where the zone cannot be told."""
        shown = self.shown()
        if shown is None:
            raise PostgresError("0A000", f"the session TimeZone cannot be told: {self.current}")
        return shown

    def written_in(self) -> datetime.tzinfo:
        """What a timestamptz is written in as text, refused where that cannot be told."""
        zone = self._in_force()
        if zone is None or zone.tz is None:
            raise _unwritable(self.current)
        return zone.tz

    def _set_config(self, sql: str, in_block: bool) -> None:
        for call in _SET_CONFIG.finditer(sql):
            name = call.group("name")
            if name is not None and name.lower() != "'timezone'":
                continue
            given = None if name is None else _OF_ZONE.match(sql, call.end())
            if given is None:
                self._set(UNKNOWN_ZONE, False, in_block)
            else:
                local = given.group("local").lower() == "true"
                value = given.group("value").replace("''", "'")
                read_zone(value)
                self._set(value, local, in_block)

    def note(self, sql: str, was_open: bool, is_open: bool) -> None:
        """What ``sql`` does to the zone; the statement itself is answered as any is."""
        if not was_open and is_open:
            self._at_begin = self._session
        elif was_open and not is_open:
            if _ROLLED_BACK.match(sql):
                self._session = self._at_begin
            self.current = self._session
        elif _TO_SAVEPOINT.match(sql):
            if (self.current, self._session) != (self._at_begin, self._at_begin):
                self.current = self._session = UNKNOWN_ZONE
        if _RESET.match(sql):
            self._set(self.started, False, was_open)
            return
        written = _SET.match(sql)
        if written is None:
            self._set_config(sql, was_open)
        elif written.group("value").strip("'\""):  # PostgreSQL refuses an empty one
            local = (written.group("scope") or "").upper() == "LOCAL"
            self._set(self._said(written.group("value")), local, was_open)


def refuse_other_zone(sql: str, zone: str | None) -> None:
    """Refuse, by name, a statement that calls a form bound to UTC in a session on another zone."""
    if on_utc(zone):
        return
    name = zoned_call(sql)
    if name is not None:
        raise PostgresError(
            "0A000",
            f"{name} {REFUSED}: its form answers in UTC, and the session TimeZone is {zone}",
        )


def raw_row_formats(formats: list[int] | None, zone: str | None) -> list[int] | None:
    """The result formats a source's own DataRow bytes may be forwarded in, None for none: a source
    writes a timestamptz's text in its zone, and a cached copy of those bytes is every session's."""
    if on_utc(zone) or (formats and all(fmt == 1 for fmt in formats)):
        return formats
    return None


def _arrow_zone(tz: datetime.tzinfo) -> str:
    """The zone an Arrow timestamp column is written in: a zone's name, or a fixed offset."""
    key = getattr(tz, "key", None)
    if key is not None:
        return key
    offset = tz.utcoffset(None)
    if offset is None or offset.seconds % 60:
        raise ValueError(f"no Arrow time zone for {tz!r}")
    minutes = offset // datetime.timedelta(minutes=1)
    sign = "-" if minutes < 0 else "+"
    hours, minutes = divmod(abs(minutes), 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


def _in_zone(column: Any, zone: str) -> Any:
    import pyarrow as pa

    kind = column.type
    if not pa.types.is_timestamp(kind):
        return column
    if kind.tz is None:  # as the binary encoder refuses it, never taken for UTC
        raise ValueError("timestamptz value without a time zone")
    return column.cast(pa.timestamp(kind.unit, tz=zone))


class _ZonedData:
    """A result's batches with its ``columns`` written in ``zone``: an Arrow timestamp's zone is
    what timestamptz_out writes its local time and offset in, so nothing else of the encoder
    changes. The zone is the one in force when the batches are sent."""

    def __init__(self, data: Any, columns: list[int], zone: SessionZone) -> None:
        self._data = data
        self._columns = columns
        self._zone = zone
        self.raw = data.raw

    def take(self, limit: int = 0) -> Iterator[Any]:
        import pyarrow as pa

        if self.raw:
            raise _unwritable(self._zone.current)  # a portal opened on UTC, read after a SET
        try:
            zone = _arrow_zone(self._zone.written_in())
        except ValueError:
            raise _unwritable(self._zone.current) from None
        for batch in self._data.take(limit):
            arrays = list(batch.columns)
            for i in self._columns:
                arrays[i] = _in_zone(arrays[i], zone)
            yield pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)


class _WrittenInZone:
    """A result whose ``columns`` are sent as text written in ``zone`` (see ``_ZonedData``)."""

    def __init__(self, result: Any, columns: list[int], zone: SessionZone) -> None:
        self._result = result
        self._columns = columns
        self._zone = zone

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)

    def data(self) -> _ZonedData:
        return _ZonedData(self._result.data(), self._columns, self._zone)


def in_session_zone(result: Any, zone: SessionZone) -> Any:
    """``result`` with each timestamptz column sent as text written in the session's zone. A binary
    column is left as it is: it is the instant, whatever the zone."""
    if on_utc(zone.current):
        return result
    formats = result.result_format
    columns = [
        i
        for i in range(result.column_count())
        if result.column(i)[1] == BVType.TIMESTAMPTZ and not (formats and formats[i] == 1)
    ]
    return _WrittenInZone(result, columns, zone) if columns else result

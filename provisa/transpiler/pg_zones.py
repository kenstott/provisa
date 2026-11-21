# Copyright (c) 2026 Kenneth Stott
# Canary: 23c0673a-9f28-4fb3-be3b-efa86f13a0f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_timezone_names and pg_timezone_abbrevs (REQ-2167, amended 2026-11-03, Time zones follow
upstream tzdata). The zones are the pinned IANA release's
(the tzdata package pyproject pins, which zoneinfo and the wire encoder read): its zones and links
the engine knows, each with the release's reading of it now (abbreviation, offset, daylight). The abbreviations are PostgreSQL 17's Default set, its
configuration rather than the release's."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
import importlib.resources
import re
import zoneinfo

from provisa.pgwire.catalog_pg17_abbrevs import TIMEZONE_ABBREVS

#: The pinned release's zone names, links included: its ``zones`` list.
ZONE_NAMES: tuple[str, ...] = tuple(
    sorted(importlib.resources.files("tzdata").joinpath("zones").read_text().split())
)


#: The zone names and abbreviations PostgreSQL finds a zone's text among, in lower case, before it
#: reads the text as a POSIX zone.
_NAMED = frozenset(
    [name.lower() for name in ZONE_NAMES] + [abbrev.lower() for abbrev, _, _ in TIMEZONE_ABBREVS]
)
#: A POSIX zone of no daylight rule: an optional standard name, then its offset WEST of UTC.
_POSIX_FIXED = re.compile(
    r"(?:<[A-Za-z0-9+-]+>|[A-Za-z]{3,})?(?P<sign>[+-]?)(?P<hours>\d{1,3})"
    r"(?::(?P<minutes>\d{1,2})(?::(?P<seconds>\d{1,2}))?)?"
)
#: A numeric zone as DecodeTimezone reads it: a sign, then its offset EAST of UTC.
_NUMERIC = re.compile(
    r"(?P<sign>[+-])(?P<hours>\d{1,2})(?::?(?P<minutes>\d{2})(?::?(?P<seconds>\d{2}))?)?"
)
_DAY_MINUTES = 24 * 60


def _icu(found: re.Match[str] | None, east_sign: str) -> str | None:
    """The ICU zone of the offset ``found`` says, east when its sign is ``east_sign``; None for
    one no ICU zone has (seconds, or a day or more)."""
    if found is None or found["seconds"] not in (None, "0", "00"):
        return None
    minutes = int(found["hours"]) * 60 + int(found["minutes"] or 0)
    east = minutes if (found["sign"] or "+") == east_sign else -minutes
    if abs(east) >= _DAY_MINUTES:
        return None
    hours, rest = divmod(abs(east), 60)
    return f"GMT{'+' if east >= 0 else '-'}{hours:02d}:{rest:02d}"


def icu_fixed_zone(written: str) -> str | None:
    """The ICU zone DuckDB reads for a zone's text PostgreSQL reads as a POSIX zone of a fixed
    offset (pg_tzset, as timezone() and AT TIME ZONE look a zone up): ``'12'`` is twelve hours
    WEST of UTC, ``GMT-12:00``, and ``'+05:30'`` five and a half, ``GMT-05:30``. None for a text
    PostgreSQL finds by name or abbreviation, and for an offset no ICU zone has."""
    if written.lower() in _NAMED:
        return None
    return _icu(_POSIX_FIXED.fullmatch(written.strip()), "-")


def icu_numeric_zone(written: str) -> str | None:
    """The ICU zone of a numeric zone as make_timestamptz reads one (parse_sane_timezone, ISO
    8601's sign): ``'+05:30'`` is five and a half hours EAST of UTC, ``GMT+05:30``. None for any
    other text."""
    return _icu(_NUMERIC.fullmatch(written.strip()), "+")


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


#: How far ahead of the process's start the readings are taken: a zone's reading after it
#: is the one it has then.
_HORIZON = datetime.timedelta(days=2 * 366)

#: The step the readings are sampled at: shorter than any period a zone keeps one.
_STEP = datetime.timedelta(days=7)


#: A zone's reading at an instant: its abbreviation, its offset from UTC in seconds, and whether it
#: is daylight time (a nonzero daylight offset, a negative one too: Europe/Dublin's winter).
Reading = tuple[str, int, bool]


def _reading(instant: datetime.datetime, rules: zoneinfo.ZoneInfo) -> Reading:
    local = instant.astimezone(rules)
    offset = local.utcoffset() or datetime.timedelta(0)
    return local.tzname() or "", int(offset.total_seconds()), bool(local.dst())


def _changes(zone: str, start: datetime.datetime) -> list[tuple[int, Reading]]:
    """The readings ``zone`` takes from ``start`` to the horizon, each from the second (since the
    epoch) it takes effect: the first in effect at ``start``, from 0."""
    rules = zoneinfo.ZoneInfo(zone)
    changes = [(0, _reading(start, rules))]
    at = start
    while at < start + _HORIZON:
        after = at + _STEP
        if _reading(after, rules) != changes[-1][1]:
            low, high = at, after
            while high - low > datetime.timedelta(seconds=1):
                middle = low + (high - low) / 2
                same = _reading(middle, rules) == changes[-1][1]
                low, high = (middle, high) if same else (low, middle)
            changes.append((int(high.timestamp()), _reading(high, rules)))
        at = after
    return changes


def _readings() -> str:
    """Each pinned zone's readings, as (zone, effective second, abbreviation, offset in seconds,
    daylight) rows."""
    start = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    start -= datetime.timedelta(days=1)
    return ", ".join(
        f"({_quoted(zone)}, {since}, {_quoted(abbrev)}, {offset}, {'TRUE' if dst else 'FALSE'})"
        for zone in ZONE_NAMES
        for since, (abbrev, offset, dst) in _changes(zone, start)
    )


#: pg_timezone_names' rows: each zone the engine (DuckDB) knows, by its built-in table function's
#: qualified name, so a macro of the same name answering this is not it, read now as PostgreSQL
#: reads it from the pinned release: its abbreviation, offset and daylight flag in effect now.
#: DuckDB's own are its ICU release's: its abbreviation is the ICU zone's alias (Iceland for
#: Africa/Abidjan), and its offsets and daylight flags an older release's (America/Edmonton in
#: daylight time where the pinned release has it in standard time). The rows' own columns are
#: underscored, as a form's own names are.
NAMES = (
    "SELECT _name AS name, _said AS abbrev, to_seconds(_offset) AS utc_offset, _dst AS is_dst "
    "FROM system.main.pg_timezone_names() AS _n(_name, _icu_abbrev, _icu_offset, _icu_dst) "
    "JOIN (SELECT _zone, arg_max(_abbrev, _since) AS _said, arg_max(_seconds, _since) AS _offset, "
    "arg_max(_daylight, _since) AS _dst FROM (VALUES "
    + _readings()
    + ") AS _t(_zone, _since, _abbrev, _seconds, _daylight) WHERE _since <= epoch(now()) "
    "GROUP BY _zone) AS _a ON _a._zone = _n._name"
)

#: pg_timezone_abbrevs' rows. The rows' own columns are underscored, as a form's own names are.
ABBREVS = (
    "SELECT _abbrev AS abbrev, CAST(_utc_offset AS INTERVAL) AS utc_offset, _is_dst AS is_dst "
    "FROM (VALUES "
    + ", ".join(
        f"({_quoted(abbrev)}, {_quoted(offset)}, {'TRUE' if dst else 'FALSE'})"
        for abbrev, offset, dst in TIMEZONE_ABBREVS
    )
    + ") AS _a(_abbrev, _utc_offset, _is_dst)"
)

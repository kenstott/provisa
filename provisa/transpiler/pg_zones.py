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
(the tzdata package pyproject pins, which zoneinfo and the wire encoder read): its zones and links,
each with that release's reading of it now. The abbreviations are PostgreSQL 17's Default set, its
configuration rather than the release's."""

# Requirements: REQ-2167

from __future__ import annotations

import bisect
import datetime
import importlib.resources
import struct
import zoneinfo

from provisa.pgwire.catalog_pg17_abbrevs import TIMEZONE_ABBREVS

#: The pinned release's zone names, links included: its ``zones`` list.
ZONE_NAMES: tuple[str, ...] = tuple(
    sorted(importlib.resources.files("tzdata").joinpath("zones").read_text().split())
)


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


#: How far ahead of the process's start the readings are taken: a zone's reading after it is the
#: one it has then.
_HORIZON = datetime.timedelta(days=2 * 366)

#: The step the readings are sampled at: shorter than any period a zone keeps one.
_STEP = datetime.timedelta(days=7)

#: A zone's reading at an instant: its abbreviation, offset in seconds and daylight flag.
Reading = tuple[str, int, bool]


def _daylight_flags(zone: str) -> tuple[tuple[int, ...], tuple[bool, ...]]:
    """The pinned release's compiled ``zone`` (its TZif v2 body): each transition's second since
    the epoch, and the daylight flag of the local time type it begins."""
    data = importlib.resources.files("tzdata.zoneinfo").joinpath(*zone.split("/")).read_bytes()

    def counts(at: int) -> tuple[int, ...]:
        return struct.unpack(">6l", data[at + 20 : at + 44])

    isutc, isstd, leap, times, types, chars = counts(0)
    at = 44 + times * 5 + types * 6 + chars + leap * 8 + isstd + isutc
    _, _, _, times, types, _ = counts(at)
    at += 44
    seconds = struct.unpack(f">{times}q", data[at : at + 8 * times])
    begun = data[at + 8 * times : at + 9 * times]
    at += 9 * times
    flags = [bool(data[at + 6 * k + 4]) for k in range(types)]
    return seconds, tuple(flags[k] for k in begun)


def _changes(zone: str, start: datetime.datetime) -> list[tuple[int, Reading]]:
    """The readings ``zone`` takes from ``start`` to the horizon, each from the second (since the
    epoch) it takes effect: the first in effect at ``start``, from 0. The daylight flag is the
    one the local time type in effect carries, as PostgreSQL's localtime reads it; past the last
    transition it is the release's rule's (zoneinfo's daylight offset there, which a rule states;
    before it zoneinfo guesses one, and guesses 0 for Havana's CDT)."""
    rules = zoneinfo.ZoneInfo(zone)
    seconds, flags = _daylight_flags(zone)

    def said(instant: datetime.datetime) -> Reading:
        local = instant.astimezone(rules)
        offset = local.utcoffset() or datetime.timedelta()
        after = bisect.bisect_right(seconds, int(instant.timestamp()))
        if 0 < after < len(seconds):
            dst = flags[after - 1]
        else:
            dst = bool(local.dst())
        return local.tzname() or "", int(offset.total_seconds()), dst

    changes = [(0, said(start))]
    at = start
    while at < start + _HORIZON:
        after = at + _STEP
        if said(after) != changes[-1][1]:
            low, high = at, after
            while high - low > datetime.timedelta(seconds=1):
                middle = low + (high - low) / 2
                low, high = (middle, high) if said(middle) == changes[-1][1] else (low, middle)
            changes.append((int(high.timestamp()), said(high)))
        at = after
    return changes


def _readings() -> str:
    """Each pinned zone's readings, as (zone, effective second, abbreviation, offset seconds,
    daylight flag) rows."""
    start = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    start -= datetime.timedelta(days=1)
    return ", ".join(
        f"({_quoted(zone)}, {since}, {_quoted(abbrev)}, {offset}, {'TRUE' if dst else 'FALSE'})"
        for zone in ZONE_NAMES
        for since, (abbrev, offset, dst) in _changes(zone, start)
    )


#: pg_timezone_names' rows: each pinned zone's abbreviation, offset and daylight flag as
#: PostgreSQL computes them from the pinned release, the ones in effect now, read together so they
#: agree (DuckDB's own are its ICU zone's: Iceland for Africa/Abidjan's abbreviation, -08 for
#: Vancouver's MST). The rows' own columns are underscored, as a form's own names are.
NAMES = (
    "SELECT _zone AS name, _now._abbrev AS abbrev, to_seconds(_now._offset) AS utc_offset, "
    "_now._dst AS is_dst FROM (SELECT _zone, "
    "arg_max({'_abbrev': _abbrev, '_offset': _offset, '_dst': _dst}, _since) AS _now "
    "FROM (VALUES "
    + _readings()
    + ") AS _t(_zone, _since, _abbrev, _offset, _dst) WHERE _since <= epoch(now()) "
    "GROUP BY _zone) AS _a ORDER BY _zone"
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

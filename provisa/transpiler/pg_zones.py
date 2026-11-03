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
each with the engine's reading of it now. The abbreviations are PostgreSQL 17's Default set, its
configuration rather than the release's."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
import importlib.resources
import zoneinfo

from provisa.pgwire.catalog_pg17_abbrevs import TIMEZONE_ABBREVS

#: The pinned release's zone names, links included: its ``zones`` list.
ZONE_NAMES: tuple[str, ...] = tuple(
    sorted(importlib.resources.files("tzdata").joinpath("zones").read_text().split())
)


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


#: How far ahead of the process's start the abbreviations are read: a zone's abbreviation after it
#: is the one it has then.
_HORIZON = datetime.timedelta(days=2 * 366)

#: The step the abbreviations are sampled at: shorter than any period a zone keeps one.
_STEP = datetime.timedelta(days=7)


def _changes(zone: str, start: datetime.datetime) -> list[tuple[int, str]]:
    """The abbreviations ``zone`` takes from ``start`` to the horizon, each from the second (since
    the epoch) it takes effect: the first in effect at ``start``, from 0."""
    rules = zoneinfo.ZoneInfo(zone)

    def said(instant: datetime.datetime) -> str:
        return instant.astimezone(rules).tzname() or ""

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


def _abbreviations() -> str:
    """Each pinned zone's abbreviations, as (zone, effective second, abbreviation) rows."""
    start = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    start -= datetime.timedelta(days=1)
    return ", ".join(
        f"({_quoted(zone)}, {since}, {_quoted(abbrev)})"
        for zone in ZONE_NAMES
        for since, abbrev in _changes(zone, start)
    )


#: pg_timezone_names' rows: each pinned zone's offset and daylight flag as the engine (DuckDB) reads
#: it now, from its built-in table function by its qualified name, so a macro of the same name
#: answering this is not it; and its abbreviation as PostgreSQL computes it from the pinned
#: release, the one in effect now (DuckDB's own is its ICU zone's alias, Iceland for
#: Africa/Abidjan). The rows' own columns are underscored, as a form's own names are.
NAMES = (
    "SELECT _name AS name, _said AS abbrev, _utc_offset AS utc_offset, _is_dst AS is_dst "
    "FROM system.main.pg_timezone_names() AS _n(_name, _icu_abbrev, _utc_offset, _is_dst) "
    "JOIN (SELECT _zone, arg_max(_abbrev, _since) AS _said FROM (VALUES "
    + _abbreviations()
    + ") AS _t(_zone, _since, _abbrev) WHERE _since <= epoch(now()) GROUP BY _zone) AS _a "
    "ON _a._zone = _n._name"
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

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

import importlib.resources

from provisa.pgwire.catalog_pg17_abbrevs import TIMEZONE_ABBREVS

#: The pinned release's zone names, links included: its ``zones`` list.
ZONE_NAMES: tuple[str, ...] = tuple(
    sorted(importlib.resources.files("tzdata").joinpath("zones").read_text().split())
)


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


#: pg_timezone_names' rows: the engine's own (DuckDB's) of each pinned zone, read from its built-in
#: table function by its qualified name, so a macro of the same name answering this is not it.
NAMES = (
    "SELECT _n.name, _n.abbrev, _n.utc_offset, _n.is_dst FROM system.main.pg_timezone_names() _n "
    f"WHERE _n.name IN ({', '.join(map(_quoted, ZONE_NAMES))})"
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

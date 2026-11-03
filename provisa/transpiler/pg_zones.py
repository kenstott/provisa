# Copyright (c) 2026 Kenneth Stott
# Canary: 23c0673a-9f28-4fb3-be3b-efa86f13a0f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_timezone_abbrevs (REQ-2167): PostgreSQL 17's Default abbreviation set, its configuration
rather than the IANA release's."""

# Requirements: REQ-2167

from __future__ import annotations

from provisa.pgwire.catalog_pg17_abbrevs import TIMEZONE_ABBREVS


def _quoted(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


#: pg_timezone_abbrevs' rows.
ABBREVS = (
    "SELECT abbrev, CAST(utc_offset AS INTERVAL) AS utc_offset, is_dst FROM (VALUES "
    + ", ".join(
        f"({_quoted(abbrev)}, {_quoted(offset)}, {'TRUE' if dst else 'FALSE'})"
        for abbrev, offset, dst in TIMEZONE_ABBREVS
    )
    + ") AS _a(abbrev, utc_offset, is_dst)"
)

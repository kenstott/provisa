# Copyright (c) 2026 Kenneth Stott
# Canary: 4fd64bdb-6452-44c9-8688-1ed8e19a9ec5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Merge connector-discovered columns with steward-provided columns (REQ-252).

When a NoSQL/non-relational source table is registered with ``discover: true``, the
connector's inferred columns fill in the schema — but any explicitly-provided column
takes precedence (the steward's type/governance choices are authoritative).
"""

from __future__ import annotations

# Requirements: REQ-252


def _column_name(col) -> str:
    """Read a column name from a dict (discovered) or a model with a name attr."""
    if isinstance(col, dict):
        return col.get("name") or col.get("column_name") or ""
    return getattr(col, "name", None) or getattr(col, "column_name", "") or ""


def merge_discovered_columns(explicit: list, discovered: list) -> list:  # REQ-252
    """Return explicit columns plus any discovered column not already named explicitly.

    Explicit columns are kept verbatim and in order; discovered columns whose name
    collides with an explicit one are dropped (explicit wins). Remaining discovered
    columns are appended in their original order.
    """
    explicit_names = {_column_name(c).lower() for c in explicit if _column_name(c)}
    merged = list(explicit)
    for col in discovered:
        name = _column_name(col)
        if name and name.lower() not in explicit_names:
            merged.append(col)
            explicit_names.add(name.lower())
    return merged

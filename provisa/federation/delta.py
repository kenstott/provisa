# Copyright (c) 2026 Kenneth Stott
# Canary: 3c2d9a71-6b08-4e75-8f12-3c7a0d4f9c11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Delta-fetch incremental reload for MATERIALIZED replicas (REQ-874).

An incremental reload for datasets whose federation strategy is MATERIALIZED (REQ-826) —
the data is landed, so a full re-pull is the cost delta avoids. VIRTUAL and SCAN are
excluded (always fresh / read-in-place).

KEY SIMPLIFICATION — PROBE == DELTA for monotonic-cursor entries: the delta query IS the
freshness evaluation. Run it; a non-empty result means changed (apply the rows), empty means
fresh (no-op). No separate watermark/probe query.

The delta_query is ONE author-supplied, source-native query with two placeholders Provisa
SUBSTITUTES but never parses: ``$wm`` (bound to the stored cursor value) and ``{{fields}}``
(the table's registered selection set). The cursor field is implicit — the field ``$wm``
filters on — and after applying delta rows the stored cursor advances to max(cursor-field)
over the returned rows. This module is the UNIFORM part (field injection, the PROBE==DELTA
decision, cursor advance); the per-source-type authoring and native execution are elsewhere.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from provisa.federation.strategy import Strategy

_FIELDS_PLACEHOLDER = "{{fields}}"
_WM_PLACEHOLDER = "$wm"


def delta_applies(strategy: Strategy) -> bool:  # REQ-874
    """Delta reload is defined only for MATERIALIZED entries (VIRTUAL/SCAN are excluded)."""
    from provisa.federation.strategy import Strategy as _S

    return strategy is _S.MATERIALIZED


def render_delta_fields(template: str, fields: Sequence[str], *, separator: str = ", ") -> str:
    """Substitute the ``{{fields}}`` placeholder with the registered selection set (REQ-874).

    Pure textual substitution — Provisa never parses the source-native filter. ``$wm`` is left
    intact to be bound natively to the stored cursor value at execution.
    """
    return template.replace(_FIELDS_PLACEHOLDER, separator.join(fields))


def has_wm_placeholder(template: str) -> bool:
    """A well-formed delta_query must carry the ``$wm`` cursor placeholder."""
    return _WM_PLACEHOLDER in template


def delta_is_fresh(rows: Sequence[object]) -> bool:  # REQ-874 PROBE == DELTA
    """The delta query IS the freshness check: empty result ⇒ fresh (no-op), non-empty ⇒ changed."""
    return len(rows) == 0


def advance_cursor(
    rows: Sequence[dict], cursor_field: str, current: object | None
) -> object | None:  # REQ-874
    """Advance the stored cursor to max(cursor-field) over the returned delta rows.

    Empty result keeps the current cursor. Cursor and monotonicity are the registrant's
    responsibility; Provisa does no dedup or boundary-inclusivity logic.
    """
    values = [row[cursor_field] for row in rows if cursor_field in row]
    if not values:
        return current
    return max(values)

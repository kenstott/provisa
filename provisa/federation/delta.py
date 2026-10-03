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


# Why a delta reload falls back to a whole rebuild, by declared rule (REQ-874). Shown on the
# replica status as "whole rebuild: <reason>"; never a silent rebuild.
SKIP_NO_DELTA = "no_delta_declared"
SKIP_FIRST_BUILD = "first_build"  # no replica or no stored cursor yet
SKIP_DEFINITION = "definition_changed"  # columns/key/address changed, or reason model/definition
SKIP_OPERATOR = "operator_requested"  # an operator start-build
SKIP_REBUILD_EVERY = "rebuild_interval_elapsed"
SKIP_STORE = "store_cannot_apply_delta"


def delta_build_reason(
    *,
    has_delta: bool,
    has_cursor: bool,
    reason: str,
    definition_changed: bool,
    store_applies_delta: bool,
    rebuild_due: bool,
) -> str | None:
    """None when this build applies a delta; otherwise the declared ``delta_skipped`` code for
    why it falls back to a whole rebuild (REQ-874). The order is the rule's: no delta declared,
    then the cases that force a whole copy (first build, a definition change or a model/
    definition/operator request, the rebuild interval), then a store that cannot apply a delta.
    A delta read or apply that FAILS is a failed build, never routed here."""
    from provisa.federation import replica_state as rs

    if not has_delta:
        return SKIP_NO_DELTA
    if not has_cursor:
        return SKIP_FIRST_BUILD
    if definition_changed or reason in (rs.REASON_MODEL, rs.REASON_DEFINITION):
        return SKIP_DEFINITION
    if reason == rs.REASON_OPERATOR:
        return SKIP_OPERATOR
    if rebuild_due:
        return SKIP_REBUILD_EVERY
    if not store_applies_delta:
        return SKIP_STORE
    return None

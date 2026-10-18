# Copyright (c) 2026 Kenneth Stott
# Canary: bf9caa48-2fad-4907-96f9-09937313e34b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A materialized view's load management window: when consumers may read it (REQ-2066).

It is the window a source table has (REQ-1141's off-peak window, ``off_peak_window`` /
``off_peak_tz``), in the same place in the table's load management. On a source table it says when
Provisa may load from the source; on a materialized view, when the view may be read. Outside it a
read by a person's or a client's identity is refused by name, saying when the window next opens.

Provisa's own work is never refused: a refresh or a replica build (run under the system's
authorization, with no identity bound), the view cascade, and a scheduled job or trigger (bound as
``scheduler:<id>`` on the ``scheduler`` surface). A view with no window is unaffected.
"""

# Requirements: REQ-2066, REQ-1141

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from provisa.core.read_refusal import ReadRefused
from provisa.federation.scheduled_refresh import (
    OffPeakWindow,
    next_window_open,
    parse_off_peak_window,
    window_open,
)

#: The surface Provisa's scheduled jobs and triggers run on (scheduler/jobs.py, trigger_runs.py).
_SCHEDULER_SURFACE = "scheduler"


class ViewOutsideLoadWindow(ReadRefused):
    """A materialized view read outside its load management window."""

    code = "data.view_outside_load_window"

    def __init__(self, view: str, opens_at: float) -> None:
        when = datetime.fromtimestamp(opens_at, tz=timezone.utc).isoformat(timespec="minutes")
        super().__init__(
            f"the view {view!r} can be read only within its read window (its load management "
            f"window); it next opens at {when}"
        )
        self.params = {"view": view, "opens_at": when}


def users_read() -> bool:
    """Whether the read running now is a person's or a client's: an identity is bound, and it is
    not one of Provisa's scheduled jobs or triggers."""
    from provisa.audit.context import current_audit_identity

    identity = current_audit_identity()
    return identity is not None and identity.surface != _SCHEDULER_SURFACE


def _window(row: dict, state: Any) -> OffPeakWindow | None:
    """The load management window of ``row`` when it is a materialized view that has one."""
    if not (row.get("view_sql") and row.get("off_peak_window")):
        return None
    if state.mv_registry.get(f"view-{row['table_name']}") is None:
        return None  # a view that is not materialized is read through its SQL, never stored
    # A view's own zone, else its source's: the derived source's is UTC (Source.off_peak_tz).
    return parse_off_peak_window(row["off_peak_window"], row.get("off_peak_tz") or "UTC")


def refuse_outside_window(state: Any, table_ids: tuple[int, ...] | list[int]) -> None:
    """Refuse a user's read of a materialized view the statement names, outside its window."""
    if not users_read():
        return
    ids = set(table_ids)
    now = time.time()
    for row in getattr(state, "tables", None) or []:
        if row.get("id") not in ids:
            continue
        window = _window(row, state)
        if window is not None and not window_open(window, now):
            raise ViewOutsideLoadWindow(row["table_name"], next_window_open(window, now))


def refuse_named_outside_window(state: Any, sql: str) -> None:
    """Refuse a user's read of ``sql`` when a materialized view it names is outside its window.

    Judged on the statement as written, on every call: governance replaces each view by its SQL
    (``mv.view_read.semantic_statement``), so the governed statement's tables are the view's
    inputs, and a governed plan is kept and reused while the window opens and closes."""
    if not users_read():
        return
    named = {view for view in getattr(state, "view_sql_map", None) or {} if view in sql}
    if named:
        rows = getattr(state, "tables", None) or []
        refuse_outside_window(state, [r["id"] for r in rows if r.get("table_name") in named])


def readable_now(mvs: list, state: Any) -> list:
    """Of ``mvs`` (fresh views that could answer a statement in its place), those a user's read
    may be answered from now: a view outside its window is not read, and the statement is answered
    from what it names instead."""
    if not users_read():
        return mvs
    now = time.time()
    closed = {
        f"view-{row['table_name']}"
        for row in getattr(state, "tables", None) or []
        if (window := _window(row, state)) is not None and not window_open(window, now)
    }
    return [mv for mv in mvs if mv.id not in closed]

# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Deadline SOURCES — the two ways a claim's fire-by deadline is derived (REQ-961/962/963).

Periodic and live MVs share ONE claim+deadline primitive (REQ-959); only the deadline SOURCE differs.
A source answers three things the processor loop needs at claim time:

- ``window(now)`` — the calendar-addressable ``[start, end)`` the fire is pegged to (the as-of peg
  and the window-boundary land key), or ``None`` for a live MV (computes as-of *now*, REPLACE-lands,
  no peg).
- ``gated(now)`` — True when NO window exists (a business-day grain on a holiday): the MV
  deterministically does not fire and raises no alarm (REQ-962 existence gate).
- ``deadline(now, peeked)`` — the fire-by. Periodic: ``window.end + allowed_lateness`` (REQ-961/962).
  Live: ``min(last_change + quiet, first_change + max_delay)`` — trailing-edge debounce with a
  MANDATORY ``max_delay`` cap (REQ-963). ``None`` = fire immediately (real-time / no debounce).

``peeked`` is the un-claimed fan-in set ``[{created_at}]`` (only the live source reads it). The
processor treats "now past the deadline" as ready-to-fire and stamps that deadline on the claim for
the REQ-959 reaper.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from provisa.events.calendars import Calendar, Window, window_for


def _as_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class DeadlineSource(Protocol):
    """A claim's deadline derivation (REQ-961/963). Feeds the shared REQ-959 claim primitive."""

    def window(self, now: datetime) -> Window | None: ...

    def gated(self, now: datetime) -> bool: ...

    def deadline(self, now: datetime, peeked: list[dict]) -> datetime | None: ...

    def claim_deadline(self, now: datetime) -> datetime | None: ...


@dataclass(frozen=True)
class LiveDebounce:
    """Live (always-current) MV debounce (REQ-963): trailing-edge with a MANDATORY hard cap.

    ``deadline = min(last_change + quiet, first_change + max_delay)``. The ``max_delay`` cap is not
    optional — pure quiet-reset starves under continuous churn (a report over many busy tables never
    goes quiet), so the cap guarantees a fire and IS the staleness SLA. ``quiet <= 0`` disables
    debounce (real-time). A live MV has no window peg and is never gated."""

    quiet: float
    max_delay: float

    def __post_init__(self) -> None:
        if self.quiet > 0 and self.max_delay <= 0:
            raise ValueError(
                "REQ-963: live debounce with quiet>0 requires a positive max_delay cap "
                "(the mandatory staleness SLA); set quiet=0 to disable debounce"
            )

    def window(self, now: datetime) -> Window | None:
        return None  # live computes as-of now, no calendar peg

    def gated(self, now: datetime) -> bool:
        return False

    def deadline(self, now: datetime, peeked: list[dict]) -> datetime | None:
        if self.quiet <= 0:
            return None  # real-time — fire immediately
        stamps = [_as_utc(p["created_at"]) for p in peeked if p.get("created_at") is not None]
        if not stamps:
            return None
        quiet_deadline = max(stamps) + timedelta(seconds=self.quiet)
        cap = min(stamps) + timedelta(seconds=self.max_delay)
        return min(quiet_deadline, cap)

    def claim_deadline(self, now: datetime) -> datetime | None:
        # A stuck recompute past the staleness cap is reclaimable (REQ-959).
        return now + timedelta(seconds=self.max_delay) if self.quiet > 0 else None


_EPSILON = timedelta(microseconds=1)


@dataclass(frozen=True)
class PeriodicCalendar:
    """Periodic MV trigger = the CALENDAR boundary (REQ-962), bounded by the claim deadline
    ``window.end + allowed_lateness`` (REQ-961) — NOT a completeness barrier.

    A periodic fire SEALS the window that most recently CLOSED: at poll time ``now`` sits inside the
    OPEN window, so the target is the prior window whose end is the boundary just crossed. The fire is
    ready once ``now >= target.end + allowed_lateness`` (lateness defers the seal past the boundary).
    A business-day grain whose target lands on a holiday is GATED (no window → no fire, no alarm). The
    fire pegs as-of ``target.end``."""

    calendar: Calendar
    grain: str
    allowed_lateness: float = 0.0
    business_day: bool = False

    def window(self, now: datetime) -> Window | None:
        """The just-closed window (the one being sealed at ``now``): the window preceding the one that
        contains ``now``. None when the calendar gates it out (a business-day grain on a holiday)."""
        current = window_for(self.calendar, self.grain, now, business_day=False)
        assert current is not None  # business_day=False always yields a window
        return window_for(
            self.calendar, self.grain, current.start - _EPSILON, business_day=self.business_day
        )

    def gated(self, now: datetime) -> bool:
        return self.window(now) is None

    def deadline(self, now: datetime, peeked: list[dict]) -> datetime | None:
        del peeked  # periodic fires on the calendar boundary, not on fan-in timing
        win = self.window(now)
        return None if win is None else win.end + timedelta(seconds=self.allowed_lateness)

    def claim_deadline(self, now: datetime) -> datetime | None:
        return self.deadline(now, [])


def build_deadline_source(
    *,
    debounce_quiet: float,
    debounce_max_delay: float | None,
    calendar: Any | None = None,
    grain: str | None = None,
    allowed_lateness: float = 0.0,
    business_day: bool = False,
) -> DeadlineSource | None:
    """Resolve a node's deadline source from its config. A declared ``calendar`` → periodic
    (REQ-961/962); a ``debounce_quiet > 0`` → live debounce (REQ-963). Declaring BOTH is a
    contradiction and fails loud. Neither → ``None`` (real-time / event-driven fire)."""
    if calendar is not None and debounce_quiet > 0:
        raise ValueError(
            "a node is either periodic (calendar) or live (debounce), not both; "
            "declare one deadline source"
        )
    if calendar is not None:
        if grain is None:
            raise ValueError("a calendar deadline source requires a declared grain (REQ-962)")
        return PeriodicCalendar(
            calendar=calendar,
            grain=grain,
            allowed_lateness=allowed_lateness,
            business_day=business_day,
        )
    if debounce_quiet > 0:
        if debounce_max_delay is None:
            raise ValueError("REQ-963: live debounce (quiet>0) requires a max_delay staleness cap")
        return LiveDebounce(quiet=debounce_quiet, max_delay=debounce_max_delay)
    return None

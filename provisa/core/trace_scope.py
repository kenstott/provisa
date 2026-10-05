# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6b8d42-93ae-4c75-b0e7-6a2d9c4f5e18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Who gets a debug trace, and for how long (REQ-1910).

Tracing is lean by default. Debug detail is scoped and temporary, switched on two ways:

* The operator opens a WINDOW for an org, a role in an org, or a source in an org, for a stated
  number of minutes. The window ends on its own: a row past its ``expires_at`` covers nothing.
* A single request carries the debug-trace HINT (``@debugTrace``, ``-- @provisa trace=debug``,
  ``// @provisa trace=debug``, or the ``x-provisa-trace: debug`` gRPC metadata / HTTP header).
  A debug trace adds platform load, so the operator permits the hint per role; a hint from a role
  that is not permitted is rejected naming the setting (REQ-030), never silently ignored.

Both live in the platform control plane (``debug_trace_windows``, ``debug_trace_hint_roles``), so
every worker process and every instance resolves the same answer. The request path does not read
the control plane: it reads a snapshot held in the process and refreshed at most once per
``SNAPSHOT_TTL_SECONDS``, so a window opened on another instance takes effect here within that
many seconds, and expiry needs no refresh at all (it is a clock comparison against the snapshot).
"""

# Requirements: REQ-1910, REQ-030

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from provisa.core.operator_floor import OperatorFloorError
from provisa.core.schema_admin import debug_trace_hint_roles, debug_trace_windows

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from provisa.core.database import Database

# The operator setting a rejected hint names: the per-role permission for the debug-trace hint.
HINT_SETTING = "debug_trace_hint_roles"

SCOPES = ("org", "role", "source")

# A window is temporary by definition; a day is the longest one the operator can state.
MAX_WINDOW_MINUTES = 24 * 60

# How stale the request path's snapshot may be. A window opened or stopped on another instance
# takes effect here within this many seconds.
SNAPSHOT_TTL_SECONDS = 5.0

_monotonic = time.monotonic


class DebugTraceHintNotPermitted(OperatorFloorError):
    """A request carried the debug-trace hint from a role the operator has not permitted it for."""

    def __init__(self, role_id: str) -> None:
        super().__init__(
            f"the debug-trace hint is not permitted for role {role_id!r}: the operator's "
            f"{HINT_SETTING} setting does not include it. Remove the hint."
        )


@dataclass(frozen=True)
class DebugWindow:
    id: str
    scope: str  # one of SCOPES
    org_id: str
    target: str | None  # role id or source id; None for an org-wide window
    started_at: datetime
    expires_at: datetime
    created_by: str | None

    def covers(self, org_id: str | None, role_id: str, source_ids: Collection[str]) -> bool:
        if self.org_id != org_id:
            return False
        if self.scope == "org":
            return True
        if self.scope == "role":
            return self.target == role_id
        return self.target is not None and self.target in source_ids


@dataclass(frozen=True)
class TraceScopeSnapshot:
    """The operator's debug-trace settings as one process last read them."""

    windows: tuple[DebugWindow, ...]
    hint_roles: frozenset[tuple[str, str]]  # (org_id, role_id) pairs permitted the hint

    def debug_for(
        self,
        *,
        org_id: str | None,
        role_id: str,
        source_ids: Iterable[str],
        hint: bool,
        now: datetime,
    ) -> bool:
        """Whether this request is traced in debug mode.

        Raises :class:`DebugTraceHintNotPermitted` for a hint from a role the operator has not
        permitted — also inside an open window, since the hint is still a request the operator
        did not allow."""
        if hint:
            if (org_id, role_id) not in self.hint_roles:
                raise DebugTraceHintNotPermitted(role_id)
            return True
        sources = tuple(source_ids)
        return any(w.expires_at > now and w.covers(org_id, role_id, sources) for w in self.windows)


def _aware(moment: datetime) -> datetime:
    """The stored instant as UTC. SQLite hands back a naive datetime for a timezone-aware column,
    and comparing that to an aware ``now`` raises rather than answering."""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def _window(row: Any) -> DebugWindow:
    return DebugWindow(
        id=row.id,
        scope=row.scope,
        org_id=row.org_id,
        target=row.target,
        started_at=_aware(row.started_at),
        expires_at=_aware(row.expires_at),
        created_by=row.created_by,
    )


# ---------------------------------------------------------------------------
# The control-plane store
# ---------------------------------------------------------------------------


async def start_window(
    db: Database,
    *,
    scope: str,
    org_id: str,
    target: str | None,
    minutes: int,
    created_by: str | None,
    now: datetime | None = None,
) -> DebugWindow:
    """Open a debug-trace window that ends ``minutes`` from now."""
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {', '.join(SCOPES)}, got {scope!r}")
    if not 1 <= minutes <= MAX_WINDOW_MINUTES:
        raise ValueError(f"minutes must be between 1 and {MAX_WINDOW_MINUTES}, got {minutes}")
    if scope == "org":
        if target is not None:
            raise ValueError("an org window takes no target")
    elif not target:
        raise ValueError(f"a {scope} window needs a target: the {scope} id it covers")
    started = now or datetime.now(timezone.utc)
    window = DebugWindow(
        id=uuid.uuid4().hex,
        scope=scope,
        org_id=org_id,
        target=target,
        started_at=started,
        expires_at=started + timedelta(minutes=minutes),
        created_by=created_by,
    )
    async with db.acquire() as conn:
        # Rows past their end cover nothing; clearing them here keeps the table to live windows.
        await conn.execute_core(
            debug_trace_windows.delete().where(debug_trace_windows.c.expires_at <= started)
        )
        await conn.execute_core(
            debug_trace_windows.insert().values(
                id=window.id,
                scope=window.scope,
                org_id=window.org_id,
                target=window.target,
                started_at=window.started_at,
                expires_at=window.expires_at,
                created_by=window.created_by,
            )
        )
    invalidate()
    return window


async def stop_window(db: Database, window_id: str) -> bool:
    """End a window before its time. False when no such window exists."""
    async with db.acquire() as conn:
        result = await conn.execute_core(
            debug_trace_windows.delete().where(debug_trace_windows.c.id == window_id)
        )
    invalidate()
    return bool(result.rowcount)


async def list_windows(db: Database, now: datetime | None = None) -> list[DebugWindow]:
    """The windows still open, soonest to end first."""
    moment = now or datetime.now(timezone.utc)
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(debug_trace_windows)
            .where(debug_trace_windows.c.expires_at > moment)
            .order_by(debug_trace_windows.c.expires_at)
        )
        return [_window(row) for row in result.all()]


async def set_hint_permission(
    db: Database, org_id: str, role_id: str, permitted: bool, *, updated_by: str | None
) -> None:
    """Permit, or stop permitting, the per-request debug-trace hint for a role."""
    async with db.acquire() as conn:
        if permitted:
            await conn.upsert(
                debug_trace_hint_roles,
                {
                    "org_id": org_id,
                    "role_id": role_id,
                    "updated_at": datetime.now(timezone.utc),
                    "updated_by": updated_by,
                },
                index_elements=["org_id", "role_id"],
                update_columns=["updated_at", "updated_by"],
            )
        else:
            await conn.execute_core(
                debug_trace_hint_roles.delete().where(
                    (debug_trace_hint_roles.c.org_id == org_id)
                    & (debug_trace_hint_roles.c.role_id == role_id)
                )
            )
    invalidate()


async def list_hint_roles(db: Database) -> list[tuple[str, str]]:
    """The (org_id, role_id) pairs permitted the hint."""
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(debug_trace_hint_roles.c.org_id, debug_trace_hint_roles.c.role_id).order_by(
                debug_trace_hint_roles.c.org_id, debug_trace_hint_roles.c.role_id
            )
        )
        return [(row.org_id, row.role_id) for row in result.all()]


async def load_snapshot(db: Database) -> TraceScopeSnapshot:
    """Read the operator's debug-trace settings from the control plane."""
    return TraceScopeSnapshot(
        windows=tuple(await list_windows(db)),
        hint_roles=frozenset(await list_hint_roles(db)),
    )


# ---------------------------------------------------------------------------
# The request path: a snapshot held in the process, refreshed on a short TTL
# ---------------------------------------------------------------------------

# (the Database it was read from, when, the snapshot). One entry: a process has one control plane.
_held: tuple[Database, float, TraceScopeSnapshot] | None = None
_refreshing = threading.Lock()


def invalidate() -> None:
    """Drop the held snapshot, so this process's next request reads the control plane. Called by
    every write above: the instance that made a change sees it at once."""
    global _held
    _held = None


async def current_snapshot(db: Database) -> TraceScopeSnapshot:
    """The snapshot the request path resolves against — no control-plane read while it is fresh."""
    global _held
    held = _held
    if held is not None and held[0] is db and _monotonic() - held[1] < SNAPSHOT_TTL_SECONDS:
        return held[2]
    # One request thread refreshes; the others keep serving the snapshot they have rather than
    # queueing on the control plane. A thread with nothing to serve reads for itself.
    stale = held[2] if held is not None and held[0] is db else None
    owns_refresh = _refreshing.acquire(blocking=False)
    if stale is not None and not owns_refresh:
        return stale
    try:
        snapshot = await load_snapshot(db)
        _held = (db, _monotonic(), snapshot)
        return snapshot
    finally:
        if owns_refresh:
            _refreshing.release()


# ---------------------------------------------------------------------------
# Request entry: the one resolution every transport's request passes through
# ---------------------------------------------------------------------------


async def request_is_debug(
    state: Any, role_id: str, *, hint: bool, source_ids: Collection[str] = ()
) -> bool:
    """Whether the request being served is traced in debug mode.

    Called by the one pipeline at request entry (org, role and hint known) and again once the
    plan's sources are known (a source window). Raises :class:`DebugTraceHintNotPermitted` for a
    hint the operator has not permitted for ``role_id``.

    A process with no control plane (tooling, a unit under test) has no operator settings: no
    window is open and no role is permitted the hint."""
    from provisa.core.request_context import require_current_org

    admin_db = state.admin_db
    if admin_db is None:
        if hint:
            raise DebugTraceHintNotPermitted(role_id)
        return False
    snapshot = await current_snapshot(admin_db)
    return snapshot.debug_for(
        # The org the request is bound to; unbound work has no org to answer for (REQ-1266).
        org_id=require_current_org(),
        role_id=role_id,
        source_ids=source_ids,
        hint=hint,
        now=datetime.now(timezone.utc),
    )

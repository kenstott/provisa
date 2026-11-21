# Copyright (c) 2026 Kenneth Stott
# Canary: 8b4f2d6e-1c93-4a07-b5e8-f2a9c0d3e761
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Discovery runs in the background (REQ-413), kept as the glossary's term scans are kept
(provisa/term_finding/queue.py): a ``discovery_runs`` row that every worker reads.

A run is running, then done, stopped or failed. The worker running it writes its progress after
each request; Stop is asked on the row (:func:`ask_stop`), and the worker reads it before each
request, so the request in flight finishes and what was found is kept. A running run of an earlier
launch, or one that has not moved on for :data:`STALLED_AFTER`, has failed
(:func:`give_up_on_left_behind`), checked whenever runs are read.
"""

# Requirements: REQ-413

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, insert, or_, select, update

from provisa.core.schema_org import discovery_runs as runs_t
from provisa.llm.client import CALL_TIMEOUT_S

if TYPE_CHECKING:
    from provisa.core.database import Connection

RUNNING, DONE, STOPPED, FAILED = "running", "done", "stopped", "failed"

#: A running run that has not moved on for this long has failed. A request in flight counts as
#: moving on: the worker says it is still going every provisa.discovery.run.STILL_GOING_EVERY_S,
#: however long the request may take (provisa.llm.client, ``call_timeout_s``).
STALLED_AFTER = timedelta(seconds=2 * CALL_TIMEOUT_S + 60)
RESTARTED = "the server restarted while discovery was running; run it again"
STALLED = f"discovery made no progress for {STALLED_AFTER.total_seconds():g} seconds; run it again"
SHUT_DOWN = "the server stopped while discovery was running; run it again"

#: This process, when it is not one of a launch's workers (provisa.core.boot_lock).
_PROCESS = uuid.uuid4().hex


def this_launch() -> str:
    """The launch a run starts in: a launch's workers, respawned ones included, share it."""
    from provisa.core.boot_lock import launch_id

    return launch_id() or _PROCESS


@dataclass(frozen=True)
class RunRow:
    id: int
    status: str
    scope: dict[str, Any]
    model: str | None
    requests_total: int
    requests_done: int
    suggestions: int
    report: dict[str, Any] | None
    error: str | None
    stop_requested: bool
    stopped_by: str | None
    started_by: str | None
    started_at: datetime
    finished_at: datetime | None

    def view(self) -> dict[str, Any]:
        """The run as the API answers it."""
        return {
            "id": self.id,
            "status": self.status,
            "scope": self.scope,
            "model": self.model,
            "requests_total": self.requests_total,
            "requests_done": self.requests_done,
            "suggestions": self.suggestions,
            "report": self.report,
            "error": self.error,
            "stop_requested": self.stop_requested,
            "stopped_by": self.stopped_by,
            "started_by": self.started_by,
            "started_at": self.started_at.isoformat(),
            "finished_at": None if self.finished_at is None else self.finished_at.isoformat(),
        }


_COLUMNS = (
    runs_t.c.id,
    runs_t.c.status,
    runs_t.c.scope,
    runs_t.c.model,
    runs_t.c.requests_total,
    runs_t.c.requests_done,
    runs_t.c.suggestions,
    runs_t.c.report,
    runs_t.c.error,
    runs_t.c.stop_requested,
    runs_t.c.stopped_by,
    runs_t.c.started_by,
    runs_t.c.started_at,
    runs_t.c.finished_at,
)


def _row(r: Any) -> RunRow:
    return RunRow(
        id=int(r.id),
        status=str(r.status),
        scope=json.loads(r.scope),
        model=r.model,
        requests_total=int(r.requests_total),
        requests_done=int(r.requests_done),
        suggestions=int(r.suggestions),
        report=None if r.report is None else json.loads(r.report),
        error=r.error,
        stop_requested=bool(r.stop_requested),
        stopped_by=r.stopped_by,
        started_by=r.started_by,
        started_at=r.started_at,
        finished_at=r.finished_at,
    )


async def start(
    conn: "Connection", *, scope: dict[str, Any], model: str | None, started_by: str | None
) -> int:
    """A running run."""
    result = await conn.execute_core(
        insert(runs_t)
        .values(
            status=RUNNING,
            scope=json.dumps(scope, sort_keys=True),
            model=model,
            started_by=started_by,
            launch=this_launch(),
            progress_at=datetime.now(UTC),
        )
        .returning(runs_t.c.id)
    )
    row = result.fetchone()
    assert row is not None
    return int(row[0])


async def record_progress(conn: "Connection", run_id: int, report: dict[str, Any]) -> bool:
    """The run has moved on; ``report`` is what it has found so far. False when it is no longer
    running."""
    semantics = report.get("semantics") or {}
    result = await conn.execute_core(
        update(runs_t)
        .where(runs_t.c.id == run_id, runs_t.c.status == RUNNING)
        .values(
            requests_total=int(semantics.get("requests_total", 0)),
            requests_done=int(semantics.get("requests_done", 0)),
            suggestions=int(report.get("suggestions", 0)),
            report=json.dumps(report, sort_keys=True),
            progress_at=datetime.now(UTC),
        )
        .returning(runs_t.c.id)
    )
    return result.fetchone() is not None


async def stop_asked(conn: "Connection", run_id: int) -> bool:
    """Whether the run should stop before its next request: Stop was asked, or it has ended."""
    row = (
        await conn.execute_core(
            select(runs_t.c.status, runs_t.c.stop_requested).where(runs_t.c.id == run_id)
        )
    ).fetchone()
    assert row is not None, f"discovery run {run_id} has no row"
    return row.status != RUNNING or bool(row.stop_requested)


async def ask_stop(conn: "Connection", run_id: int, by: str | None) -> bool:
    """Ask a running run to stop after the request in flight. False when it is not running."""
    result = await conn.execute_core(
        update(runs_t)
        .where(runs_t.c.id == run_id, runs_t.c.status == RUNNING)
        .values(stop_requested=True, stopped_by=by)
        .returning(runs_t.c.id)
    )
    return result.fetchone() is not None


async def finish(conn: "Connection", run_id: int, report: dict[str, Any]) -> bool:
    """The run ended with ``report``: stopped when Stop was asked, else done. False when it had
    already ended: the first ending stands."""
    semantics = report.get("semantics") or {}
    stopped = bool(semantics.get("stopped"))
    result = await conn.execute_core(
        update(runs_t)
        .where(runs_t.c.id == run_id, runs_t.c.status == RUNNING)
        .values(
            status=STOPPED if stopped else DONE,
            requests_total=int(semantics.get("requests_total", 0)),
            requests_done=int(semantics.get("requests_done", 0)),
            suggestions=int(report.get("suggestions", 0)),
            report=json.dumps(report, sort_keys=True),
            finished_at=func.now(),
            progress_at=datetime.now(UTC),
        )
        .returning(runs_t.c.id)
    )
    return result.fetchone() is not None


async def fail(conn: "Connection", run_id: int, error: str) -> bool:
    """The run stopped, naming why. False when it had already ended."""
    result = await conn.execute_core(
        update(runs_t)
        .where(runs_t.c.id == run_id, runs_t.c.status == RUNNING)
        .values(status=FAILED, error=error, finished_at=func.now())
        .returning(runs_t.c.id)
    )
    return result.fetchone() is not None


async def give_up_on_left_behind(conn: "Connection", launch: str) -> int:
    """Fail the runs no server is running any more: those of an earlier launch, and those that
    have stopped moving; how many."""
    restarted = await conn.execute_core(
        update(runs_t)
        .where(
            runs_t.c.status == RUNNING,
            or_(runs_t.c.launch.is_(None), runs_t.c.launch != launch),
        )
        .values(status=FAILED, error=RESTARTED, finished_at=func.now())
        .returning(runs_t.c.id)
    )
    stalled = await conn.execute_core(
        update(runs_t)
        .where(
            runs_t.c.status == RUNNING,
            runs_t.c.progress_at < datetime.now(UTC) - STALLED_AFTER,
        )
        .values(status=FAILED, error=STALLED, finished_at=func.now())
        .returning(runs_t.c.id)
    )
    return len(restarted.fetchall()) + len(stalled.fetchall())


async def run(conn: "Connection", run_id: int) -> RunRow | None:
    row = (await conn.execute_core(select(*_COLUMNS).where(runs_t.c.id == run_id))).fetchone()
    return None if row is None else _row(row)


#: Every run, whoever started it.
ANYONE = object()


async def recent(conn: "Connection", limit: int, started_by: Any = ANYONE) -> list[RunRow]:
    """The latest runs, newest first; with ``started_by``, those that person started (None: those
    started with no person signed in)."""
    query = select(*_COLUMNS)
    if started_by is not ANYONE:
        query = query.where(
            runs_t.c.started_by.is_(None)
            if started_by is None
            else runs_t.c.started_by == started_by
        )
    rows = (
        await conn.execute_core(
            query.order_by(runs_t.c.started_at.desc(), runs_t.c.id.desc()).limit(limit)
        )
    ).fetchall()
    return [_row(r) for r in rows]

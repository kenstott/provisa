# Copyright (c) 2026 Kenneth Stott
# Canary: 602992f3-344e-46e5-b6de-6a545987af83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Sensitive values held inside text: a scan of a table's text columns, and the hints it leaves
(REQ-2147 KINDS ARE PATTERN RULES; CONTAINED VALUES ARE HINTS).

A kind is tested two ways. Whole value is the profile run's classification, which a steward
accepts or rejects (``classifications``). Contained in is this module's: a kind's pattern found
inside a longer text, its check passing the matched part (``classify.contains``). What it finds is
a hint -- "matches a US social security number in 12% of sampled values" -- never a
classification, and never applied by itself: the column's grant, masks and fake stay as a person
sets them.

A scan runs when a person asks for it, at registration or from a column's access settings, in the
background, since reading a sample may take time. It reads up to :data:`SCAN_ROWS` rows of the
table as the org admin reads it -- a draft as well as a table in service (``table_read``), so a
table being registered can be scanned before it is released -- tests each text column's values in
memory, and keeps only the share of values that held each kind. The values and the matched text
are never stored, logged or returned. A column that classifies as a kind by whole value gets no
hint of that kind: the classification is what it is.

``profiler_sensitive_scans`` holds each scan (running, then succeeded or failed with why);
``profiler_sensitive_hints`` each column's hints from the table's latest successful scan.
"""

# Requirements: REQ-2147

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, insert, select, update

from provisa.profiler import classify

log = logging.getLogger(__name__)

#: The rows a scan reads: the steward's own Profile of a table reads as many.
SCAN_ROWS = 2000
#: How long a scan may run. One that has not ended by then has failed (it is stopped, or its
#: server stopped under it), and says so.
SCAN_TIMEOUT = timedelta(minutes=10)
SCAN_STATUSES = ("running", "succeeded", "failed")
TIMED_OUT = (
    f"the scan did not finish within {int(SCAN_TIMEOUT.total_seconds() // 60)} minutes; "
    "scan the table again"
)
STOPPED = "the server stopped while the scan was running; scan the table again"


@dataclass(frozen=True)
class TextColumn:
    name: str  # as a statement over the table names it
    physical: str  # the column's registered name


def text_columns(table: dict) -> list[TextColumn]:
    """The registered table's text columns, each with the name its rows are read by."""
    from provisa.compiler.naming import apply_sql_name
    from provisa.profiler.statement import family_of

    return [
        TextColumn(c.get("alias") or apply_sql_name(c["column_name"]), c["column_name"])
        for c in table["columns"]
        if family_of(c.get("data_type")) == "text"
    ]


def hint_rows(
    table_id: int,
    scan_id: str,
    columns: list[TextColumn],
    names: list[str],
    rows: list[tuple],
    kinds: tuple[classify.Kind, ...],
) -> list[dict]:
    """``profiler_sensitive_hints`` rows of one scan: per text column read, each kind found inside
    its values, as counts. ``names`` and ``rows`` are what the read returned."""
    out = []
    for col in columns:
        if col.name not in names:
            continue  # a column the org admin does not read is not scanned
        at = names.index(col.name)
        values = [None if r[at] is None else str(r[at]) for r in rows]
        for hint in classify.contained_hints(values, kinds):
            out.append(
                {
                    "table_id": table_id,
                    "physical_column": col.physical,
                    "kind": hint.kind.id,
                    "scan_id": scan_id,
                    "values_tested": hint.tested,
                    "values_matched": hint.matched,
                    "share": hint.share,
                    "evidence": hint.evidence(),
                }
            )
    return out


# -- the scan's record -----------------------------------------------------------------------------


async def start(conn: Any, table_id: int, by: str | None) -> str:
    """Record a new scan of ``table_id`` as running; its id."""
    from provisa.core.schema_org import profiler_sensitive_scans as ss

    scan_id = str(uuid.uuid4())
    await conn.execute_core(
        insert(ss).values(scan_id=scan_id, table_id=table_id, status="running", requested_by=by)
    )
    return scan_id


async def _finish(conn: Any, scan_id: str, **values: Any) -> None:
    from provisa.core.schema_org import profiler_sensitive_scans as ss

    await conn.execute_core(
        update(ss)
        .where(ss.c.scan_id == scan_id, ss.c.status == "running")
        .values(finished_at=datetime.now(UTC), **values)
    )


async def _store(conn: Any, table_id: int, scan_id: str, rows: list[dict], read: int, scanned: int):
    """The scan's hints replace the table's earlier ones, and the scan has succeeded."""
    from provisa.core.schema_org import profiler_sensitive_hints as sh

    async with conn.transaction():
        await conn.execute_core(delete(sh).where(sh.c.table_id == table_id))
        if rows:
            await conn.execute_core_many(insert(sh), rows)
        await _finish(conn, scan_id, status="succeeded", sampled_rows=read, scanned_columns=scanned)


async def run(state: Any, table_id: int, scan_id: str) -> None:
    """Scan ``table_id`` and record what was found. Always ends: succeeded, or failed with why."""
    from provisa.api.admin.table_read import _in_service, draft_table, read_as_caller
    from provisa.profiler.classifications import enabled_kinds
    from provisa.profiler.run import PROFILE_ROLE

    pool = state.model_db
    try:
        async with asyncio.timeout(SCAN_TIMEOUT.total_seconds()):
            table = _in_service(state, table_id) or draft_table(state, table_id)
            if table is None:
                raise ValueError(f"table {table_id} is no longer registered")
            columns = text_columns(table)
            kinds = await enabled_kinds(pool)
            names: list[str] = []
            rows: list[tuple] = []
            if columns:
                result = await read_as_caller(state, table_id, PROFILE_ROLE, limit=SCAN_ROWS)
                names = list(result.column_names)
                rows = [tuple(r) for r in result.rows]
            found = hint_rows(table_id, scan_id, columns, names, rows, kinds)
            scanned = sum(1 for c in columns if c.name in names)
            async with pool.acquire() as conn:
                await _store(conn, table_id, scan_id, found, len(rows), scanned)
    except asyncio.CancelledError:  # the process is shutting down
        async with pool.acquire() as conn:
            await _finish(conn, scan_id, status="failed", error=STOPPED)
        raise
    except TimeoutError:
        async with pool.acquire() as conn:
            await _finish(conn, scan_id, status="failed", error=TIMED_OUT)
    except Exception as exc:  # the scan is marked failed with what stopped it, never left running
        log.exception("sensitive-value scan %s of table %s failed", scan_id, table_id)
        async with pool.acquire() as conn:
            await _finish(conn, scan_id, status="failed", error=str(exc) or type(exc).__name__)


# -- reading ---------------------------------------------------------------------------------------


def _as_utc(at: datetime) -> datetime:
    return at if at.tzinfo is not None else at.replace(tzinfo=UTC)


async def latest_scan(conn: Any, table_id: int) -> dict | None:
    """The table's latest scan; None when it has never been scanned. A scan still running past
    :data:`SCAN_TIMEOUT` was left behind by a server that stopped: it is failed here, saying so."""
    from provisa.core.schema_org import profiler_sensitive_scans as ss

    row = (
        await conn.execute_core(
            select(ss).where(ss.c.table_id == table_id).order_by(ss.c.started_at.desc()).limit(1)
        )
    ).fetchone()
    if row is None:
        return None
    scan = dict(row._mapping)
    if scan["status"] == "running" and datetime.now(UTC) - _as_utc(scan["started_at"]) > (
        SCAN_TIMEOUT
    ):
        await _finish(conn, scan["scan_id"], status="failed", error=TIMED_OUT)
        scan.update(status="failed", error=TIMED_OUT)
    return scan


async def hints_of(conn: Any, table_id: int) -> list[dict]:
    """The table's hints, from its latest successful scan, by column then kind."""
    from provisa.core.schema_org import profiler_sensitive_hints as sh

    result = await conn.execute_core(
        select(sh).where(sh.c.table_id == table_id).order_by(sh.c.physical_column, sh.c.kind)
    )
    return [dict(r._mapping) for r in result.fetchall()]

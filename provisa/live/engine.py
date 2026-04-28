# Copyright (c) 2026 Kenneth Stott
# Canary: a9b0c1d2-e3f4-5678-9abc-def012345678
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Query Engine (Phase AM).

Registers live queries by their stable_id, polls for new rows using
APScheduler, and fans results out to SSE subscribers and Kafka sinks.

Architecture:
- One APScheduler AsyncIOScheduler manages poll jobs.
- Each live query has an independent watermark (persisted to ``live_query_state``).
- SSEFanout maintains per-query subscriber queues; KafkaSinkOutput produces messages.
- On each poll, rows with watermark_column > last_watermark are fetched and delivered.

Usage::

    engine = LiveEngine(pg_pool=pool)
    await engine.start()
    await engine.register(live_cfg, stable_id="abc-123")
    queue = engine.subscribe("abc-123")  # returns asyncio.Queue for SSE
    engine.unsubscribe("abc-123", queue)
    await engine.stop()
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
except ImportError:  # pragma: no cover
    AsyncIOScheduler = None  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)


@dataclass
class _LiveJob:
    """Runtime state for one registered live query."""

    query_id: str
    watermark_column: str
    poll_interval: int
    fanout: object  # SSEFanout
    kafka_outputs: list  # list[KafkaSinkOutput]
    scheduler_job_id: str = ""


class LiveEngine:
    """APScheduler-backed live query engine.

    Args:
        pg_pool: asyncpg connection pool for watermark persistence and query
                 execution (queries run via direct PG route).
    """

    def __init__(self, pg_pool) -> None:
        self._pg_pool = pg_pool
        self._jobs: dict[str, _LiveJob] = {}
        self._scheduler = None

    async def start(self) -> None:
        """Start the APScheduler scheduler."""
        if AsyncIOScheduler is None:
            raise RuntimeError("apscheduler is required for the live query engine")
        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        log.info("[LIVE ENGINE] started")

    async def stop(self) -> None:
        """Stop the scheduler and close all outputs."""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
        for job in list(self._jobs.values()):
            await job.fanout.close()
            for kout in job.kafka_outputs:
                await kout.close()
        self._jobs.clear()
        log.info("[LIVE ENGINE] stopped")

    def register(self, query_id: str, watermark_column: str, poll_interval: int,
                 kafka_outputs: list | None = None) -> None:
        """Register a live query for polling.

        Args:
            query_id: stable_id of the approved persisted query.
            watermark_column: column whose MAX value is tracked as watermark.
            poll_interval: seconds between polls.
            kafka_outputs: list of KafkaSinkOutput instances to receive rows.
        """
        if query_id in self._jobs:
            log.debug("[LIVE ENGINE] query %s already registered", query_id)
            return

        from provisa.live.outputs.sse import SSEFanout

        fanout = SSEFanout(query_id)
        job = _LiveJob(
            query_id=query_id,
            watermark_column=watermark_column,
            poll_interval=poll_interval,
            fanout=fanout,
            kafka_outputs=kafka_outputs or [],
        )
        self._jobs[query_id] = job

        if self._scheduler is not None:
            sched_job = self._scheduler.add_job(
                self._poll,
                "interval",
                seconds=poll_interval,
                args=[query_id],
                id=f"live_{query_id}",
                replace_existing=True,
            )
            job.scheduler_job_id = sched_job.id
            log.info("[LIVE ENGINE] registered query %s (interval=%ds)", query_id, poll_interval)

    def unregister(self, query_id: str) -> None:
        """Remove a live query from the engine."""
        job = self._jobs.pop(query_id, None)
        if job is None:
            return
        if self._scheduler and job.scheduler_job_id:
            try:
                self._scheduler.remove_job(job.scheduler_job_id)
            except Exception:
                pass

    def subscribe(self, query_id: str) -> asyncio.Queue:
        """Subscribe to SSE fan-out for *query_id*. Returns an asyncio.Queue."""
        job = self._jobs.get(query_id)
        if job is None:
            raise KeyError(f"Live query {query_id!r} not registered")
        return job.fanout.subscribe()

    def unsubscribe(self, query_id: str, queue: asyncio.Queue) -> None:
        """Remove a SSE subscriber queue."""
        job = self._jobs.get(query_id)
        if job:
            job.fanout.unsubscribe(queue)

    def is_registered(self, query_id: str) -> bool:
        return query_id in self._jobs

    async def _poll(self, query_id: str) -> None:
        """Poll for new rows and deliver to outputs."""
        job = self._jobs.get(query_id)
        if job is None:
            return

        try:
            from provisa.live.watermark import get_watermark, set_watermark
            from provisa.registry.store import get_by_stable_id

            async with self._pg_pool.acquire() as conn:
                # Load the approved query
                record = await get_by_stable_id(conn, query_id)
                if record is None:
                    log.warning("[LIVE ENGINE] query %s not found in registry", query_id)
                    return

                query_text = record.get("query_text", "")
                if not query_text:
                    return

                # Get current watermark
                watermark = await get_watermark(conn, query_id)

                # Build incremental SQL by injecting watermark filter.
                # compiled_sql may be a JSON array for multi-root queries; use
                # the first statement since the watermark column lives in one table.
                _raw_sql = record.get("compiled_sql", query_text)
                try:
                    _parsed = json.loads(_raw_sql)
                    _base_sql = _parsed[0] if isinstance(_parsed, list) and _parsed else _raw_sql
                except (json.JSONDecodeError, TypeError):
                    _base_sql = _raw_sql
                incremental_sql = _build_incremental_sql(
                    _base_sql,
                    job.watermark_column,
                    watermark,
                )

                rows_raw = await conn.fetch(incremental_sql)
                if not rows_raw:
                    return

                rows = [dict(r) for r in rows_raw]

                # Update watermark to the max value seen
                max_val = max(str(r.get(job.watermark_column, "")) for r in rows)
                await set_watermark(conn, query_id, max_val)

            # Deliver to outputs
            await job.fanout.send(rows)
            for kout in job.kafka_outputs:
                await kout.send(rows)

            log.debug("[LIVE ENGINE] polled %s: %d new rows", query_id, len(rows))

        except Exception:
            log.exception("[LIVE ENGINE] poll failed for query %s", query_id)


def _build_incremental_sql(base_sql: str, watermark_column: str, watermark: str | None) -> str:
    """Inject a watermark WHERE filter into the base SQL.

    If the query already has a WHERE clause, ANDs the filter in.
    Otherwise adds WHERE.  This is a best-effort injection — complex CTEs
    should use a named watermark parameter pattern instead.
    """
    import re

    filter_expr = (
        f"{watermark_column} > '{watermark}'"
        if watermark is not None
        else f"{watermark_column} IS NOT NULL"
    )

    # Strip trailing semicolon to avoid syntax errors
    sql = base_sql.rstrip().rstrip(";")

    # Check for existing WHERE clause (not inside a subquery)
    if re.search(r"\bWHERE\b", sql, re.IGNORECASE):
        return f"{sql} AND {filter_expr}"
    # Check for GROUP BY / ORDER BY / LIMIT to insert before them
    for keyword in ("GROUP BY", "ORDER BY", "LIMIT", "HAVING"):
        match = re.search(rf"\b{keyword}\b", sql, re.IGNORECASE)
        if match:
            return f"{sql[:match.start()]}WHERE {filter_expr} {sql[match.start():]}"
    return f"{sql} WHERE {filter_expr}"

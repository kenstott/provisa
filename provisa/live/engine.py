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
import logging
from dataclasses import dataclass, field

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
except ImportError:  # pragma: no cover
    AsyncIOScheduler = None  # type: ignore[assignment,misc]

from provisa.live.outputs.sse import SSEFanout
from provisa.live.outputs.kafka import KafkaSinkOutput

log = logging.getLogger(__name__)

# Requirements: REQ-260, REQ-282, REQ-283, REQ-285, REQ-286, REQ-287


@dataclass
class _LiveJob:
    """Runtime state for one registered live query."""

    query_id: str
    sql: str
    watermark_column: str
    poll_interval: int
    fanout: SSEFanout
    kafka_outputs: list[KafkaSinkOutput]
    scheduler_job_id: str = ""
    signature: tuple = ()


@dataclass
class LiveSpec:  # REQ-565
    """Declarative desired-state for one live poll job (used by reconcile)."""

    query_id: str
    sql: str
    watermark_column: str
    poll_interval: int = 10
    kafka_outputs: list[dict] = field(default_factory=list)

    def signature(self) -> tuple:
        return (
            self.sql,
            self.watermark_column,
            self.poll_interval,
            tuple(
                (k.get("bootstrap_servers"), k.get("topic"), k.get("key_column"))
                for k in self.kafka_outputs
            ),
        )


class LiveEngine:  # REQ-282, REQ-285, REQ-286, REQ-287
    """APScheduler-backed live query engine.

    Args:
        pg_pool: asyncpg connection pool used ONLY for watermark bookkeeping
                 (``live_query_state``). Data polls never hit this pool.
        trino_conn: Trino DBAPI connection used to execute every data poll.
                 Routing through Trino makes any federated source pollable —
                 PostgreSQL sources use ``delivery=cdc`` (LISTEN/NOTIFY) and are
                 never polled here.
    """

    def __init__(self, pg_pool, trino_conn=None) -> None:
        self._pg_pool = pg_pool
        self._trino_conn = trino_conn
        self._jobs: dict[str, _LiveJob] = {}
        self._scheduler = None

    async def start(self) -> None:  # REQ-565
        """Start the APScheduler scheduler."""
        if AsyncIOScheduler is None:
            raise RuntimeError("apscheduler is required for the live query engine")
        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        log.info("[LIVE ENGINE] started")

    async def stop(self) -> None:  # REQ-565
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

    def register(  # REQ-283, REQ-285, REQ-286
        self,
        query_id: str,
        sql: str,
        watermark_column: str,
        poll_interval: int,
        kafka_outputs: list | None = None,
        signature: tuple = (),
    ) -> None:
        """Register a live query for polling.

        Args:
            query_id: identifier for this live query.
            sql: compiled SQL to execute on each poll.
            watermark_column: column whose MAX value is tracked as watermark.
            poll_interval: seconds between polls.
            kafka_outputs: list of KafkaSinkOutput instances to receive rows.
            signature: opaque config fingerprint used by reconcile() to detect changes.
        """
        if query_id in self._jobs:
            log.debug("[LIVE ENGINE] query %s already registered", query_id)
            return

        fanout = SSEFanout(query_id)
        job = _LiveJob(
            query_id=query_id,
            sql=sql,
            watermark_column=watermark_column,
            poll_interval=poll_interval,
            fanout=fanout,
            kafka_outputs=kafka_outputs or [],
            signature=signature,
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

    def reconcile(self, specs: list[LiveSpec]) -> None:  # REQ-565
        """Drive engine poll jobs to match *specs* (desired state from the DB).

        Registers new jobs, unregisters removed ones, and re-registers a job
        whose config fingerprint changed. Unchanged jobs are left untouched so
        their SSE subscribers are preserved. CDC-delivered tables are handled by
        subscription providers, not here — callers pass poll jobs only.
        """
        desired = {s.query_id: s for s in specs}
        for qid in list(self._jobs):
            if qid not in desired:
                self.unregister(qid)
        for qid, spec in desired.items():
            sig = spec.signature()
            existing = self._jobs.get(qid)
            if existing is not None and existing.signature == sig:
                continue
            if existing is not None:
                self.unregister(qid)
            kouts = [
                KafkaSinkOutput(
                    bootstrap_servers=k["bootstrap_servers"],
                    topic=k["topic"],
                    key_column=k.get("key_column"),
                )
                for k in spec.kafka_outputs
            ]
            self.register(
                qid, spec.sql, spec.watermark_column, spec.poll_interval, kouts, signature=sig
            )

    def unregister(self, query_id: str) -> None:  # REQ-565
        """Remove a live query from the engine."""
        job = self._jobs.pop(query_id, None)
        if job is None:
            return
        if self._scheduler and job.scheduler_job_id:
            try:
                self._scheduler.remove_job(job.scheduler_job_id)
            except Exception:
                pass

    def subscribe(self, query_id: str) -> asyncio.Queue:  # REQ-260, REQ-286
        """Subscribe to SSE fan-out for *query_id*. Returns an asyncio.Queue."""
        job = self._jobs.get(query_id)
        if job is None:
            raise KeyError(f"Live query {query_id!r} not registered")
        return job.fanout.subscribe()

    def unsubscribe(self, query_id: str, queue: asyncio.Queue) -> None:  # REQ-565
        """Remove a SSE subscriber queue."""
        job = self._jobs.get(query_id)
        if job:
            job.fanout.unsubscribe(queue)

    def is_registered(self, query_id: str) -> bool:  # REQ-565
        return query_id in self._jobs

    async def _poll(self, query_id: str) -> None:  # REQ-260, REQ-283, REQ-286, REQ-287
        """Poll for new rows and deliver to outputs."""
        job = self._jobs.get(query_id)
        if job is None:
            return

        try:
            from provisa.live.watermark import get_watermark, set_watermark

            # Watermark bookkeeping lives in PG (live_query_state), independent of
            # where the data query runs.
            async with self._pg_pool.acquire() as conn:
                sse_watermark = await get_watermark(conn, query_id, "sse")
                kafka_watermark = (
                    await get_watermark(conn, query_id, "kafka") if job.kafka_outputs else None
                )

            # Use the earliest watermark as the query lower bound
            watermarks = [w for w in [sse_watermark, kafka_watermark] if w is not None]
            query_watermark = min(watermarks) if watermarks else None

            incremental_sql = _build_incremental_sql(
                job.sql,
                job.watermark_column,
                query_watermark,
            )

            # Data poll always routes through Trino (federated). execute_trino is
            # blocking, so run it off the event loop.
            if self._trino_conn is None:
                raise RuntimeError("LiveEngine has no Trino connection for polling")
            from provisa.executor.trino import execute_trino

            _trino_conn = self._trino_conn
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: execute_trino(_trino_conn, incremental_sql)
            )
            if not result.rows:
                return

            rows = [dict(zip(result.column_names, r)) for r in result.rows]
            max_val = max(str(r.get(job.watermark_column, "")) for r in rows)

            # Deliver to outputs independently (neither blocks the other)
            async def _deliver_sse():
                await job.fanout.send(rows)
                async with self._pg_pool.acquire() as conn:
                    await set_watermark(conn, query_id, "sse", max_val)

            async def _deliver_kafka():
                for kout in job.kafka_outputs:
                    await kout.send(rows)
                async with self._pg_pool.acquire() as conn:
                    await set_watermark(conn, query_id, "kafka", max_val)

            tasks = [_deliver_sse()]
            if job.kafka_outputs:
                tasks.append(_deliver_kafka())
            await asyncio.gather(*tasks, return_exceptions=True)

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
            return f"{sql[: match.start()]}WHERE {filter_expr} {sql[match.start() :]}"
    return f"{sql} WHERE {filter_expr}"

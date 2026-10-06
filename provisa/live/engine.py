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

One engine per org's prod runtime (REQ-1266). Reconcile hands it the org's live tables as specs;
nothing is polled for a spec until something subscribes. A subscriber (an SSE client) or a
configured output (a Kafka sink, publishing as its named role) is served by a GROUP: one poll of
the spec, governed as one key (``provisa.live.governed``), fanned out to that key's subscribers
only. Each poll runs through the one governed pipeline as its key -- row rules, column visibility
and masks -- and keeps its own watermark (or replace digest) in ``live_query_state``.

Usage::

    engine = LiveEngine(tenant_db=pool, org_id="acme", scheduler=process_scheduler)
    await engine.start()
    engine.reconcile([LiveSpec(...)])
    queue = await engine.subscribe("src.orders", key)  # governed as key; refusal raised here
    engine.unsubscribe("src.orders", key, queue)
    await engine.stop()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from provisa.live.governed import GovernanceKey
from provisa.live.outputs.base import LiveOutput
from provisa.live.outputs.kafka import KafkaSinkOutput
from provisa.live.outputs.sse import SSEFanout

log = logging.getLogger(__name__)

# Requirements: REQ-260, REQ-282, REQ-283, REQ-285, REQ-286, REQ-287, REQ-1266


@dataclass
class LiveSpec:  # REQ-565
    """Declarative desired-state for one live table (used by reconcile).

    ``kafka_outputs`` entries name ``bootstrap_servers``, ``topic``, ``key_column`` and ``role``:
    the role the output publishes as (REQ-286)."""

    query_id: str
    table_id: int
    watermark_column: str
    poll_interval: int = 10
    kafka_outputs: list[dict] = field(default_factory=list)
    mode: str = "append"  # REQ-932: append (watermark delta) | replace (full re-scan)

    def signature(self) -> tuple:
        return (
            self.table_id,
            self.watermark_column,
            self.poll_interval,
            self.mode,
            tuple(
                (k.get("bootstrap_servers"), k.get("topic"), k.get("key_column"), k.get("role"))
                for k in self.kafka_outputs
            ),
        )


@dataclass
class _Group:
    """One governed poll of a spec, for one key, delivered to one output."""

    spec: LiveSpec
    key: GovernanceKey
    output: LiveOutput
    output_type: str  # the live_query_state row this group's watermark / digest is kept under
    job_id: str


class LiveEngine:  # REQ-282, REQ-285, REQ-286, REQ-287
    """Live query engine for one org, its polls jobs on the process's scheduler.

    Args:
        tenant_db: the org's state store, used ONLY for watermark bookkeeping
                 (``live_query_state``). Data polls never hit this pool.
        org_id: the org whose model and engine this engine polls; each poll binds it -- a
                 scheduled job fires with nothing bound.
        scheduler: the process's scheduler (every worker runs one). A poll serves this
                 process's subscribers, so its job runs in every worker (``live_`` job ids,
                 provisa/scheduler/executor.py).
    """

    def __init__(self, tenant_db, *, org_id: str, scheduler) -> None:
        self._tenant_db = tenant_db
        self._org_id = org_id
        self._specs: dict[str, LiveSpec] = {}
        # (query_id, output_type) -> group. SSE groups come and go with their subscribers; a
        # Kafka group lives as long as its spec.
        self._groups: dict[tuple[str, str], _Group] = {}
        self._process_scheduler = scheduler
        self._scheduler = None

    @property
    def org_id(self) -> str:
        """The org this engine polls for (REQ-1266)."""
        return self._org_id

    async def start(self) -> None:  # REQ-565
        """Begin scheduling polls on the process's scheduler."""
        self._scheduler = self._process_scheduler
        log.info("[LIVE ENGINE] started for org %s", self._org_id)

    async def stop(self) -> None:  # REQ-565
        """Remove this engine's polls and close all outputs. The process's scheduler runs on."""
        scheduler, self._scheduler = self._scheduler, None
        groups = list(self._groups.values())
        self._groups.clear()
        self._specs.clear()
        for group in groups:
            if scheduler is not None:
                from apscheduler.jobstores.base import JobLookupError

                try:
                    scheduler.remove_job(group.job_id)
                except JobLookupError:
                    pass  # already gone -- nothing to remove
            await group.output.close()
        log.info("[LIVE ENGINE] stopped for org %s", self._org_id)

    # --- desired state ---------------------------------------------------------------------------

    def reconcile(self, specs: list[LiveSpec]) -> None:  # REQ-565
        """Drive the engine to *specs* (desired state from the org's model).

        A spec that is gone or changed ends its groups (their subscribers' streams end); an
        unchanged spec keeps its groups and their subscribers. Each spec's Kafka outputs are
        (re)opened as groups governed as their named role. Called with the engine's org bound.
        """
        from provisa.live.governed import output_key

        desired = {s.query_id: s for s in specs}
        for qid, current in list(self._specs.items()):
            spec = desired.get(qid)
            if spec is None or spec.signature() != current.signature():
                self._drop_spec(qid)
        for qid, spec in desired.items():
            if qid in self._specs:
                continue
            self._specs[qid] = spec
            for index, kafka in enumerate(spec.kafka_outputs):
                try:
                    key = output_key(kafka["role"])
                except LookupError:
                    log.exception(
                        "[LIVE ENGINE] Kafka output %d of %s is not published", index, qid
                    )
                    continue
                sink = KafkaSinkOutput(
                    bootstrap_servers=kafka["bootstrap_servers"],
                    topic=kafka["topic"],
                    key_column=kafka.get("key_column"),
                )
                self._open_group(spec, key, sink, f"kafka{index}:{key.digest}")

    def _drop_spec(self, query_id: str) -> None:
        self._specs.pop(query_id, None)
        for gkey in [g for g in self._groups if g[0] == query_id]:
            self._close_group(gkey)

    # --- groups ----------------------------------------------------------------------------------

    def _open_group(
        self, spec: LiveSpec, key: GovernanceKey, output: LiveOutput, output_type: str
    ) -> _Group:
        group = _Group(
            spec=spec,
            key=key,
            output=output,
            output_type=output_type,
            # REQ-1266: the job is the engine's org's, for one key; its id says both. An SSE poll
            # feeds this worker's subscribers and runs in every worker (``live_``); a Kafka output
            # publishes once per org, from the scheduler's holder (``livekafka_``, REQ-1900).
            job_id=(
                f"{'live' if isinstance(output, SSEFanout) else 'livekafka'}_{spec.query_id}"
                f":org_{self._org_id}:{output_type}"
            ),
        )
        self._groups[(spec.query_id, output_type)] = group
        if self._scheduler is not None:
            self._scheduler.add_job(
                self._poll,
                "interval",
                seconds=spec.poll_interval,
                args=[spec.query_id, output_type],
                id=group.job_id,
                replace_existing=True,
            )
        log.info("[LIVE ENGINE] polling %s as %s", spec.query_id, output_type)
        return group

    def _close_group(self, gkey: tuple[str, str]) -> None:
        group = self._groups.pop(gkey, None)
        if group is None:
            return
        if self._scheduler is not None:
            from apscheduler.jobstores.base import JobLookupError

            try:
                self._scheduler.remove_job(group.job_id)
            except JobLookupError:
                pass  # already gone -- nothing to remove
        from provisa.core.connection_loop import spawn_background

        spawn_background(group.output.close(), name=f"live-close-{group.job_id}")

    # --- subscribers -----------------------------------------------------------------------------

    def is_registered(self, query_id: str) -> bool:  # REQ-565
        return query_id in self._specs

    async def subscribe(self, query_id: str, key: GovernanceKey) -> asyncio.Queue:  # REQ-286
        """Subscribe ``key`` to *query_id*'s rows, governed as ``key``. The first subscriber of a
        key governs the poll once before it starts, so a refusal (the table or its watermark not
        visible to the role, ...) reaches the subscriber, by name, and starts nothing."""
        spec = self._specs.get(query_id)
        if spec is None:
            raise KeyError(f"Live query {query_id!r} not registered")
        if key.org_id != self._org_id:
            raise PermissionError(
                f"a subscriber of org {key.org_id!r} is not served by org {self._org_id!r}'s engine"
            )
        output_type = f"sse:{key.digest}"
        group = self._groups.get((query_id, output_type))
        if group is None:
            await self._check_governed(spec, key)
            group = self._groups.get((query_id, output_type)) or self._open_group(
                spec, key, SSEFanout(f"{query_id}:{key.digest}"), output_type
            )
        output = group.output
        assert isinstance(output, SSEFanout)
        return output.subscribe()

    def unsubscribe(self, query_id: str, key: GovernanceKey, queue: asyncio.Queue) -> None:
        """Remove a subscriber; the key's poll stops with its last subscriber (REQ-565)."""
        gkey = (query_id, f"sse:{key.digest}")
        group = self._groups.get(gkey)
        if group is None:
            return
        output = group.output
        assert isinstance(output, SSEFanout)
        output.unsubscribe(queue)
        if output.subscriber_count == 0:
            self._close_group(gkey)

    async def _check_governed(self, spec: LiveSpec, key: GovernanceKey) -> None:
        from provisa.live.governed import governed_rows, table_meta, table_ref

        ref = table_ref(table_meta(spec.table_id))
        probe = (
            f'SELECT "{spec.watermark_column}" FROM {ref} LIMIT 0'
            if spec.mode == "append"
            else f"SELECT * FROM {ref} LIMIT 0"
        )
        await governed_rows(probe, key)

    # --- polls -----------------------------------------------------------------------------------

    async def _poll(self, query_id: str, output_type: str) -> None:  # REQ-260, REQ-283, REQ-286
        """Poll one group and deliver its rows, as the work of the engine's org."""
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(self._org_id)
        try:
            group = self._groups.get((query_id, output_type))
            if group is None:
                return
            try:
                if group.spec.mode == "replace":
                    await self._poll_replace(group)
                else:
                    await self._poll_append(group)
            except Exception:
                log.exception("[LIVE ENGINE] poll failed for %s as %s", query_id, output_type)
        finally:
            reset_current_org(token)

    async def _poll_append(self, group: _Group) -> None:
        from provisa.live.governed import governed_rows, table_meta, table_ref
        from provisa.live.watermark import get_watermark, set_watermark

        spec = group.spec
        async with self._tenant_db.acquire() as conn:
            watermark = await get_watermark(conn, spec.query_id, group.output_type)
        sql = _build_incremental_sql(
            f"SELECT * FROM {table_ref(table_meta(spec.table_id))}",
            f'"{spec.watermark_column}"',
            watermark,
        )
        rows = await governed_rows(sql, group.key)
        if not rows:
            return
        await group.output.send(rows)
        high = max(str(r[spec.watermark_column]) for r in rows)
        async with self._tenant_db.acquire() as conn:
            await set_watermark(conn, spec.query_id, group.output_type, high)
        log.debug(
            "[LIVE ENGINE] polled %s as %s: %d rows", spec.query_id, group.output_type, len(rows)
        )

    async def _poll_replace(self, group: _Group) -> None:  # REQ-932
        """Full-replace poll for tables with no watermark column.

        Re-reads the whole governed result each interval and delivers it as a replace snapshot,
        only when its content changed: an order-independent hash of the rows is compared with the
        group's last delivered one, so a quiet table produces no ripple.
        """
        import hashlib

        from provisa.live.governed import governed_rows, table_meta, table_ref
        from provisa.live.watermark import get_watermark, set_watermark

        spec = group.spec
        rows = await governed_rows(
            f"SELECT * FROM {table_ref(table_meta(spec.table_id))}", group.key
        )
        # Order-independent: a reordered-but-equal result is not a change.
        digest = hashlib.sha256("\n".join(sorted(repr(r) for r in rows)).encode()).hexdigest()
        async with self._tenant_db.acquire() as conn:
            if await get_watermark(conn, spec.query_id, group.output_type) == digest:
                return
        await group.output.send(rows)
        async with self._tenant_db.acquire() as conn:
            await set_watermark(conn, spec.query_id, group.output_type, digest)


def _build_incremental_sql(base_sql: str, watermark_column: str, watermark: str | None) -> str:
    """Inject a watermark WHERE filter into the base SQL.

    If the query already has a WHERE clause, ANDs the filter in.
    Otherwise adds WHERE.  This is a best-effort injection — complex CTEs
    should use a named watermark parameter pattern instead.
    """
    import re

    # The watermark is a value read from the table: it is quoted as a literal, never spliced.
    filter_expr = (
        f"{watermark_column} > '{watermark.replace(chr(39), chr(39) * 2)}'"
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

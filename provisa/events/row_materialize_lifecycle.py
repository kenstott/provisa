# Copyright (c) 2026 Kenneth Stott
# Canary: b89cbfec-503e-45be-9d1b-d5525b790865
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Out-of-band lifecycle for row-materialize tables (REQ-1865, design doc sections 6b/6c).

Two independent mechanisms, both taking each candidate key's ``row_lock`` so they never race a
query-driven fetch (or each other) on the same key:

- ``process_row_refresh_events`` claims ``row_refresh`` events posted by
  ``provisa.federation.row_materialize_cdc.handle_row_materialize_cdc`` (the same claim/heartbeat/
  complete TABLE PROCESSOR shape ``provisa.events.queue``'s own docstring describes — claim
  granularity is the target table) and is the ONE place that actually calls
  ``ensure_rows_resident(force=True)`` for a CDC batch's already-cached keys.
- ``reap_expired_rows`` is a periodic batched ``DELETE`` of rows expired past an operator-set
  ``reap_grace_period`` — storage hygiene only, never freshness enforcement (that's ``_row_expires_
  at``, checked on every touch by ``ensure_rows_resident``).

``wire_row_materialize_background`` registers both as APScheduler interval jobs on the SAME
embedded scheduler the rest of the event loop uses (``provisa.events.boot.register_runtime``), one
job pair per ``row_materialize`` table found in the registry at wiring time. This is a standalone
scheduling path rather than a ``TableProcessor`` subclass — row_refresh/reap have none of the
debounce/freshness-contract/emit-outcome concerns ``TableProcessor``'s own machinery exists for.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

_PROCESSOR_NAME = "row_refresh"


async def _read_event_payloads(conn: Any, event_ids: list[int]) -> list[dict]:
    from sqlalchemy import select

    from provisa.core.schema_org import events

    if not event_ids:
        return []
    result = await conn.execute_core(select(events.c.payload).where(events.c.id.in_(event_ids)))
    return [row[0] or {} for row in result.fetchall()]


async def process_row_refresh_events(
    state: Any,
    *,
    node: str,
    source_id: str,
    schema_name: str,
    table_name: str,
    pk_columns: list[str],
) -> int:
    """Claim and drain every pending ``row_refresh`` event for ``node`` (one table's claim unit),
    calling ``ensure_rows_resident(force=True)`` once for the union of every claimed event's keys.
    Returns the number of events completed. Best-effort: an ownership loss (a peer's claim CAS beat
    this one) just means fewer events complete this pass — the next tick tries again."""
    from provisa.compiler.pk_bounds import PkBound
    from provisa.events import queue
    from provisa.federation.query_residency import ensure_rows_resident

    db = getattr(state, "tenant_db", None)
    if db is None:
        return 0

    now = datetime.now(timezone.utc)
    async with db.acquire() as conn:
        event_ids = await queue.claim(
            conn, dependent_table=node, processor_name=_PROCESSOR_NAME, now=now
        )
        if not event_ids:
            return 0
        payloads = await _read_event_payloads(conn, event_ids)

    keys: set[tuple[Any, ...]] = set()
    for payload in payloads:
        for key in payload.get("keys", []):
            keys.add(tuple(key))

    if keys:
        bound = PkBound(
            source_id=source_id,
            schema_name=schema_name,
            table_name=table_name,
            pk_columns=tuple(pk_columns),
            values=tuple(keys),
        )
        await ensure_rows_resident(state, [bound], force=True)

    completed = 0
    async with db.acquire() as conn:
        for event_id in event_ids:
            ok = await queue.complete(
                conn,
                event_id=event_id,
                dependent_table=node,
                processor_name=_PROCESSOR_NAME,
                now=now,
            )
            if ok:
                completed += 1
    return completed


async def reap_expired_rows(
    state: Any,
    *,
    node: str,
    schema_name: str,
    table_name: str,
    pk_columns: list[str],
    reap_grace_period: float,
    batch_size: int,
) -> int:
    """Delete rows expired past ``reap_grace_period`` (operator-set, section 6c — this mechanism
    never auto-derives a value), taking each candidate row's ``row_lock`` before deleting it, in
    batches of at most ``batch_size``. Never touches a row inside its ``cache_ttl`` freshness
    window, only ones already-expired for at least the grace period. Returns the count deleted."""
    from sqlalchemy import select

    from provisa.events.row_lock import row_lock
    from provisa.federation import store_writer
    from provisa.federation.backend import _env_store_schema
    from provisa.federation.materialize_exec import build_row_cache_table

    engine = getattr(state, "federation_engine", None)
    backend = getattr(getattr(engine, "engine", None), "backend", None)
    if engine is None or backend is None:
        return 0

    from provisa.federation.registry_view import registered_sources, registered_tables

    tables_by_name = {t.table_name: t for t in await registered_tables(state)}
    table = tables_by_name.get(table_name)
    if table is None:
        return 0
    sources_by_id = {s.id: s for s in await registered_sources(state)}
    source = sources_by_id.get(table.source_id)
    if source is None:
        return 0

    from provisa.federation.residency import resolve_landing_args

    args = resolve_landing_args(source, table, platform=backend.dialect)
    store_schema = _env_store_schema(engine.engine.materialize_store())
    schema, name = backend.landing_target(
        store_schema=store_schema,
        source_id=source.id,
        source_type=source.type,
        schema_name=schema_name,
        table_name=table_name,
    )
    cache_table = build_row_cache_table(
        schema, name, args.columns, pk_columns, dialect_name=backend.dialect
    )
    pk_cols = [cache_table.c[c] for c in pk_columns]

    cutoff = datetime.now(timezone.utc)
    # _row_expires_at < now - reap_grace_period
    from datetime import timedelta

    threshold = cutoff - timedelta(seconds=reap_grace_period)

    dsn = engine.engine.materialize_store()
    deleted = 0
    while True:
        async with store_writer.store_connection(dsn) as conn:
            result = await conn.execute_core(
                select(*pk_cols)
                .where(cache_table.c["_row_expires_at"] < threshold)
                .limit(batch_size)
            )
            candidates = [tuple(row) for row in result.fetchall()]
        if not candidates:
            break

        from contextlib import AsyncExitStack

        async with AsyncExitStack() as held:
            for key in candidates:
                await held.enter_async_context(row_lock(node, key))
            from sqlalchemy import tuple_

            async with store_writer.store_connection(dsn) as conn:
                cond = (
                    pk_cols[0].in_([k[0] for k in candidates])
                    if len(pk_cols) == 1
                    else tuple_(*pk_cols).in_(candidates)
                )
                await conn.execute_core(cache_table.delete().where(cond))
        deleted += len(candidates)
        if len(candidates) < batch_size:
            break
    return deleted


async def wire_row_materialize_background(
    scheduler: Any,
    *,
    state: Any,
    log: Any,
    tick_seconds: int = 5,
    reap_interval_seconds: int = 300,
    reap_grace_period: float,
    reap_batch_size: int = 1000,
) -> int:
    """Register the row_refresh drain + reaper sweep as APScheduler interval jobs, one pair per
    ``row_materialize`` table currently in the registry. ``reap_grace_period``/``reap_batch_size``/
    ``reap_interval_seconds`` are ordinary operator-set config (section 6c) — no default here is
    derived from any assumption about a deployment's source characteristics; the caller supplies
    them from config. Returns the number of tables wired. Best-effort: a table whose source can no
    longer be resolved is skipped and logged, never aborts wiring for every other table."""
    from apscheduler.triggers.interval import IntervalTrigger

    from provisa.federation.query_residency import row_materialized_tables_by_name

    db = getattr(state, "tenant_db", None)
    engine = getattr(state, "federation_engine", None)
    if db is None or engine is None:
        return 0

    row_tables = await row_materialized_tables_by_name(state)
    wired = 0
    for table_name, table in row_tables.items():
        node = f"{table.schema_name}.{table.table_name}"
        pk_columns = [c.name for c in table.columns if c.is_primary_key]
        if not pk_columns:
            log.warning("row-materialize %s: no primary key column — skipping wiring", node)
            continue

        async def _refresh(
            _node: str = node,
            _sid: str = table.source_id,
            _schema: str = table.schema_name,
            _table: str = table_name,
            _pk: list[str] = pk_columns,
        ) -> None:
            await process_row_refresh_events(
                state,
                node=_node,
                source_id=_sid,
                schema_name=_schema,
                table_name=_table,
                pk_columns=_pk,
            )

        async def _reap(
            _node: str = node,
            _schema: str = table.schema_name,
            _table: str = table_name,
            _pk: list[str] = pk_columns,
        ) -> None:
            await reap_expired_rows(
                state,
                node=_node,
                schema_name=_schema,
                table_name=_table,
                pk_columns=_pk,
                reap_grace_period=reap_grace_period,
                batch_size=reap_batch_size,
            )

        scheduler.add_job(
            _refresh,
            trigger=IntervalTrigger(seconds=tick_seconds),
            id=f"row_materialize:refresh:{node}",
            replace_existing=True,
        )
        scheduler.add_job(
            _reap,
            trigger=IntervalTrigger(seconds=reap_interval_seconds),
            id=f"row_materialize:reap:{node}",
            replace_existing=True,
        )
        wired += 1
    return wired

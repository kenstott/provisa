# Copyright (c) 2026 Kenneth Stott
# Canary: 2dadb1c4-7dac-45cd-bece-d8d8955e690d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Scheduled trigger execution via APScheduler (REQ-216).

Registers cron-based jobs from config. Supports webhook URLs and
internal function names. Reuses existing async patterns.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

if TYPE_CHECKING:
    from provisa.core.models import ScheduledTrigger

logger = logging.getLogger(__name__)


async def _execute_webhook(url: str, trigger_id: str) -> None:
    """Fire a webhook for a scheduled trigger."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json={"trigger_id": trigger_id})
            resp.raise_for_status()
            logger.info("Trigger %s: webhook %s returned %s", trigger_id, url, resp.status_code)
    except Exception:
        logger.exception("Trigger %s: webhook %s failed", trigger_id, url)


def _parse_compiled_sql(raw: str) -> list[str]:
    """Return SQL statements from a compiled_sql value (plain string or JSON array)."""
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [s for s in parsed if isinstance(s, str)]
    except (json.JSONDecodeError, TypeError):
        pass
    return [raw]


async def run_scheduled_query(
    stable_id: str,
    output_type: str,
    output_format: str | None,
    destination: str | None,
) -> None:
    """Execute an approved persisted query and dispatch output on schedule.

    output_type: 'redirect' (upload to S3), 'webhook' (POST results), 'kafka' (publish to topic)
    output_format: parquet | csv | json | ndjson | arrow (for redirect only)
    destination: S3 key prefix, webhook URL, or Kafka topic name
    """
    from provisa.api.app import state
    from provisa.registry.store import get_by_stable_id

    logger.info("Scheduled query %s: starting (output_type=%s)", stable_id, output_type)

    async with state.pg_pool.acquire() as conn:
        row = await get_by_stable_id(conn, stable_id)
    if not row:
        logger.error("Scheduled query %s: not found in registry", stable_id)
        return

    sql_list = _parse_compiled_sql(row["compiled_sql"])
    role_id = row.get("developer_id") or "admin"

    from provisa.transpiler.router import Route, decide_route
    from provisa.transpiler.transpile import transpile, transpile_to_trino
    from provisa.executor.direct import execute_direct
    from provisa.executor.trino import execute_trino

    routing_hint = row.get("routing_hint")
    decision = decide_route(
        sources=set(),
        source_types=state.source_types,
        source_dialects=state.source_dialects,
        steward_hint=routing_hint,
    )

    results_list = []
    for sql_item in sql_list:
        try:
            if decision.route == Route.TRINO:
                results_list.append(await execute_trino(transpile_to_trino(sql_item), []))
            else:
                dialect = decision.dialect or "postgres"
                results_list.append(await execute_direct(
                    state.source_pools,
                    decision.source_id or next(iter(state.source_pools), "pg"),
                    transpile(sql_item, dialect),
                    [],
                ))
        except Exception:
            logger.exception("Scheduled query %s: execution failed", stable_id)
            return

    if not results_list:
        return

    rows_as_dicts = [
        dict(zip(r.column_names, row))
        for r in results_list
        for row in r.rows
    ]

    if output_type == "redirect":
        from provisa.executor.redirect import RedirectConfig, upload_result
        from provisa.compiler.sql_gen import ColumnRef
        cfg = RedirectConfig.from_env()
        fmt = output_format or cfg.default_format
        for i, r in enumerate(results_list):
            suffix = f"_{i}" if len(results_list) > 1 else ""
            columns = [ColumnRef(field_name=c, column=c) for c in r.column_names]
            key = f"{destination or stable_id}/{stable_id}{suffix}.{fmt}"
            try:
                url, _ = await upload_result(r, columns, fmt, key, cfg)
                logger.info("Scheduled query %s: uploaded to %s", stable_id, url)
            except Exception:
                logger.exception("Scheduled query %s: redirect upload failed", stable_id)

    elif output_type == "webhook":
        if not destination:
            logger.error("Scheduled query %s: webhook output_type but no destination URL", stable_id)
            return
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    destination,
                    json={"stable_id": stable_id, "rows": rows_as_dicts},
                )
                resp.raise_for_status()
            logger.info("Scheduled query %s: webhook %s returned %s", stable_id, destination, resp.status_code)
        except Exception:
            logger.exception("Scheduled query %s: webhook delivery failed", stable_id)

    elif output_type == "kafka":
        topic = destination or row.get("sink_topic")
        if not topic:
            logger.error("Scheduled query %s: kafka output_type but no topic", stable_id)
            return
        try:
            from provisa.kafka.producer import publish_rows
            await publish_rows(topic, rows_as_dicts, key_column=row.get("sink_key_column"))
            logger.info("Scheduled query %s: published %d rows to %s", stable_id, len(rows_as_dicts), topic)
        except Exception:
            logger.exception("Scheduled query %s: kafka publish failed", stable_id)

    else:
        logger.warning("Scheduled query %s: unknown output_type %r", stable_id, output_type)


def build_scheduler(triggers: list[ScheduledTrigger]) -> AsyncIOScheduler | None:
    """Build an APScheduler instance from config triggers.

    Returns None if no enabled triggers exist.
    """
    enabled = [t for t in triggers if t.enabled]
    if not enabled:
        return None

    scheduler = AsyncIOScheduler()

    for trigger in enabled:
        cron = CronTrigger.from_crontab(trigger.cron)
        if trigger.url:
            scheduler.add_job(
                _execute_webhook,
                trigger=cron,
                args=[trigger.url, trigger.id],
                id=trigger.id,
                name=f"trigger:{trigger.id}",
                replace_existing=True,
            )
            logger.info("Scheduled trigger %s: %s -> %s", trigger.id, trigger.cron, trigger.url)
        elif trigger.function:
            logger.warning(
                "Trigger %s: internal function %s not yet supported",
                trigger.id, trigger.function,
            )

    return scheduler

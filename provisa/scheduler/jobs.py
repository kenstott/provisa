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

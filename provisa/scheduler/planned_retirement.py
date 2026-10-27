# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8c1e27-9b3d-4a6e-8c52-1d7e0b9a3f64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Planned retirements: each org's scheduler retires, once a day, the glossary terms (REQ-2144)
and the data products (REQ-1634) a curator deprecated with a retirement date that has come."""

# Requirements: REQ-2144, REQ-1634

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from provisa.core.database import Database

log = logging.getLogger(__name__)

JOB_ID = "planned_retirement"
#: Shortly after midnight, so a date's retirements happen on that date.
CRON = "5 0 * * *"


async def retire_due_terms(db: "Database", today: datetime.date) -> list[int]:
    """Retire the terms of ``db`` due on or before ``today``; their ids."""
    from provisa.core.repositories import glossary as glossary_repo

    async with db.acquire() as conn:
        async with conn.transaction():
            return await glossary_repo.retire_due(conn, today)


async def retire_due_products(db: "Database", today: datetime.date) -> list[str]:
    """Retire the data products of ``db`` due on or before ``today``; their ids."""
    from provisa.core.repositories import data_product as data_product_repo

    async with db.acquire() as conn:
        async with conn.transaction():
            return await data_product_repo.retire_due(conn, today)


async def retire_org(org_id: str) -> tuple[list[int], list[str]]:
    """``org_id``'s retirements, run by the scheduler: the terms', then the products'."""
    from provisa.api.app import state
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(org_id)
    try:
        db = state.model_db
        assert db is not None, "the org's model store is bound with its runtime"
        today = datetime.date.today()
        terms = await retire_due_terms(db, today)
        products = await retire_due_products(db, today)
    finally:
        reset_current_org(token)
    if terms or products:
        log.info(
            "org %s: %d deprecated term(s) and %d data product(s) retired on their planned date",
            org_id,
            len(terms),
            len(products),
        )
    return terms, products


def job_id(org_id: str) -> str:
    return f"{JOB_ID}:org_{org_id}"


def register_org(scheduler: "AsyncIOScheduler", org_id: str) -> None:
    """Retire ``org_id``'s due terms daily. A job already held is kept."""
    from apscheduler.triggers.cron import CronTrigger

    if scheduler.get_job(job_id(org_id)) is not None:
        return
    scheduler.add_job(
        retire_org,
        trigger=CronTrigger.from_crontab(CRON),
        args=[org_id],
        id=job_id(org_id),
        name=JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

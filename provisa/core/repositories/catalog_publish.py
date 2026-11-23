# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e4b19-5a2d-4f86-9b31-e8d26a7f4c50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The publishes of a Syncing entry and each data product's outcome in them (REQ-2259).

One row per publish of an entry: its outcome (``ok`` or ``failed``), the error that stopped the whole
entry, if any, and when it ran. Beside it, one row per data product the publish sent: ``published``,
or ``failed`` with the catalog's refusal. A product's sync status is its latest row per entry.
"""

# Requirements: REQ-2259

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, insert, select

from provisa.core.schema_org import catalog_publish_products as cpp
from provisa.core.schema_org import catalog_publishes as cp

if TYPE_CHECKING:
    from collections.abc import Iterable

    from provisa.core.database import Connection

OK = "ok"
FAILED = "failed"
OUTCOMES = (OK, FAILED)

#: A data product's outcome in one publish.
PUBLISHED = "published"
PRODUCT_FAILED = "failed"
PRODUCT_OUTCOMES = (PUBLISHED, PRODUCT_FAILED)


@dataclass(frozen=True)
class ProductOutcome:  # REQ-2259
    """One data product's outcome in one publish: ``error`` is the catalog's refusal of a failed one."""

    product_id: str
    outcome: str
    error: str | None = None


@dataclass(frozen=True)
class ProductStatus:  # REQ-2259
    """A data product's latest outcome on one entry, and when that publish ran."""

    entry: str
    product_id: str
    outcome: str
    error: str | None
    published_at: datetime


async def record(
    conn: "Connection",
    entry: str,
    *,
    outcome: str,
    products: "Iterable[ProductOutcome]",
    error: str | None = None,
) -> int:
    """Record one publish of ``entry`` with each data product's outcome in it; returns its id."""
    if outcome not in OUTCOMES:
        raise ValueError(f"unknown publish outcome {outcome!r}")
    rows = list(products)
    for row in rows:
        if row.outcome not in PRODUCT_OUTCOMES:
            raise ValueError(f"unknown data product outcome {row.outcome!r}")
    async with conn.transaction():
        publish_id = await conn.insert_returning(
            cp, {"entry": entry, "outcome": outcome, "error": error}, returning="id"
        )
        await conn.execute_core_many(
            insert(cpp),
            [
                {
                    "publish_id": publish_id,
                    "entry": entry,
                    "product_id": r.product_id,
                    "outcome": r.outcome,
                    "error": r.error,
                }
                for r in rows
            ],
        )
    return int(publish_id)


async def product_statuses(
    conn: "Connection", *, entries: "Iterable[str] | None" = None
) -> list[ProductStatus]:
    """Each data product's latest outcome per entry, by entry then product; only ``entries`` when
    given."""
    latest = select(func.max(cpp.c.id).label("id")).group_by(cpp.c.entry, cpp.c.product_id)
    if entries is not None:
        latest = latest.where(cpp.c.entry.in_(list(entries)))
    latest_ids = latest.subquery()
    result = await conn.execute_core(
        select(cpp.c.entry, cpp.c.product_id, cpp.c.outcome, cpp.c.error, cp.c.published_at)
        .join(latest_ids, latest_ids.c.id == cpp.c.id)
        .join(cp, cp.c.id == cpp.c.publish_id)
        .order_by(cpp.c.entry, cpp.c.product_id)
    )
    return [
        ProductStatus(
            entry=row.entry,
            product_id=row.product_id,
            outcome=row.outcome,
            error=row.error,
            published_at=row.published_at,
        )
        for row in result.fetchall()
    ]

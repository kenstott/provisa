# Copyright (c) 2026 Kenneth Stott
# Canary: 96dc9c41-90d8-4858-a69e-d81b8d3da0f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The nodes now in the cluster (REQ-1916): a cluster is the set of processes started against
one control plane. Each node registers when it starts serving, beats while it runs and leaves
when it stops; the node list — the health endpoint and the admin pages — is every node that beat
within :data:`STALE_AFTER`, each with its mode and, when the platform declares regions, its
region. A node killed without a clean stop drops off when it stops beating."""

# Requirements: REQ-1916, REQ-1922

from __future__ import annotations

import os
import socket
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select, update

from provisa.core import process_mode, process_region
from provisa.core.regions import DEFAULT_REGION
from provisa.core.schema_admin import cluster_nodes

if TYPE_CHECKING:
    from provisa.core.database import Database

TABLES = ("cluster_nodes",)

# This process, for the life of the process.
NODE_ID = uuid.uuid4().hex

# How often a node beats, and how long a silent node is still listed: three missed beats.
HEARTBEAT = timedelta(seconds=15)
STALE_AFTER = HEARTBEAT * 3


def _now() -> datetime:
    return datetime.now(UTC)


async def register(db: "Database") -> None:
    """List this node: its host and process, its mode and its region (None for the implicit one)."""
    region = process_region.region()
    now = _now()
    async with db.acquire() as conn:
        await conn.execute_core(delete(cluster_nodes).where(cluster_nodes.c.node_id == NODE_ID))
        await conn.execute_core(
            cluster_nodes.insert().values(
                node_id=NODE_ID,
                host=socket.gethostname(),
                pid=os.getpid(),
                mode=process_mode.mode(),
                region=None if region == DEFAULT_REGION else region,
                started_at=now,
                last_seen=now,
            )
        )


async def beat(db: "Database") -> None:
    """Say this node is still running."""
    async with db.acquire() as conn:
        await conn.execute_core(
            update(cluster_nodes).where(cluster_nodes.c.node_id == NODE_ID).values(last_seen=_now())
        )


async def unregister(db: "Database") -> None:
    """Take this node off the list (a clean stop)."""
    async with db.acquire() as conn:
        await conn.execute_core(delete(cluster_nodes).where(cluster_nodes.c.node_id == NODE_ID))


async def live(db: "Database", *, now: datetime | None = None) -> list[dict[str, Any]]:
    """The nodes now in the cluster, oldest first. A node's ``region`` is listed only when the
    platform declares regions (REQ-1922: with none, region is never shown)."""
    since = (now or _now()) - STALE_AFTER
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(cluster_nodes)
                .where(cluster_nodes.c.last_seen >= since)
                .order_by(cluster_nodes.c.started_at, cluster_nodes.c.node_id)
            )
        ).fetchall()
    shows_region = process_region.region() != DEFAULT_REGION
    out = []
    for row in rows:
        node = dict(row._mapping)
        if not shows_region:
            node.pop("region")
        out.append(node)
    return out


async def heartbeat_loop(db: "Database") -> None:
    """Beat every :data:`HEARTBEAT` while this node runs. A beat the platform database fails is
    logged and the next one tried: a node that keeps failing drops off the list, which is what
    the list should then say."""
    import asyncio
    import logging

    from sqlalchemy.exc import SQLAlchemyError

    log = logging.getLogger(__name__)
    while True:
        await asyncio.sleep(HEARTBEAT.total_seconds())
        try:
            await beat(db)
        except (SQLAlchemyError, OSError):
            log.exception("node %s could not beat into the platform database", NODE_ID)

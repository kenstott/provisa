# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4e1c97-3f52-4b6d-9e08-d7c1a2f5b340
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a worker reports about its own health — the report ``GET /health`` answers with, for the
surfaces that are not HTTP (Arrow Flight's ``healthcheck`` action).

The shape is ``/health``'s: ``status``, the ``dependencies`` it reaches (``postgres``), the
launch's ``workers`` roll call (ready / expected, REQ-1900) and the ``config`` stamps this worker
has loaded beside the ones stored (REQ-1914)."""

# Requirements: REQ-1900, REQ-1914

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy.exc import SQLAlchemyError


async def health_report(state: Any) -> dict[str, Any]:
    """The health of the worker answering, in the shape of ``GET /health``."""
    from provisa.core.request_context import reset_current_org, set_current_org

    pg_status = "unavailable"
    # The probe asks for no org (it needs no credential): the dependency it reports is the state
    # store of the deployment's own org, named here rather than reached through an unbound read.
    token = set_current_org(state.org_id)
    try:
        tenant_db = state.tenant_db
    finally:
        reset_current_org(token)
    if tenant_db is not None:
        try:
            async with tenant_db.acquire() as conn:
                await conn.fetchval("SELECT 1")
            pg_status = "ok"
        except (SQLAlchemyError, OSError, asyncio.TimeoutError):
            # The control plane cannot be reached: that IS the report, as on /health.
            pg_status = "unavailable"
    from provisa.core.boot_lock import expected_workers, launch_id, ready_worker_count

    launch = launch_id()
    if launch is None:
        ready = 1
    else:
        assert state.admin_db is not None
        ready = await ready_worker_count(state.admin_db, launch)
    from provisa.api.model_reload import health as config_health
    from provisa.core.platform_state import nodes as cluster_nodes

    assert state.platform_state_db is not None  # brought up with the control planes at boot
    return {
        "status": "ok",
        "dependencies": {"postgres": pg_status},
        "workers": {"ready": ready, "expected": expected_workers()},
        "config": await config_health() if pg_status == "ok" else None,
        # REQ-1916: the nodes now in the cluster, each with its mode (and region, when the
        # platform declares regions), so an operator sees whether any node does coordinator work.
        "nodes": [_node_view(n) for n in await cluster_nodes.live(state.platform_state_db)],
    }


def _node_view(node: dict[str, Any]) -> dict[str, Any]:
    """A node as the health report shows it: timestamps as ISO text."""
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in node.items()}

# Copyright (c) 2026 Kenneth Stott
# Canary: 6a17c4e9-2b85-4f30-91d6-7e0c58a3b421
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1072: how a metadata change reaches the catalog, and what happens when it does not.

The queue is a real SQLite control plane — the claim/complete semantics under test are the
substrate's own, so a stubbed queue would prove nothing. The publish is stubbed, because what is
being asserted is the lifecycle around it: that a change queues work, that a drain publishes it
once, and that a failed publish stays claimable instead of being completed away.
"""

# Requirements: REQ-1072, REQ-1073

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest

from sqlalchemy import select

from provisa.api.metadata_export import publishing
from provisa.api.metadata_export.provider import AssetError, AssetRefStub, PublishResult
from provisa.core.schema_org import event_status
from provisa.events import queue
from tests.pg_control_plane import clone

_T0 = datetime(2026, 8, 4, 9, 0, 0, tzinfo=timezone.utc)


@asynccontextmanager
async def _conn(plane):
    async with plane.org().acquire() as conn:
        yield conn


def _ok(count: int = 7) -> PublishResult:
    result = PublishResult(provider_name="atlas")
    result.published["table"] = count
    return result


def _partial() -> PublishResult:
    result = PublishResult(provider_name="atlas")
    result.published["table"] = 1
    result.errors.append(AssetError(asset=AssetRefStub("wh.public.orders"), message="HTTP 500"))
    return result


async def _claim_status(conn) -> list[str]:
    result = await conn.execute_core(select(event_status.c.claim_status))
    return sorted(row[0] for row in result.fetchall())


@pytest.mark.asyncio
async def test_a_metadata_change_queues_work_for_the_export_target(control_plane):
    async with _conn(control_plane) as conn:
        event_id = await publishing.notify_metadata_change(
            conn, table="wh.public.orders", reason="column description edited"
        )
        rows = await queue.get_events(conn, [event_id])
        assert rows[0]["payload"]["metadata_change"] is True
        assert rows[0]["payload"]["reason"] == "column description edited"
        # Fanned to the export target only — a metadata change is not a data change, so it must
        # not enqueue recompute work for the MVs that read the table.
        result = await conn.execute_core(select(event_status.c.dependent_table))
        assert [row[0] for row in result.fetchall()] == [publishing.EGRESS_TARGET]


@pytest.mark.asyncio
async def test_a_drain_publishes_the_claimed_work_once_and_completes_it(monkeypatch, control_plane):
    published: list[str] = []

    async def _publish(org_id: str) -> dict[str, PublishResult]:
        published.append(org_id)
        return {"atlas": _ok()}

    monkeypatch.setattr(publishing, "publish_snapshot", _publish)
    async with _conn(control_plane) as conn:
        await publishing.notify_metadata_change(conn, table="wh.public.orders", reason="edit")
        await publishing.notify_metadata_change(conn, table="wh.public.customers", reason="edit")

        result = await publishing.drain(conn, "acme", now=_T0)

        assert result is not None and result["atlas"].ok
        # Two events, ONE publish: the drain claims the target's whole pending set and coalesces
        # it, so a burst of edits costs one snapshot rather than one per edit.
        assert published == ["acme"]
        assert await _claim_status(conn) == ["completed", "completed"]


@pytest.mark.asyncio
async def test_a_drain_with_nothing_pending_does_not_publish(monkeypatch, control_plane):
    async def _publish(org_id: str) -> dict[str, PublishResult]:
        raise AssertionError("published with no pending work")

    monkeypatch.setattr(publishing, "publish_snapshot", _publish)
    async with _conn(control_plane) as conn:
        assert await publishing.drain(conn, "acme", now=_T0) is None


@pytest.mark.asyncio
async def test_a_failed_publish_leaves_the_work_reclaimable(monkeypatch, control_plane):
    """A completed work item is a promise the catalog holds the change. A partial publish did
    not deliver it, so completing it would drop the change until the next reconcile."""

    async def _publish(org_id: str) -> dict[str, PublishResult]:
        # REQ-2118: one target delivered and one did not; the work is not done until both have.
        return {"datahub": _ok(), "atlas": _partial()}

    monkeypatch.setattr(publishing, "publish_snapshot", _publish)
    async with _conn(control_plane) as conn:
        await publishing.notify_metadata_change(conn, table="wh.public.orders", reason="edit")
        result = await publishing.drain(conn, "acme", now=_T0)
        assert result is not None and not result["atlas"].ok
        assert await _claim_status(conn) == ["claimed"]

        # The REQ-959 reaper returns it to the pool once the lease lapses, and the next drain
        # retries it — which is what "reclaimable" has to mean for the change not to be lost.
        reclaimed = await queue.reclaim(
            conn, now=_T0, heartbeat_cutoff=datetime(2026, 8, 4, 9, 5, tzinfo=timezone.utc)
        )
        assert reclaimed == 1
        assert await _claim_status(conn) == ["unclaimed"]

        monkeypatch.setattr(publishing, "publish_snapshot", _succeed)
        retried = await publishing.drain(conn, "acme", now=_T0)
        assert retried is not None and all(r.ok for r in retried.values())
        assert await _claim_status(conn) == ["completed"]


async def _succeed(org_id: str) -> dict[str, PublishResult]:
    return {"datahub": _ok(), "atlas": _ok()}


@pytest.mark.asyncio
async def test_a_publish_that_raises_leaves_the_work_claimed_rather_than_completed(
    monkeypatch, control_plane
):
    async def _publish(org_id: str) -> dict[str, PublishResult]:
        raise RuntimeError("the catalog refused the connection")

    monkeypatch.setattr(publishing, "publish_snapshot", _publish)
    async with _conn(control_plane) as conn:
        await publishing.notify_metadata_change(conn, table="wh.public.orders", reason="edit")
        with pytest.raises(RuntimeError, match="refused the connection"):
            await publishing.drain(conn, "acme", now=_T0)
        assert await _claim_status(conn) == ["claimed"]


@pytest.mark.asyncio
async def test_two_drains_of_the_same_org_do_not_both_publish(monkeypatch, control_plane):
    """Two processes draining one org would send the catalog the same snapshot twice. The claim
    is what prevents it — which is why this path claims rather than reading the event stream."""
    published: list[str] = []

    async def _publish(org_id: str) -> dict[str, PublishResult]:
        published.append(org_id)
        return {"atlas": _ok()}

    monkeypatch.setattr(publishing, "publish_snapshot", _publish)
    async with _conn(control_plane) as conn:
        await publishing.notify_metadata_change(conn, table="wh.public.orders", reason="edit")
        assert await publishing.drain(conn, "acme", now=_T0) is not None
        assert await publishing.drain(conn, "acme", now=_T0) is None
        assert published == ["acme"]


@pytest.mark.asyncio
async def test_one_orgs_work_is_invisible_to_another(
    control_plane, embedded_postgres, control_plane_template
):
    """Tenant isolation is the queue's own boundary: the events table lives in the org's schema,
    so a second org's drain finds nothing rather than finding the first org's change."""
    globex_plane = clone(embedded_postgres, control_plane_template)
    try:
        await _one_orgs_work_is_invisible_to_another(control_plane, globex_plane)
    finally:
        globex_plane.close()


async def _one_orgs_work_is_invisible_to_another(acme_plane, globex_plane):
    async with _conn(acme_plane) as acme:
        await publishing.notify_metadata_change(acme, table="wh.public.orders", reason="edit")
        async with _conn(globex_plane) as globex:
            assert (
                await queue.claim(
                    globex,
                    dependent_table=publishing.EGRESS_TARGET,
                    processor_name=publishing.PROCESSOR_NAME,
                    now=_T0,
                )
                == []
            )
        assert (
            await queue.claim(
                acme,
                dependent_table=publishing.EGRESS_TARGET,
                processor_name=publishing.PROCESSOR_NAME,
                now=_T0,
            )
            != []
        )

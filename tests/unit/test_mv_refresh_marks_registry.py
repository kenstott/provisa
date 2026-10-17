# Copyright (c) 2026 Kenneth Stott
# Canary: 1858433e-1f2c-4617-a79a-349404b58c6c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view the event loop refreshes is read as fresh.

The loop's refresh landed the view's rows in its store but never told the MV registry, which is
what a read asks whether a view is fresh: only refresh_mv marked it. So a view the loop kept current
stayed stale -- its status stale, no row count -- and every read of it went to its definition. A
committed refresh now tells the registry: a change marks it fresh (with its rows when a full
replace knows them), an unchanged recompute keeps it fresh, a failure marks it failed."""

# Requirements: REQ-543, REQ-941

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.events import queue
from provisa.events.app_wiring import registry_marking
from provisa.events.processor import MVTableProcessor


@asynccontextmanager
async def _db(control_plane):
    yield control_plane.org()


def _view(db, generate, told):
    async def tell(change):
        told.append(change)

    return MVTableProcessor(
        "mv.gold",
        change_signal="ttl",
        watermark_column=None,
        dependents_of=lambda n: [],
        db=db,
        name="box-1",
        generate=generate,
        on_refreshed=tell,
    )


async def _refresh(control_plane, generate) -> list:
    told: list = []
    async with _db(control_plane) as db, db.acquire() as conn:
        up = await queue.post_event(conn, source_table="mv.silver", event_type="replace")
        await queue.fan_out(conn, up, ["mv.gold"])
        await _view(db, generate, told).process_pending(conn)
    return told


@pytest.mark.asyncio
async def test_a_changed_view_tells_its_change(control_plane):
    async def generate(pending, **_kw):
        return ("replace", {"rows": 5, "landed": "store.gold"}, "h1")

    assert await _refresh(control_plane, generate) == [
        ("replace", {"rows": 5, "landed": "store.gold"})
    ]


@pytest.mark.asyncio
async def test_an_unchanged_view_tells_none(control_plane):
    async def generate(pending, **_kw):
        return None

    assert await _refresh(control_plane, generate) == [None]


@pytest.mark.asyncio
async def test_a_failed_view_tells_its_error(control_plane):
    async def generate(pending, **_kw):
        raise RuntimeError("the engine is gone")

    [(event_type, payload)] = await _refresh(control_plane, generate)
    assert event_type == "error" and "the engine is gone" in payload["error"]


class _Registry:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def mark_refreshed(self, mv_id, row_count):
        self.calls.append(("refreshed", mv_id, row_count))

    def mark_unchanged(self, mv_id):
        self.calls.append(("unchanged", mv_id))

    def mark_refresh_failed(self, mv_id, error):
        self.calls.append(("failed", mv_id, error))


@pytest.mark.parametrize(
    ("change", "call"),
    [
        (("replace", {"rows": 5, "landed": "x"}), ("refreshed", "view-gold", 5)),
        (("delta", {"rows": 2, "delta": [], "landed": "x"}), ("refreshed", "view-gold", None)),
        (("replace", {"window_id": "w", "sealed": True}), ("refreshed", "view-gold", None)),
        (None, ("unchanged", "view-gold")),
        (("error", {"error": "boom"}), ("failed", "view-gold", "boom")),
    ],
    ids=["replace", "delta", "bitemporal-seal", "unchanged", "error"],
)
@pytest.mark.asyncio
async def test_the_registry_is_told_what_the_refresh_did(change, call, control_plane, monkeypatch):
    """Only a full replace knows the view's whole row count; a delta's rows are the delta's."""
    from provisa.core import process_region

    monkeypatch.setattr(process_region, "_region", "eu")
    registry = _Registry()
    await registry_marking(registry, control_plane.org(holds="state"))(
        SimpleNamespace(id="view-gold")
    )(change)
    assert registry.calls == [call]


@pytest.mark.asyncio
async def test_a_view_marked_refreshed_is_read_as_fresh(control_plane, monkeypatch):
    from provisa.core import process_region
    from provisa.mv.models import MVDefinition, MVStatus
    from provisa.mv.registry import MVRegistry

    registry = MVRegistry()
    mv = MVDefinition(
        id="view-gold",
        source_tables=[],
        target_catalog="store",
        target_schema="mv_cache",
        sql="SELECT 1 AS x",
        refresh_interval=60,
    )
    registry.register(mv)
    assert mv.status != MVStatus.FRESH
    monkeypatch.setattr(process_region, "_region", "eu")
    await registry_marking(registry, control_plane.org(holds="state"))(mv)(
        ("replace", {"rows": 5, "landed": "x"})
    )
    assert mv.status == MVStatus.FRESH and mv.row_count == 5
    assert registry.get_fresh() == [mv]

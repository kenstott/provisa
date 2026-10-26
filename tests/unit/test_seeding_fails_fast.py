# Copyright (c) 2026 Kenneth Stott
# Canary: 9d5016c1-e213-45a1-ac04-21cc8193ee23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A bundle build's seeding run fails by name, at once (REQ-2068): a template that cannot be
seeded says why beside the record the build waits for, and a regions template whose views wait on
another region's node that does not answer is such a failure."""

from __future__ import annotations

import pytest

from provisa.api import demo_region_seed, demo_templates


async def test_a_seeding_template_that_is_not_ready_says_why_beside_its_record(
    tmp_path, monkeypatch
):
    demo = type("Demo", (), {"name": "shop", "directory": tmp_path / "shop"})()
    demo.directory.mkdir()

    async def _not_ready(d, build):
        demo_templates._seeding_failures[d.name] = "replicas failed to land: shop.orders"
        return False

    monkeypatch.setattr(demo_templates, "_prepare", _not_ready)
    monkeypatch.setattr(demo_region_seed, "regional", lambda: False)
    assert await demo_templates._prepare_seeding(demo, True) is False
    assert (demo.directory / "seeded" / "failed").read_text() == (
        "replicas failed to land: shop.orders"
    )


async def test_a_seeding_template_that_raises_says_so_and_raises(tmp_path, monkeypatch):
    demo = type("Demo", (), {"name": "shop", "directory": tmp_path / "shop"})()
    demo.directory.mkdir()

    async def _raises(d, build):
        raise RuntimeError("these bitemporal views were never built: mv_x")

    monkeypatch.setattr(demo_templates, "_prepare", _raises)
    monkeypatch.setattr(demo_region_seed, "regional", lambda: True)
    monkeypatch.setattr(demo_region_seed, "here", lambda: "warehouse")
    with pytest.raises(RuntimeError):
        await demo_templates._prepare_seeding(demo, True)
    marker = demo.directory / "seeded" / "regions" / "warehouse" / "failed"
    assert marker.read_text() == "RuntimeError: these bitemporal views were never built: mv_x"


async def test_another_region_whose_node_does_not_answer_is_named(monkeypatch):
    monkeypatch.setattr(demo_region_seed, "here", lambda: "warehouse")
    addresses = {"ledger": "http://127.0.0.1:41001", "warehouse": "http://127.0.0.1:41002"}

    async def _probe(address):
        return address != "http://127.0.0.1:41001"

    assert await demo_region_seed.unanswered_regions(addresses, probe=_probe) == [
        ("ledger", "http://127.0.0.1:41001")
    ]

    async def _all_up(address):
        return True

    assert await demo_region_seed.unanswered_regions(addresses, probe=_all_up) == []


def test_a_region_fails_the_wait_only_once_it_has_not_answered_for_the_grace():
    """Its nodes start together: one still starting is not dead. It is once it has gone
    unanswered for the grace, and one that answers again starts over."""
    grace = demo_templates._REGION_ANSWER_GRACE_S
    down = {"ledger": "http://127.0.0.1:41001"}
    dead, since = demo_templates.silent_regions(down, {}, 100.0)
    assert dead == []
    dead, since = demo_templates.silent_regions(down, since, 100.0 + grace - 1)
    assert dead == []
    dead, since = demo_templates.silent_regions(down, since, 100.0 + grace)
    assert dead == [("ledger", "http://127.0.0.1:41001")]
    dead, since = demo_templates.silent_regions({}, since, 100.0 + grace + 1)
    assert (dead, since) == ([], {})


def test_every_view_its_region_keeps_is_built_once_its_replicas_have_landed(monkeypatch):
    """Each view a region keeps is built by its seeding run once that region's replicas have
    landed: banking's end-of-day totals read only in place, so no replica build fans out to it
    (CI run 40318311383: 0 of 1 views built, no build recorded, for 1800 s), and retail's EU
    marketing audience, built as the node started from replicas not yet landed, was carried empty
    (CI run 40357867904). A view kept in another region is that region's."""
    from types import SimpleNamespace

    from provisa.core.models import DERIVED_SOURCE_ID

    monkeypatch.setattr(demo_region_seed, "here", lambda: "warehouse")

    def view(name, region):
        return SimpleNamespace(
            source_id=DERIVED_SOURCE_ID, materialize=True, table_name=name, region=region
        )

    config = SimpleNamespace(
        tables=[
            view("view_eod_totals", "warehouse"),
            view("view_rollup", "warehouse"),
            view("view_ledger_side", "ledger"),
        ]
    )
    assert demo_region_seed.views_kept_here_ids(config) == [
        "view-view_eod_totals",
        "view-view_rollup",
    ]


def test_a_view_built_before_its_replicas_landed_is_not_built(monkeypatch):
    """A build counts once it ran after every replica its region keeps had landed."""
    from datetime import UTC, datetime, timedelta

    monkeypatch.setattr(demo_region_seed, "here", lambda: "eu")
    landed = datetime(2026, 10, 26, 16, 21, tzinfo=UTC)
    early = {"region": "eu", "last_refresh_at": landed - timedelta(seconds=5)}
    after = {"region": "eu", "last_refresh_at": landed + timedelta(seconds=5)}
    never = {"region": "eu", "last_refresh_at": None}
    elsewhere = {"region": "us", "last_refresh_at": landed + timedelta(seconds=5)}
    assert [demo_region_seed._built_since(b, landed) for b in (early, after, never, elsewhere)] == [
        False,
        True,
        False,
        False,
    ]

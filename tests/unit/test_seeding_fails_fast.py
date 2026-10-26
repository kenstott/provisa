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

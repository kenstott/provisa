# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1f8e42-a7b0-4d93-9e64-0b3a7d2f6c85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One build per replica in a process, outliving the request that asked (REQ-826, REQ-1661)."""

# Requirements: REQ-826, REQ-1661

from __future__ import annotations

import asyncio
import threading

import pytest

from provisa.core import request_deadline
from provisa.federation import replica_build
from provisa.federation.replica_build import ReplicaBuilding, build_once


@pytest.fixture(autouse=True)
def _fresh_registry(monkeypatch):
    monkeypatch.setattr(replica_build, "_builds", {})


async def test_requests_for_one_replica_share_one_build():
    started = 0
    release = threading.Event()

    async def _build() -> int:
        nonlocal started
        started += 1
        await asyncio.to_thread(release.wait)
        return 150_000

    waiting = [
        asyncio.ensure_future(build_once("src.public.wide", _build, budget=None)) for _ in range(5)
    ]
    await asyncio.sleep(0.2)
    release.set()
    assert await asyncio.gather(*waiting) == [150_000] * 5
    assert started == 1


async def test_a_request_out_of_time_fails_by_name_and_the_build_goes_on():
    release = threading.Event()
    finished = threading.Event()

    async def _build() -> str:
        await asyncio.to_thread(release.wait)
        finished.set()
        return "built"

    with pytest.raises(ReplicaBuilding) as refused:
        await build_once("src.public.wide", _build, budget=0.6)
    assert refused.value.replica == "src.public.wide"
    assert "the replica of src.public.wide is still being built" in str(refused.value)
    assert isinstance(refused.value, TimeoutError)  # transports report it as a deadline
    assert not finished.is_set()

    async def _must_not_start() -> str:
        raise AssertionError("a second build was started while the first was running")

    # The next request joins the running build instead of starting another.
    joined = asyncio.ensure_future(build_once("src.public.wide", _must_not_start, budget=None))
    await asyncio.sleep(0.1)
    release.set()
    assert await joined == "built"
    assert finished.is_set()


async def test_a_failed_build_fails_every_waiter_and_the_next_request_builds_again():
    async def _fails() -> None:
        raise RuntimeError("source unreachable")

    with pytest.raises(RuntimeError, match="source unreachable"):
        await build_once("src.public.wide", _fails, budget=None)

    async def _ok() -> str:
        return "built"

    assert await build_once("src.public.wide", _ok, budget=None) == "built"


async def test_the_build_does_not_inherit_the_requests_deadline():
    seen: list[float | None] = []

    async def _build() -> None:
        seen.append(request_deadline.remaining())

    with request_deadline.within(30):
        assert request_deadline.remaining() is not None
        await build_once("src.public.wide", _build, budget=None)
    assert seen == [None]


async def test_two_replicas_build_independently():
    async def _build(value: str) -> str:
        return value

    first = await build_once("src.public.orders", lambda: _build("orders"), budget=None)
    second = await build_once("src.public.customers", lambda: _build("customers"), budget=None)
    assert (first, second) == ("orders", "customers")

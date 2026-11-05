# Copyright (c) 2026 Kenneth Stott
# Canary: 604d7a61-74a2-48f7-8ff7-aabb15b0b8f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A lane's cleanup leaves the Fabric capacity paused from whatever state it finds it in.

A bare suspend is refused (400) unless the capacity is Active, and that refusal says nothing
about billing: the core UI lane reported "NOT paused" on one without knowing the state (run
38050265191)."""

from __future__ import annotations

import pytest

from tests.integration import fabric_capacity


class _Response:
    def raise_for_status(self):
        return self


class _Client:
    def __init__(self):
        self.posts = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def post(self, url, **kwargs):
        self.posts.append(url)
        return _Response()


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _capacity(monkeypatch, states):
    """A capacity found in states[0] that then reads as each later state in turn."""
    client = _Client()
    remaining = list(states[1:])
    product = fabric_capacity._product
    monkeypatch.setattr(product, "capacity_configured", lambda: True)
    monkeypatch.setattr(product, "_client", lambda: client)
    monkeypatch.setattr(product, "_locate", lambda c: ("https://arm/capacity", states[0]))
    monkeypatch.setattr(
        product, "_state", lambda c, url: remaining.pop(0) if remaining else states[-1]
    )
    return client


@pytest.mark.parametrize("state", ["Paused", "Suspended"])
def test_a_paused_capacity_is_left_alone(monkeypatch, state):
    client = _capacity(monkeypatch, [state])
    clock = _Clock()
    assert fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock) == state
    assert client.posts == []


def test_an_active_capacity_is_suspended_once_and_waited_for(monkeypatch):
    client = _capacity(monkeypatch, ["Active", "Pausing", "Pausing", "Paused"])
    clock = _Clock()
    assert fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock) == "Paused"
    assert client.posts == ["https://arm/capacity/suspend"]


def test_each_state_the_capacity_passes_through_is_logged_with_its_elapsed_seconds(
    monkeypatch, capsys
):
    """How long a pause takes is read from the lane's log: the state found, then each change with
    the seconds since the capacity was first read."""
    _capacity(monkeypatch, ["Active", "Pausing", "Pausing", "Paused"])
    clock = _Clock()
    fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock)
    assert capsys.readouterr().out.splitlines() == [
        "Fabric capacity: Active at 0s",
        "Fabric capacity: Active -> Pausing at 10s",
        "Fabric capacity: Pausing -> Paused at 30s",
    ]


def test_a_capacity_still_resuming_is_waited_out_then_suspended(monkeypatch):
    client = _capacity(monkeypatch, ["Resuming", "Resuming", "Active", "Pausing", "Paused"])
    clock = _Clock()
    assert fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock) == "Paused"
    assert client.posts == ["https://arm/capacity/suspend"]


def test_a_capacity_already_pausing_is_not_sent_a_suspend(monkeypatch):
    client = _capacity(monkeypatch, ["Pausing", "Paused"])
    clock = _Clock()
    assert fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock) == "Paused"
    assert client.posts == []


def test_a_capacity_that_does_not_end_paused_is_reported_as_possibly_billing(monkeypatch):
    _capacity(monkeypatch, ["Active", "Active"])
    clock = _Clock()
    with pytest.raises(RuntimeError, match="may be billing"):
        fabric_capacity.ensure_capacity_paused(sleep=clock.sleep, clock=clock)
    assert clock.now >= fabric_capacity._PAUSE_TIMEOUT_S


def test_an_unconfigured_capacity_is_a_misconfiguration(monkeypatch):
    monkeypatch.setattr(fabric_capacity._product, "capacity_configured", lambda: False)
    with pytest.raises(RuntimeError, match="FABRIC_RESOURCE_GROUP"):
        fabric_capacity.ensure_capacity_paused()


@pytest.mark.parametrize("state", ["Paused", "Suspended", "Pausing"])
def test_teardowns_suspend_sends_no_suspend_to_a_capacity_that_is_not_active(monkeypatch, state):
    """Run 40024234397: a teardown suspended a capacity that was not Active and ARM refused it
    (400), failing the test. The lane's suspend is the pause that suspends only from Active."""
    client = _capacity(monkeypatch, [state, "Paused"])
    monkeypatch.setattr(fabric_capacity, "_PAUSE_POLL_S", 0.0)
    fabric_capacity.suspend_capacity()
    assert client.posts == []

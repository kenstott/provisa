# Copyright (c) 2026 Kenneth Stott
# Canary: 9c2f4a81-3d67-4e15-8b0a-2a6e3c7f41d9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1149: the `signal` change_signal — a data-less external refresh trigger.

Covers the signal vocabulary (push-timed, poll-style re-pull, not CDC), the trigger registry that
receives a data-less trigger and exposes it as a freshness token, and that the REQ-1141 scheduler
treats an arrived trigger as a changed probe (deferring the heavy pull to the window).
"""

from __future__ import annotations

import pytest

from provisa.core import change_signal as cs
from provisa.federation.change_trigger import ChangeTriggerRegistry
from provisa.federation.scheduled_refresh import should_scheduled_refresh


# ------------------------------------------------------- vocabulary


def test_signal_is_valid_and_a_trigger():
    assert "signal" in cs.VALID_SIGNALS
    assert cs.is_trigger("signal")
    assert not cs.is_push("signal")  # not a CDC carrier
    assert not cs.is_poll("signal")


def test_signal_landing_is_poll_style_not_cdc():
    # a signal carries no rows: it re-pulls (replace / watermark-append), never CDC upserts
    assert cs.select_landing_shape("signal", watermark_column=None) == cs.REPLACE
    assert cs.select_landing_shape("signal", watermark_column="ts") == cs.APPEND


def test_signal_provider_is_a_receiver():
    assert cs.to_provider("signal", "mongodb") == "signal"


def test_signal_has_no_poll_freshness_mode():
    assert cs.to_freshness_mode("signal") is None


# ------------------------------------------------------- trigger registry


@pytest.mark.asyncio
async def test_registry_token_starts_at_zero():
    reg = ChangeTriggerRegistry()
    assert reg.token("src.s.t") == "0"


@pytest.mark.asyncio
async def test_registry_signal_bumps_token():
    reg = ChangeTriggerRegistry()
    await reg.signal("k")
    assert reg.token("k") == "1"
    await reg.signal("k")
    assert reg.token("k") == "2"


@pytest.mark.asyncio
async def test_registry_keys_are_independent():
    reg = ChangeTriggerRegistry()
    await reg.signal("a")
    assert reg.token("a") == "1"
    assert reg.token("b") == "0"


# ------------------------------------------------------- scheduler integration


@pytest.mark.asyncio
async def test_scheduler_pulls_when_trigger_arrives():
    reg = ChangeTriggerRegistry()
    key = "orders.public.t"
    stored = reg.token(key)  # "0" — baseline captured at last refresh

    # No trigger yet: token unchanged → scheduler must NOT pull.
    probe_changed = reg.token(key) != stored
    assert should_scheduled_refresh(
        now=1000.0, window=None, last_refresh_at=100.0, cadence=None,
        probe_changed=probe_changed,
    ) is False

    # A data-less trigger arrives → token bumps → scheduler pulls on the next evaluation.
    await reg.signal(key)
    probe_changed = reg.token(key) != stored
    assert probe_changed is True
    assert should_scheduled_refresh(
        now=1000.0, window=None, last_refresh_at=100.0, cadence=None,
        probe_changed=probe_changed,
    ) is True


@pytest.mark.asyncio
async def test_receive_trigger_module_entrypoint():
    from provisa.federation.change_trigger import get_registry, receive_trigger, trigger_token

    key = "webhook.test.k"
    before = trigger_token(key)
    await receive_trigger(key)
    assert trigger_token(key) != before
    assert get_registry().token(key) == trigger_token(key)


def test_signal_is_probe_capable_in_policy():
    # a signal source arms the probe gate so the REQ-1141 scheduler runs its trigger-token check
    from types import SimpleNamespace

    from provisa.federation.scheduled_refresh import resolve_refresh_policy

    source = SimpleNamespace(
        load_protected=True, off_peak_window=None, off_peak_tz="UTC",
        cache_ttl=None, change_signal="signal",
    )
    table = SimpleNamespace(
        load_protected=None, off_peak_window=None, off_peak_tz=None,
        cache_ttl=None, change_signal=None, live=None,
    )
    policy = resolve_refresh_policy(source, table)
    assert policy.probe_capable
    assert policy.armed

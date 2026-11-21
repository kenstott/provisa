# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3b1c94-2f60-4a8d-b6d1-95c0d4e8a217
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A push listener keeps landing, so a subscription on its table keeps delivering (REQ-1733,
REQ-1734, REQ-258).

A Kafka table's subscription sends a frame when its listener lands a batch. On 2026-11-01 the broker
dropped the consumer of a streaming sandbox as it opened (its landing held the consumer past the
session timeout): the commit after a landed batch failed, that failure ended the listener, and its
key, left behind, kept every later wiring from starting another. The subscription's first result
arrived and nothing after it."""

# Requirements: REQ-1733, REQ-1734, REQ-258

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from aiokafka.errors import CommitFailedError

from provisa.events import push_wiring
from provisa.subscriptions import cdc_landing
from provisa.subscriptions.base import ChangeEvent
from provisa.subscriptions.kafka_provider import commit_landed


def _event() -> ChangeEvent:
    return ChangeEvent(
        operation="insert",
        table="orders",
        row={"order_id": "1"},
        timestamp=datetime.now(timezone.utc),
        ack_token=("orders", 0, 7),
    )


async def test_a_commit_the_group_refused_ends_nothing(caplog):
    """The batch is landed, and a landing applies by key: read again after the consumer rejoins, it
    changes nothing. Not committing it costs that read, and is said."""

    async def _refused(offsets):
        raise CommitFailedError("the group has rebalanced")

    with caplog.at_level(logging.WARNING):
        await commit_landed(SimpleNamespace(commit=_refused), [_event()])
    assert "read again" in caplog.text


async def test_any_other_commit_failure_is_raised():
    async def _broken(offsets):
        raise OSError("broker gone")

    with pytest.raises(OSError):
        await commit_landed(SimpleNamespace(commit=_broken), [_event()])


def _listener(disconnect: threading.Event) -> dict:
    return dict(
        state=SimpleNamespace(),
        engine=SimpleNamespace(),
        provider=object(),
        watch_target="orders",
        land_schema="s",
        land_table="orders",
        columns=[("order_id", "varchar")],
        pk_columns=["order_id"],
        disconnect=disconnect,
        debounce_quiet=0.0,
        debounce_max_delay=5.0,
        node="orders/default.orders",
        log=logging.getLogger("test"),
        key=("orders", "default", "orders"),
    )


async def test_a_listener_that_crashed_starts_again(monkeypatch):
    runs: list[int] = []

    async def _consume(*args, **kwargs):
        runs.append(1)
        if len(runs) < 3:
            raise RuntimeError("the stream failed")
        return {"upsert": 0, "delete": 0}

    monkeypatch.setattr(cdc_landing, "consume_cdc_into_store", _consume)
    monkeypatch.setattr(push_wiring, "_RESTART_AFTER_S", 0.0)
    await push_wiring._run_listener(**_listener(threading.Event()))
    assert len(runs) == 3


async def test_a_listener_told_to_stop_does_not_start_again(monkeypatch):
    disconnect = threading.Event()
    runs: list[int] = []

    async def _consume(*args, **kwargs):
        runs.append(1)
        disconnect.set()
        raise RuntimeError("the stream failed")

    monkeypatch.setattr(cdc_landing, "consume_cdc_into_store", _consume)
    monkeypatch.setattr(push_wiring, "_RESTART_AFTER_S", 0.0)
    await asyncio.wait_for(push_wiring._run_listener(**_listener(disconnect)), timeout=2)
    assert len(runs) == 1


async def test_the_wait_before_a_restart_grows_to_its_limit():
    assert push_wiring._restart_delay(0) == push_wiring._RESTART_AFTER_S
    assert push_wiring._restart_delay(1) == 2 * push_wiring._RESTART_AFTER_S
    assert push_wiring._restart_delay(50) == push_wiring._RESTART_AT_MOST_S

# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-813 (amended 2026-10-21): a live Kafka table may declare a landing-time column, which
Provisa fills with the time it lands each message -- stamped as mail landing stamps its rows,
one stamp per landed batch, each later than the last. It is when the row was recorded here,
whatever time the message itself carries.
"""

# Requirements: REQ-813

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.core.models import Column, LiveDeliveryConfig, LiveKafkaParams, Table
from provisa.subscriptions.base import ChangeEvent

pytestmark = pytest.mark.asyncio

# Placed a week ago, as the feed backdates them; landed now.
_PLACED = datetime(2026, 10, 14, 9, 0, tzinfo=UTC)


async def _land(batches: list[list[dict]], landing_time_column: str | None) -> list[list[dict]]:
    """Run one listener over ``batches`` (each applied as one batch) and return the rows each
    apply was given."""
    from provisa.events.push_wiring import _run_listener

    applied: list[list[dict]] = []

    class _Engine:
        async def apply_cdc_events(self, *, events, **kwargs):
            applied.append([dict(e.row) for e in events])
            return {"upsert": len(events), "delete": 0, "unchanged": []}

    offsets = iter(range(1000))

    class _Provider:
        async def watch(self, target):
            for batch in batches:
                for row in batch:
                    yield ChangeEvent(
                        operation="insert",
                        table="orders",
                        row=row,
                        timestamp=_PLACED,
                        ack_token=("orders", 0, next(offsets)),
                    )

        async def ack(self, events):
            pass

        async def close(self):
            pass

    with (
        patch("provisa.events.ripple.ripple", AsyncMock()),
        patch("provisa.scheduler.table_triggers.table_changed", MagicMock()),
    ):
        await _run_listener(
            state=SimpleNamespace(),
            engine=_Engine(),
            provider=_Provider(),
            watch_target="orders",
            land_schema="s",
            land_table="orders",
            columns=[("id", "varchar"), ("ordered_at", "timestamp"), ("landed_at", "timestamp")],
            pk_columns=["id"],
            disconnect=threading.Event(),
            debounce_quiet=0.0,  # one batch per message
            debounce_max_delay=5.0,
            node="feeds/s.orders",
            log=logging.getLogger("test"),
            key=("feeds", "s", "orders"),
            landing_time_column=landing_time_column,
        )
    return applied


async def test_each_landed_row_carries_the_time_provisa_landed_it():
    before = datetime.now(UTC)
    applied = await _land(
        [[{"id": "o-1", "ordered_at": _PLACED}], [{"id": "o-2", "ordered_at": _PLACED}]],
        "landed_at",
    )
    after = datetime.now(UTC)
    stamps = [row["landed_at"] for batch in applied for row in batch]
    assert len(stamps) == 2
    # When it landed here, not when it was placed.
    assert all(before <= s <= after for s in stamps)
    assert all(s - _PLACED > timedelta(days=1) for s in stamps)
    # A batch landed later is stamped later.
    assert stamps[0] < stamps[1]


async def test_a_table_declaring_no_landing_time_gets_none():
    applied = await _land([[{"id": "o-1", "ordered_at": _PLACED}]], None)
    assert applied == [[{"id": "o-1", "ordered_at": _PLACED}]]


def _table(landing: str, data_type: str = "timestamp") -> Table:
    return Table(
        source_id="feeds",
        domain_id="d",
        schema_name="default",
        table_name="orders",
        live=LiveDeliveryConfig(
            strategy="kafka",
            kafka=LiveKafkaParams(topic="orders", landing_time_column=landing),
        ),
        columns=[
            Column(name="id", data_type="varchar", is_primary_key=True, visible_to=["*"]),
            Column(name="landed_at", data_type=data_type, visible_to=["*"]),
        ],
    )


async def test_the_landing_time_column_is_a_timestamp_column_of_the_table():
    assert _table("landed_at").live.kafka.landing_time_column == "landed_at"
    with pytest.raises(ValueError, match="not one of its columns"):
        _table("arrived_at")
    with pytest.raises(ValueError, match="not a timestamp"):
        _table("landed_at", data_type="varchar")

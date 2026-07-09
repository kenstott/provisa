# Copyright (c) 2026 Kenneth Stott
# Canary: 5c855b61-b793-49ba-abec-3107dc015741
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""pytest-bdd step implementations for REQ-922 - Change Data Capture."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.subscriptions.debezium_provider import (
    DebeziumNotificationProvider,
    _UNPARSEABLE_TS,
)

scenarios("../features/REQ-922.feature")


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_provider() -> DebeziumNotificationProvider:
    return DebeziumNotificationProvider(
        bootstrap_servers="localhost:9092",
        topic_prefix="dbserver1",
        database="mydb",
        consumer_group_id="test-group",
    )


# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------


@given("a Debezium envelope with missing or unparseable ts_ms")
def step_given_envelope_missing_ts_ms(shared_data: dict) -> None:
    """Prepare two representative envelopes: one with ts_ms absent, one unparseable."""
    # Envelope 1: ts_ms key entirely absent (snapshot/tombstone scenario)
    envelope_missing = {
        "payload": {
            "op": "r",
            "after": {"id": 1, "name": "Alice"},
            # ts_ms intentionally omitted
        }
    }

    # Envelope 2: ts_ms present but unparseable (corrupted / non-numeric value)
    envelope_bad = {
        "payload": {
            "op": "c",
            "after": {"id": 2, "name": "Bob"},
            "ts_ms": "not-a-number",
        }
    }

    shared_data["envelopes"] = [envelope_missing, envelope_bad]
    shared_data["provider"] = _make_provider()


@when("the CDC provider processes the record")
def step_when_provider_processes_record(shared_data: dict) -> None:
    """Call _extract_event for every envelope stored in shared_data."""
    provider: DebeziumNotificationProvider = shared_data["provider"]
    events = []
    for envelope in shared_data["envelopes"]:
        event = provider._extract_event(envelope, table="orders")
        assert event is not None, (
            f"_extract_event returned None for envelope {envelope!r}; expected a ChangeEvent"
        )
        events.append(event)
    shared_data["events"] = events


@then("it uses datetime.min as the event timestamp")
def step_then_uses_datetime_min(shared_data: dict) -> None:
    """Every event produced from a missing/unparseable ts_ms must carry the sentinel."""
    expected = _UNPARSEABLE_TS
    # Confirm the module-level sentinel is datetime.min (UTC-aware) - REQ-922 invariant.
    assert expected == datetime.min.replace(tzinfo=timezone.utc), (
        "_UNPARSEABLE_TS must equal datetime.min.replace(tzinfo=timezone.utc)"
    )

    for event in shared_data["events"]:
        assert event.timestamp == expected, (
            f"Expected sentinel {expected!r}, got {event.timestamp!r} for event {event!r}"
        )


@then("the watermark does not advance beyond real events")
def step_then_watermark_does_not_advance(shared_data: dict) -> None:
    """Demonstrate that mixing sentinel events with real events keeps the watermark
    anchored to the earliest *real* timestamp rather than being polluted by clock
    time or a fabricated now()-based fallback.
    """
    provider: DebeziumNotificationProvider = shared_data["provider"]

    # Construct a real event with a known ts_ms (1 Jan 2024 noon UTC).
    real_ts_ms = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    real_envelope = {
        "payload": {
            "op": "c",
            "after": {"id": 99, "name": "Real"},
            "ts_ms": real_ts_ms,
        }
    }
    real_event = provider._extract_event(real_envelope, table="orders")
    assert real_event is not None

    expected_real_ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert real_event.timestamp == expected_real_ts, (
        f"Real event timestamp mismatch: expected {expected_real_ts!r}, "
        f"got {real_event.timestamp!r}"
    )

    # Confirm that the sentinel sorts strictly before the real timestamp,
    # which means a watermark implemented as max(seen timestamps) will only
    # advance when it sees a genuine ts_ms - never because of a missing one.
    sentinel_ts = shared_data["events"][0].timestamp  # from missing ts_ms envelope
    assert sentinel_ts < real_event.timestamp, (
        f"Sentinel {sentinel_ts!r} must sort before real event timestamp "
        f"{real_event.timestamp!r} to preserve watermark monotonicity"
    )

    # Also verify the sentinel is strictly less than *now*, proving that using
    # datetime.min never masquerades as a current wall-clock time.
    now_utc = datetime.now(tz=timezone.utc)
    assert sentinel_ts < now_utc, (
        f"Sentinel {sentinel_ts!r} must be less than now ({now_utc!r}); "
        "a now()-based fallback would equal now and could advance the watermark"
    )

    # Store for potential downstream inspection.
    shared_data["real_event"] = real_event
    shared_data["sentinel_ts"] = sentinel_ts

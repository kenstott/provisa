# Copyright (c) 2026 Kenneth Stott
# Canary: 9c3e7b52-1a8d-4f64-8e27-5b0d2f9a6c13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A subscription request cannot move the operator's settings (REQ-030, amended 2026-09-30).

The operator decides which Kafka cluster a sink writes to and which column (if any) a table is
polled by — the watermark governs how, and whether, the upstream is polled. A ``@sink(broker:)`` /
``X-Provisa-Sink`` broker or a ``@watermark`` that differs from the operator's is rejected naming
the setting; repeating the operator's value is accepted.
"""

# Requirements: REQ-030, REQ-176, REQ-260

from __future__ import annotations

import pytest

from provisa.api.data.subscription_sse import sink_broker_within_floor, watermark_within_floor
from provisa.core.operator_floor import OperatorFloorError


def test_no_requested_broker_uses_the_operators(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.internal:9092")
    assert sink_broker_within_floor(None) == "kafka.internal:9092"


def test_the_operators_broker_repeated_is_accepted(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.internal:9092")
    assert sink_broker_within_floor("kafka.internal:9092") == "kafka.internal:9092"


def test_a_different_broker_is_rejected(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.internal:9092")
    with pytest.raises(OperatorFloorError) as exc:
        sink_broker_within_floor("attacker.example:9092")
    assert "KAFKA_BOOTSTRAP_SERVERS" in str(exc.value)


def test_a_sink_with_no_operator_broker_is_rejected(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(OperatorFloorError):
        sink_broker_within_floor("anywhere:9092")
    with pytest.raises(OperatorFloorError):
        sink_broker_within_floor(None)


def test_watermark_defaults_to_the_registered_one():
    assert watermark_within_floor(None, "updated_at", "orders") == "updated_at"
    assert watermark_within_floor(None, None, "orders") is None


def test_repeating_the_registered_watermark_is_accepted():
    assert watermark_within_floor("updated_at", "updated_at", "orders") == "updated_at"


@pytest.mark.parametrize("registered", ["updated_at", None])
def test_a_different_or_new_watermark_is_rejected(registered):
    with pytest.raises(OperatorFloorError) as exc:
        watermark_within_floor("created_at", registered, "orders")
    assert "watermark" in str(exc.value) and "orders" in str(exc.value)

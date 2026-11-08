# Copyright (c) 2026 Kenneth Stott
# Canary: 0e85515e-3f57-46e8-88a8-6b68d9948f0f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A sandbox starts with its demo's live-landed data (REQ-2049, amended 2026-11-03).

A table a listener lands from a Kafka topic (``live: {strategy: kafka}``) is neither a replica nor
a view: the provisioning demo's orders, prices and stock counts. Its template lands it from the
topic; a sandbox opened from the template used to land it itself, from the topic's first message,
so its first query, and its views' first refresh, read it empty -- the view recorded every order
gone. Now:

- the template is ready only once each listener has landed its topic to where it ended when the
  template's preparation began;
- an open records where the template's listener stopped as where the sandbox's landing stopped,
  then copies the template's landed rows, so the sandbox's listener continues from there: a
  message landed between the two is read again and upserted by key, never missed."""

# Requirements: REQ-2049

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api import demo_replicas

pytestmark = pytest.mark.unit


def _source(
    source_id: str,
    type_: str,
    host: str = "broker",
    port: int = 9092,
    username: str | None = None,
    password: str | None = None,
    federation_hints: dict | None = None,
):
    return SimpleNamespace(
        id=source_id,
        type=SimpleNamespace(value=type_),
        host=host,
        port=port,
        username=username,
        password=password,
        federation_hints=federation_hints,
    )


def _table(source_id: str, table: str, topic: str | None):
    live = (
        None
        if topic is None
        else SimpleNamespace(strategy="kafka", kafka=SimpleNamespace(topic=topic))
    )
    return SimpleNamespace(source_id=source_id, schema_name="default", table_name=table, live=live)


def _config(feeds=None):
    return SimpleNamespace(
        sources=[feeds or _source("feeds", "kafka"), _source("warehouse", "postgresql")],
        tables=[
            _table("feeds", "orders", "orders_ns"),
            _table("feeds", "prices", "prices_ns"),
            _table("warehouse", "products", None),
        ],
    )


def test_the_live_landed_tables_are_those_a_listener_lands_from_a_topic():
    assert [t.table_name for t in demo_replicas.live_landed_tables(_config())] == [
        "orders",
        "prices",
    ]


class _Broker:
    """Each consumer group's committed offsets, by topic and partition; and each topic's end."""

    def __init__(self, committed: dict, ends: dict | None = None):
        self.committed = committed
        self.ends = ends or {}
        self.calls: list[tuple] = []

    async def recorded(self, bootstrap, group, topic, *, security):
        self.calls.append(("recorded", bootstrap, group, security))
        return dict(self.committed.get((group, topic), {}))

    async def record(self, bootstrap, group, offsets, *, security):
        self.calls.append(("record", bootstrap, group, security))
        for tp, offset in offsets.items():
            self.committed.setdefault((group, tp.topic), {})[tp.partition] = offset

    async def end_offsets(self, bootstrap, topic, *, security):
        from aiokafka import TopicPartition  # type: ignore[import-untyped]

        self.calls.append(("end_offsets", bootstrap, topic, security))

        return {TopicPartition(topic, p): o for p, o in self.ends.get(topic, {}).items()}


def _patch(monkeypatch, broker: _Broker) -> None:
    from provisa.kafka import landed_offsets

    monkeypatch.setattr(landed_offsets, "recorded", broker.recorded)
    monkeypatch.setattr(landed_offsets, "record", broker.record)
    monkeypatch.setattr(landed_offsets, "end_offsets", broker.end_offsets)


async def test_a_sandbox_landing_continues_where_the_templates_listener_stopped(monkeypatch):
    broker = _Broker(
        {
            ("provisa-sbxtplprovisioning-feeds", "orders_ns"): {0: 412},
            ("provisa-sbxtplprovisioning-feeds", "prices_ns"): {0: 26},
        }
    )
    _patch(monkeypatch, broker)
    await demo_replicas.continue_live_landing("sbxtplprovisioning", "sbx1234", _config())
    assert broker.committed[("provisa-landed-sbx1234-feeds", "orders_ns")] == {0: 412}
    assert broker.committed[("provisa-landed-sbx1234-feeds", "prices_ns")] == {0: 26}
    assert {call[1] for call in broker.calls} == {"broker:9092"}


async def test_a_template_waits_until_its_listeners_land_their_topics_to_where_they_ended(
    monkeypatch,
):
    broker = _Broker(
        {("provisa-sbxtplprovisioning-feeds", "orders_ns"): {0: 100}},
        ends={"orders_ns": {0: 412}, "prices_ns": {}},
    )
    _patch(monkeypatch, broker)
    ends = await demo_replicas.live_topic_ends(_config())
    assert await demo_replicas.live_landing_waits("sbxtplprovisioning", _config(), ends) == [
        "feeds.default.orders: landed to 100 of 412 on partition 0"
    ]
    broker.committed[("provisa-sbxtplprovisioning-feeds", "orders_ns")] = {0: 412}
    assert await demo_replicas.live_landing_waits("sbxtplprovisioning", _config(), ends) == []


async def test_a_secured_sources_offsets_are_read_and_recorded_over_its_tls_and_sasl(
    monkeypatch,
):
    """REQ-147: the template's listener reaches a secured cluster with the source's settings, and
    so does every offset an open reads or records on that cluster for the sandbox."""
    from provisa.kafka.security import KafkaSecurity

    feeds = _source(
        "feeds",
        "kafka",
        username="svc",
        password="s3cret",
        federation_hints={
            "kafka_security_protocol": "SASL_SSL",
            "kafka_sasl_mechanism": "SCRAM-SHA-512",
        },
    )
    broker = _Broker(
        {("provisa-sbxtplprovisioning-feeds", "orders_ns"): {0: 412}},
        ends={"orders_ns": {0: 412}, "prices_ns": {}},
    )
    _patch(monkeypatch, broker)
    config = _config(feeds)
    ends = await demo_replicas.live_topic_ends(config)
    await demo_replicas.live_landing_waits("sbxtplprovisioning", config, ends)
    await demo_replicas.continue_live_landing("sbxtplprovisioning", "sbx1234", config)
    secured = KafkaSecurity.of_source(feeds)
    assert secured.sasl and secured.tls
    assert {call[0] for call in broker.calls} == {"end_offsets", "recorded", "record"}
    assert all(call[3] == secured for call in broker.calls)


async def test_a_topic_with_no_message_has_nothing_to_wait_for(monkeypatch):
    broker = _Broker({}, ends={"orders_ns": {0: 0}, "prices_ns": {}})
    _patch(monkeypatch, broker)
    ends = await demo_replicas.live_topic_ends(_config())
    assert await demo_replicas.live_landing_waits("sbxtplprovisioning", _config(), ends) == []


async def test_an_open_records_where_the_landing_stopped_before_it_copies_the_rows(monkeypatch):
    """A demo with live-landed tables and no replica (provisioning) has its template's store
    copied too; where the template's listeners stopped is recorded first."""
    from provisa.api import demo_sandbox

    order: list[str] = []

    async def recorded_first(template_org, org_id, config):
        order.append(f"continue {template_org}->{org_id}")

    async def copied(template_org, org_id):
        order.append(f"copy {template_org}->{org_id}")

    monkeypatch.setattr(demo_replicas, "continue_live_landing", recorded_first)
    monkeypatch.setattr(demo_replicas, "copy_to_sandbox", copied)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    await demo_sandbox.carry_landed_data("sbxtplprovisioning", "sbx1234", _config(), "provisioning")
    assert order == [
        "continue sbxtplprovisioning->sbx1234",
        "copy sbxtplprovisioning->sbx1234",
    ]


async def test_an_open_of_a_demo_landing_nothing_copies_nothing(monkeypatch):
    from provisa.api import demo_sandbox

    order: list[str] = []

    async def copied(template_org, org_id):
        order.append("copy")

    monkeypatch.setattr(demo_replicas, "copy_to_sandbox", copied)
    monkeypatch.setattr("provisa.api.demo_region_seed.regional", lambda: False)
    config = SimpleNamespace(sources=[_source("warehouse", "postgresql")], tables=[])
    await demo_sandbox.carry_landed_data("sbxtplstarthere", "sbx1234", config, "starthere")
    assert order == []

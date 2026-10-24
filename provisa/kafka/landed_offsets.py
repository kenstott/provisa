# Copyright (c) 2026 Kenneth Stott
# Canary: c02a6c8e-9b96-45fd-8c93-235cca38166b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a table's landing stopped on its Kafka topic, so what reads onward starts there
(REQ-1733, REQ-1734, REQ-990).

A table fed by a topic is landed whole by a build -- the topic drained, or, for a table its CDC
topic follows, the source database read -- and then kept by what reads the topic onward: a
listener's consumer group, or a SingleStore pipeline. That reader must start exactly where the
landing stopped. From the newest offset it misses what was published in between; from the
earliest it applies again what the landing applied, and the table's triggers run again for it.

So a landing records the offset it stopped at on each partition of its topic, committed under a
consumer group of its own per org, environment and source (:func:`landed_group`), in Kafka beside
the offsets it names. A listener's group that has committed nothing on a partition starts there
(:func:`start_where_landed`); a new pipeline is set there (:func:`recorded`)."""

# Requirements: REQ-1733, REQ-1734, REQ-990

from __future__ import annotations

from typing import Any

#: The prefix of the consumer group a landing's offsets are committed under.
LANDED_GROUP_PREFIX = "provisa-landed"


def landed_group(source_id: str) -> str:
    """The group the bound org's (and environment's) landings of ``source_id``'s tables commit
    where they stopped under: each org lands a topic into its own table, at its own time. Named
    where the org is bound, and handed to what reads the topic onward."""
    from provisa.core.environments import PROD
    from provisa.core.request_context import active_env, require_current_org

    org, env = require_current_org(), active_env()
    if env == PROD:
        return f"{LANDED_GROUP_PREFIX}-{org}-{source_id}"
    return f"{LANDED_GROUP_PREFIX}-{org}-{env}-{source_id}"


def _consumer(bootstrap: str, group: str | None) -> Any:
    from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

    return AIOKafkaConsumer(bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False)


async def _partitions(consumer: Any, topic: str) -> list[Any]:
    from aiokafka import TopicPartition  # type: ignore[import-untyped]

    # The cluster's metadata as fetched now, which names the topic's partitions. Not
    # consumer.topics() and partitions_for_topic(): the first returns only the names of the
    # metadata it fetched, and the second reads the client's own, which for a consumer
    # subscribed to nothing names no topic at all.
    cluster = await consumer._client.fetch_all_metadata()  # noqa: SLF001
    return [TopicPartition(topic, p) for p in sorted(cluster.partitions_for_topic(topic) or ())]


async def _committed(consumer: Any, partitions: list[Any]) -> dict[Any, int]:
    found = {tp: await consumer.committed(tp) for tp in partitions}
    return {tp: offset for tp, offset in found.items() if offset is not None}


async def drained_to(consumer: Any) -> dict[Any, int]:
    """The next offset ``consumer`` reads on each partition it is assigned: where its drain
    stopped."""
    return {tp: await consumer.position(tp) for tp in consumer.assignment()}


async def end_offsets(bootstrap: str, topic: str) -> dict[Any, int]:
    """The offset each of ``topic``'s partitions publishes its next message at."""
    consumer = _consumer(bootstrap, None)
    await consumer.start()
    try:
        partitions = await _partitions(consumer, topic)
        return dict(await consumer.end_offsets(partitions)) if partitions else {}
    finally:
        await consumer.stop()


async def record(bootstrap: str, landed: str, offsets: dict[Any, int]) -> None:
    """Record, under the landing group ``landed`` (:func:`landed_group`), that a landing stopped
    at ``offsets``."""
    if not offsets:
        return
    consumer = _consumer(bootstrap, landed)
    await consumer.start()
    try:
        consumer.assign(list(offsets))
        await consumer.commit(dict(offsets))
    finally:
        await consumer.stop()


async def recorded(bootstrap: str, landed: str, topic: str) -> dict[int, int]:
    """Where the last landing recorded under ``landed`` stopped on ``topic``, by partition;
    empty when none recorded any."""
    consumer = _consumer(bootstrap, landed)
    await consumer.start()
    try:
        partitions = await _partitions(consumer, topic)
        consumer.assign(partitions)
        return {tp.partition: o for tp, o in (await _committed(consumer, partitions)).items()}
    finally:
        await consumer.stop()


async def start_where_landed(bootstrap: str, group: str, topic: str, landed: str) -> None:
    """Commit for ``group``, on each partition of ``topic`` it has committed nothing on, where the
    landing stopped: its first start reads from there, neither missing what was published since
    nor reading again what the landing applied. A partition the group has committed on resumes
    where it did."""
    stopped = await recorded(bootstrap, landed, topic)
    consumer = _consumer(bootstrap, group)
    await consumer.start()
    try:
        partitions = await _partitions(consumer, topic)
        consumer.assign(partitions)
        resumed = await _committed(consumer, partitions)
        fresh = {
            tp: stopped[tp.partition]
            for tp in partitions
            if tp not in resumed and tp.partition in stopped
        }
        if fresh:
            await consumer.commit(fresh)
    finally:
        await consumer.stop()

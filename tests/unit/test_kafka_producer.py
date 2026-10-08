# Copyright (c) 2026 Kenneth Stott
# Canary: 452310ec-7eff-4051-8817-817682182137
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's Kafka producer (provisa/kafka/producer.py): aiokafka, on a thread of its own.

The broker is a stand-in here (tests/integration/test_change_events_kafka_e2e.py has a real one):
what is asserted is the producer's own conduct -- a message handed over from any thread is sent
from the producer's thread, a message that cannot be delivered neither raises to the caller nor
stops the producer, its reason is logged once, and stop sends what was handed over."""

from __future__ import annotations

import logging
import threading

import pytest

import aiokafka
from provisa.kafka.producer import Producer


class _Broker:
    """What every stand-in client of a test delivers to, and whether it is reachable."""

    def __init__(self) -> None:
        self.delivered: list[tuple[str, bytes, bytes | None]] = []
        self.sending_threads: set[str] = set()
        self.down: Exception | None = None
        self.clients_started = 0
        self.clients_stopped = 0
        self.got = threading.Semaphore(0)


@pytest.fixture
def broker(monkeypatch):
    broker = _Broker()

    class _Client:
        def __init__(self, *, bootstrap_servers: str, client_id: str) -> None:
            self.bootstrap_servers, self.client_id = bootstrap_servers, client_id

        async def start(self) -> None:
            if broker.down is not None:
                raise broker.down
            broker.clients_started += 1

        async def stop(self) -> None:
            broker.clients_stopped += 1

        async def send_and_wait(self, topic, value=None, key=None) -> None:
            try:
                if broker.down is not None:
                    raise broker.down
                broker.delivered.append((topic, value, key))
                broker.sending_threads.add(threading.current_thread().name)
            finally:
                broker.got.release()

    monkeypatch.setattr(aiokafka, "AIOKafkaProducer", _Client)
    return broker


async def test_a_message_handed_over_from_any_thread_is_sent_from_the_producers_own(broker):
    producer = Producer("broker:9092", client_id="unit")
    try:
        other = threading.Thread(target=producer.send, args=("t", b"v1", b"k1"), name="request-1")
        other.start()
        other.join()
        producer.send("t", b"v2")
        assert broker.got.acquire(timeout=10) and broker.got.acquire(timeout=10)
    finally:
        await producer.stop()
    assert broker.delivered == [("t", b"v1", b"k1"), ("t", b"v2", None)]
    assert broker.sending_threads == {"provisa-bg:kafka-producer:unit"}


async def test_stop_sends_what_was_handed_over_and_ends_the_thread(broker):
    producer = Producer("broker:9092", client_id="unit")
    for n in range(20):
        producer.send("t", str(n).encode())
    await producer.stop()
    assert [value for _t, value, _k in broker.delivered] == [str(n).encode() for n in range(20)]
    assert producer._task.done()  # noqa: SLF001 - its thread ended
    assert broker.clients_stopped == broker.clients_started == 1


async def test_an_undeliverable_message_does_not_raise_and_its_reason_is_logged_once(
    broker, caplog
):
    broker.down = ConnectionError("no broker at broker:9092")
    producer = Producer("broker:9092", client_id="unit")
    try:
        with caplog.at_level(logging.DEBUG, logger="provisa.kafka.producer"):
            for n in range(5):
                producer.send("t", str(n).encode())  # returns; the caller's write is unaffected
            await producer.stop()
    finally:
        await producer.stop()
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warnings) == 1, [r.getMessage() for r in warnings]
    assert "no broker at broker:9092" in warnings[0].getMessage()
    assert warnings[0].exc_info is None
    assert broker.delivered == []


async def test_delivery_resumes_when_the_broker_is_back_and_says_so(broker, caplog):
    broker.down = ConnectionError("no broker at broker:9092")
    producer = Producer("broker:9092", client_id="unit")
    try:
        with caplog.at_level(logging.DEBUG, logger="provisa.kafka.producer"):
            producer.send("t", b"lost")
            _settle(producer)
            broker.down = None
            producer.send("t", b"kept")
            assert broker.got.acquire(timeout=10)
            _settle(producer)
    finally:
        await producer.stop()
    assert broker.delivered == [("t", b"kept", None)]
    said = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(said) == 2 and "delivered again" in said[1], said


def _settle(producer: Producer) -> None:
    """Wait until the producer's thread has taken everything handed over so far."""
    import asyncio

    assert producer._loop is not None and producer._queue is not None  # noqa: SLF001
    done = threading.Event()

    async def _mark() -> None:
        while not producer._queue.empty():  # noqa: SLF001
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)  # the message being sent when the queue emptied
        done.set()

    asyncio.run_coroutine_threadsafe(_mark(), producer._loop)  # noqa: SLF001
    assert done.wait(10)

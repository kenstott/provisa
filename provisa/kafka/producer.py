# Copyright (c) 2026 Kenneth Stott
# Canary: 727e0c26-d075-4f5b-bebc-572b7b5e3dec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The product's Kafka producer: aiokafka, the one Kafka client the product ships.

A producer belongs to one event loop, and requests run on threads of their own with loops of their
own (REQ-1882). So a producer runs on a thread of its own for the life of the process, and
``send`` hands it a message from any thread without waiting: the write that caused the message
does not wait on a broker and is not failed by one.

A message that cannot be delivered is dropped and the reason logged -- once, until delivery works
again or the reason changes, so a broker that is down does not write a line per message."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any

from provisa.core.connection_loop import LongLived, spawn_long_lived

log = logging.getLogger(__name__)

# How long ``stop`` waits for the messages already handed over to be sent.
_STOP_SECONDS = 10.0
_STOP = object()

# The process's producers, one per cluster: change events, sinks and live outputs that name the
# same brokers share one connection.
_shared: dict[str, "Producer"] = {}
_shared_lock = threading.Lock()


def shared(bootstrap_servers: str) -> "Producer":
    """The process's producer for ``bootstrap_servers``, started with its first use and stopped
    by the lifespan (:func:`stop_all`)."""
    with _shared_lock:
        producer = _shared.get(bootstrap_servers)
        if producer is None:
            producer = _shared[bootstrap_servers] = Producer(bootstrap_servers, client_id="provisa")
        return producer


async def stop_all() -> None:
    """Send what every producer of the process was handed, and stop them. Called by the lifespan
    after everything that writes has stopped."""
    with _shared_lock:
        producers = list(_shared.values())
        _shared.clear()
    for producer in producers:
        await producer.stop()


class Producer:
    """Messages to one Kafka cluster, sent from a thread of this producer's own."""

    def __init__(self, bootstrap_servers: str, *, client_id: str) -> None:
        self._bootstrap = bootstrap_servers
        self._client_id = client_id
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[Any] | None = None
        self._ready = threading.Event()
        self._undelivered: str | None = None  # the reason last logged, while delivery is failing
        self._task: LongLived = spawn_long_lived(self._run(), name=f"kafka-producer:{client_id}")
        self._ready.wait()

    def send(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        """Hand one message over. Returns at once, from any thread."""
        assert self._loop is not None and self._queue is not None  # set before __init__ returns
        self._loop.call_soon_threadsafe(self._queue.put_nowait, (topic, value, key))

    async def stop(self) -> None:
        """Send what was handed over, then end the producer's thread (bounded)."""
        assert self._loop is not None and self._queue is not None
        self._loop.call_soon_threadsafe(self._queue.put_nowait, _STOP)
        if not await self._task.wait(_STOP_SECONDS):
            self._task.cancel()
            await self._task.wait(_STOP_SECONDS)

    async def _run(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()
        self._ready.set()
        client: Any = None
        try:
            while True:
                message = await self._queue.get()
                if message is _STOP:
                    return
                topic, value, key = message
                try:
                    if client is None:
                        client = await self._connect()
                    await client.send_and_wait(topic, value=value, key=key)
                except Exception as exc:  # allow-ble: a message that cannot be delivered must not stop the producer; the reason is logged below
                    self._say_undelivered(topic, exc)
                    if client is not None:
                        await client.stop()
                        client = None  # connect again with the next message
                    continue
                if self._undelivered is not None:
                    log.warning("%s: messages are being delivered again", self._client_id)
                    self._undelivered = None
        finally:
            if client is not None:
                await client.stop()

    async def _connect(self) -> Any:
        from aiokafka import AIOKafkaProducer

        client = AIOKafkaProducer(bootstrap_servers=self._bootstrap, client_id=self._client_id)
        try:
            await client.start()
        except BaseException:
            await client.stop()
            raise
        log.info("%s: connected to %s", self._client_id, self._bootstrap)
        return client

    def _say_undelivered(self, topic: str, exc: Exception) -> None:
        reason = f"{type(exc).__name__}: {exc}"
        if reason == self._undelivered:
            return
        self._undelivered = reason
        log.warning(
            "%s: a message to %s on %s was not delivered, and those after it are not being "
            "delivered until this clears: %s",
            self._client_id,
            topic,
            self._bootstrap,
            reason,
        )

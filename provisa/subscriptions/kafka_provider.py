# Copyright (c) 2026 Kenneth Stott
# Canary: daf5e228-2ef2-4813-a0d0-154babce6989
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Kafka subscription provider using aiokafka."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

from provisa.subscriptions.base import ChangeEvent, NotificationProvider

log = logging.getLogger(__name__)


class KafkaNotificationProvider(NotificationProvider):
    """Consumes from a Kafka topic and maps messages to ChangeEvent."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str = "provisa-subscriptions",
        **consumer_kwargs: Any,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._consumer_kwargs = consumer_kwargs
        self._consumer: Any | None = None

    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]

        topic = table
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            auto_offset_reset="latest",
            **self._consumer_kwargs,
        )
        await self._consumer.start()
        log.info("KafkaProvider: consuming topic %s", topic)

        try:
            async for msg in self._consumer:
                try:
                    value = json.loads(msg.value) if isinstance(msg.value, (bytes, str)) else msg.value
                except (json.JSONDecodeError, TypeError):
                    log.warning("KafkaProvider: invalid message on %s", topic)
                    continue

                if isinstance(value, dict):
                    op = value.pop("op", "insert").lower()
                    row = value.pop("row", value)
                else:
                    op = "insert"
                    row = {"value": value}

                yield ChangeEvent(
                    operation=op,
                    table=table,
                    row=row if isinstance(row, dict) else {"value": row},
                    timestamp=datetime.fromtimestamp(
                        msg.timestamp / 1000, tz=timezone.utc
                    ) if msg.timestamp else datetime.now(timezone.utc),
                )
        finally:
            await self._consumer.stop()
            self._consumer = None

    async def close(self) -> None:
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

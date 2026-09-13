# Copyright (c) 2026 Kenneth Stott
# Canary: 109f9cce-e2f9-4266-ada2-7a6a226678f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Base types and abstract provider for subscription notifications."""

# Requirements: REQ-258, REQ-260, REQ-261, REQ-282, REQ-338, REQ-1734

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncGenerator


@dataclass
class ChangeEvent:  # REQ-258, REQ-260, REQ-261, REQ-282, REQ-338
    """A single change notification."""

    operation: str  # "insert", "update", "delete"
    table: str
    row: dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # REQ-1734: opaque, provider-specific position identity (Kafka: (topic, partition, offset)) —
    # carried through so a consumer can call ack() on exactly the events it has durably applied,
    # never on events still sitting unflushed in a debounce buffer. None for a provider with no
    # offset/position concept (websocket, mongo, postgres LISTEN/NOTIFY).
    ack_token: Any = None


class NotificationProvider(abc.ABC):  # REQ-258, REQ-260, REQ-261, REQ-282, REQ-338
    """Abstract base for change-notification providers."""

    @abc.abstractmethod
    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        """Yield change events for *table*, optionally filtered."""
        yield  # type: ignore[misc]  # bare yield required to make Python treat this as an async generator; Pyright can't infer the yield type without an expression  # pragma: no cover  # noqa: B027
        raise NotImplementedError  # pragma: no cover

    async def ack(self, events: list[ChangeEvent]) -> None:  # REQ-1734
        """Acknowledge that *events* have been durably applied (e.g. landed in the store) — commit
        any provider-side read position past them, so a restart never replays them. No-op by
        default: most providers (websocket, mongo, postgres LISTEN/NOTIFY) have no offset/position
        concept at all. KafkaNotificationProvider overrides this to commit the given events'
        offsets specifically — never "wherever the consumer currently is", which could be past
        OTHER events still sitting unflushed in a caller's debounce buffer."""
        return

    @abc.abstractmethod
    async def close(self) -> None:
        """Release provider resources."""
        ...

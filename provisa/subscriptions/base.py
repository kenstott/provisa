# Copyright (c) 2025 Kenneth Stott
# Canary: 109f9cce-e2f9-4266-ada2-7a6a226678f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Base types and abstract provider for subscription notifications."""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncGenerator


@dataclass
class ChangeEvent:
    """A single change notification."""

    operation: str  # "insert", "update", "delete"
    table: str
    row: dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class NotificationProvider(abc.ABC):
    """Abstract base for change-notification providers."""

    @abc.abstractmethod
    async def watch(
        self, table: str, filter_expr: str | None = None
    ) -> AsyncGenerator[ChangeEvent, None]:
        """Yield change events for *table*, optionally filtered."""
        yield  # pragma: no cover  # noqa: B027
        raise NotImplementedError  # pragma: no cover

    @abc.abstractmethod
    async def close(self) -> None:
        """Release provider resources."""
        ...

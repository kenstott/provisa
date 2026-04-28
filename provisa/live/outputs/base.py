# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Abstract base class for live query output sinks."""

from __future__ import annotations

from abc import ABC, abstractmethod


class LiveOutput(ABC):
    """Abstract output sink for live query rows."""

    @abstractmethod
    async def send(self, rows: list[dict]) -> None:
        """Deliver *rows* to the output destination."""

    @abstractmethod
    async def close(self) -> None:
        """Release resources held by the output."""

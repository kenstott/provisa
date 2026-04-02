# Copyright (c) 2025 Kenneth Stott
# Canary: 9395989b-3971-4ae2-b073-444f3d6120fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Abstract driver interface for direct database execution."""

from __future__ import annotations

from abc import ABC, abstractmethod

from provisa.executor.trino import QueryResult


class DirectDriver(ABC):
    """Abstract async driver for direct database execution."""

    @abstractmethod
    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        """Establish connection/pool."""

    @abstractmethod
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        """Execute SQL and return rows + column names."""

    @abstractmethod
    async def close(self) -> None:
        """Close connection/pool."""

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Whether the driver has an active connection."""

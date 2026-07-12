# Copyright (c) 2026 Kenneth Stott
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

from provisa.executor.result import QueryResult

# Requirements: REQ-027, REQ-052


class DirectDriver(ABC):  # REQ-027, REQ-052
    """Abstract async driver for direct database execution."""

    def configure(self, extra: dict[str, str]) -> None:  # REQ-986/987/988
        """Accept source-specific connection extras from ``Source.federation_hints`` before
        ``connect`` — e.g. Databricks ``http_path``, Snowflake ``account``/``warehouse``, ClickHouse
        native-vs-http ``scheme``. The standard ``connect(host, port, database, user, password)`` args
        can't carry these, so warehouse drivers override this to stash them. No-op by default, so the
        RDBMS drivers are untouched."""

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

    async def execute_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE/DROP/ALTER). No return value."""
        # Default: fall back to execute() and discard result
        await self.execute(sql)

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Whether the driver has an active connection."""

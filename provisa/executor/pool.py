# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8bc2a3-5f4f-4831-a6d4-73d3fcc84295
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warm connection pool per registered RDBMS source (REQ-052).

Uses pluggable drivers from provisa.executor.drivers.registry.
Pools created at startup, destroyed on shutdown.
"""

from __future__ import annotations

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.registry import create_driver
from provisa.executor.trino import QueryResult

# Requirements: REQ-027, REQ-031, REQ-052, REQ-053

_SOURCE_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "singlestore": "mysql",
    "mariadb": "mysql",
    "duckdb": "duckdb",
    "sqlserver": "tsql",
    "oracle": "oracle",
}


class SourcePool:  # REQ-052, REQ-053
    """Manages DirectDriver instances keyed by source_id."""

    def __init__(self) -> None:
        self._drivers: dict[str, DirectDriver] = {}
        self._dialects: dict[str, str] = {}

    async def add(  # REQ-052, REQ-053
        self,
        source_id: str,
        source_type: str,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_size: int = 1,
        max_size: int = 5,
        use_pgbouncer: bool = False,
        pgbouncer_port: int = 6432,
    ) -> None:
        """Create a driver connection for a source.

        For PostgreSQL with use_pgbouncer=True, connects through PgBouncer
        on pgbouncer_port instead of direct PG port.
        """
        if source_id in self._drivers:
            return
        driver = create_driver(source_type, use_pgbouncer=use_pgbouncer)
        connect_port = pgbouncer_port if use_pgbouncer else port
        await driver.connect(host, connect_port, database, user, password, min_size, max_size)
        self._drivers[source_id] = driver
        self._dialects[source_id] = _SOURCE_DIALECT.get(source_type, source_type)

    def dialect_for(self, source_id: str) -> str | None:  # REQ-550
        """Return the sqlglot dialect string for a source, or None if unknown."""
        return self._dialects.get(source_id)

    def get(self, source_id: str) -> DirectDriver:  # REQ-550
        """Get driver for a source. Raises KeyError if not registered."""
        return self._drivers[source_id]

    def has(self, source_id: str) -> bool:  # REQ-550
        return source_id in self._drivers

    async def execute(  # REQ-027, REQ-031
        self,
        source_id: str,
        sql: str,
        params: list | None = None,
    ) -> QueryResult:
        """Execute SQL against a source's driver."""
        driver = self._drivers[source_id]
        return await driver.execute(sql, params)

    async def execute_ddl(self, source_id: str, sql: str) -> None:  # REQ-031
        driver = self._drivers[source_id]
        await driver.execute_ddl(sql)

    async def remove(self, source_id: str) -> None:  # REQ-550
        """Close and remove a driver for a source."""
        driver = self._drivers.pop(source_id, None)
        if driver is not None:
            await driver.close()

    async def close_all(self) -> None:
        """Close all drivers."""
        for driver in self._drivers.values():
            await driver.close()
        self._drivers.clear()

    async def close(self, source_id: str) -> None:
        """Close and remove a single driver."""
        driver = self._drivers.pop(source_id, None)
        if driver:
            await driver.close()

    @property
    def source_ids(self) -> list[str]:  # REQ-550
        return list(self._drivers.keys())

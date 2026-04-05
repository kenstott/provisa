# Copyright (c) 2026 Kenneth Stott
# Canary: 7c892555-98ad-475c-87f6-1f574db4b90b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warm Tables — auto-promote frequently queried RDBMS tables to Iceberg (REQ-AD5).

QueryCounter tracks per-table query frequency in memory.
WarmTableManager promotes/demotes tables based on threshold and size limits.
"""

from __future__ import annotations

import logging
import threading

log = logging.getLogger(__name__)

DEFAULT_QUERY_THRESHOLD = 100
DEFAULT_MAX_ROWS = 10_000_000
DEFAULT_ICEBERG_CATALOG = "iceberg"
DEFAULT_ICEBERG_SCHEMA = "warm_cache"


class QueryCounter:
    """Thread-safe in-memory counter for per-table query frequency."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def increment(self, table: str) -> None:
        with self._lock:
            self._counts[table] = self._counts.get(table, 0) + 1

    def get_counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def get_count(self, table: str) -> int:
        with self._lock:
            return self._counts.get(table, 0)

    def reset(self, table: str) -> None:
        with self._lock:
            self._counts.pop(table, None)


class WarmTableManager:
    """Manages promotion/demotion of frequently queried tables to Iceberg."""

    def __init__(
        self,
        iceberg_catalog: str = DEFAULT_ICEBERG_CATALOG,
        iceberg_schema: str = DEFAULT_ICEBERG_SCHEMA,
    ) -> None:
        self._warm_tables: set[str] = set()
        self._iceberg_catalog = iceberg_catalog
        self._iceberg_schema = iceberg_schema
        self._lock = threading.Lock()

    def get_warm_tables(self) -> set[str]:
        with self._lock:
            return set(self._warm_tables)

    def _iceberg_ref(self, table: str) -> str:
        safe = table.replace('"', '""')
        return f'"{self._iceberg_catalog}"."{self._iceberg_schema}"."{safe}"'

    def check_promotions(
        self,
        counter: QueryCounter,
        trino_conn: object,
        threshold: int = DEFAULT_QUERY_THRESHOLD,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> list[str]:
        """Promote tables exceeding query threshold if under max_rows.

        Returns list of newly promoted table names.
        """
        counts = counter.get_counts()
        promoted: list[str] = []

        for table, count in counts.items():
            if count < threshold:
                continue
            with self._lock:
                if table in self._warm_tables:
                    continue

            # Size check
            cursor = trino_conn.cursor()
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            row_count = cursor.fetchone()[0]

            if row_count > max_rows:
                log.info(
                    "Skipping warm promotion for %s: %d rows exceeds max %d",
                    table, row_count, max_rows,
                )
                continue

            # CTAS into Iceberg
            target = self._iceberg_ref(table)
            cursor.execute(f'CREATE TABLE {target} AS SELECT * FROM {table}')
            cursor.fetchall()

            with self._lock:
                self._warm_tables.add(table)
            promoted.append(table)
            log.info("Promoted %s to warm Iceberg cache (%d rows)", table, row_count)

        return promoted

    def check_demotions(
        self,
        counter: QueryCounter,
        trino_conn: object,
        threshold: int = DEFAULT_QUERY_THRESHOLD,
    ) -> list[str]:
        """Demote tables that have fallen below query threshold.

        Returns list of demoted table names.
        """
        counts = counter.get_counts()
        demoted: list[str] = []

        with self._lock:
            candidates = set(self._warm_tables)

        for table in candidates:
            if counts.get(table, 0) >= threshold:
                continue

            target = self._iceberg_ref(table)
            cursor = trino_conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS {target}')
            cursor.fetchall()

            with self._lock:
                self._warm_tables.discard(table)
            demoted.append(table)
            counter.reset(table)
            log.info("Demoted %s from warm Iceberg cache", table)

        return demoted

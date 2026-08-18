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

# Requirements: REQ-238, REQ-239, REQ-240, REQ-241

from __future__ import annotations

import logging
import threading
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_QUERY_THRESHOLD = 100
DEFAULT_MAX_ROWS = 10_000_000
DEFAULT_ICEBERG_CATALOG = "iceberg"
DEFAULT_ICEBERG_SCHEMA = "warm_cache"


class QueryCounter:  # REQ-239
    """Thread-safe in-memory counter for per-table query frequency."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def increment(self, table: str) -> None:  # REQ-595
        with self._lock:
            self._counts[table] = self._counts.get(table, 0) + 1

    def get_counts(self) -> dict[str, int]:  # REQ-595
        with self._lock:
            return dict(self._counts)

    def get_count(self, table: str) -> int:  # REQ-595
        with self._lock:
            return self._counts.get(table, 0)

    def reset(self, table: str) -> None:  # REQ-595
        with self._lock:
            self._counts.pop(table, None)


class WarmTableManager:  # REQ-238, REQ-240, REQ-241
    """Manages promotion/demotion of frequently queried tables to Iceberg.

    In multi-tenant mode, instantiate one WarmTableManager per tenant, passing
    the tenant_id so each tenant gets an isolated Iceberg schema
    (``warm_cache_<tenant_id>``). The per-tenant instance should be obtained
    from TenantContext — do not share a single instance across tenants.
    """

    def __init__(
        self,
        iceberg_catalog: str = DEFAULT_ICEBERG_CATALOG,
        iceberg_schema: str = DEFAULT_ICEBERG_SCHEMA,
        tenant_id: str | None = None,
    ) -> None:
        self._warm_tables: set[str] = set()
        # Row count measured at promotion time, kept so the admin cache view can report the size of
        # each warm copy without re-counting through the engine on every page load.
        self._warm_rows: dict[str, int] = {}
        self._iceberg_catalog = iceberg_catalog
        # Each tenant gets an isolated schema so warm tables never bleed across tenants.
        if tenant_id is not None:
            self._iceberg_schema = f"warm_cache_{tenant_id.replace('-', '_')}"
        else:
            self._iceberg_schema = iceberg_schema
        self._lock = threading.Lock()

    def get_warm_tables(self) -> set[str]:  # REQ-544
        with self._lock:
            return set(self._warm_tables)

    def snapshot(self) -> list[dict]:
        """Admin view of the warm tier, shaped like HotTableManager.snapshot().

        Each entry: table_name, catalog, schema, row_count, is_api, loaded. The name a warm table
        is promoted under is the source FQN the query counter increments ("catalog"."schema"."tbl"),
        so catalog and schema are read back off it; a warm table is landed the moment it is in the
        set, which is why ``loaded`` is always True here.
        """
        with self._lock:
            names = sorted(self._warm_tables)
            rows = dict(self._warm_rows)
        out: list[dict] = []
        for name in names:
            parts = [p.strip('"') for p in name.split('"."')]
            out.append(
                {
                    "table_name": parts[-1],
                    "catalog": parts[0] if len(parts) == 3 else "",
                    "schema": parts[1] if len(parts) == 3 else "",
                    "row_count": rows[name],
                    "is_api": False,
                    "loaded": True,
                }
            )
        return out

    def _iceberg_ref(self, table: str) -> str:
        safe = table.replace('"', '""')
        return f'"{self._iceberg_catalog}"."{self._iceberg_schema}"."{safe}"'

    async def check_promotions(  # REQ-239, REQ-240, REQ-241
        self,
        counter: QueryCounter,
        engine: Any,
        threshold: int = DEFAULT_QUERY_THRESHOLD,
        max_rows: int = DEFAULT_MAX_ROWS,
        hot_tables: set[str] | None = None,
        excluded: set[str] | None = None,
        forced: set[str] | None = None,
    ) -> list[str]:
        """Promote tables exceeding query threshold if under max_rows.

        REQ-241: a table that is hot (in ``hot_tables``) is never also promoted to warm —
        hot wins (a table lives in at most one tier). REQ-240: ``excluded`` (warm: false)
        tables are never promoted; ``forced`` (warm: true) tables are promoted regardless of
        query count. Returns list of newly promoted table names.
        """
        counts = counter.get_counts()
        hot_tables = hot_tables or set()
        excluded = excluded or set()
        forced = forced or set()
        # Consider forced tables even if they have not hit the query threshold.
        candidates = dict(counts)
        for t in forced:
            candidates.setdefault(t, threshold)
        promoted: list[str] = []

        for table, count in candidates.items():
            if table in excluded:
                continue
            # Hot-over-warm precedence: a hot table is not duplicated into the warm tier.
            if table in hot_tables:
                continue
            if table not in forced and count < threshold:
                continue
            with self._lock:
                if table in self._warm_tables:
                    continue

            # Size check — through the engine terminal
            _cnt = await engine.execute_engine(f"SELECT COUNT(*) FROM {table}")
            row_count = _cnt.rows[0][0]

            if row_count > max_rows:
                log.info(
                    "Skipping warm promotion for %s: %d rows exceeds max %d",
                    table,
                    row_count,
                    max_rows,
                )
                continue

            # CTAS into Iceberg via the engine
            target = self._iceberg_ref(table)
            await engine.execute_engine(f"CREATE TABLE {target} AS SELECT * FROM {table}")

            with self._lock:
                self._warm_tables.add(table)
                self._warm_rows[table] = row_count
            promoted.append(table)
            log.info("Promoted %s to warm Iceberg cache (%d rows)", table, row_count)

        return promoted

    async def check_demotions(  # REQ-239
        self,
        counter: QueryCounter,
        engine: Any,
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
            await engine.execute_engine(f"DROP TABLE IF EXISTS {target}")

            with self._lock:
                self._warm_tables.discard(table)
                self._warm_rows.pop(table, None)
            demoted.append(table)
            counter.reset(table)
            log.info("Demoted %s from warm Iceberg cache", table)

        return demoted

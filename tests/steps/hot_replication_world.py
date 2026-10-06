# Copyright (c) 2026 Kenneth Stott
# Canary: 0616b5d6-fefc-40a2-9749-2b7e9e0c0403
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One org environment for the Hot replication scenarios (REQ-238, REQ-239): a real
replica-state control plane (SQLite), a registry of tables on a source the engine reads in
place, an embedded count store, and an engine stand-in for the size check.

The scenarios drive the mechanism itself — ``replica_hot.evaluate`` and the state store — and
read what it wrote back through the state store."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from provisa.core import config_stamp, settings_registry
from provisa.core.database import Database, create_engine_from_url
from provisa.core.models import Source, SourceType
from provisa.core.schema_org import config_stamp as stamps
from provisa.core.schema_org import metadata, replica_state as replica_state_table
from provisa.federation import replica_state
from provisa.federation.engine import build_engine
from provisa.federation.replica_hot import HotCounts, count_scope, evaluate

INTERVAL = 60
THRESHOLD = 100
STORE = "this-engines-store"
SETTINGS = {
    "replication.hot_interval": INTERVAL,
    "replication.hot_threshold": THRESHOLD,
    "replication.hot_max_rows": 10_000_000,
}


def _registry_row(table_id: int, name: str) -> SimpleNamespace:
    return SimpleNamespace(
        **{
            "id": table_id,
            "source_id": "pg",
            "schema_name": "public",
            "table_name": name,
            "replicate": None,  # Default: replicated once busy, at the global threshold
            "region": None,  # REQ-1921: its source's region (this world declares no regions)
            "load_protected": None,
            "change_signal": None,
            "cache_ttl": None,
            "columns": [SimpleNamespace(name="id", native_filter_type=None)],
            "row_materialize": False,
        }
    )


class _Engine:
    """The engine runtime's calls the size check makes; every table holds 5,000 rows."""

    def __init__(self) -> None:
        self.engine = build_engine("trino")
        self.sent: list[str] = []

    async def read_ref(self, table: Any) -> str:
        """The registered table (its identity) at its address on this engine."""
        return f'"{table.source_id}"."{table.schema_name}"."{table.table_name}"'

    async def execute_engine(self, sql: str, *_a: Any, **_k: Any) -> Any:
        self.sent.append(sql)
        return SimpleNamespace(rows=[(5000,)])


class World:
    def __init__(self, tmp_path: Any, monkeypatch: Any, tables: dict[str, int]) -> None:
        self._engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
        with self._engine.begin() as raw:
            metadata.create_all(raw, tables=[replica_state_table, stamps])
            config_stamp.install(raw, {}, advanced=config_stamp.TENANT_ADVANCED)
        self.db = Database(self._engine, "test")
        self.ids = tables
        self.source = Source(
            id="pg",
            type=SourceType.postgresql,
            host="h",
            port=5432,
            database="d",
            username="u",
            cache_ttl=60,
        )
        rows = [_registry_row(table_id, name) for name, table_id in tables.items()]

        async def _tables(_state: Any, _conn: Any = None) -> list[SimpleNamespace]:
            return rows

        async def _sources(_state: Any, _conn: Any = None) -> list[Source]:
            return [self.source]

        monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
        monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
        monkeypatch.setattr(settings_registry, "value", lambda key: SETTINGS[key])
        monkeypatch.setattr(
            "provisa.federation.replica_builds.store_identity", lambda _state: STORE
        )
        org = f"org-{uuid.uuid4().hex}"
        self.scope = count_scope(org, "prod")
        self.state = SimpleNamespace(
            model_db=self.db,
            tenant_db=self.db,
            org_id=org,
            hot_counts=HotCounts(None, clock=lambda: 9_000_000.0),
            hot_manager=None,
            federation_engine=_Engine(),
        )

    @staticmethod
    def key(table: str) -> tuple[str, str, str]:
        return ("pg", "public", table)

    def statements_read(self, table: str, statements: int) -> None:
        """Governed statements that read ``table`` in the current interval, as the audit writer
        counts them."""
        self.state.hot_counts.add({(self.scope, self.ids[table]): statements}, INTERVAL)

    def count(self, table: str) -> float:
        return self.state.hot_counts.counts(self.scope, [self.ids[table]], INTERVAL)[
            self.ids[table]
        ]

    def promoted_and_built(self, table: str) -> None:
        """``table`` passed its threshold earlier and its replica stands in this engine's store."""

        async def _go() -> None:
            async with self.db.acquire() as conn:
                await replica_state.set_promoted(conn, self.key(table), True)
                await replica_state.record_completed(
                    conn,
                    self.key(table),
                    rows_copied=5000,
                    method="engine_statement",
                    content_hash=None,
                    store=STORE,
                    next_refresh_at=None,
                    now=datetime.now(UTC),
                )

        asyncio.run(_go())

    def evaluate(self) -> Any:
        """The promotion check, run as work for this world's org (REQ-1266): it counts the
        bound org's reads."""
        from provisa.core.request_context import reset_current_org, set_current_org

        token = set_current_org(self.state.org_id)
        try:
            return asyncio.run(evaluate(self.state, workers=1))
        finally:
            reset_current_org(token)

    def record(self, table: str) -> Any:
        async def _go() -> Any:
            async with self.db.acquire() as conn:
                return await replica_state.read(conn, self.key(table))

        return asyncio.run(_go())

    def promotion(self) -> tuple[frozenset, frozenset]:
        """``(promoted, serving)`` in this engine's store, as routing reads them."""

        async def _go() -> tuple[frozenset, frozenset]:
            async with self.db.acquire() as conn:
                return await replica_state.promotion(conn, lambda: STORE)

        return asyncio.run(_go())

    def close(self) -> None:
        self._engine.dispose()

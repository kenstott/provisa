# Copyright (c) 2026 Kenneth Stott
# Canary: 5c7e1a93-2f4b-4d86-a0e9-6b3d8f2c1e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shared checks: a real engine runtime's describe_sync and run_sync report the same column names
and declared types, and an asyncpg client reading through pgwire — whose Describe is answered from
REGISTERED metadata, never by asking the engine (REQ-589, amended 2026-10-01) — gets exact values in
binary for every type. Used by the DuckDB (unit) and Postgres (integration) parity tests."""

# Requirements: REQ-589

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import asyncpg

from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan


class RuntimeEngine:
    """Adapts an engine runtime's run_sync/describe_sync to the federation-engine seam pgwire calls,
    counting executions."""

    def __init__(self, runtime: Any, dialect: str) -> None:
        self._rt = runtime
        self.dialect = dialect
        self.executed: list[str] = []
        self.described: list[str] = []

    def execute_engine_sync(self, sql, params, *, session_hints=None, authorization=None):
        self.executed.append(sql)
        return self._rt.run_sync(sql, params)

    def describe_engine_sync(self, sql, params=None):
        self.described.append(sql)
        return self._rt.describe_sync(sql, params)


class PassthroughEngine(RuntimeEngine):
    """A Postgres engine whose Execute takes the REAL REQ-1863 raw-DataRow passthrough against
    ``dsn`` — the source's bytes reach the client verbatim, typed only by the Describe's OIDs."""

    def __init__(self, runtime: Any, dsn: str) -> None:
        super().__init__(runtime, "postgres")
        self._dsn = dsn
        self.passthrough: list[str] = []

    def execute_pg_engine_passthrough(self, sql, params, result_formats, *, described_oids):
        from provisa.federation.runtime import EngineRuntime
        from provisa.pgwire.pg_passthrough import open_passthrough

        self.passthrough.append(sql)
        # The runtime's own read pool lends the connection, exactly as the server's engine does.
        return EngineRuntime._pg_passthrough_stream(
            cast(Any, None),
            open_passthrough(
                self._rt.borrow_raw, sql, list(params or []), result_formats, described_oids
            ),
        )


def describes_as(*columns: tuple[str, str]):
    """A ``describe_pgwire_statement`` double for tests that stub the pipeline below pgwire: the
    statement describes as ``columns`` and holds no governed statement, so its Execute goes through
    the test's own ``govern_pgwire_plan`` double."""
    from provisa.pgwire._pipeline import _Described

    async def _describe(sql, role_id):
        del sql, role_id
        return _Described(list(columns), None)

    return _describe


def registered_shape(sql: str, registry: dict[str, list[tuple[str, str]]]) -> list[tuple[str, str]]:
    """``sql``'s result shape derived from ``registry`` (table -> [(column, registered type)]) by
    the real derivation pgwire's Describe uses."""
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.pgwire.result_shape import derive_result_shape

    table_map = {name: i for i, name in enumerate(registry, 1)}
    column_types = {
        i: [ColumnMetadata(column_name=c, data_type=t, is_nullable=True) for c, t in cols]
        for i, cols in enumerate(registry.values(), 1)
    }
    ctx = SimpleNamespace(physical_to_sql={}, virtual_columns={})
    return derive_result_shape(sql, table_map, ctx, column_types)


def assert_runtime_parity(runtime: Any, sql: str) -> list[str]:
    shape = runtime.describe_sync(sql)
    ran = runtime.run_sync(sql)
    try:
        assert shape.rows == []
        assert shape.column_names == ran.column_names
        assert shape.column_types == ran.column_types
        assert shape.column_types and all(shape.column_types)
    finally:
        ran.close()
    return list(shape.column_types)


async def fetch_through_pgwire(
    port: int, engine: RuntimeEngine, sql: str, registry: dict[str, list[tuple[str, str]]]
) -> list[Any]:
    from provisa.pgwire._pipeline import _Described
    from provisa.pgwire.server import ProvisaSession

    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = engine

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver):
        return _engine_plan(sql)

    async def _describe(text, role_id):
        return _Described(registered_shape(text, registry), cast(Any, "governed"))

    async def _plan(held, params, wire_formats=None, *, deliver):
        return _engine_plan(sql)

    async def _no_cache(plan, st):
        return None

    async def _resident(st, plan):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.describe_pgwire_statement", _describe),
        patch("provisa.pgwire._pipeline.plan_pgwire_statement", _plan),
        patch("provisa.pgwire._pipeline.governed_statement_is_current", lambda held, st: True),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch("provisa.federation.query_residency.prepare_engine_residency", _resident),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="x",
            database="provisa",
            statement_cache_size=0,
        )
        try:
            return [tuple(r) for r in await conn.fetch(sql)]
        finally:
            await conn.close()

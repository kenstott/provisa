# Copyright (c) 2026 Kenneth Stott
# Canary: bbeef488-6261-455d-b158-db1ec28db4a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865/REQ-1915: a direct-driver source the engine cannot attach is read by key through its
OWN pool, in its own dialect. Before, the keyed read fell through to a SELECT through the engine
terminal, addressing a catalog the engine does not have (the source is row-level precisely because
the engine cannot attach it)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from provisa.events.source_loader import SourceRowLoader, make_direct_keyed_loader


class _Engine:
    """Records which terminal a read went through."""

    def __init__(self) -> None:
        self.native: list[tuple[str, str]] = []
        self.engine_sql: list[str] = []

    async def execute_native(self, pools, source_id, sql, params):
        self.native.append((source_id, sql))
        return SimpleNamespace(column_names=["id", "status"], rows=[(1, "a"), (2, "b")])

    async def execute_engine(self, sql, authorization=None):  # pragma: no cover - the failure
        self.engine_sql.append(sql)
        raise AssertionError("a direct-driver source's keyed read went through the engine")


_TABLE = SimpleNamespace(table_name="orders", schema_name="sales")


def test_a_keyed_read_goes_through_the_source_s_own_pool_in_its_dialect():
    engine = _Engine()
    state = SimpleNamespace(source_pools=object(), source_dialects={"ms": "tsql"})
    loader = SourceRowLoader(
        engine, keyed_adapter_loaders={"sqlserver": make_direct_keyed_loader(state, engine)}
    )
    source = SimpleNamespace(id="ms", type="sqlserver")

    rows = asyncio.run(loader.load_keys(source, _TABLE, ["id"], [(1,), (2,)]))

    assert rows == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]
    assert engine.native == [("ms", "SELECT * FROM [sales].[orders] WHERE [id] IN (1, 2)")]
    assert engine.engine_sql == []


def test_the_direct_keyed_loader_is_registered_only_where_the_engine_cannot_attach(monkeypatch):
    from provisa.events import app_wiring

    attached = {"postgresql"}
    monkeypatch.setattr(
        "provisa.federation.strategy.engine_attaches", lambda engine, stype: stype in attached
    )
    state = SimpleNamespace(
        source_types={"pg": "postgresql", "ms": "sqlserver", "api": "openapi"},
        source_dialects={},
        config=SimpleNamespace(sources=[]),
    )
    loaders = app_wiring.build_keyed_adapter_loaders(state, SimpleNamespace(engine=object()))
    assert "sqlserver" in loaders  # a driver, not attached: its own pool
    assert "postgresql" not in loaders  # attached: the engine reads it in place
    assert "openapi" not in loaders  # an API source has no driver

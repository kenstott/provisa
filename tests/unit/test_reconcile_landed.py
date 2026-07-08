# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846/932: the schema-currency controller — reconcile_landed_tables converges the store's
landing schema for MATERIALIZED tables only, skips still-untyped ones, and attaches the read view."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.duckdb_backend import DuckDBBackend
from provisa.federation.engine import build_duckdb_engine


def _col(name, data_type: str | None = "bigint", pk=False):
    return SimpleNamespace(name=name, data_type=data_type, is_primary_key=pk)


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype), change_signal="ttl")


def _tbl(sid, tname, cols):
    return SimpleNamespace(
        source_id=sid,
        schema_name="default",
        table_name=tname,
        change_signal=None,
        watermark_column=None,
        live=None,
        columns=cols,
    )


class _FakeRuntime:
    def __init__(self):
        self.landed: list = []

    def attach_source(self, source):  # exercised by _attach_registered — no-op record
        pass

    async def attach_landed_source(self, source, columns, *, pk_columns=None):
        self.landed.append((source.id, source.table_name, columns, pk_columns))


@pytest.mark.asyncio
async def test_reconciles_only_materialized_and_skips_untyped():
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _FakeRuntime()
    backend._runtime = rt  # inject fake runtime (skip real duckdb build)
    cfg = SimpleNamespace(
        sources=[_src("api", "openapi"), _src("pg", "postgresql")],
        tables=[
            _tbl("api", "events", [_col("id", "bigint", pk=True), _col("status", "text")]),
            _tbl("pg", "users", [_col("id", "bigint", pk=True)]),  # ATTACH → VIRTUAL, not landed
            _tbl("api", "bad", [_col("id", None)]),  # MATERIALIZED but untyped → skipped
        ],
    )
    reconciled = await backend.reconcile_landed_tables(SimpleNamespace(config=cfg))

    assert reconciled == [("api", "events")]  # only the typed materialized table
    assert rt.landed == [("api", "events", [("id", "bigint"), ("status", "text")], ["id"])]


@pytest.mark.asyncio
async def test_no_config_is_noop():
    backend = DuckDBBackend(build_duckdb_engine())
    backend._runtime = _FakeRuntime()
    assert await backend.reconcile_landed_tables(SimpleNamespace()) == []

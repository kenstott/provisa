# Copyright (c) 2026 Kenneth Stott
# Canary: c8450386-959f-4308-a730-155f9c00b413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846/932: the schema-currency controller — reconcile_landed_tables converges the store's
landing schema for MATERIALIZED tables only, skips still-untyped ones, and attaches the read view.

Drives off the design-time REGISTERED tables (control plane: semantic sql names + resolved types),
not the raw YAML — so the test feeds the registered shape through a fake ``fetch_tables``."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.duckdb_backend import DuckDBBackend
from provisa.federation.engine import build_duckdb_engine


def _rcol(name, data_type: str | None = "bigint", pk=False, nf=None):
    return {
        "column_name": name,
        "data_type": data_type,
        "is_primary_key": pk,
        "native_filter_type": nf,
    }


def _rtbl(sid, tname, cols):
    return {"source_id": sid, "schema_name": "default", "table_name": tname, "columns": cols}


def _src(sid, stype):
    return SimpleNamespace(id=sid, type=SimpleNamespace(value=stype), change_signal="ttl")


class _FakeRuntime:
    def __init__(self):
        self.landed: list = []

    def attach_source(self, source):  # exercised by _attach_registered — no-op record
        pass

    async def attach_landed_source(self, source, columns, *, pk_columns=None):
        self.landed.append((source.id, source.table_name, columns, pk_columns))


class _FakeConn:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *a):
        return False


def _fake_tenant_db():
    return SimpleNamespace(acquire=lambda: _FakeConn())


def _state(cfg, registered, monkeypatch):
    async def _fetch_tables(_conn):
        return registered

    monkeypatch.setattr("provisa.api.admin.db_queries.fetch_tables", _fetch_tables)
    return SimpleNamespace(config=cfg, tenant_db=_fake_tenant_db())


@pytest.mark.asyncio
async def test_reconciles_only_materialized_and_skips_untyped(monkeypatch):
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _FakeRuntime()
    backend._runtime = rt  # inject fake runtime (skip real duckdb build)
    cfg = SimpleNamespace(sources=[_src("api", "openapi"), _src("pg", "postgresql")], tables=[])
    registered = [
        _rtbl("api", "events", [_rcol("id", "bigint", pk=True), _rcol("status", "text")]),
        _rtbl("pg", "users", [_rcol("id", "bigint", pk=True)]),  # ATTACH → VIRTUAL, not landed
        _rtbl("api", "bad", [_rcol("id", None)]),  # MATERIALIZED but untyped → skipped
        # PARAMETERIZED (native-filter arg) → a function, no snapshot → never materialized
        _rtbl(
            "api",
            "one",
            [_rcol("_nf_key", "text", nf="query_param"), _rcol("val", "text")],
        ),
    ]
    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == [("api", "events")]  # only the typed, non-parameterized materialized table
    assert rt.landed == [("api", "events", [("id", "bigint"), ("status", "text")], ["id"])]


@pytest.mark.asyncio
async def test_no_config_is_noop():
    backend = DuckDBBackend(build_duckdb_engine())
    backend._runtime = _FakeRuntime()
    assert await backend.reconcile_landed_tables(SimpleNamespace()) == []

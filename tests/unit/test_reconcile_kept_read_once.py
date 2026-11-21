# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0f8a52-6c1e-4d0b-9a3e-2b7f1c9d4e61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-846/932: the schema-currency reconcile reads an embedded DuckDB store's columns once per
rebuild. A replica whose columns already match is kept without a write of its own (no open of the
store for writing, no advance of its write generation); only a missing or drifted one is
reconciled. A failed read is the failure of the table it was read for, as a reconcile's is."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.federation.duckdb_backend import DuckDBBackend
from provisa.federation.engine import build_duckdb_engine
from provisa.federation.materialize_broker import get_broker, store_canary
from tests.unit.test_reconcile_landed import _FakeRuntime, _rcol, _rtbl, _src, _state


@pytest.fixture(autouse=True)
def _bound_to_acme(bind_org):
    bind_org("acme")


class _ListingRuntime(_FakeRuntime):
    def __init__(self, held):
        super().__init__()
        self.held = held
        self.reads = 0

    def replica_column_lists(self):
        self.reads += 1
        return self.held


class _UnreadableRuntime(_FakeRuntime):
    def replica_column_lists(self):
        raise OSError("store unreadable")


def _model():
    cfg = SimpleNamespace(sources=[_src("api", "openapi")], tables=[])
    registered = [
        _rtbl("api", "kept", [_rcol("id", "bigint", pk=True), _rcol("status", "text")]),
        _rtbl("api", "drifted", [_rcol("id", "bigint", pk=True), _rcol("added", "text")]),
        _rtbl("api", "absent", [_rcol("id", "bigint", pk=True)]),
    ]
    return cfg, registered


@pytest.mark.asyncio
async def test_a_matching_replica_is_kept_without_a_reconcile(monkeypatch):
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _ListingRuntime(
        {
            ("org_acme_replicas", "api__default__kept"): ["id", "status"],
            ("org_acme_replicas", "api__default__drifted"): ["id"],
        }
    )
    backend._runtime = rt
    cfg, registered = _model()

    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == [("api", "kept"), ("api", "drifted"), ("api", "absent")]
    assert rt.reads == 1
    assert [table for _schema, table, _cols, _pk in rt.landed] == [
        "api__default__drifted",
        "api__default__absent",
    ]


@pytest.mark.asyncio
async def test_a_failed_read_fails_each_table_by_name(monkeypatch):
    backend = DuckDBBackend(build_duckdb_engine())
    rt = _UnreadableRuntime()
    backend._runtime = rt
    cfg, registered = _model()

    reconciled = await backend.reconcile_landed_tables(_state(cfg, registered, monkeypatch))

    assert reconciled == []
    assert rt.landed == []
    assert set(backend._unreconciled) == {("api", "kept"), ("api", "drifted"), ("api", "absent")}
    assert all(isinstance(exc, OSError) for exc in backend._unreconciled.values())


def test_column_lists_reads_without_advancing_the_generation(tmp_path):
    db_path = str(tmp_path / "materialize.duckdb")
    broker = get_broker(db_path)

    # A store whose file was never written holds no tables.
    assert broker.column_lists() == {}

    broker.reconcile("s", "t", [("id", "bigint"), ("name", "text")])
    generation = store_canary(db_path)

    assert broker.column_lists() == {("s", "t"): ["id", "name"]}
    assert store_canary(db_path) == generation

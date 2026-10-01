# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3e1a5f-7b2d-4e96-a0c4-3d9f6b1e2a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865 (amended 2026-09-30): row_materialize applies only when the engine DECLARES it cannot
direct-attach the source. An engine that declares it can attaches and reads live; the flag is
ignored, and a failed attach is an error -- never a detour through the row cache."""

# Requirements: REQ-1865

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.federation.engine import build_duckdb_engine
from provisa.federation.query_residency import (
    active_row_materialize_tables,
    pushdown_row_materialize,
    row_materialized_tables_by_name,
)

pytestmark = pytest.mark.unit


def _table(source_id: str, table_name: str):
    from provisa.core.models import Column, Table

    return Table(
        source_id=source_id,
        domain_id="d",
        schema_name="s",
        table_name=table_name,
        row_materialize=True,
        cache_ttl=300,
        columns=[
            Column(name="id", visible_to=["org_admin"], data_type="integer", is_primary_key=True)
        ],
    )


class _NoEngineCalls:
    """Stands in for the runtime wrapper: the real DuckDB engine declaration, recording any engine
    call."""

    def __init__(self) -> None:
        self.engine = build_duckdb_engine()
        self.calls: list[str] = []

    async def execute_engine(self, sql, *_a, **_k):
        self.calls.append(sql)
        return SimpleNamespace(column_names=[], rows=[])


@pytest.fixture
def registry(monkeypatch):
    sources = [
        SimpleNamespace(id="docs", type=SimpleNamespace(value="mongodb")),  # DuckDB attaches
        SimpleNamespace(id="graph", type=SimpleNamespace(value="neo4j")),  # DuckDB cannot
    ]
    tables = [_table("docs", "order_docs"), _table("graph", "order_node")]

    async def _sources(state):
        return sources

    async def _tables(state):
        return tables

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)


def test_the_duckdb_engine_declares_mongodb_attachable_and_neo4j_not():
    from provisa.federation.strategy import engine_attaches

    engine = build_duckdb_engine()
    assert engine_attaches(engine, "mongodb")
    assert not engine_attaches(engine, "neo4j")


def test_row_materialize_applies_only_where_the_engine_cannot_attach(registry):
    state = SimpleNamespace(federation_engine=_NoEngineCalls())
    active = asyncio.run(active_row_materialize_tables(state))
    assert [t.table_name for t in active] == ["order_node"]
    assert set(asyncio.run(row_materialized_tables_by_name(state))) == {"order_node"}


def test_a_declared_attach_table_runs_no_key_pushdown(registry):
    """The query JOINs the attachable table; pushdown must not probe, fetch or land it."""
    engine = _NoEngineCalls()
    state = SimpleNamespace(federation_engine=engine)
    sql = 'SELECT o.id FROM "s"."orders" AS o JOIN "s"."order_docs" AS d ON d.id = o.id'
    assert asyncio.run(pushdown_row_materialize(state, sql, "duckdb")) == set()
    assert engine.calls == []  # no probe query at all


def test_a_non_attachable_row_materialize_join_still_probes(registry):
    """Control: the SAME join shape against the table DuckDB cannot attach does run the probe --
    proving the test above passes because of the attach rule, not a vacuous pushdown."""
    engine = _NoEngineCalls()
    state = SimpleNamespace(federation_engine=engine)
    sql = 'SELECT o.id FROM "s"."orders" AS o JOIN "s"."order_node" AS n ON n.id = o.id'
    asyncio.run(pushdown_row_materialize(state, sql, "duckdb"))
    assert len(engine.calls) == 1


def test_a_failed_attach_is_an_error_not_a_row_cache_detour(registry):
    """A declared-attach source whose attach fails raises -- at attach and at query time -- with the
    attach's own cause; nothing routes the table through row_materialize instead."""
    import duckdb

    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    src = SimpleNamespace(
        id="lite",
        type=SimpleNamespace(value="sqlite"),
        path="/nonexistent/dir/x.db",
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        base_url=None,
        federation_hints={},
        mapping={},
        schema_name="main",
        table_name="t",
    )
    with pytest.raises(duckdb.Error, match="Unable to open database"):
        rt.attach_source(src)
    with pytest.raises(duckdb.Error, match="Unable to open database"):
        asyncio.run(rt.run("SELECT * FROM main.t"))


class _FailingProbe(_NoEngineCalls):
    async def execute_engine(self, sql, *_a, **_k):
        raise RuntimeError("probe boom")


def test_a_failed_key_pushdown_probe_raises(registry):
    """A failed probe used to be logged and skipped, leaving the table unlanded and the query
    answered from whatever the row cache already held."""
    state = SimpleNamespace(federation_engine=_FailingProbe())
    sql = 'SELECT o.id FROM "s"."orders" AS o JOIN "s"."order_node" AS n ON n.id = o.id'
    with pytest.raises(RuntimeError, match="probe boom"):
        asyncio.run(pushdown_row_materialize(state, sql, "duckdb"))


class _ProbeFindsKeys(_NoEngineCalls):
    async def execute_engine(self, sql, *_a, **_k):
        self.calls.append(sql)
        return SimpleNamespace(column_names=["id", "__pushdown_order_node"], rows=[(1, 1), (2, 2)])


def test_a_failed_keyed_fetch_raises(registry, monkeypatch):
    """Confirmed live: ClickHouse rejected large_federated_join's 1M-key fetch, the failure was
    logged and skipped, and the query returned 3030 rows instead of ~3.03M. It must raise."""
    import provisa.federation.query_residency as qr
    from provisa.events.source_loader import SourceRowLoader

    async def _nothing_cached(*_a, **_k):
        return None, {}

    async def _fetch_fails(self, *_a, **_k):
        raise RuntimeError("Max query size exceeded")

    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda *_a: {})
    monkeypatch.setattr("provisa.events.app_wiring.build_keyed_adapter_loaders", lambda *_a: {})
    monkeypatch.setattr(qr, "resolve_landing_args_for", lambda *_a: SimpleNamespace(columns=[]))
    monkeypatch.setattr(qr, "_ensure_and_read_row_cache", _nothing_cached)
    monkeypatch.setattr(SourceRowLoader, "load_keys", _fetch_fails)
    sources = [
        SimpleNamespace(id="docs", type=SimpleNamespace(value="mongodb"), cache_ttl=300),
        SimpleNamespace(id="graph", type=SimpleNamespace(value="neo4j"), cache_ttl=300),
    ]

    async def _sources(state):
        return sources

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    state = SimpleNamespace(federation_engine=_ProbeFindsKeys())
    sql = 'SELECT o.id FROM "s"."orders" AS o JOIN "s"."order_node" AS n ON n.id = o.id'
    with pytest.raises(RuntimeError, match="Max query size exceeded"):
        asyncio.run(pushdown_row_materialize(state, sql, "duckdb"))

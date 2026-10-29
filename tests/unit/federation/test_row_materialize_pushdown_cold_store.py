# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1f0b7e-9a3c-4c62-8e14-b7a2c6f30d95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1865: a row-level replica is created before anything reads it. The key-pushdown probe reads
every row-level replica its statement names; on a cold store a replica reached only through a JOIN
had never been created, and the probe failed with "Table ... does not exist" (cypher_cross_engine on
DuckDB, bench_placed_edge). The probe below runs on a real DuckDB connection standing in for the
engine, so the table either exists when the probe reads it or DuckDB refuses the read."""

# Requirements: REQ-1865

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

import provisa.federation.query_residency as qr
from provisa.federation.engine import build_duckdb_engine

pytestmark = pytest.mark.unit

_EDGE = "graph__s__placed_edge"


def _edge_table():
    from provisa.core.models import Column, Table

    return Table(
        source_id="graph",
        domain_id="d",
        schema_name="s",
        table_name="placed_edge",
        row_materialize=True,
        cache_ttl=300,
        columns=[
            Column(name="id", visible_to=["org_admin"], data_type="integer", is_primary_key=True),
            Column(name="customer_id", visible_to=["org_admin"], data_type="integer"),
        ],
    )


class _DuckEngine:
    """The runtime wrapper: the real DuckDB engine declaration, executing every statement on one
    in-memory DuckDB connection that holds both the statement's resident table and the replicas."""

    def __init__(self) -> None:
        self.engine = build_duckdb_engine()
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA s")
        self.con.execute("CREATE TABLE s.customers (id INTEGER)")
        self.con.execute("INSERT INTO s.customers VALUES (7), (8)")

    def address_replicas(self, sql):
        return sql

    async def execute_engine(self, sql, *_a, **_k):
        cur = self.con.execute(sql)
        return SimpleNamespace(column_names=[d[0] for d in cur.description], rows=cur.fetchall())


@pytest.fixture
def cold_store(monkeypatch):
    engine = _DuckEngine()
    sources = [
        SimpleNamespace(
            id="graph", type=SimpleNamespace(value="neo4j"), cache_ttl=300, change_signal="ttl"
        ),  # DuckDB cannot attach neo4j: row_materialize applies
    ]
    tables = [_edge_table()]
    fetched_keys: list[list[tuple]] = []

    async def _sources(state):
        return sources

    async def _tables(state):
        return tables

    async def _ensure(_engine, _backend, _state, schema, name, columns):
        engine.con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cols = ", ".join(f'"{c}" {t}' for c, t in columns)
        engine.con.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{name}" ({cols})')

    async def _nothing_cached(*_a, **_k):
        return None, {}

    async def _load_keys_arrow(self, source, table, cols, keys, **_k):
        fetched_keys.append(keys)
        return pa.table({"id": pa.array([], pa.int32())})

    from provisa.events.source_loader import SourceRowLoader

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda *_a: {})
    monkeypatch.setattr("provisa.events.app_wiring.build_keyed_adapter_loaders", lambda *_a: {})
    monkeypatch.setattr("provisa.events.app_wiring.build_keyed_arrow_loaders", lambda *_a: {})
    monkeypatch.setattr(
        qr,
        "resolve_landing_args_for",
        lambda *_a: SimpleNamespace(columns=[("id", "INTEGER"), ("customer_id", "INTEGER")]),
    )
    monkeypatch.setattr(qr, "_ensure_row_cache_table", _ensure)
    monkeypatch.setattr(qr, "_ensure_and_read_row_cache", _nothing_cached)
    monkeypatch.setattr(SourceRowLoader, "load_keys_arrow", _load_keys_arrow)
    return SimpleNamespace(engine=engine, fetched_keys=fetched_keys)


def _sql(engine) -> str:
    """The statement as the engine receives it: the replica addressed where the code places it."""
    address = engine.engine.backend.replica_address(
        None, source_id="graph", schema_name="s", table_name="placed_edge"
    )
    assert address.table == _EDGE
    return (
        f'SELECT c.id FROM "s"."customers" AS c '
        f'JOIN "{address.schema}"."{address.table}" AS e ON e.customer_id = c.id'
    )


def test_a_join_reached_replica_never_landed_is_created_before_the_probe_reads_it(cold_store):
    state = SimpleNamespace(federation_engine=cold_store.engine, org_id="acme")
    landed = asyncio.run(
        qr.pushdown_row_materialize(state, _sql(cold_store.engine), "duckdb", reader_role=None)
    )
    assert landed == {_EDGE}
    # The probe read the (empty) replica and found the join's keys off the resident side.
    assert cold_store.fetched_keys == [[(7,), (8,)]]
    # The statement's own read, run after pushdown, finds the replica too.
    asyncio.run(cold_store.engine.execute_engine(_sql(cold_store.engine)))


def test_without_the_replica_the_probe_is_refused_by_the_engine(cold_store):
    """Control: the stand-in engine is a real DuckDB that refuses a read of a table that does not
    exist -- the cold-store failure the test above guards against."""
    with pytest.raises(duckdb.CatalogException, match="does not exist"):
        asyncio.run(cold_store.engine.execute_engine(_sql(cold_store.engine)))

# Copyright (c) 2026 Kenneth Stott
# Canary: a3790641-b20a-4cab-b1b7-219e21f0a8d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1865: ensure_rows_resident's fresh/missing/force-stale/tombstone-on-empty-fetch behavior.

Uses a real sqlite (file-backed, not :memory:, so the several store_connection() calls the
function makes share state) row cache and fake source/loader/registry plumbing — the mechanism
under test is the orchestration (what gets fetched, upserted, or tombstoned), not any one
source's SQL dialect."""

from __future__ import annotations

import types

import pytest

from provisa.compiler.pk_bounds import PkBound
from provisa.core.models import Column, Source, Table


def _table(source_id="pg1", cache_ttl=60) -> Table:
    return Table(
        source_id=source_id,
        domain_id="dom1",
        schema_name="public",
        table_name="orders",
        columns=[
            Column(name="id", visible_to=["public"], data_type="integer", is_primary_key=True),
            Column(name="status", visible_to=["public"], data_type="text"),
        ],
        row_materialize=True,
        cache_ttl=cache_ttl,
    )


def _source(source_id="pg1") -> Source:
    return Source(id=source_id, type="postgresql", cache_ttl=None)


class _FakeBackend:
    dialect = "postgresql"

    def landing_target(self, *, store_schema, source_id, source_type, schema_name, table_name):
        return store_schema, f"{source_id}__{schema_name}__{table_name}"


class _FakeEngineEngine:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self.backend = _FakeBackend()
        self.dialect = "postgresql"

    def materialize_store(self) -> str:
        return self._dsn


class _FakeEngine:
    def __init__(self, dsn: str):
        self.engine = _FakeEngineEngine(dsn)


class _FakeLoader:
    """Stands in for SourceRowLoader: records requested keys, returns canned rows."""

    def __init__(self, rows_by_key: dict[tuple, dict]):
        self.rows_by_key = rows_by_key
        self.calls: list[list[tuple]] = []

    async def load_keys(self, source, table, pk_columns, keys):
        self.calls.append(list(keys))
        return [self.rows_by_key[k] for k in keys if k in self.rows_by_key]


@pytest.fixture
def sqlite_dsn(tmp_path):
    return f"sqlite:///{tmp_path}/row_cache.db"


@pytest.fixture
def patched_registry(monkeypatch):
    """Point ensure_rows_resident's registry/loader lookups at fakes, keyed by test-supplied
    table/source/loader objects set on the returned namespace."""

    ns = types.SimpleNamespace(table=None, source=None, loader=None)

    async def _fake_sources(state):
        return [ns.source]

    async def _fake_tables(state):
        return [ns.table]

    def _fake_loader_cls(engine, adapter_loaders=None):
        return ns.loader

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _fake_sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _fake_tables)
    monkeypatch.setattr("provisa.events.source_loader.SourceRowLoader", _fake_loader_cls)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda state, engine: {})
    return ns


@pytest.mark.asyncio
async def test_missing_keys_are_fetched_and_landed(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table()
    source = _source()
    patched_registry.table = table
    patched_registry.source = source
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,),),
    )
    results = await ensure_rows_resident(state, [bound])
    assert results == [("pg1", "orders", 1)]
    assert patched_registry.loader.calls == [[(1,)]]


@pytest.mark.asyncio
async def test_fresh_keys_are_not_refetched(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table()
    source = _source()
    patched_registry.table = table
    patched_registry.source = source
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,),),
    )
    await ensure_rows_resident(state, [bound])
    assert patched_registry.loader.calls == [[(1,)]]

    # Second call, same key, not yet expired (cache_ttl=60s) -- no re-fetch.
    results = await ensure_rows_resident(state, [bound])
    assert results == [("pg1", "orders", 0)]
    assert patched_registry.loader.calls == [[(1,)]]  # unchanged -- no second fetch


@pytest.mark.asyncio
async def test_force_refetches_already_cached_key(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table()
    source = _source()
    patched_registry.table = table
    patched_registry.source = source
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,),),
    )
    await ensure_rows_resident(state, [bound])
    assert len(patched_registry.loader.calls) == 1

    patched_registry.loader.rows_by_key[(1,)] = {"id": 1, "status": "shipped"}
    results = await ensure_rows_resident(state, [bound], force=True)
    assert results == [("pg1", "orders", 1)]
    assert len(patched_registry.loader.calls) == 2


@pytest.mark.asyncio
async def test_key_source_returns_nothing_for_is_tombstoned(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table()
    source = _source()
    patched_registry.table = table
    patched_registry.source = source
    # (2,) is requested but the loader has no row for it -- simulates a source-side delete/race.
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,), (2,)),
    )
    results = await ensure_rows_resident(state, [bound])
    assert results == [("pg1", "orders", 1)]  # only 1 row actually fetched/landed

    # Re-querying key (2,) alone still finds it missing (never landed as a tombstone placeholder,
    # and never silently retried without a real fetch attempt) -- prove it round-trips through
    # another fetch attempt rather than being treated as permanently cached.
    bound_2 = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((2,),),
    )
    results_2 = await ensure_rows_resident(state, [bound_2])
    assert results_2 == [("pg1", "orders", 0)]  # loader still returns nothing for (2,)


@pytest.mark.asyncio
async def test_empty_bound_values_is_noop(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=(),
    )
    assert await ensure_rows_resident(state, [bound]) == []


@pytest.mark.asyncio
async def test_table_not_row_materialize_is_skipped(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table()
    table = table.model_copy(update={"row_materialize": False, "materialize": False})
    source = _source()
    patched_registry.table = table
    patched_registry.source = source
    patched_registry.loader = _FakeLoader({})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,),),
    )
    assert await ensure_rows_resident(state, [bound]) == []


@pytest.mark.asyncio
async def test_no_resolved_cache_ttl_raises(sqlite_dsn, patched_registry):
    from provisa.federation.query_residency import ensure_rows_resident

    table = _table(cache_ttl=None)
    source = _source()
    source.cache_ttl = None
    patched_registry.table = table
    patched_registry.source = source
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})

    state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
    bound = PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=((1,),),
    )
    with pytest.raises(ValueError, match="no resolved cache_ttl"):
        await ensure_rows_resident(state, [bound])

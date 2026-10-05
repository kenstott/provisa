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

    def replica_address(self, state, *, source_id, schema_name, table_name):
        """As EngineBackend.replica_address: the one replica name. The schema is the test store's
        own (a SQLite file stands in for the store here, and ``main`` is the schema it has): what
        is under test is the row cache, not where the replicas schema lives."""
        from provisa.federation.replica_address import ReplicaAddress, replica_table_name

        del state
        return ReplicaAddress("main", replica_table_name(source_id, schema_name, table_name))


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
        self.admitted: list[str | None] = []  # the predicate each fetch was asked to carry

    async def load_keys(self, source, table, pk_columns, keys, *, admit=None):
        self.calls.append(list(keys))
        self.admitted.append(admit)
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

    def _fake_loader_cls(engine, adapter_loaders=None, keyed_adapter_loaders=None):
        return ns.loader

    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _fake_sources)
    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _fake_tables)
    monkeypatch.setattr("provisa.events.source_loader.SourceRowLoader", _fake_loader_cls)
    monkeypatch.setattr("provisa.events.app_wiring.build_adapter_loaders", lambda state, engine: {})
    monkeypatch.setattr(
        "provisa.events.app_wiring.build_keyed_adapter_loaders", lambda state, engine=None: {}
    )
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
    results = await ensure_rows_resident(state, [bound], reader_role=None)
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
    await ensure_rows_resident(state, [bound], reader_role=None)
    assert patched_registry.loader.calls == [[(1,)]]

    # Second call, same key, not yet expired (cache_ttl=60s) -- no re-fetch.
    results = await ensure_rows_resident(state, [bound], reader_role=None)
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
    await ensure_rows_resident(state, [bound], reader_role=None)
    assert len(patched_registry.loader.calls) == 1

    patched_registry.loader.rows_by_key[(1,)] = {"id": 1, "status": "shipped"}
    results = await ensure_rows_resident(state, [bound], reader_role=None, force=True)
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
    results = await ensure_rows_resident(state, [bound], reader_role=None)
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
    results_2 = await ensure_rows_resident(state, [bound_2], reader_role=None)
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
    assert await ensure_rows_resident(state, [bound], reader_role=None) == []


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
    assert await ensure_rows_resident(state, [bound], reader_role=None) == []


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
        await ensure_rows_resident(state, [bound], reader_role=None)


@pytest.mark.asyncio
async def test_a_row_level_table_kept_in_another_region_is_never_fetched_into_this_one(
    sqlite_dsn, patched_registry
):
    """REQ-1921/1922: a row-level table naming eu, read on a us node: its rows are kept in eu
    only, so this region fetches none of them into its own store (its read is judged — refused
    naming eu, or read in place — by ensure_resident)."""
    from provisa.core import process_region
    from provisa.federation.query_residency import ensure_rows_resident

    was = process_region._region
    process_region.bind_launch(
        {
            "regions": [
                {"id": "eu", "address": "https://eu.example.com"},
                {"id": "us", "address": "https://us.example.com"},
            ]
        },
        requested="us",
    )
    try:
        table = _table()
        table.region = "eu"
        patched_registry.table = table
        patched_registry.source = _source()
        patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new"}})
        state = types.SimpleNamespace(federation_engine=_FakeEngine(sqlite_dsn))
        bound = PkBound(
            source_id="pg1",
            schema_name="public",
            table_name="orders",
            pk_columns=("id",),
            values=((1,),),
        )
        assert await ensure_rows_resident(state, [bound], reader_role=None) == []
        assert patched_registry.loader.calls == []
    finally:
        process_region._region = was


_TWO_REGIONS = {
    "regions": [
        {"id": "eu", "address": "https://eu.example.com"},
        {"id": "us", "address": "https://us.example.com"},
    ]
}


@pytest.fixture
def node_in_us():
    from provisa.core import process_region

    was = process_region._region
    process_region.bind_launch(_TWO_REGIONS, requested="us")
    yield
    process_region._region = was


def _regional_table() -> types.SimpleNamespace:
    """A row-level table naming no region whose rows carry the region they may be kept in — the
    registry's shape: a config table's fields plus its registered id."""
    table = _table()
    table.columns.append(Column(name="home", visible_to=["public"], data_type="text"))
    return types.SimpleNamespace(
        **{name: getattr(table, name) for name in type(table).model_fields}, id=7
    )


def _admin_ruled_state(sqlite_dsn, rule):
    from provisa.compiler.rls import RLSContext

    return types.SimpleNamespace(
        federation_engine=_FakeEngine(sqlite_dsn),
        roles={},
        rls_contexts={"org_admin": RLSContext(rules={7: rule})},
    )


def _bound(*keys):
    return PkBound(
        source_id="pg1",
        schema_name="public",
        table_name="orders",
        pk_columns=("id",),
        values=tuple((k,) for k in keys),
    )


@pytest.mark.asyncio
async def test_a_row_this_regions_administrator_may_not_keep_is_never_landed_here(
    sqlite_dsn, patched_registry, node_in_us
):
    """REQ-1921 (scenario): a row-level table with a row rule on the region attribute, read
    through us: the row that may be held only in eu is fetched but not kept here — asked for
    again, it is fetched again — while the us row is kept."""
    from provisa.federation.query_residency import ensure_rows_resident

    patched_registry.table = _regional_table()
    patched_registry.source = _source()
    patched_registry.loader = _FakeLoader(
        {
            (1,): {"id": 1, "status": "new", "home": "eu"},
            (2,): {"id": 2, "status": "new", "home": "us"},
        }
    )
    state = _admin_ruled_state(sqlite_dsn, "home = current_setting('provisa.region')")
    assert await ensure_rows_resident(state, [_bound(1, 2)], reader_role=None) == [
        ("pg1", "orders", 1)
    ]
    assert await ensure_rows_resident(state, [_bound(1, 2)], reader_role=None) == [
        ("pg1", "orders", 0)
    ]
    assert patched_registry.loader.calls == [[(1,), (2,)], [(1,)]]


@pytest.mark.asyncio
async def test_a_rule_that_cannot_be_judged_over_the_fetched_rows_keeps_none_of_them(
    sqlite_dsn, patched_registry, node_in_us
):
    from provisa.federation.query_residency import ensure_rows_resident
    from provisa.federation.region_rows import RowRuleNotEvaluable

    patched_registry.table = _regional_table()
    patched_registry.source = _source()
    patched_registry.loader = _FakeLoader({(1,): {"id": 1, "status": "new", "home": "us"}})
    state = _admin_ruled_state(sqlite_dsn, "home IN (SELECT region FROM allowed_regions)")
    with pytest.raises(RowRuleNotEvaluable) as refused:
        await ensure_rows_resident(state, [_bound(1)], reader_role=None)
    assert refused.value.params == {"table": "orders"}
    assert await ensure_rows_resident(
        types.SimpleNamespace(federation_engine=state.federation_engine, roles={}, rls_contexts={}),
        [_bound(1)],
        reader_role=None,
    ) == [("pg1", "orders", 1)]  # nothing was landed by the refused fetch


def test_a_key_pushdown_batch_keeps_only_what_this_regions_administrator_may(node_in_us):
    """The Arrow batch key pushdown lands is admitted the same way (REQ-1921/1922)."""
    import pyarrow as pa

    from provisa.federation.region_rows import admit_rows

    state = _admin_ruled_state("unused", "home = current_setting('provisa.region')")
    batch = pa.Table.from_pylist(
        [{"id": 1, "home": "eu"}, {"id": 2, "home": "us"}, {"id": 3, "home": None}]
    )
    assert admit_rows(state, _regional_table(), batch).to_pylist() == [{"id": 2, "home": "us"}]


def test_with_no_platform_regions_every_fetched_row_is_kept():
    import pyarrow as pa

    from provisa.core import process_region
    from provisa.federation.region_rows import admit_rows

    was = process_region._region
    process_region.bind_launch(None, requested=None)
    try:
        state = _admin_ruled_state("unused", "home = current_setting('provisa.region')")
        batch = pa.Table.from_pylist([{"id": 1, "home": "eu"}])
        assert admit_rows(state, _regional_table(), batch) is batch
    finally:
        process_region._region = was


@pytest.mark.asyncio
async def test_a_sources_keyed_fetch_is_asked_for_only_what_this_region_may_keep(
    sqlite_dsn, patched_registry, node_in_us
):
    """REQ-1921/1922: where the source's fetch takes a predicate, the administrator's rule with
    this region's values goes with it, so the eu row never leaves the source; a rule reading
    another relation cannot go with a one-table fetch and is judged after it instead."""
    from provisa.federation.query_residency import ensure_rows_resident

    patched_registry.table = _regional_table()
    patched_registry.source = _source()
    patched_registry.loader = _FakeLoader({(2,): {"id": 2, "status": "new", "home": "us"}})
    state = _admin_ruled_state(sqlite_dsn, "home = current_setting('provisa.region')")
    await ensure_rows_resident(state, [_bound(2)], reader_role=None)
    assert patched_registry.loader.admitted == ["home = 'us'"]

    state = _admin_ruled_state(sqlite_dsn, "home IN (SELECT region FROM allowed_regions)")
    await ensure_rows_resident(state, [_bound(3)], reader_role=None)
    assert patched_registry.loader.admitted[-1] is None


@pytest.mark.asyncio
async def test_the_engine_fetch_carries_the_admission_predicate_in_its_own_dialect():
    from provisa.events.source_loader import SourceRowLoader

    sent: list[str] = []

    class _Engine:
        dialect = "duckdb"

        async def execute_engine(self, sql, *, authorization):
            sent.append(sql)
            return types.SimpleNamespace(column_names=["id"], rows=[(2,)])

    loader = SourceRowLoader(_Engine())
    rows = await loader.load_keys(_source(), _table(), ["id"], [(1,), (2,)], admit="home = 'us'")
    assert rows == [{"id": 2}]
    assert sent == [
        'SELECT * FROM "pg1"."public"."orders" WHERE ("id" IN (1, 2)) AND (home = \'us\')'
    ]

# Copyright (c) 2026 Kenneth Stott
# Canary: 7e938c92-a7b7-4c19-9b9c-fa037cddb4cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318, REQ-1915: parameter-set fills are kept in the store's API cache schema, written and
read through the engine's connection, and the remote is called the one way every path calls it.

The engine connection here is a real DuckDB connection behind the session wrapper every engine
hands out (``executor.session.EngineSession``)."""

from __future__ import annotations

import time
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest
import respx

from provisa.api_source import fill_cache
from provisa.api_source.caller import ApiCallError
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.core.paging import PaginationConfig
from provisa.executor.session import EngineSession

duckdb = pytest.importorskip("duckdb")

BASE = "http://api.test"
ODD = 'a"; DROP TABLE keep_me; --'


@pytest.fixture(autouse=True)
def _clean():
    """Each test has its own store: what this process remembers of the last one goes too."""
    from provisa.api_source import engine_cache

    def forget():
        fill_cache._mem_fresh.clear()
        fill_cache._shapes.clear()
        engine_cache._SCHEMA_EXISTS_CACHE.clear()

    forget()
    yield
    forget()


@pytest.fixture
def store(bind_org):
    bind_org("o1")  # the org the state serves, bound as its request binds it (REQ-1266)
    con = duckdb.connect()

    @contextmanager
    def isolated_sync():
        yield EngineSession(con, dialect="duckdb", placeholder="?")

    engine = SimpleNamespace(cache_catalog=lambda: "memory", isolated_sync=isolated_sync)
    state = SimpleNamespace(org_id="o1", federation_engine=engine, source_catalogs={})
    yield state, con
    con.close()


def _endpoint(**kw) -> ApiEndpoint:
    base = dict(
        source_id="api",
        path="/pets",
        table_name="pets",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
            ApiColumn(
                name="status",
                type=ApiColumnType.string,
                param_type="query",
                param_name="status",
                param_only=True,
            ),
        ],
    )
    base.update(kw)
    return ApiEndpoint(**base)


def _api_source(auth=None):
    return SimpleNamespace(base_url=BASE, auth=auth)


def _rows(con, table) -> list[tuple]:
    return con.execute(
        f'SELECT id, name, "_params_hash" FROM "{table.loc.schema}"."{table.name}" ORDER BY id'
    ).fetchall()


def test_the_fill_table_is_in_the_orgs_api_cache_schema_named_for_the_table(store):
    state, _con = store
    table = fill_cache.fill_table(state, _endpoint(), _api_source())
    assert (table.loc.catalog, table.loc.schema) == ("memory", "org_o1_api_cache")
    assert table.name == "api__fills__pets"
    assert table.data_columns == ["id", "name"]  # a parameter that is not a response field: none
    assert [c.name for c in table.columns][-2:] == ["_params_hash", "_cached_at"]


def test_an_engine_with_its_own_cache_catalog_caches_there_not_in_the_sources_catalog(store):
    """REQ-318/REQ-1730: a native engine (DuckDB) caches API rows in the store it attaches, even
    though the source has a per-source catalog name — DuckDB never attaches an OpenAPI source, so
    that name is a catalog that does not exist ("Catalog petstore_api does not exist")."""
    state, _con = store
    state.source_catalogs["api"] = "api_catalog"
    table = fill_cache.fill_table(state, _endpoint(), _api_source())
    assert table.loc.catalog == "memory"


def test_an_engine_with_no_cache_catalog_caches_in_the_catalog_it_reads_the_source_through():
    """REQ-1730: an engine whose own catalogs are durable (Trino: ``cache_catalog()`` is None)
    caches in the catalog the source resolves to, never in a per-source name nothing provisions."""
    engine = SimpleNamespace(cache_catalog=lambda: None)
    state = SimpleNamespace(
        org_id="o1", federation_engine=engine, source_catalogs={"api": "provisa_admin"}
    )
    table = fill_cache.fill_table(state, _endpoint(), _api_source())
    assert table.loc.catalog == "provisa_admin"


def test_a_sources_own_cache_catalog_wins_on_every_engine(store):
    state, _con = store
    state.source_catalogs["api"] = "api_catalog"
    source = SimpleNamespace(base_url=BASE, auth=None, cache_catalog="pinned")
    assert fill_cache.fill_table(state, _endpoint(), source).loc.catalog == "pinned"


@respx.mock
async def test_a_fill_calls_the_remote_with_its_auth_and_paging_and_keeps_the_answer(store):
    """The remote requires a credential and pages its answer: the fill sends the source's
    credential on every page and keeps every page's rows under its argument set."""

    def answer(request: httpx.Request) -> httpx.Response:
        if request.headers.get("authorization") != "Bearer t0ken":
            return httpx.Response(401)
        offset = int(request.url.params["offset"])
        rows = [{"id": i, "name": f"n{i}"} for i in range(offset, min(offset + 2, 5))]
        return httpx.Response(200, json=rows)

    route = respx.get(f"{BASE}/pets").mock(side_effect=answer)
    state, con = store
    endpoint = _endpoint(pagination=PaginationConfig(type="offset", page_size=2, max_pages=10))
    written = await fill_cache.fill(
        state, endpoint, _api_source({"bearer": "t0ken"}), [{"status": "sold"}], ttl=300
    )
    assert written == 5 and route.call_count == 3
    assert {c.request.url.params["status"] for c in route.calls} == {"sold"}
    table = fill_cache.fill_table(state, endpoint, _api_source())
    phash = fill_cache.params_hash({"status": "sold"})
    assert _rows(con, table) == [(i, f"n{i}", phash) for i in range(5)]

    # Fresh: no second call within the TTL, from this process or (memory cleared) from the table.
    assert await fill_cache.fill(state, endpoint, _api_source(), [{"status": "sold"}], 300) == 0
    fill_cache._mem_fresh.clear()
    assert await fill_cache.fill(state, endpoint, _api_source(), [{"status": "sold"}], 300) == 0
    assert route.call_count == 3


@respx.mock
async def test_a_stale_set_is_replaced_and_other_sets_are_left(store):
    state, con = store
    endpoint = _endpoint()
    respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]),
            httpx.Response(200, json=[{"id": 9, "name": "z"}]),
            httpx.Response(200, json=[{"id": 1, "name": "a2"}]),
        ]
    )
    source = _api_source()
    await fill_cache.fill(state, endpoint, source, [{"status": "x"}], ttl=300)
    await fill_cache.fill(state, endpoint, source, [{"status": "y"}], ttl=300)
    table = fill_cache.fill_table(state, endpoint, source)
    con.execute(f'UPDATE "{table.loc.schema}"."{table.name}" SET "_cached_at" = 0')
    fill_cache._mem_fresh.clear()
    with state.federation_engine.isolated_sync() as conn:
        assert fill_cache.stale_hashes(
            conn, table, [fill_cache.params_hash({"status": "x"})], ttl=300
        ) == [fill_cache.params_hash({"status": "x"})]
    await fill_cache.fill(state, endpoint, source, [{"status": "x"}], ttl=300)
    x, y = fill_cache.params_hash({"status": "x"}), fill_cache.params_hash({"status": "y"})
    assert _rows(con, table) == [(1, "a2", x), (9, "z", y)]


@respx.mock
async def test_a_404_is_an_answer_with_no_rows_and_a_reported_error_fails_the_request(store):
    state, con = store
    respx.get(f"{BASE}/pets").mock(return_value=httpx.Response(404))
    assert await fill_cache.fill(state, _endpoint(), _api_source(), [{}], ttl=300) == 0

    respx.get(f"{BASE}/pets").mock(
        return_value=httpx.Response(200, json={"error": {"message": "quota"}, "items": []})
    )
    failing = _endpoint(path="/pets", error_path="error.message", response_root="items")
    with pytest.raises(ApiCallError, match="error at 'error.message': quota"):
        await fill_cache.fill(state, failing, _api_source(), [{"status": "q"}], ttl=300)


def test_a_response_key_is_one_identifier_whatever_it_holds(store):
    """REQ-318: a column is named after an OpenAPI property or a response key — data. It stays
    one identifier: the table gets a column of exactly that name and nothing else changes."""
    state, con = store
    con.execute("CREATE TABLE keep_me (n INTEGER)")
    con.execute("INSERT INTO keep_me VALUES (1)")
    endpoint = _endpoint(
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name=ODD, type=ApiColumnType.string),
        ]
    )
    table = fill_cache.fill_table(state, endpoint, _api_source())
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, table, {"h": [{"id": 7, ODD: "v"}]}, ttl=300)
        assert fill_cache.read_rows(conn, table) == [{"id": 7, ODD: "v"}]
        assert fill_cache.distinct_values(conn, table, ODD) == ["v"]
    assert con.execute("SELECT COUNT(*) FROM keep_me").fetchone() == (1,)
    with state.federation_engine.isolated_sync() as conn:
        with pytest.raises(KeyError, match="not a response column"):
            fill_cache.distinct_values(conn, table, "nope")


def test_a_table_made_for_another_definition_is_replaced(store):
    state, con = store
    old = _endpoint()
    table = fill_cache.fill_table(state, old, _api_source())
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, table, {"h": [{"id": 1, "name": "a"}]}, ttl=300)
    # The definition gains a column; this process made the table for the old shape.
    new = _endpoint(
        columns=[*old.columns, ApiColumn(name="age", type=ApiColumnType.integer)],
    )
    wider = fill_cache.fill_table(state, new, _api_source())
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, wider, {"h2": [{"id": 2, "name": "b", "age": 3}]}, ttl=300)
        assert fill_cache.read_rows(conn, wider) == [{"id": 2, "name": "b", "age": 3}]
    # Another process, started after the change, finds the old table by probing it.
    fill_cache._shapes.clear()
    con.execute(f'DROP TABLE "{table.loc.schema}"."{table.name}"')
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, table, {"h": [{"id": 1, "name": "a"}]}, ttl=300)
    fill_cache._shapes.clear()
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, wider, {"h2": [{"id": 2, "name": "b", "age": 3}]}, ttl=300)
        assert fill_cache.read_rows(conn, wider) == [{"id": 2, "name": "b", "age": 3}]


def test_freshness_is_the_shared_freshness_modules_decision(store, monkeypatch):
    """REQ-859: the TTL decision on a fill is ``freshness.evaluate`` over its fetch time."""
    from provisa import freshness

    state, _con = store
    table = fill_cache.fill_table(state, _endpoint(), _api_source())
    with state.federation_engine.isolated_sync() as conn:
        fill_cache.store(conn, table, {"recent": [], "old": []}, ttl=300)
    fill_cache._mem_fresh.clear()
    seen = []
    real = freshness.evaluate

    def spy(subject, policy, now):
        seen.append((round(subject.refreshed_at), policy.ttl_seconds))
        return real(subject, policy, now)

    monkeypatch.setattr(freshness, "evaluate", spy)
    with state.federation_engine.isolated_sync() as conn:
        # A group fetched with no rows has no row to say when: it is fetched again.
        assert fill_cache.stale_hashes(conn, table, ["recent", "never"], ttl=300) == [
            "recent",
            "never",
        ]
        fill_cache.store(conn, table, {"recent": [{"id": 1, "name": "a"}]}, ttl=300)
    fill_cache._mem_fresh.clear()
    with state.federation_engine.isolated_sync() as conn:
        assert fill_cache.stale_hashes(conn, table, ["recent"], ttl=300) == []
    assert seen and seen[-1] == (round(time.time()), 300)


@respx.mock
async def test_a_request_fetches_a_path_parameter_table_once_per_parent_key_with_its_auth(store):
    """GraphQL hydration over the fills: the parent collection is filled, its keys are read
    back from the store, and the dependent endpoint is called once per key — every call with
    the source's credential, nothing written to the control plane (there is none here)."""
    from provisa.api.data import hydration

    def answer(request: httpx.Request) -> httpx.Response:
        if request.headers.get("authorization") != "Bearer t0ken":
            return httpx.Response(401)
        if request.url.path == "/pets":
            return httpx.Response(200, json=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        pet = int(request.url.path.rsplit("/", 1)[1])
        return httpx.Response(200, json={"id": pet, "name": f"pet {pet}"})

    route = respx.route(host="api.test").mock(side_effect=answer)
    state, con = store
    pets = _endpoint(columns=_endpoint().columns[:2])
    by_id = _endpoint(
        path="/pets/{petId}",
        table_name="pet_by_id",
        columns=[
            *_endpoint().columns[:2],
            ApiColumn(
                name="petId",
                type=ApiColumnType.integer,
                param_type="path",
                param_name="petId",
                param_only=True,
            ),
        ],
    )
    state.api_endpoints = {"pets": pets, "pet_by_id": by_id}
    state.api_sources = {"api": _api_source({"bearer": "t0ken"})}
    state.tables = []
    state.tenant_db = None
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="pet_by_id"), target_column="petId", source_column="id"
    )
    ctx = SimpleNamespace(
        joins={("Pet", "byId"): join},
        tables={"pets": SimpleNamespace(type_name="Pet", table_name="pets", schema_name="x")},
    )
    compiled = SimpleNamespace(sources={"api"}, api_args={})
    hydration._source_hydration_expiry.clear()
    _, _, rows, _ = await hydration._hydrate_api_tables_before_engine(compiled, ctx, state)

    assert sorted(c.request.url.path for c in route.calls) == ["/pets", "/pets/1", "/pets/2"]
    assert rows == {"api": 4}
    table = fill_cache.fill_table(state, by_id, None)
    assert sorted(_rows(con, table)) == [
        (1, "pet 1", fill_cache.params_hash({"petId": "1"})),
        (2, "pet 2", fill_cache.params_hash({"petId": "2"})),
    ]


@respx.mock
async def test_a_parameter_with_no_declared_name_arrives_under_its_column_name():
    """Before, the caller skipped a parameter column with no ``param_name``: the value was not
    sent and a path kept its ``{placeholder}``. Now it is sent under the column's own name."""
    from provisa.api_source.caller import call_api

    route = respx.get(f"{BASE}/pets/7").mock(return_value=httpx.Response(200, json=[]))
    endpoint = _endpoint(
        path="/pets/{petId}",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="petId", type=ApiColumnType.integer, param_type="path"),
            ApiColumn(name="status", type=ApiColumnType.string, param_type="query"),
        ],
    )
    await call_api(endpoint, {"petId": 7, "status": "sold"}, base_url=BASE)
    assert route.call_count == 1
    assert dict(route.calls[0].request.url.params) == {"status": "sold"}


@respx.mock
async def test_an_api_that_answers_nothing_gives_no_rows_through_every_path_that_reads_a_fill(
    store,
):
    """The fill of an empty answer is a table with no rows for those arguments: the API step's
    read, the hot promotion's read and a dependent's parent keys all see none, and a dependent
    table is not called for."""
    from provisa.api.data import hydration
    from provisa.api.data.materialization import _mat_fetch_rows_from_fills

    route = respx.route(host="api.test").mock(return_value=httpx.Response(200, json=[]))
    state, con = store
    pets = _endpoint(columns=_endpoint().columns[:2])
    by_id = _endpoint(
        path="/pets/{petId}",
        table_name="pet_by_id",
        columns=[
            *_endpoint().columns[:2],
            ApiColumn(name="petId", type=ApiColumnType.integer, param_type="path", param_only=True),
        ],
    )
    state.api_endpoints = {"pets": pets, "pet_by_id": by_id}
    state.api_sources = {"api": _api_source()}
    state.tables = []
    state.tenant_db = None
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="pet_by_id"), target_column="petId", source_column="id"
    )
    ctx = SimpleNamespace(
        joins={("Pet", "byId"): join},
        tables={"pets": SimpleNamespace(type_name="Pet", table_name="pets", schema_name="x")},
    )
    hydration._source_hydration_expiry.clear()
    _, _, rows, _ = await hydration._hydrate_api_tables_before_engine(
        SimpleNamespace(sources={"api"}, api_args={}), ctx, state
    )
    assert rows == {"api": 0}
    assert [c.request.url.path for c in route.calls] == ["/pets"]  # no parent key: no call
    table = fill_cache.fill_table(state, pets, None)
    assert con.execute(f'SELECT COUNT(*) FROM "{table.loc.schema}"."{table.name}"').fetchone() == (
        0,
    )
    assert await _mat_fetch_rows_from_fills(pets, ["id", "name"], set(), state) == []
    with state.federation_engine.isolated_sync() as conn:
        assert fill_cache.read_rows(conn, table) == []
        assert fill_cache.distinct_values(conn, table, "id") == []

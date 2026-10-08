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
    return SimpleNamespace(base_url=BASE, auth=auth, headers={})


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
    source = SimpleNamespace(base_url=BASE, auth=None, headers={}, cache_catalog="pinned")
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
    state.api_endpoints = {(pets.source_id, "pets"): pets, (by_id.source_id, "pet_by_id"): by_id}
    state.api_sources = {"api": _api_source({"bearer": "t0ken"})}
    state.tables = []
    state.tenant_db = None
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="pet_by_id"), target_column="petId", source_column="id"
    )
    ctx = SimpleNamespace(
        joins={("Pet", "byId"): join},
        tables={
            "pets": SimpleNamespace(
                type_name="Pet", source_id="api", table_name="pets", schema_name="x"
            )
        },
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
    state.api_endpoints = {(pets.source_id, "pets"): pets, (by_id.source_id, "pet_by_id"): by_id}
    state.api_sources = {"api": _api_source()}
    state.tables = []
    state.tenant_db = None
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="pet_by_id"), target_column="petId", source_column="id"
    )
    ctx = SimpleNamespace(
        joins={("Pet", "byId"): join},
        tables={
            "pets": SimpleNamespace(
                type_name="Pet", source_id="api", table_name="pets", schema_name="x"
            )
        },
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


# --- an answer is kept under its arguments (REQ-318) ----------------------------------------------

_META = {fill_cache.PARAMS_HASH, fill_cache.CACHED_AT}
_NO_JOINS = SimpleNamespace(joins={}, tables={})


def _by_id() -> ApiEndpoint:
    return _endpoint(
        path="/pets/{petId}",
        table_name="pet_by_id",
        columns=[
            *_endpoint().columns[:2],
            ApiColumn(name="petId", type=ApiColumnType.integer, param_type="path", param_only=True),
        ],
    )


def _pets_feeding_by_id() -> SimpleNamespace:
    """A model in which ``pets.id`` feeds ``pet_by_id``'s path parameter."""
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="pet_by_id"), target_column="petId", source_column="id"
    )
    parent = SimpleNamespace(type_name="Pet", source_id="api", table_name="pets", schema_name="x")
    return SimpleNamespace(joins={("Pet", "byId"): join}, tables={"pets": parent})


@pytest.fixture
def api_step(store, monkeypatch):
    """The API step of a statement over the store: ``read(endpoint, args, ctx)`` gives the cache
    table the statement is pointed at and the rows it holds."""
    from provisa.api.data import hydration
    from provisa.api.data.materialization import _mat_api_ep_table, _StatementHot
    from provisa.api_source import engine_cache

    state, con = store
    state.api_sources = {"api": _api_source()}
    state.source_cache = {}
    state.response_cache_default_ttl = 300
    state.tables = []
    state.tenant_db = None
    engine_cache._TABLE_EXISTS_CACHE.clear()
    hydration._source_hydration_expiry.clear()
    monkeypatch.setattr(engine_cache, "_scope", lambda: "scope")
    # No org runtime is built here: redirect settings are the platform's, as a deployment that
    # installs no org resolver reads them (an earlier test's app import leaves one installed).
    monkeypatch.setattr("provisa.executor.redirect._org_overrides_resolver", None)
    monkeypatch.setattr(engine_cache, "schedule_drop", lambda *a, **k: None)

    async def read(endpoint, args=None, ctx=_NO_JOINS):
        rewrites: dict = {}
        await _mat_api_ep_table(
            endpoint.table_name,
            endpoint,
            state,
            _StatementHot(None, state, []),
            0,  # nothing is inlined: every answer is read from its cache table
            _META,
            rewrites,
            {},
            nf_args=args,
            ctx=ctx,
        )
        loc, table = rewrites[endpoint.table_name]
        held = con.execute(
            f'SELECT id, name FROM "{loc.catalog}"."{loc.schema}"."{table}" ORDER BY id'
        )
        return table, held.fetchall()

    yield state, read
    engine_cache._TABLE_EXISTS_CACHE.clear()
    hydration._source_hydration_expiry.clear()


def _one_pet(request: httpx.Request) -> httpx.Response:
    pet = int(request.url.path.rsplit("/", 1)[1])
    return httpx.Response(200, json={"id": pet, "name": f"pet {pet}"})


def _pets_by_status(request: httpx.Request) -> httpx.Response:
    pets = {"sold": [{"id": 1, "name": "a"}], "available": [{"id": 2, "name": "b"}]}
    status = request.url.params.get("status")
    return httpx.Response(200, json=pets[status] if status else pets["sold"] + pets["available"])


def test_a_statements_arguments_for_an_endpoint_are_its_own_parameters_under_their_sent_names():
    ep = _endpoint(
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="petId", type=ApiColumnType.integer, param_type="path"),
            ApiColumn(
                name="state", type=ApiColumnType.string, param_type="query", param_name="status"
            ),
        ]
    )
    # By column or by sent name, re-cased and ``_``-prefixed as a statement may carry them; an
    # argument that is another table's parameter, and one given as nothing, are not its own.
    assert fill_cache.endpoint_args(ep, {"_pet_id": 3, "status": "sold", "owner": 9}) == {
        "petId": 3,
        "status": "sold",
    }
    assert fill_cache.endpoint_args(ep, {"state": "sold", "petId": None}) == {"status": "sold"}
    assert fill_cache.endpoint_args(ep, None) == {}


def test_an_endpoint_is_read_by_parent_keys_when_a_join_fed_parameter_is_not_given():
    by_id = _by_id()
    assert fill_cache.read_by_parent_keys(by_id, {}, _NO_JOINS)  # a path parameter, not given
    assert not fill_cache.read_by_parent_keys(by_id, {"petId": 1}, _NO_JOINS)

    orders = _endpoint(
        path="/orders",
        table_name="orders",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="customer_id", type=ApiColumnType.integer, param_type="query"),
            ApiColumn(name="status", type=ApiColumnType.string, param_type="query"),
        ],
    )
    join = SimpleNamespace(
        target=SimpleNamespace(table_name="orders"), target_column="customer_id", source_column="id"
    )
    fed = SimpleNamespace(
        joins={("Customer", "orders"): join},
        tables={"customers": SimpleNamespace(type_name="Customer", table_name="customers")},
    )
    assert fill_cache.read_by_parent_keys(orders, {"status": "open"}, fed)  # the join's is not
    assert not fill_cache.read_by_parent_keys(orders, {"customer_id": 5}, fed)
    assert not fill_cache.read_by_parent_keys(orders, {}, _NO_JOINS)  # no join feeds it
    assert not fill_cache.read_by_parent_keys(orders, {}, None)


@respx.mock
async def test_a_path_parameter_table_read_for_one_argument_never_answers_another(api_step):
    """The defect: the cache table of ``/pets/{petId}`` was named without the argument, so the
    read for pet 2 was answered with pet 1's row for as long as that table lived."""
    route = respx.route(host="api.test").mock(side_effect=_one_pet)
    _state, read = api_step
    by_id = _by_id()

    first, rows = await read(by_id, {"petId": "1"})
    assert rows == [(1, "pet 1")]
    second, rows = await read(by_id, {"petId": "2"})
    assert rows == [(2, "pet 2")] and second != first
    again, rows = await read(by_id, {"petId": "1"})
    assert (again, rows) == (first, [(1, "pet 1")])
    assert [c.request.url.path for c in route.calls] == ["/pets/1", "/pets/2"]  # a hit, no call


@respx.mock
async def test_a_query_argument_a_statement_gives_is_sent_and_names_its_answer(api_step):
    """A SQL read with ``_nf_status = 'sold'`` has the condition taken out of the engine's
    statement: the remote must apply it, and the answer is that argument's alone."""
    route = respx.route(host="api.test").mock(side_effect=_pets_by_status)
    _state, read = api_step

    sold, rows = await read(_endpoint(), {"status": "sold"})
    assert rows == [(1, "a")]
    everything, rows = await read(_endpoint())
    assert rows == [(1, "a"), (2, "b")] and everything != sold
    assert [dict(c.request.url.params) for c in route.calls] == [{"status": "sold"}, {}]


@respx.mock
async def test_a_fill_is_read_back_by_the_arguments_it_was_fetched_for(api_step):
    """GraphQL fills one group per argument set. Read back, a statement gets the group of its
    own arguments -- not every group the table holds, and not the first one a cache table was
    made from. An argument that is another table's parameter makes no group of its own."""
    from provisa.api.data import hydration

    route = respx.route(host="api.test").mock(side_effect=_pets_by_status)
    state, read = api_step
    pets = _endpoint()
    state.api_endpoints = {("api", "pets"): pets}
    for args in ({"status": "sold"}, {"status": "available"}, {"status": "sold", "owner": 9}):
        hydration._source_hydration_expiry.clear()
        await hydration._hydrate_api_tables_before_engine(
            SimpleNamespace(sources={"api"}, api_args=args), _NO_JOINS, state
        )
        _table, rows = await read(pets, args)
        assert rows == ([(1, "a")] if args["status"] == "sold" else [(2, "b")])
    assert [dict(c.request.url.params) for c in route.calls] == [
        {"status": "sold"},
        {"status": "available"},
    ]


@respx.mock
async def test_a_table_read_by_its_parents_keys_is_every_keys_fill_and_one_key_is_its_own(
    api_step,
):
    """A path-parameter table joined to its parent is filled once per parent key, and a
    statement that gives no argument reads all of them to join to. A statement that names one
    pet reads that pet's fill, with no call."""
    from provisa.api.data import hydration

    def answer(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/pets":
            return httpx.Response(200, json=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        return _one_pet(request)

    route = respx.route(host="api.test").mock(side_effect=answer)
    state, read = api_step
    pets, by_id = _endpoint(columns=_endpoint().columns[:2]), _by_id()
    state.api_endpoints = {("api", "pets"): pets, ("api", "pet_by_id"): by_id}
    ctx = _pets_feeding_by_id()
    await hydration._hydrate_api_tables_before_engine(
        SimpleNamespace(sources={"api"}, api_args={}), ctx, state
    )
    calls = len(route.calls)

    joined, rows = await read(by_id, None, ctx)
    assert rows == [(1, "pet 1"), (2, "pet 2")]
    one, rows = await read(by_id, {"petId": "2"}, ctx)
    assert rows == [(2, "pet 2")] and one != joined
    assert len(route.calls) == calls


@respx.mock
async def test_a_fill_calls_the_api_with_the_endpoints_default_parameters_under_its_arguments(
    store,
):
    """The default parameters are what make the endpoint's whole collection (REQ-318). A fill
    left them out, so a GraphQL read called the API differently from a SQL read of the same
    table. They are sent under what the statement gives, and are no part of the fill's group."""
    route = respx.get(f"{BASE}/pets").mock(return_value=httpx.Response(200, json=[]))
    state, _con = store
    pets = _endpoint(
        columns=[
            *_endpoint().columns,
            ApiColumn(name="scope", type=ApiColumnType.string, param_type="query", param_only=True),
        ],
        default_params={"scope": "all", "status": "available"},
    )
    await fill_cache.fill(state, pets, _api_source(), [{}, {"status": "sold"}], ttl=300)
    assert [dict(c.request.url.params) for c in route.calls] == [
        {"scope": "all", "status": "available"},
        {"scope": "all", "status": "sold"},
    ]
    table = fill_cache.fill_table(state, pets, _api_source())
    assert fill_cache.is_mem_fresh(table, {}) and fill_cache.is_mem_fresh(table, {"status": "sold"})

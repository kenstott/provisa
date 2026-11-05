# Copyright (c) 2026 Kenneth Stott
# Canary: f10620b1-a63b-43e4-b3a6-8b2bebd316cc
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1986: a parameterized table keeps its rows only for a TTL of its own.

A table with declared parameters is read live for each statement and nothing it answers is
reused by another, whatever its source or the organisation keeps -- unless the table sets
``cache_ttl``, when the answer to each set of arguments is reused for it. A table with no
parameter keeps the chain it had (table, source, organisation, endpoint). A join's parent keys
are still fetched in one batched call per statement, and a statement never reads another's
fills."""

# Requirements: REQ-1986

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from provisa.api.data import hydration
from provisa.api.data.materialization import (
    _mat_api_ep_table,
    _mat_gql_remote_table,
    _StatementHot,
    statement_scratch,
)
from provisa.api_source import engine_cache
from provisa.api_source.models import ApiColumn, ApiColumnType
from provisa.api_source.models import ParamType
from tests.unit.test_api_fill_cache import (  # noqa: F401 -- store, _clean: its fixtures
    BASE,
    _api_source,
    _clean,
    _endpoint,
    _registered,
    store,
)

pytestmark = pytest.mark.asyncio

_META = {"_params_hash", "_cached_at"}


@pytest.fixture
def statement(request, monkeypatch):
    """One statement's API step over the store: ``read(endpoint, registration, args, ctx)``
    plans a statement reading the endpoint and gives the rows it was handed, inline or from the
    table it was pointed at."""
    state, con = request.getfixturevalue("store")
    state.api_sources = {"api": _api_source()}
    state.source_cache = {"api": {"cache_ttl": 600}}  # the source keeps for 10 minutes
    state.response_cache_default_ttl = 300
    state.tenant_db = None
    monkeypatch.setattr(engine_cache, "_scope", lambda: "scope")
    monkeypatch.setattr("provisa.executor.redirect._org_overrides_resolver", None)
    monkeypatch.setattr(engine_cache, "schedule_drop", lambda *a, **k: None)

    async def read(endpoint, registration, args=None, ctx=None, *, hydrate=False):
        rewrites: dict = {}
        inline: dict = {}
        with statement_scratch():
            if hydrate:
                hydration._source_hydration_expiry.clear()
                await hydration._hydrate_api_tables_before_engine(
                    MagicMock(sources={"api"}, api_args=args or {}), ctx, state
                )
            await _mat_api_ep_table(
                endpoint.table_name,
                endpoint,
                state,
                _StatementHot(None, state, []),
                0,
                _META,
                rewrites,
                inline,
                nf_args=args,
                ctx=ctx,
                registration=registration,
            )
        if endpoint.table_name in inline:
            return sorted((r["id"], r["name"]) for r in inline[endpoint.table_name].rows)
        loc, table = rewrites[endpoint.table_name]
        held = con.execute(f'SELECT id, name FROM "{loc.catalog}"."{loc.schema}"."{table}"')
        return sorted(held.fetchall())

    return state, read


@respx.mock
async def test_a_parameterized_endpoint_with_no_ttl_of_its_own_is_called_for_every_statement(
    statement,
):
    """The source keeps for 10 minutes; the endpoint has a query parameter and no TTL: each
    statement calls it, and each is answered with its own call's rows."""
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=lambda r: httpx.Response(200, json=[{"id": 1, "name": f"n{len(route.calls)}"}])
    )
    state, read = statement
    pets = _endpoint()
    state.tables = [_registered(pets, cache_ttl=None)]
    # The name counts the calls made before this one.
    assert await read(pets, _registered(pets, cache_ttl=None), {"status": "sold"}) == [(1, "n0")]
    assert await read(pets, _registered(pets, cache_ttl=None), {"status": "sold"}) == [(1, "n1")]
    assert route.call_count == 2


@respx.mock
async def test_a_parameterized_endpoint_with_a_ttl_of_its_own_reuses_its_answer_within_it(
    statement,
):
    route = respx.get(f"{BASE}/pets").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "name": "a"}])
    )
    state, read = statement
    pets = _endpoint()
    state.tables = [_registered(pets, cache_ttl=45)]
    for _ in range(2):
        assert await read(pets, _registered(pets, cache_ttl=45), {"status": "sold"}) == [(1, "a")]
    assert route.call_count == 1


@respx.mock
async def test_an_endpoint_with_no_parameter_keeps_its_sources_ttl(statement):
    route = respx.get(f"{BASE}/pets").mock(
        return_value=httpx.Response(200, json=[{"id": 1, "name": "a"}])
    )
    state, read = statement
    pets = _endpoint(columns=_endpoint().columns[:2])  # no parameter
    state.tables = [_registered(pets, cache_ttl=None)]
    for _ in range(2):
        assert await read(pets, _registered(pets, cache_ttl=None)) == [(1, "a")]
    assert route.call_count == 1  # kept for the source's 600 s


# -- a join's parent keys, batched, at TTL 0 ----------------------------------------------------


def _toys():
    """``toys``, fetched for a list of pets by its ``petId`` query parameter (a DataLoader)."""
    return _endpoint(
        path="/toys",
        table_name="toys",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
            ApiColumn(
                name="petId",
                type=ApiColumnType.integer,
                param_type=ParamType.query,
                param_name="petId",
                param_only=True,
            ),
        ],
    )


def _pets_feeding_toys():
    from types import SimpleNamespace

    join = SimpleNamespace(
        target=SimpleNamespace(table_name="toys"), target_column="petId", source_column="id"
    )
    parent = SimpleNamespace(type_name="Pet", source_id="api", table_name="pets", schema_name="x")
    return SimpleNamespace(joins={("Pet", "toys"): join}, tables={"pets": parent})


def _api(toy_calls: list):
    def answer(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/pets":
            return httpx.Response(200, json=[{"id": i, "name": f"pet {i}"} for i in (1, 2, 3)])
        toy_calls.append(sorted(request.url.params.get_list("petId")))
        n = len(toy_calls)
        return httpx.Response(200, json=[{"id": 10 * n + 1, "name": f"toy of call {n}"}])

    return answer


@respx.mock
async def test_a_joins_parent_keys_are_one_call_per_statement_and_the_next_statement_calls_again(
    statement,
):
    """Three pets feed ``toys``: one call carries all three keys, not three calls. The next
    statement calls again -- ``toys`` has no TTL of its own -- and is answered by its own call."""
    toy_calls: list = []
    respx.route(host="api.test").mock(side_effect=_api(toy_calls))
    state, read = statement
    pets, toys = _endpoint(columns=_endpoint().columns[:2]), _toys()
    state.api_endpoints = {("api", "pets"): pets, ("api", "toys"): toys}
    state.tables = [_registered(pets), _registered(toys, cache_ttl=None)]
    ctx = _pets_feeding_toys()

    first = await read(toys, _registered(toys, cache_ttl=None), None, ctx, hydrate=True)
    assert toy_calls == [["1", "2", "3"]]
    assert first == [(11, "toy of call 1")]
    second = await read(toys, _registered(toys, cache_ttl=None), None, ctx, hydrate=True)
    assert toy_calls == [["1", "2", "3"], ["1", "2", "3"]]
    assert second == [(21, "toy of call 2")]


@respx.mock
async def test_a_raw_sql_read_never_takes_another_statements_fills(statement):
    """A statement planned with no fills of its own (the raw-SQL path runs no hydration) calls
    the endpoint; the fills an earlier statement fetched are never its answer."""
    toy_calls: list = []
    respx.route(host="api.test").mock(side_effect=_api(toy_calls))
    state, read = statement
    pets, toys = _endpoint(columns=_endpoint().columns[:2]), _toys()
    state.api_endpoints = {("api", "pets"): pets, ("api", "toys"): toys}
    state.tables = [_registered(pets), _registered(toys, cache_ttl=None)]
    ctx = _pets_feeding_toys()

    await read(toys, _registered(toys, cache_ttl=None), None, ctx, hydrate=True)
    assert len(toy_calls) == 1
    later = await read(toys, _registered(toys, cache_ttl=None), None, ctx)  # no hydration
    assert len(toy_calls) == 2
    assert later == [(21, "toy of call 2")]


# -- GraphQL-remote ------------------------------------------------------------------------------


def _gql_state():
    from types import SimpleNamespace

    from tests.unit.test_materialization_api import _fake_isolated_sync

    return SimpleNamespace(
        graphql_remote_sources={},
        org_id="default",
        source_cache={"ghsrc": {"cache_ttl": 600}},
        response_cache_default_ttl=300,
        federation_engine=SimpleNamespace(
            materialize_store_dsn=lambda: "postgresql://x/y",
            cache_catalog=lambda: "cat",
            isolated_sync=_fake_isolated_sync,
        ),
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100, max_rows=10000)),
    )


@pytest.mark.parametrize(
    ("columns", "own", "kept"),
    [
        ([{"column_name": "id"}], None, 600),  # no argument: the source's TTL, not a fixed 300
        ([{"column_name": "id"}], 45, 45),
        ([{"column_name": "_nf_breed", "native_filter_type": "graphql_arg"}], 45, 45),
    ],
)
async def test_a_graphql_remote_table_keeps_for_its_resolved_ttl(columns, own, kept):
    from tests.unit.test_materialization_api import RemoteAnswer, _gql_reg, _gql_tbl

    drop = MagicMock()
    registration = {"table_name": "pets", "source_id": "ghsrc", "cache_ttl": own}
    with (
        patch("provisa.api_source.engine_cache.ensure_cache_schema"),
        patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
        patch(
            "provisa.graphql_remote.executor.execute_remote",
            new=AsyncMock(return_value=RemoteAnswer([{"id": 1, "name": "Fido"}])),
        ),
        patch("provisa.api_source.engine_cache.land_api_cache", new=AsyncMock()),
        patch("provisa.api_source.engine_cache.schedule_drop", new=drop),
    ):
        await _mat_gql_remote_table(
            "pets",
            _gql_reg(),
            _gql_tbl(),
            _gql_state(),
            _StatementHot(None, _gql_state(), []),
            500,
            {},
            {},
            registration={**registration, "columns": columns},
        )
    assert drop.call_args.args[3] == kept


async def test_a_graphql_remote_table_with_arguments_and_no_ttl_is_read_per_statement():
    from tests.unit.test_materialization_api import RemoteAnswer, _gql_reg, _gql_tbl

    remote = AsyncMock(return_value=RemoteAnswer([{"id": 1, "name": "Fido"}]))
    land = AsyncMock()
    registration = {
        "table_name": "pets",
        "source_id": "ghsrc",
        "columns": [{"column_name": "_nf_breed", "native_filter_type": "graphql_arg"}],
    }
    with (
        patch("provisa.graphql_remote.executor.execute_remote", new=remote),
        patch("provisa.api_source.engine_cache.land_api_cache", new=land),
    ):
        for _ in range(2):
            inline: dict = {}
            with statement_scratch():
                await _mat_gql_remote_table(
                    "pets",
                    _gql_reg(),
                    _gql_tbl(),
                    _gql_state(),
                    _StatementHot(None, _gql_state(), []),
                    500,
                    {},
                    inline,
                    registration=registration,
                )
            assert inline["pets"].rows == [{"id": 1, "name": "Fido"}]
    assert remote.await_count == 2
    land.assert_not_awaited()

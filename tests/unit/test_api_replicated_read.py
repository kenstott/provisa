# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e51a7-6d02-4b8f-9e14-a7d25c0f8b31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An API table the operator's setting puts on its replica is read from it, never fetched
(REQ-030, REQ-826).

A Wikidata class table is a SPARQL API endpoint the brand registers with replicate Always. Its
reads go to its replica (``replica_routing.table_floor``): the routes publish it as floored, the
read backstop waits on its first build, and routing reads the replica. The API stage must not
fetch it from its endpoint into the TTL'd API cache meanwhile: that read the live service on the
first statement of each shape after a finished build (2 s for LIMIT 25, 21 s for the whole
table). A parameterized table has no whole copy to read and is still fetched; an API table not
on its replica is read through its endpoint as before."""

# Requirements: REQ-030, REQ-826

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.api.data import hydration
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unit

BEER = 52
_READ = 'SELECT * FROM "shelter"."beer" LIMIT 25'


def _beer(*, parameterized: bool = False) -> dict:
    columns = [
        {"column_name": "item", "data_type": "varchar", "native_filter_type": None},
        {"column_name": "label", "data_type": "varchar", "native_filter_type": None},
    ]
    if parameterized:
        columns.append(
            {
                "column_name": "_nf_lang",
                "data_type": "varchar",
                "native_filter_type": "query_param",
                "native_filter_required": False,
                "required_by_steward": False,
            }
        )
    return {
        "id": BEER,
        "source_id": "test",
        "schema_name": "sparql",
        "table_name": "beer",
        "cache_ttl": None,
        "replicate": 0,
        "columns": columns,
    }


def _endpoint() -> ApiEndpoint:
    return ApiEndpoint(
        source_id="test",
        table_name="beer",
        path="",
        columns=[
            ApiColumn(name="item", type=ApiColumnType.string),
            ApiColumn(name="label", type=ApiColumnType.string),
        ],
        response_root="results.bindings",
    )


def _state(*, floored: bool, parameterized: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={("test", "beer"): _endpoint()},
        api_sources={"test": SimpleNamespace(base_url="https://query.wikidata.org/sparql")},
        graphql_remote_sources={},
        source_types={"test": "sparql"},
        tables=[_beer(parameterized=parameterized)],
        relationships=[],
        replica_routes=ReplicaRoutes(floored={BEER: ("test", "replicate")} if floored else {}),
        source_cache={"test": {"cache_ttl": 86400}},
        response_cache_default_ttl=300,
    )


async def _materialize(state) -> tuple[tuple, list[str]]:
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    fetched: list[str] = []

    async def fetch(tn, *args, **kwargs):
        fetched.append(tn)

    with patch("provisa.api.data.materialization._mat_api_ep_table", side_effect=fetch):
        out = await _materialize_api_to_engine_cache(
            _READ, state, table_ids=[BEER], role_id="admin"
        )
    return out, fetched


async def test_a_table_on_its_replica_is_not_fetched_from_its_endpoint():
    (rewrites, ctes, dropped), fetched = await _materialize(_state(floored=True))
    assert fetched == [], "the API stage fetched a table whose reads go to its replica"
    assert (rewrites, ctes, dropped) == ({}, {}, {})


def test_a_table_on_its_replica_leaves_the_api_stage_nothing_to_do():
    from provisa.api.data.materialization import would_materialize_optimize

    assert not would_materialize_optimize(_READ, _state(floored=True), table_ids=[BEER])


async def test_a_parameterized_table_on_its_replica_is_still_fetched():
    _out, fetched = await _materialize(_state(floored=True, parameterized=True))
    assert fetched == ["beer"]


async def test_an_api_table_not_on_its_replica_is_read_through_its_endpoint():
    _out, fetched = await _materialize(_state(floored=False))
    assert fetched == ["beer"]


@pytest.fixture
def fresh_expiry(monkeypatch):
    monkeypatch.setattr(hydration, "_source_hydration_expiry", {})


async def _hydrate(state) -> list[str]:
    filled: list[str] = []

    async def collection(src, endpoint, *args):
        filled.append(endpoint.table_name)

    with patch.object(hydration, "_hydrate_collection", collection):
        await hydration._hydrate_api_tables_before_engine(
            SimpleNamespace(sources={"test"}, api_args={}),
            SimpleNamespace(joins={}, tables={}),
            state,
        )
    return filled


async def test_a_graphql_read_fills_no_table_on_its_replica(fresh_expiry):
    assert await _hydrate(_state(floored=True)) == []


async def test_a_graphql_read_still_fills_an_api_table_not_on_its_replica(fresh_expiry):
    assert await _hydrate(_state(floored=False)) == ["beer"]

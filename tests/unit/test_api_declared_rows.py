# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0e7d21-9c4a-4f3e-a8d6-2e71c40b9f58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An API table whose rows its config declares is read from them, never fetched (REQ-2089).

A cache_ttl -1 OpenAPI table -- a branded SaaS source such as Stripe -- whose source declares
replica rows is answered from the replica those rows landed as (REQ-2031): the API stage neither
fetches it over REST nor fills it for a GraphQL read, and routing reads its replica as it does a
database-backed one's. Such a table that has not landed is refused by name before anything is
sent to the service. An API table that is not kept is read through its endpoint as before."""

# Requirements: REQ-2089, REQ-2031

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from provisa.api.data import hydration
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.federation.replica_address import ReplicaRoutes
from provisa.federation.replica_state import ReplicaNotLanded

pytestmark = pytest.mark.unit

CUSTOMERS = 41
_KEY = ("stripe", "customer360", "stripe_customers")
_READ = 'SELECT COUNT(*) FROM "customer360"."stripe_customers"'


def _customers(cache_ttl: int | None) -> dict:
    return {
        "id": CUSTOMERS,
        "source_id": "stripe",
        "schema_name": "customer360",
        "table_name": "stripe_customers",
        "cache_ttl": cache_ttl,
        "columns": [
            {"column_name": "id", "data_type": "varchar", "native_filter_type": None},
            {"column_name": "email", "data_type": "varchar", "native_filter_type": None},
        ],
    }


def _endpoint() -> ApiEndpoint:
    return ApiEndpoint(
        source_id="stripe",
        table_name="stripe_customers",
        path="/v1/customers",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.string),
            ApiColumn(name="email", type=ApiColumnType.string),
        ],
        response_root="data",
        pk_column="id",
    )


def _state(kept: frozenset, *, table_ttl: int | None = None, source_ttl: int = -1):
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={("stripe", "stripe_customers"): _endpoint()},
        api_sources={"stripe": SimpleNamespace(base_url="https://api.stripe.com")},
        graphql_remote_sources={},
        source_types={"stripe": "openapi"},
        tables=[_customers(table_ttl)],
        relationships=[],
        replica_routes=ReplicaRoutes(kept_for_good=kept),
        source_cache={"stripe": {"cache_ttl": source_ttl}},
        response_cache_default_ttl=300,
    )


@pytest.fixture
def declared(tmp_path: Path):
    """The bound org's config declares rows of source ``stripe`` (REQ-2089)."""
    (tmp_path / "replica_rows" / "stripe").mkdir(parents=True)
    with (
        patch("provisa.api.demo_templates.declared_rows_directory", return_value=tmp_path),
        patch("provisa.core.request_context.require_current_org", return_value="org"),
    ):
        yield tmp_path


@pytest.fixture
def undeclared(tmp_path: Path):
    """The bound org's config declares no replica rows."""
    with (
        patch("provisa.api.demo_templates.declared_rows_directory", return_value=tmp_path),
        patch("provisa.core.request_context.require_current_org", return_value="org"),
    ):
        yield tmp_path


@pytest.fixture
def no_http():
    """Any HTTP request fails the test: the live service is never reached (REQ-2031)."""

    async def refuse(self, request, *args, **kwargs):
        raise AssertionError(f"the live service was called: {request.url}")

    with patch.object(httpx.AsyncClient, "send", refuse):
        yield


async def _materialize(state) -> tuple[tuple, list[str]]:
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    fetched: list[str] = []

    async def fetch(tn, *args, **kwargs):
        fetched.append(tn)

    with patch("provisa.api.data.materialization._mat_api_ep_table", side_effect=fetch):
        out = await _materialize_api_to_engine_cache(
            _READ, state, table_ids=[CUSTOMERS], role_id="analyst"
        )
    return out, fetched


async def test_a_landed_declared_api_table_is_not_fetched(declared, no_http):
    (rewrites, ctes, dropped), fetched = await _materialize(_state(frozenset({_KEY})))
    assert fetched == [], "the API stage fetched a table its declared rows answer"
    assert (rewrites, ctes, dropped) == ({}, {}, {})


def test_a_landed_declared_api_table_leaves_the_api_stage_nothing_to_do(declared):
    from provisa.api.data.materialization import would_materialize_optimize

    assert not would_materialize_optimize(_READ, _state(frozenset({_KEY})), table_ids=[CUSTOMERS])


async def test_a_declared_api_table_not_landed_is_refused_by_name(declared, no_http):
    with pytest.raises(ReplicaNotLanded, match=r"stripe\.customer360\.stripe_customers"):
        await _materialize(_state(frozenset()))


async def test_an_api_table_not_kept_is_still_read_through_its_endpoint(undeclared):
    state = _state(frozenset(), source_ttl=300)
    _out, fetched = await _materialize(state)
    assert fetched == ["stripe_customers"]


async def test_an_undeclared_never_expiring_api_table_is_still_read_through_its_endpoint(
    undeclared,
):
    _out, fetched = await _materialize(_state(frozenset()))
    assert fetched == ["stripe_customers"]


@pytest.fixture
def fresh_expiry(monkeypatch):
    monkeypatch.setattr(hydration, "_source_hydration_expiry", {})


async def _hydrate(state) -> list[str]:
    filled: list[str] = []

    async def collection(src, endpoint, *args):
        filled.append(endpoint.table_name)

    with patch.object(hydration, "_hydrate_collection", collection):
        await hydration._hydrate_api_tables_before_engine(
            SimpleNamespace(sources={"stripe"}, api_args={}),
            SimpleNamespace(joins={}, tables={}),
            state,
        )
    return filled


async def test_a_graphql_read_fills_no_landed_declared_api_table(declared, fresh_expiry, no_http):
    assert await _hydrate(_state(frozenset({_KEY}))) == []


async def test_a_graphql_read_of_a_declared_api_table_not_landed_is_refused_by_name(
    declared, fresh_expiry, no_http
):
    with pytest.raises(ReplicaNotLanded, match=r"stripe\.customer360\.stripe_customers"):
        await _hydrate(_state(frozenset()))


async def test_a_graphql_read_still_fills_an_api_table_not_kept(undeclared, fresh_expiry):
    assert await _hydrate(_state(frozenset(), source_ttl=300)) == ["stripe_customers"]


def _with_parameters(state) -> SimpleNamespace:
    """``state`` with the customers table taking a required ``email`` query parameter and an
    optional ``created`` one, as an OpenAPI list endpoint registers them."""
    state.tables[0]["columns"] += [
        {
            "column_name": "_nf_email",
            "data_type": "varchar",
            "native_filter_type": "query_param",
            "native_filter_required": True,
            "required_by_steward": False,
        },
        {
            "column_name": "_nf_created",
            "data_type": "varchar",
            "native_filter_type": "query_param",
            "native_filter_required": False,
            "required_by_steward": False,
        },
    ]
    return state


async def test_a_landed_declared_api_table_needs_no_parameter(declared, no_http):
    state = _with_parameters(_state(frozenset({_KEY})))
    (rewrites, ctes, dropped), fetched = await _materialize(state)
    assert fetched == [] and (rewrites, ctes, dropped) == ({}, {}, {})


def test_each_parameter_of_a_landed_api_table_filters_its_landed_column():
    from provisa.api.data.materialization import frozen_parameter_filters

    kept = _with_parameters(_state(frozenset({_KEY})))
    assert frozen_parameter_filters(kept, [CUSTOMERS]) == {
        "_nf_email": "email",
        "_nf_created": None,
    }
    assert frozen_parameter_filters(_with_parameters(_state(frozenset())), [CUSTOMERS]) == {}

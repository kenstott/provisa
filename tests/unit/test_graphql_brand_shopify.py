# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f61a2-4c0e-4b7a-9e15-2a6c7f9b0d48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shopify's Admin API as a branded source carried by the remote GraphQL source (REQ-1923).

These run against the schema that ships with Provisa. The store is mocked with answers shaped as
Shopify's are -- its cost refusal is the one shopify.dev's public Admin API proxy returned for a
query priced at 1,850 points. What a real store accepts is checked against a dev store, which no
unit test can do.
"""

import io
import json
from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace

import httpx
import pytest
import respx
from graphql import parse

import provisa.api.app  # noqa: F401  -- says at import whether the deployment holds many orgs
from provisa.api.admin import _graphql_table_registration as registration
from provisa.api.errors import ApiError
from provisa.core.models import Column
from provisa.graphql_remote import executor
from provisa.graphql_remote.brands import (
    BRANDS,
    SHOPIFY_API_VERSION,
    SetupInvalid,
    available_tables,
    brand_schema,
    map_table,
)
from provisa.graphql_remote.executor import (
    Throttled,
    column_selection,
    execute_remote,
    iter_remote_rows_spooled,
    table_query,
    whole_connection,
)
from provisa.graphql_remote.introspect import _build_headers
from provisa.graphql_remote.mapper import map_schema
from provisa.graphql_remote.probe import QueryTooComplex

SHOPIFY = BRANDS["shopify"]
STORE_URL = f"https://acme.myshopify.com/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

# Shopify's answer to a query priced over its single-query limit, as its Admin API returns it.
COST_REFUSAL = {
    "message": (
        "Query cost is 1850, which exceeds the single query max cost limit (1000).\n\n"
        "See https://shopify.dev/docs/api/usage/rate-limits for more information on how the\n"
        "cost of a query is calculated.\n"
    ),
    "extensions": {
        "code": "MAX_COST_EXCEEDED",
        "cost": 1850,
        "maxCost": 1000,
        "documentation": "https://shopify.dev/api/usage/rate-limits",
    },
}


def _throttled(requested: float, available: float, rate: float = 100.0) -> dict:
    """Shopify's answer when the store's bucket of query points has too little left."""
    return {
        "errors": [
            {
                "message": "Throttled",
                "extensions": {
                    "code": "THROTTLED",
                    "documentation": "https://shopify.dev/api/usage/rate-limits",
                },
            }
        ],
        "extensions": {
            "cost": {
                "requestedQueryCost": requested,
                "actualQueryCost": None,
                "throttleStatus": {
                    "maximumAvailable": 2000.0,
                    "currentlyAvailable": available,
                    "restoreRate": rate,
                },
            }
        },
    }


def _reg(**over) -> dict:
    return {
        "source_id": "my-shop",
        "url": STORE_URL,
        "namespace": "shopify",
        "auth": SHOPIFY.auth("shpat_x"),
        "brand": "shopify",
        "tables": [],
        **over,
    }


@pytest.fixture
def slept(monkeypatch) -> list[float]:
    """The waits a read took, without taking them."""
    waits: list[float] = []

    async def asleep(seconds: float) -> None:
        waits.append(seconds)

    # Only the executor's own waits: httpx yields with asyncio.sleep(0) as it reads.
    monkeypatch.setattr(executor, "asyncio", SimpleNamespace(sleep=asleep))
    monkeypatch.setattr(executor, "time", SimpleNamespace(sleep=waits.append))
    return waits


# --- the brand and its setup ---


def test_shopify_asks_for_the_store_and_defaults_the_api_version():
    assert [f.name for f in SHOPIFY.setup_fields] == ["store", "api_version"]
    assert SHOPIFY.setup({"store": "acme"}) == {"store": "acme", "api_version": SHOPIFY_API_VERSION}
    assert SHOPIFY.endpoint(SHOPIFY.setup({"store": "acme"})) == STORE_URL
    assert (
        SHOPIFY.endpoint(SHOPIFY.setup({"store": "acme", "api_version": "2026-07"}))
        == "https://acme.myshopify.com/admin/api/2026-07/graphql.json"
    )


@pytest.mark.parametrize(
    "given, field",
    [
        ({}, "store"),
        ({"store": "  "}, "store"),
        # Only a store handle is taken, so the endpoint is always a store's on myshopify.com.
        ({"store": "evil.example.com/x?"}, "store"),
        ({"store": "Acme"}, "store"),
        ({"store": "acme", "api_version": "latest"}, "api_version"),
        ({"store": "acme", "region": "eu"}, "region"),
    ],
)
def test_a_setup_value_shopify_does_not_take_is_refused_by_name(given, field):
    with pytest.raises(SetupInvalid) as refused:
        SHOPIFY.setup(given)
    assert refused.value.field == field


def test_the_token_is_sent_bare_in_shopifys_own_header():
    assert _build_headers(SHOPIFY.auth("shpat_x")) == {"X-Shopify-Access-Token": "shpat_x"}
    assert _build_headers(BRANDS["github"].auth("t")) == {"Authorization": "Bearer t"}


def test_the_shipped_schema_is_the_admin_api_at_the_pinned_version():
    names = {t["sql_name"] for t in available_tables(SHOPIFY, "shopify")}
    assert {"shopify__products", "shopify__orders", "shopify__customers", "shopify__shop"} <= names
    assert {"shopify__product_variants", "shopify__inventory_items"} <= names
    assert SHOPIFY.introspection_url.endswith(f"/admin-graphql-direct-proxy/{SHOPIFY_API_VERSION}")


def test_every_offered_shopify_table_maps_to_a_query_that_parses():
    tables, _, _ = map_schema(brand_schema("shopify"), "shopify", "my-shop", "", max_object_depth=0)
    assert {t["sql_name"] for t in tables} == {
        t["sql_name"] for t in available_tables(SHOPIFY, "shopify")
    }
    for table in tables:
        assert table["columns"], table["sql_name"]
        parse(table_query(table, [column_selection(c) for c in table["columns"]]))


def test_a_root_field_with_nothing_but_connections_is_not_a_table_but_its_connections_are():
    names = {t["sql_name"] for t in available_tables(SHOPIFY, "shopify")}
    assert "shopify__taxonomy" not in names
    assert "shopify__taxonomy_categories" in names


def test_products_are_a_relay_connection_read_by_cursor():
    table = map_table(SHOPIFY, "shopify", "my-shop", "", "shopify__products")
    assert table["rows_path"] == ["nodes"]
    assert table["required_args"] == []
    query = table_query(table, ["id", "title"], 100)
    assert "products(first: 100, after: $pageCursor)" in query
    assert "pageInfo { hasNextPage endCursor }" in query


# --- adding the source ---


class _Conn:
    def __init__(self):
        self.rows: list[dict] = []

    async def upsert(self, table, values, **_):
        self.rows.append(values)


@pytest.fixture
def plane(monkeypatch):
    """The process state adding a source writes to, with the sources row it upserts."""
    import provisa.api.app as app
    import provisa.core.repositories.source as source_repo

    conn = _Conn()

    @asynccontextmanager
    async def acquire():
        yield conn

    async def no_rebuild():
        return None

    async def as_given(_conn, values):
        return values

    state = SimpleNamespace(
        model_db=SimpleNamespace(acquire=acquire), graphql_remote_sources={}, multitenancy=False
    )
    monkeypatch.setattr(app, "state", state)
    monkeypatch.setattr(app, "_rebuild_schemas", no_rebuild)
    monkeypatch.setattr(source_repo, "owned_if_reconnected", as_given)
    return SimpleNamespace(state=state, conn=conn)


def _add_request(**over):
    from provisa.api.admin.graphql_remote_router import GraphQLRemoteSourceRequest

    body = {
        "source_id": "my-shop",
        "brand": "shopify",
        "setup": {"store": "acme"},
        "auth": {"type": "bearer", "token": "shpat_x"},
        **over,
    }
    return GraphQLRemoteSourceRequest(**body)


@respx.mock
async def test_adding_a_shopify_source_checks_the_token_at_the_store_and_registers_nothing(plane):
    from provisa.api.admin.graphql_remote_router import _register_branded_source

    route = respx.post(STORE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"shop": {"name": "Acme"}}})
    )
    added = await _register_branded_source(None, _add_request())  # type: ignore[arg-type]

    sent = route.calls[0].request
    assert sent.headers["x-shopify-access-token"] == "shpat_x"
    assert "authorization" not in sent.headers
    assert json.loads(sent.content)["query"] == SHOPIFY.verify_query
    assert added["brand"] == "shopify" and added["table_names"] == []
    reg = plane.state.graphql_remote_sources["my-shop"]
    assert reg["url"] == STORE_URL and reg["tables"] == []
    assert reg["setup"] == {"store": "acme", "api_version": SHOPIFY_API_VERSION}
    assert reg["error_policy"] is SHOPIFY.error_policy
    (row,) = plane.conn.rows
    assert row["path"] == STORE_URL and row["password_ref"] == "shpat_x"
    assert row["federation_hints"] == {
        "namespace": "shopify",
        "auth_type": "header",
        "auth_header": "X-Shopify-Access-Token",
        "brand": "shopify",
    }
    assert row["brand_setup"] == {"store": "acme", "api_version": SHOPIFY_API_VERSION}


@respx.mock
async def test_a_store_name_shopify_does_not_take_is_refused_before_anything_is_sent(plane):
    from provisa.api.admin.graphql_remote_router import _register_branded_source

    route = respx.route().mock(return_value=httpx.Response(500))
    with pytest.raises(ApiError) as refused:
        await _register_branded_source(
            None,  # type: ignore[arg-type]
            _add_request(setup={"store": "acme.evil.example"}),
        )
    assert refused.value.code == "graphql_remote.setup_invalid"
    assert refused.value.params["field"] == "store"
    assert not route.called and not plane.conn.rows


@respx.mock
async def test_a_token_the_store_rejects_is_not_added(plane):
    from provisa.api.admin.graphql_remote_router import _register_branded_source

    respx.post(STORE_URL).mock(
        return_value=httpx.Response(
            401, json={"errors": "[API] Invalid API key or access token (unrecognized login)"}
        )
    )
    with pytest.raises(ApiError) as refused:
        await _register_branded_source(None, _add_request())  # type: ignore[arg-type]
    assert refused.value.code == "graphql_remote.credential_rejected"
    assert not plane.conn.rows and "my-shop" not in plane.state.graphql_remote_sources


async def test_the_token_comes_back_in_shopifys_header_after_a_restart(monkeypatch):
    from provisa.api.app_loaders import _graphql_remote_auth

    monkeypatch.setenv("SHOP_TOKEN", "from-env")
    state = SimpleNamespace(active_org_id="org", admin_db=None)
    hints = {"brand": "shopify", "auth_type": "header", "auth_header": "X-Shopify-Access-Token"}
    row = {"id": "my-shop", "username": "", "password_ref": "${env:SHOP_TOKEN}"}
    assert await _graphql_remote_auth(state, {**row, "federation_hints": hints}) == {
        "type": "header",
        "header": "X-Shopify-Access-Token",
        "token": "from-env",
    }
    with pytest.raises(ValueError, match="names no auth_header"):
        await _graphql_remote_auth(state, {**row, "federation_hints": {"auth_type": "header"}})


def test_the_header_name_leaves_the_server_and_the_token_does_not():
    from provisa.api.admin.graphql_remote_router import _auth_without_secret

    assert _auth_without_secret(SHOPIFY.auth("shpat_x")) == {
        "type": "header",
        "header": "X-Shopify-Access-Token",
    }


# --- registering a table: Shopify prices the columns chosen ---


@respx.mock
async def test_the_columns_picked_are_priced_by_the_store_at_the_read_page_size():
    route = respx.post(STORE_URL).mock(
        return_value=httpx.Response(200, json={"data": {"products": {"nodes": []}}})
    )
    chosen = [Column(name="id", visible_to=["analyst"]), Column(name="title", visible_to=[])]
    columns, omitted, _ = await registration.columns_to_register(
        SHOPIFY, _reg(), "shopify__products", "eng", chosen, 100
    )
    assert omitted == []
    assert {c.name for c in columns if c.native_filter_type is None} == {"id", "title"}
    sent = route.calls[0].request
    assert sent.headers["x-shopify-access-token"] == "shpat_x"
    query = json.loads(sent.content)["query"]
    assert "products(first: 100, after: $pageCursor)" in query
    assert "nodes { id title }" in query


@respx.mock
async def test_a_table_shopify_prices_over_its_limit_is_refused_with_shopifys_numbers():
    respx.post(STORE_URL).mock(return_value=httpx.Response(200, json={"errors": [COST_REFUSAL]}))
    with pytest.raises(QueryTooComplex) as refused:
        await registration.columns_to_register(SHOPIFY, _reg(), "shopify__products", "eng", [], 100)
    assert refused.value.table == "shopify__products"
    assert "Query cost is 1850" in refused.value.reason
    assert "max cost limit (1000)" in refused.value.reason


@respx.mock
async def test_the_registration_names_shopify_and_its_numbers_and_registers_nothing(monkeypatch):
    import provisa.api.app as app
    from provisa.api.admin._table_ops import _graphql_columns_for_input

    state = SimpleNamespace(
        graphql_remote_sources={"my-shop": _reg()},
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_list_items=100)),
        multitenancy=False,
    )
    monkeypatch.setattr(app, "state", state)
    respx.post(STORE_URL).mock(return_value=httpx.Response(200, json={"errors": [COST_REFUSAL]}))
    request = SimpleNamespace(
        source_id="my-shop", schema_name="graphql", table_name="shopify__orders", domain_id="eng"
    )
    answered = await _graphql_columns_for_input(request, [])
    assert answered is not None
    columns, refusal = answered
    assert columns == [] and refusal is not None and refusal.success is False
    assert refusal.code == "schema.table_too_complex"
    assert refusal.message.startswith("Shopify will not run shopify__orders")
    assert "1850" in refusal.message and "(1000)" in refusal.message
    assert state.graphql_remote_sources["my-shop"]["tables"] == []


@respx.mock
async def test_a_field_outside_the_apps_access_scopes_is_left_out_and_named():
    def answer(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)["query"]
        assert "\n" not in query  # one line, so a column locates the field
        if "customer {" not in query:
            return httpx.Response(200, json={"data": {"orders": {"nodes": []}}})
        denied = {
            "message": (
                "Access denied for customer field. Required access: `read_customers` access scope."
            ),
            "locations": [{"line": 1, "column": query.index("customer {") + 1}],
            "path": ["orders", "nodes", 0, "customer"],
            "extensions": {"code": "ACCESS_DENIED"},
        }
        return httpx.Response(200, json={"data": None, "errors": [denied]})

    respx.post(STORE_URL).mock(side_effect=answer)
    chosen = [Column(name="name", visible_to=[]), Column(name="customer", visible_to=[])]
    columns, omitted, _ = await registration.columns_to_register(
        SHOPIFY, _reg(), "shopify__orders", "eng", chosen, 100
    )
    assert [o["field"] for o in omitted] == ["customer"]
    assert "read_customers" in omitted[0]["reason"]
    assert {c.name for c in columns if c.native_filter_type is None} == {"name"}


@respx.mock
async def test_a_registration_the_store_throttles_waits_and_prices_the_real_answer(slept):
    respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json=_throttled(requested=202, available=124)),
            httpx.Response(200, json={"errors": [COST_REFUSAL]}),
        ]
    )
    with pytest.raises(QueryTooComplex, match="Query cost is 1850"):
        await registration.columns_to_register(SHOPIFY, _reg(), "shopify__products", "eng", [], 100)
    assert slept == [pytest.approx(0.78, rel=1e-14)]


# --- reading ---


def _page(rows: list[dict], cursor: str | None) -> dict:
    return {
        "data": {
            "products": {
                "nodes": rows,
                "pageInfo": {"hasNextPage": cursor is not None, "endCursor": cursor},
            }
        },
        "extensions": {
            "cost": {
                "requestedQueryCost": 3,
                "actualQueryCost": 3,
                "throttleStatus": {
                    "maximumAvailable": 2000.0,
                    "currentlyAvailable": 1997,
                    "restoreRate": 100.0,
                },
            }
        },
    }


async def _whole_products(url: str = STORE_URL) -> list[dict]:
    table = map_table(SHOPIFY, "shopify", "my-shop", "", "shopify__products")
    rows: list[dict] = []
    async for page in whole_connection(
        url,
        SHOPIFY.auth("shpat_x"),
        table["field_name"],
        ["id", "title"],
        table["rows_path"],
        table="my-shop.shopify__products",
        page_size=100,
        max_rows=None,
        error_policy=SHOPIFY.error_policy,
    ):
        rows.extend(page)
    return rows


@respx.mock
async def test_a_table_is_read_a_page_at_a_time_following_shopifys_cursor():
    route = respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(
                200, json=_page([{"id": "gid://shopify/Product/1", "title": "A"}], "c1")
            ),
            httpx.Response(
                200, json=_page([{"id": "gid://shopify/Product/2", "title": "B"}], None)
            ),
        ]
    )
    assert [r["title"] for r in await _whole_products()] == ["A", "B"]
    cursors = [json.loads(c.request.content)["variables"]["pageCursor"] for c in route.calls]
    assert cursors == [None, "c1"]
    assert all(c.request.headers["x-shopify-access-token"] == "shpat_x" for c in route.calls)


@respx.mock
async def test_a_table_lands_at_the_page_size_its_registration_priced(monkeypatch):
    """Shopify prices a page by its size: a table accepted at registration at
    graphql_remote.max_list_items is landed at that size, never a larger one it was not priced at."""
    import provisa.api.app as app
    from provisa.api.admin._table_ops import _graphql_columns_for_input
    from provisa.events.app_wiring import build_adapter_loaders

    reg = _reg()
    state = SimpleNamespace(
        graphql_remote_sources={"my-shop": reg},
        config=SimpleNamespace(
            graphql_remote=SimpleNamespace(max_list_items=25, max_rows=1000), sources=[]
        ),
        multitenancy=False,
    )
    monkeypatch.setattr(app, "state", state)
    route = respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json=_page([], None)),  # the registration's pricing
            httpx.Response(200, json=_page([{"id": "1", "title": "A"}], "c1")),
            httpx.Response(200, json=_page([{"id": "2", "title": "B"}], None)),
        ]
    )
    request = SimpleNamespace(
        source_id="my-shop", schema_name="graphql", table_name="shopify__products", domain_id="eng"
    )
    answered = await _graphql_columns_for_input(request, [])
    assert answered is not None and answered[1] is None  # registered

    land = build_adapter_loaders(state, SimpleNamespace(engine=SimpleNamespace(connectors={})))
    rows = await land["graphql_remote"](
        SimpleNamespace(id="my-shop"),
        SimpleNamespace(table_name="shopify__products", pagination=None),
    )
    assert [r["title"] for r in rows] == ["A", "B"]
    priced, *landed = [json.loads(c.request.content)["query"] for c in route.calls]
    page = "products(first: 25, after: $pageCursor)"
    assert page in priced
    assert len(landed) == 2 and all(page in q for q in landed)


@respx.mock
async def test_a_throttled_page_waits_until_the_bucket_has_restored_what_it_costs(slept):
    route = respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json=_page([{"id": "1", "title": "A"}], "c1")),
            httpx.Response(200, json=_throttled(requested=202, available=124, rate=50)),
            httpx.Response(200, json=_page([{"id": "2", "title": "B"}], None)),
        ]
    )
    assert [r["title"] for r in await _whole_products()] == ["A", "B"]
    # (202 - 124) points at 50 a second.
    assert slept == [pytest.approx(1.56, rel=1e-14)]
    cursors = [json.loads(c.request.content)["variables"]["pageCursor"] for c in route.calls]
    assert cursors == [None, "c1", "c1"]  # the throttled page is asked for again, not skipped


@respx.mock
async def test_a_throttle_asking_for_longer_than_a_read_waits_fails_the_read_by_name(slept):
    # 2,000 points short at 10 a second: 200s, past the longest wait a read takes.
    respx.post(STORE_URL).mock(
        return_value=httpx.Response(200, json=_throttled(requested=2000, available=0, rate=10))
    )
    with pytest.raises(Throttled, match="THROTTLED") as refused:
        await _whole_products()
    assert "restoreRate" in refused.value.reason
    assert slept == []


@respx.mock
async def test_a_store_still_throttled_after_every_wait_fails_the_read(slept):
    route = respx.post(STORE_URL).mock(
        return_value=httpx.Response(200, json=_throttled(requested=150, available=50))
    )
    with pytest.raises(Throttled):
        await _whole_products()
    assert len(slept) == executor._RETRY_AFTER_ATTEMPTS
    assert route.call_count == executor._RETRY_AFTER_ATTEMPTS + 1


@respx.mock
async def test_a_throttled_answer_that_states_no_cost_fails_the_read(slept):
    answer = _throttled(requested=1, available=0)
    del answer["extensions"]
    respx.post(STORE_URL).mock(return_value=httpx.Response(200, json=answer))
    with pytest.raises(Throttled, match="states no wait"):
        await _whole_products()
    assert slept == []


@respx.mock
async def test_a_request_read_of_a_single_object_honours_the_throttle_too(slept):
    respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json=_throttled(requested=10, available=0)),
            httpx.Response(200, json={"data": {"shop": {"name": "Acme"}}}),
        ]
    )
    answer = await execute_remote(
        STORE_URL,
        SHOPIFY.auth("shpat_x"),
        "shop",
        ["name"],
        error_policy=SHOPIFY.error_policy,
    )
    assert answer.rows == [{"name": "Acme"}]
    assert slept == [pytest.approx(0.1, rel=1e-14)]


@contextmanager
def _spooled(open_response):
    """A spool held in memory: the answer, read whole, as the file a spooled read parses."""
    with open_response() as resp:
        yield io.BytesIO(resp.read())


@respx.mock
def test_a_replica_build_of_a_single_object_honours_the_throttle_too(slept):
    respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json=_throttled(requested=10, available=5, rate=100)),
            httpx.Response(200, json={"data": {"shop": {"name": "Acme"}}}),
        ]
    )
    rows = list(
        iter_remote_rows_spooled(
            STORE_URL,
            SHOPIFY.auth("shpat_x"),
            "shop",
            ["name"],
            _spooled,
            SHOPIFY.error_policy,
        )
    )
    assert rows == [{"name": "Acme"}]
    assert slept == [pytest.approx(0.05, rel=1e-14)]


@respx.mock
async def test_a_page_shopify_prices_over_its_limit_is_asked_for_at_half_the_size():
    route = respx.post(STORE_URL).mock(
        side_effect=[
            httpx.Response(200, json={"errors": [COST_REFUSAL]}),
            httpx.Response(200, json=_page([{"id": "1", "title": "A"}], None)),
        ]
    )
    table = map_table(SHOPIFY, "shopify", "my-shop", "", "shopify__products")
    answer = await execute_remote(
        STORE_URL,
        SHOPIFY.auth("shpat_x"),
        table["field_name"],
        ["id", "title"],
        rows_path=table["rows_path"],
        limit=100,
        error_policy=SHOPIFY.error_policy,
    )
    assert [r["title"] for r in answer.rows] == ["A"]
    sent = [json.loads(c.request.content)["query"] for c in route.calls]
    assert "first: 100" in sent[0] and "first: 50" in sent[1]


def test_shopifys_error_codes_are_read_where_the_graphql_spec_puts_them():
    policy = SHOPIFY.error_policy
    assert policy.overloaded_by([COST_REFUSAL]) == ["MAX_COST_EXCEEDED"]
    assert policy.throttled_by(_throttled(1, 0)["errors"])
    assert not policy.throttled_by([COST_REFUSAL])
    assert not BRANDS["github"].error_policy.throttled_by(_throttled(1, 0)["errors"])

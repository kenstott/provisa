# Copyright (c) 2026 Kenneth Stott
# Canary: c093f562-218a-45ce-ae7d-99a51366309c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-316/REQ-318: every registered OpenAPI table is read through the one paged caller.

There were two read paths for one source kind. A table declared in the config was served from
its ``api_endpoints`` row by ``api_source.caller`` (paging, max_pages, the answer cut). A table
registered in the admin had no such row and was read by a second executor with no paging at
all: a paged collection was served from its first response as if it were the whole table, and a
replica build of it had no endpoint to read. Both origins now derive the same row with the same
function, and the endpoint goes with its table."""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
import respx
from sqlalchemy import insert, select

from provisa.api_source.caller import answer_rows, call_api
from provisa.core.models import Column, Table
from provisa.core.paging import PaginationConfig
from provisa.core.schema_org import api_endpoints, api_sources, domains, registered_tables, sources

BASE = "https://pets.test"
SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer"}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer"}},
                ],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "id": {"type": "integer"},
                                            "name": {"type": "string"},
                                        },
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}
COLUMNS = [("id", "bigint"), ("name", "text")]


def _table(max_pages: int) -> Table:
    return Table(
        source_id="petstore",
        domain_id="d",
        schema_name="openapi",
        table_name="listPets",
        columns=[
            Column(name="id", data_type="integer", visible_to=["admin"]),
            Column(name="name", data_type="text", visible_to=["admin"]),
        ],
        pagination=PaginationConfig(type="offset", page_size=2, max_pages=max_pages),
    )


@pytest.fixture
async def control_plane(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    await init_schema(db, "", org_id="default")
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="petstore", type="openapi"))
    try:
        yield db
    finally:
        engine.dispose()


def _state():
    return SimpleNamespace(
        openapi_specs={"petstore": {"spec": SPEC, "base_url": BASE, "auth_config": None}},
        api_endpoints={},
        api_sources={},
    )


async def _register_in_the_admin(db, state, table: Table) -> None:
    """What the admin's register_table does for an OpenAPI table after its row lands, and what
    the next schema rebuild loads."""
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import register_openapi_source
    from provisa.core.repositories import table as table_repo

    async with db.acquire() as conn:
        await register_openapi_source(conn, "petstore", BASE)  # the source's registration
        await table_repo.upsert(conn, table)
        assert await persist_openapi_endpoint(state, conn, table) is None
        state.api_endpoints, state.api_sources = await load_api_sources(conn, {})


def _three_pages():
    pages = {0: [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], 2: [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}], 4: [{"id": 5, "name": "e"}]}  # fmt: skip
    return respx.get(f"{BASE}/pets").mock(
        side_effect=lambda request: httpx.Response(
            200, json=pages[int(request.url.params["offset"])]
        )
    )


@respx.mock
async def test_an_admin_registered_table_behind_a_three_page_collection_returns_every_page(
    control_plane,
):
    state = _state()
    await _register_in_the_admin(control_plane, state, _table(max_pages=10))
    route = _three_pages()
    endpoint = state.api_endpoints["listPets"]
    rows, cut = answer_rows(
        endpoint, await call_api(endpoint, {}, state.api_sources["petstore"].base_url)
    )
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5] and cut is None
    assert route.call_count == 3


@respx.mock
async def test_with_max_pages_two_the_answer_is_cut_and_says_so(control_plane):
    from provisa.api_source.caller import answer_cut_warning

    state = _state()
    await _register_in_the_admin(control_plane, state, _table(max_pages=2))
    _three_pages()
    endpoint = state.api_endpoints["listPets"]
    rows, cut = answer_rows(endpoint, await call_api(endpoint, {}, BASE))
    assert [r["id"] for r in rows] == [1, 2, 3, 4]
    warning = answer_cut_warning("listPets", cut)
    assert warning.code == "api.answer_cut"
    assert warning.params == {"table": "listPets", "max_pages": 2, "rows": 4}


@respx.mock
async def test_a_replica_build_of_it_fails_by_name_at_max_pages(control_plane):
    from provisa.api_source.replica_read import PageLimitReached
    from provisa.events.source_loader import make_openapi_loader

    state = _state()
    loader = make_openapi_loader(state)  # wired before the table exists
    await _register_in_the_admin(control_plane, state, _table(max_pages=2))
    _three_pages()
    source = SimpleNamespace(id="petstore")
    table = SimpleNamespace(schema_name="openapi", table_name="listPets", columns=[])
    reader = loader.replica_source(source, table, COLUMNS)
    with pytest.raises(PageLimitReached) as failed:
        async for _batch in reader.batches(100):
            pass
    assert failed.value.code == "replication.page_limit_reached"
    assert failed.value.params["max_pages"] == 2


async def test_the_config_and_the_admin_derive_the_identical_endpoint_row(control_plane):
    """One function: a table declared in the config and the same table registered in the admin
    are served from the same row."""
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.core.config_loader import _handle_openapi_table
    from provisa.core.repositories import table as table_repo

    async def _row(conn) -> dict:
        row = (await conn.execute_core(select(api_endpoints))).fetchone()
        return {k: v for k, v in dict(row._mapping).items() if k not in ("id", "created_at")}

    table = _table(max_pages=2)
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, table)
        src = SimpleNamespace(id="petstore", base_url=BASE, cache_ttl=None)
        await _handle_openapi_table(conn, table, src, SPEC)  # type: ignore[arg-type]
        from_config = await _row(conn)
        await conn.execute_core(api_endpoints.delete())
        assert await persist_openapi_endpoint(_state(), conn, table) is None
        assert await _row(conn) == from_config
        assert from_config["pagination"] == {"type": "offset", "page_size": 2, "max_pages": 2}


async def test_a_table_with_no_operation_of_its_name_is_refused(control_plane):
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint

    table = _table(max_pages=2).model_copy(update={"table_name": "listCats"})
    async with control_plane.acquire() as conn:
        refused = await persist_openapi_endpoint(_state(), conn, table)
    assert (refused.success, refused.code) == (False, "schema.openapi_no_operation")


async def test_the_endpoint_goes_with_its_table(control_plane):
    """An endpoint is derived from a registration: deleting the table removes it, so nothing is
    left that could be read without a registered table."""
    from provisa.core.repositories import table as table_repo

    state = _state()
    await _register_in_the_admin(control_plane, state, _table(max_pages=2))
    async with control_plane.acquire() as conn:
        table_id = (await conn.execute_core(select(registered_tables.c.id))).scalar_one()
        assert await table_repo.delete(conn, table_id) is True
        assert (await conn.execute_core(select(api_endpoints.c.id))).fetchall() == []


async def test_the_sources_auth_is_stored_and_reaches_the_caller_and_a_refresh_keeps_it(
    control_plane,
):
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import (
        api_auth,
        register_openapi_source,
        store_openapi_auth,
    )
    from provisa.core.auth_models import ApiAuthApiKey

    auth = api_auth({"type": "api_key", "header_name": "X-Key", "api_key": "${secret:PETS_KEY}"})
    async with control_plane.acquire() as conn:
        await register_openapi_source(conn, "petstore", BASE)
        await store_openapi_auth(conn, "petstore", auth)
        await register_openapi_source(conn, "petstore", BASE + "/v2")  # a refresh: auth is kept
        stored = (await conn.execute_core(select(api_sources.c.auth))).scalar_one()
        _endpoints, loaded = await load_api_sources(conn, {})
    assert stored is not None  # written through the encryption service (REQ-686)
    assert loaded["petstore"].base_url == BASE + "/v2"
    assert loaded["petstore"].auth == ApiAuthApiKey(key="${secret:PETS_KEY}", name="X-Key")


# -- the offer: paging suggested from the spec (REQ-318) --------------------------------------------


def _operation(params: list[str], *, is_list: bool = True, link: bool = False):
    from jsonschema_path import SchemaPath

    from provisa.openapi.mapper import propose_paging

    operation = SchemaPath.from_dict(
        {"responses": {"200": {"headers": {"Link": {}} if link else {}}}}
    )
    return propose_paging(operation, [{"name": p, "type": "integer"} for p in params], is_list)


def test_an_operation_with_offset_and_limit_is_offered_offset_paging():
    from provisa.openapi.mapper import parse_spec

    (query,), _ = parse_spec(SPEC)
    assert query.pagination == PaginationConfig(
        type="offset", page_param="offset", page_size_param="limit"
    )
    # Only what the spec declares: the page size and max pages stay the steward's.
    assert query.pagination.model_fields_set == {"type", "page_param", "page_size_param"}


@pytest.mark.parametrize(
    ("params", "link", "expected"),
    [
        (["page", "per_page"], False, {"type": "page_number", "page_param": "page", "page_size_param": "per_page"}),
        (["pageNumber"], False, {"type": "page_number", "page_param": "pageNumber"}),
        (["skip", "top"], False, {"type": "offset", "page_param": "skip", "page_size_param": "top"}),
        ([], True, {"type": "link_header"}),
        (["offset"], False, None),  # an offset with no size parameter is not a paging scheme
        (["status"], False, None),
    ],
)  # fmt: skip
def test_what_an_operation_declares_decides_the_paging_offered(params, link, expected):
    proposed = _operation(params, link=link)
    assert proposed == (None if expected is None else PaginationConfig(**expected))


def test_a_single_object_response_is_offered_no_paging():
    assert _operation(["page", "per_page"], is_list=False) is None


async def test_the_offered_table_carries_the_suggested_paging_for_the_steward():
    from provisa.api.admin.introspect import _native_tables_openapi

    (offered,) = await _native_tables_openapi("petstore", "openapi", _state())
    assert offered.name == "listPets" and offered.paging_kind == "endpoint"
    assert (offered.pagination.type, offered.pagination.page_param) == ("offset", "offset")
    assert offered.pagination.max_pages is None  # not declared by the spec


def test_the_stewards_paging_at_registration_is_checked_like_any_other():
    from provisa.api.admin._table_paging import declared_paging
    from provisa.api.admin.types import PagingInput

    state = SimpleNamespace(config=SimpleNamespace(graphql_remote=SimpleNamespace(max_rows=100)))
    accepted = declared_paging(
        state, "openapi", "petstore", "listPets", PagingInput(type="offset", max_pages=3)
    )
    assert accepted == PaginationConfig(type="offset", max_pages=3)
    assert declared_paging(state, "openapi", "petstore", "listPets", PagingInput()) is None
    refused = declared_paging(state, "postgresql", "pg", "orders", PagingInput(type="offset"))
    assert (refused.success, refused.code) == (False, "schema.paging_not_paged")


# -- a page wrapper: the rows are read where the table's paging says (REQ-316) ----------------------

WRAPPED_SPEC = {
    "openapi": "3.0.0",
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "parameters": SPEC["paths"]["/pets"]["get"]["parameters"],
                "responses": {
                    "200": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "total": {"type": "integer"},
                                        "values": SPEC["paths"]["/pets"]["get"]["responses"]["200"][
                                            "content"
                                        ]["application/json"]["schema"],
                                    },
                                }
                            }
                        }
                    }
                },
            }
        }
    },
}


def _wrapped_state():
    state = _state()
    state.openapi_specs["petstore"]["spec"] = WRAPPED_SPEC
    return state


def _wrapped_pages():
    pages = {0: [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], 2: [{"id": 3, "name": "c"}]}
    return respx.get(f"{BASE}/pets").mock(
        side_effect=lambda request: httpx.Response(
            200, json={"total": 3, "values": pages[int(request.url.params["offset"])]}
        )
    )


async def test_a_wrapped_list_is_offered_with_where_its_rows_are():
    from provisa.openapi.mapper import parse_spec

    (query,), _ = parse_spec(WRAPPED_SPEC)
    assert query.pagination == PaginationConfig(
        rows_field="values", type="offset", page_param="offset", page_size_param="limit"
    )


@respx.mock
async def test_a_wrapped_lists_rows_are_read_page_by_page_to_the_short_page(control_plane):
    state = _wrapped_state()
    table = _table(max_pages=10)
    table.pagination = PaginationConfig(type="offset", page_size=2, rows_field="values")
    await _register_in_the_admin(control_plane, state, table)
    route = _wrapped_pages()
    endpoint = state.api_endpoints["listPets"]
    assert endpoint.response_root == "values"
    assert [c.name for c in endpoint.columns][:2] == ["id", "name"]
    rows, cut = answer_rows(
        endpoint, await call_api(endpoint, {}, state.api_sources["petstore"].base_url)
    )
    assert [r["id"] for r in rows] == [1, 2, 3] and cut is None
    assert route.call_count == 2  # the short page ended it


@respx.mock
async def test_a_wrapped_answer_that_is_not_paged_is_read_once(control_plane):
    state = _wrapped_state()
    table = _table(max_pages=10)
    table.pagination = PaginationConfig(rows_field="values")
    await _register_in_the_admin(control_plane, state, table)
    route = respx.get(f"{BASE}/pets").mock(
        return_value=httpx.Response(200, json={"total": 1, "values": [{"id": 9, "name": "z"}]})
    )
    endpoint = state.api_endpoints["listPets"]
    rows, cut = answer_rows(
        endpoint, await call_api(endpoint, {}, state.api_sources["petstore"].base_url)
    )
    assert [(r["id"], r["name"]) for r in rows] == [(9, "z")]
    assert (cut, route.call_count) == (None, 1)


async def test_a_row_location_the_response_does_not_have_is_refused_by_name(control_plane):
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.core.repositories import table as table_repo

    table = _table(max_pages=10)
    table.pagination = PaginationConfig(rows_field="items")
    async with control_plane.acquire() as conn:
        await table_repo.upsert(conn, table)
        refused = await persist_openapi_endpoint(_wrapped_state(), conn, table)
    assert (refused.success, refused.code) == (False, "schema.openapi_no_rows_field")
    assert refused.params == {"table": "listPets", "rows_field": "items"}

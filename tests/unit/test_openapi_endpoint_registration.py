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
    endpoint = state.api_endpoints[("petstore", "listPets")]
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
    endpoint = state.api_endpoints[("petstore", "listPets")]
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


async def test_a_table_with_no_paging_is_stored_as_null_and_loads_unpaged(control_plane):
    """REQ-318: NULL = not paged. A table registered with no paging must store SQL NULL, not the
    JSON value ``null`` — the endpoint loader reads the column raw and would otherwise build a
    paging from the text ``"null"`` and fail the whole schema build."""
    state = _state()
    table = _table(max_pages=2).model_copy(update={"pagination": None})
    await _register_in_the_admin(control_plane, state, table)
    assert state.api_endpoints[("petstore", "listPets")].pagination is None
    async with control_plane.acquire() as conn:
        raw = await conn.fetch("SELECT pagination FROM api_endpoints")
        assert [r["pagination"] for r in raw] == [None]
        raw = await conn.fetch("SELECT pagination FROM registered_tables")
        assert [r["pagination"] for r in raw] == [None]


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


async def test_a_second_source_registering_a_table_of_the_same_name_keeps_the_firsts_endpoint(
    control_plane,
):
    """A table is named within its source. Two OpenAPI sources each registering ``listPets`` hold
    two endpoints; the second registration once took over the first source's row, which then
    served the second source's calls. (The copy is registered in a domain of its own: one SQL
    address per table in a domain, REQ-1933.)"""
    from provisa.api.admin._openapi_table_registration import persist_openapi_endpoint
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import register_openapi_source
    from provisa.core.repositories import table as table_repo

    state = _state()
    await _register_in_the_admin(control_plane, state, _table(max_pages=10))
    copy_base = "https://copy.test"
    state.openapi_specs["copy"] = {"spec": SPEC, "base_url": copy_base, "auth_config": None}
    copy_table = _table(max_pages=10).model_copy(update={"source_id": "copy", "domain_id": "e"})
    async with control_plane.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="e"))
        await conn.execute_core(insert(sources).values(id="copy", type="openapi"))
        await register_openapi_source(conn, "copy", copy_base)
        await table_repo.upsert(conn, copy_table)
        assert await persist_openapi_endpoint(state, conn, copy_table) is None
        state.api_endpoints, state.api_sources = await load_api_sources(conn, {})
        rows = (
            await conn.execute_core(select(api_endpoints.c.source_id, api_endpoints.c.table_name))
        ).fetchall()

    assert sorted(tuple(r) for r in rows) == [("copy", "listPets"), ("petstore", "listPets")]
    assert state.api_endpoints[("petstore", "listPets")].source_id == "petstore"
    assert state.api_endpoints[("copy", "listPets")].source_id == "copy"


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
    endpoint = state.api_endpoints[("petstore", "listPets")]
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
    endpoint = state.api_endpoints[("petstore", "listPets")]
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


# -- the parameters a read of the whole collection is called with ---------------------------------


def _params_spec(*parameters: dict) -> dict:
    return {"paths": {"/pets": {"get": {"parameters": list(parameters)}}}}


def test_a_list_parameter_is_sent_every_value_its_items_allow():
    from provisa.api_source.openapi_endpoint import default_params_from_spec

    listed = {"type": "array", "items": {"type": "string", "enum": ["available", "sold"]}}
    spec = _params_spec(
        {"name": "status", "in": "query", "schema": listed},
        {"name": "kind", "in": "query", **listed},  # Swagger 2.0: declared on the parameter
    )
    assert default_params_from_spec(spec, "/pets") == {
        "status": ["available", "sold"],
        "kind": ["available", "sold"],
    }


def test_a_single_valued_enum_is_sent_its_default_or_not_at_all():
    from provisa.api_source.openapi_endpoint import default_params_from_spec

    one_of = {"type": "string", "enum": ["draft", "open", "paid"]}
    spec = _params_spec(
        {"name": "status", "in": "query", "schema": one_of},
        {"name": "sort", "in": "query", "schema": {**one_of, "default": "open"}},
        {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100}},
    )
    assert default_params_from_spec(spec, "/pets") == {"sort": "open", "limit": 100}


async def test_a_branded_source_loads_with_its_brands_headers_and_another_with_none(control_plane):
    from provisa.api_source.loader import load_api_sources
    from provisa.api_source.openapi_endpoint import register_openapi_source
    from provisa.openapi.brands import BRANDS

    async with control_plane.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(id="pay", type="openapi", path="brand:stripe")
        )
        await register_openapi_source(conn, "pay", "https://api.stripe.com/")
        await register_openapi_source(conn, "petstore", BASE)
        _endpoints, loaded = await load_api_sources(conn, {})
    assert loaded["pay"].headers == BRANDS["stripe"].headers() != {}
    assert loaded["petstore"].headers == {}


# --- a source's credential never leaves the server ----------------------------------------------


def test_a_stored_auth_is_reported_without_its_credential():
    from provisa.api_source.openapi_endpoint import api_auth, auth_without_secret
    from provisa.core.auth_models import ApiAuthApiKey, ApiAuthBasic, ApiAuthBearer

    stated = [
        {"type": "bearer", "token": "s3cret"},
        {"type": "basic", "username": "ann", "password": "s3cret"},
        {"type": "api_key", "header_name": "X-Key", "api_key": "s3cret"},
    ]
    stored = [
        ApiAuthBearer(**api_auth(stated[0])),
        ApiAuthBasic(**api_auth(stated[1])),
        ApiAuthApiKey(**api_auth(stated[2])),
    ]
    reported = [auth_without_secret(auth) for auth in stored]
    assert reported == [
        {"type": "bearer"},
        {"type": "basic", "username": "ann"},
        {"type": "api_key", "header_name": "X-Key"},
    ]
    assert "s3cret" not in repr(reported) and auth_without_secret(None) is None


async def test_the_source_list_reports_the_stored_auth_and_never_the_credential(monkeypatch):
    """The auth is the stored one, so it is reported after a restart too, and the in-memory
    registration holds no credential to report."""
    from provisa.api import app
    from provisa.api.admin import openapi_router
    from provisa.api_source.models import ApiSource
    from provisa.core.auth_models import ApiAuthBearer

    monkeypatch.setattr(openapi_router, "require_capability_request", lambda *_a: None)
    monkeypatch.setattr(
        app.state,
        "openapi_specs",
        {
            "pay": {"spec_path": "brand:stripe", "spec": {}, "base_url": BASE},
            "practice": {"spec_path": "x.json", "spec": {}, "base_url": BASE},
        },
        raising=False,
    )
    called = ApiSource(id="pay", type="openapi", base_url=BASE, auth=ApiAuthBearer(token="sk_1"))
    # "practice" is bound to a synthetic store: it is not loaded as an API.
    monkeypatch.setattr(app.state, "api_sources", {"pay": called}, raising=False)
    listed = {r["source_id"]: r for r in await openapi_router.list_openapi_sources(None)}
    assert listed["pay"]["auth_config"] == {"type": "bearer"}
    assert listed["practice"]["auth_config"] is None
    assert "sk_1" not in repr(listed)


def test_an_auth_stated_without_its_credential_is_refused():
    from provisa.api.admin.openapi_router import _require_credential
    from provisa.api.errors import ApiError

    _require_credential(None)
    _require_credential({"type": "none"})
    _require_credential({"type": "bearer", "token": "${secret:PAY}"})
    for stated, field in [
        ({"type": "bearer", "token": ""}, "token"),
        ({"type": "basic", "username": "ann"}, "password"),
        ({"type": "api_key", "header_name": "X-Key", "api_key": ""}, "api_key"),
    ]:
        with pytest.raises(ApiError) as refused:
            _require_credential(stated)
        assert (refused.value.code, refused.value.params["field"]) == (
            "openapi.auth_credential_required",
            field,
        )


# --- an operation with an address of its own ---------------------------------------------------


async def test_a_table_whose_operation_declares_its_own_server_is_called_there(control_plane):
    from provisa.api_source.openapi_endpoint import (
        register_openapi_endpoint,
        register_openapi_source,
    )

    spec = {**SPEC, "paths": {"/pets": {"get": {**SPEC["paths"]["/pets"]["get"], "servers": [{"url": "https://files.pets.test/"}]}}}}  # fmt: skip
    table = _table(max_pages=3)
    async with control_plane.acquire() as conn:
        await register_openapi_source(conn, "petstore", BASE)
        await register_openapi_endpoint(conn, table, spec=spec, ttl=60)
        stored = (await conn.execute_core(select(api_endpoints.c.path))).scalar_one()
    assert stored == "https://files.pets.test/pets"

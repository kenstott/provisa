# Copyright (c) 2026 Kenneth Stott
# Canary: 668d3c84-f1e6-4116-82e9-6eab9e8503b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318: a table's paging is authored once, on the table, and set from the admin.

Before this, an endpoint's paging (``api_endpoints.pagination``) was read but nothing wrote it:
an operator setting with no way to set it, so the page cap and the request cut could never fire
on a shipped configuration. A connection table's row bound may only lower the operator's
``graphql_remote.max_rows``, refused by name at save and at load."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select, update

from provisa.core.paging import (
    CONNECTION,
    ENDPOINT,
    PaginationConfig,
    PagingRefused,
    check_paging,
    paging_row,
    stored_paging,
)

# -- the model -------------------------------------------------------------------------------------


def test_a_paged_endpoint_declares_its_type_and_a_connection_only_its_row_bound():
    with pytest.raises(ValidationError, match="need its type"):
        PaginationConfig(max_pages=3)
    with pytest.raises(ValidationError, match="bounds a connection table"):
        PaginationConfig(type="offset", max_rows=10)
    assert PaginationConfig(max_rows=10).is_endpoint is False
    assert PaginationConfig(type="cursor", cursor_param="after").is_endpoint is True


def test_the_stored_form_keeps_only_what_was_declared():
    """A connection table's bound must come back as it was saved, not carrying an endpoint's
    defaults (which only a typed paging may declare)."""
    bound = PaginationConfig(max_rows=10)
    assert paging_row(bound) == {"max_rows": 10}
    assert stored_paging(paging_row(bound)) == bound
    endpoint = PaginationConfig(type="page_number", max_pages=4)
    assert stored_paging(paging_row(endpoint)) == endpoint
    assert paging_row(None) is None and stored_paging(None) is None


@pytest.mark.parametrize(
    ("pagination", "kind", "code"),
    [
        (PaginationConfig(max_rows=5), None, "schema.paging_not_paged"),
        (PaginationConfig(max_rows=5), ENDPOINT, "schema.paging_endpoint_needs_type"),
        (PaginationConfig(type="offset"), CONNECTION, "schema.paging_connection_rows_only"),
        (PaginationConfig(max_rows=101), CONNECTION, "schema.paging_above_ceiling"),
    ],
)
def test_paging_a_table_cannot_take_is_refused_by_name(pagination, kind, code):
    with pytest.raises(PagingRefused) as refused:
        check_paging(pagination, table="t", kind=kind, ceiling_rows=100)
    assert refused.value.code == code


def test_a_connection_table_may_lower_the_operators_bound():
    check_paging(PaginationConfig(max_rows=100), table="t", kind=CONNECTION, ceiling_rows=100)
    check_paging(PaginationConfig(max_rows=1), table="t", kind=CONNECTION, ceiling_rows=100)


# -- load ------------------------------------------------------------------------------------------


def _config(source_type: str, pagination: PaginationConfig, ceiling: int = 100):
    return SimpleNamespace(
        sources=[SimpleNamespace(id="s", type=SimpleNamespace(value=source_type))],
        tables=[SimpleNamespace(source_id="s", table_name="t", pagination=pagination)],
        graphql_remote=SimpleNamespace(max_rows=ceiling),
    )


def test_a_config_raising_the_operators_bound_on_a_table_is_refused_at_load():
    from provisa.core.config_loader import _validate_paging

    with pytest.raises(PagingRefused, match="never raise it"):
        _validate_paging(_config("graphql_remote", PaginationConfig(max_rows=500)))
    with pytest.raises(PagingRefused, match="names the paging type"):
        _validate_paging(_config("openapi", PaginationConfig(max_rows=5)))
    with pytest.raises(PagingRefused, match="not read page by page"):
        _validate_paging(_config("postgresql", PaginationConfig(type="offset")))
    _validate_paging(_config("graphql_remote", PaginationConfig(max_rows=50)))
    _validate_paging(_config("openapi", PaginationConfig(type="offset", max_pages=3)))


# -- the admin save --------------------------------------------------------------------------------


@pytest.fixture
async def control_plane(tmp_path):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.schema_org import (
        api_endpoints,
        api_sources,
        domains,
        registered_tables,
        sources,
    )

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    await init_schema(db, "", org_id="default")
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="d"))
        await conn.execute_core(insert(sources).values(id="api", type="openapi"))
        await conn.execute_core(insert(sources).values(id="gh", type="graphql_remote"))
        await conn.execute_core(insert(api_sources).values(id="api", type="openapi"))
        for i, (src, name) in enumerate([("api", "pets"), ("gh", "gh__issues")], start=1):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=i, source_id=src, domain_id="d", schema_name="s", table_name=name,
                )
            )  # fmt: skip
        await conn.execute_core(
            insert(api_endpoints).values(
                source_id="api", path="/pets", table_name="pets", columns=[]
            )
        )
    try:
        yield db
    finally:
        engine.dispose()


def _state():
    from provisa.api_source.models import ApiEndpoint

    return SimpleNamespace(
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_rows=100)),
        api_endpoints={"pets": ApiEndpoint(source_id="api", path="/pets", table_name="pets", columns=[])},
        graphql_remote_sources={
            "gh": {"tables": [{"sql_name": "gh__issues", "rows_path": ["nodes"]}]}
        },
    )  # fmt: skip


async def _stored(db, table_id: int) -> tuple:
    from provisa.core.schema_org import api_endpoints, registered_tables

    async with db.acquire() as conn:
        table = (
            await conn.execute_core(
                select(registered_tables.c.pagination).where(registered_tables.c.id == table_id)
            )
        ).scalar_one()
        endpoint = (await conn.execute_core(select(api_endpoints.c.pagination))).scalar_one()
    return table, endpoint


async def test_a_rest_tables_paging_is_saved_on_the_table_and_copied_to_its_endpoint(
    control_plane,
):
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.admin.types import PagingInput

    state = _state()
    async with control_plane.acquire() as conn:
        result = await save_table_paging(
            state, conn, 1, PagingInput(type="offset", page_size=50, max_pages=3)
        )
    assert result.success and result.code == "schema.table_paging_updated"
    declared = {"type": "offset", "page_size": 50, "max_pages": 3}
    assert await _stored(control_plane, 1) == (declared, declared)
    assert state.api_endpoints["pets"].pagination == PaginationConfig(**declared)

    async with control_plane.acquire() as conn:
        cleared = await save_table_paging(state, conn, 1, None)
    assert cleared.success and await _stored(control_plane, 1) == (None, None)
    assert state.api_endpoints["pets"].pagination is None


async def test_a_connection_tables_bound_is_saved_only_below_the_operators(control_plane):
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.admin.types import PagingInput

    state = _state()
    async with control_plane.acquire() as conn:
        above = await save_table_paging(state, conn, 2, PagingInput(max_rows=500))
        typed = await save_table_paging(state, conn, 2, PagingInput(type="offset"))
        rest_bound = await save_table_paging(state, conn, 1, PagingInput(max_rows=5))
        lowered = await save_table_paging(state, conn, 2, PagingInput(max_rows=20))
    assert (above.success, above.code) == (False, "schema.paging_above_ceiling")
    assert above.params == {"table": "gh__issues", "max_rows": 500, "ceiling": 100}
    assert (typed.success, typed.code) == (False, "schema.paging_connection_rows_only")
    assert (rest_bound.success, rest_bound.code) == (False, "schema.paging_endpoint_needs_type")
    assert lowered.success
    assert (await _stored(control_plane, 2))[0] == {"max_rows": 20}


async def test_inconsistent_paging_is_refused_with_its_reason(control_plane):
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.admin.types import PagingInput

    async with control_plane.acquire() as conn:
        result = await save_table_paging(_state(), conn, 1, PagingInput(max_pages=3))
    assert (result.success, result.code) == (False, "schema.paging_invalid")
    assert "need its type" in result.params["reason"]


async def test_a_remote_graphql_table_that_is_no_connection_takes_no_paging(control_plane):
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.admin.types import PagingInput

    state = _state()
    state.graphql_remote_sources["gh"]["tables"][0]["rows_path"] = None
    async with control_plane.acquire() as conn:
        result = await save_table_paging(state, conn, 2, PagingInput(max_rows=5))
    assert (result.success, result.code) == (False, "schema.paging_not_paged")


# -- where a REST answer's rows are (REQ-316) -------------------------------------------------------


def test_a_rest_endpoint_may_say_where_its_rows_are_and_nothing_else():
    wrapped = PaginationConfig(rows_field="values")
    assert wrapped.is_endpoint and wrapped.type is None
    assert paging_row(wrapped) == {"rows_field": "values"}
    check_paging(wrapped, table="repos", kind=ENDPOINT, ceiling_rows=100)
    with pytest.raises(ValueError, match="rows_field"):
        PaginationConfig(rows_field="values", max_rows=10)


async def test_an_edit_of_a_tables_paging_keeps_where_its_rows_are(control_plane):
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.admin.types import PagingInput
    from provisa.core.schema_org import registered_tables

    state = _state()
    async with control_plane.acquire() as conn:
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == 1)
            .values(pagination={"rows_field": "values"})
        )
        moved = await save_table_paging(state, conn, 1, PagingInput(type="offset"))
        kept = await save_table_paging(
            state, conn, 1, PagingInput(type="offset", rows_field="values", max_pages=3)
        )
    assert (moved.success, moved.code) == (False, "schema.paging_rows_field_fixed")
    assert moved.params == {"table": "pets", "rows_field": "values"}
    assert kept.success
    assert state.api_endpoints["pets"].pagination.rows_field == "values"

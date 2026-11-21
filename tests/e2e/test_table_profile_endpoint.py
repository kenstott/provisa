# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c1a4e-3b6d-4c8a-9e1f-0a2b3c4d5e6f
# (run scripts/canary_stamp.py on this file after creating it)

"""E2E tests: POST /admin/tables/{id}/profile → sampled rows (REQ-452).

Regression: a __derived__ view's SQL is semantic (domain.field refs) and must be
compiled + governed + routed like an interactive /data/sql query. Handing it raw
to the federation engine 500s because domain refs never resolve.
"""

# Requirements: REQ-452, REQ-273, REQ-1327

import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, insert, select

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

# The columns the profile view selects from orders.
_VIEW_COLUMNS = ("id", "amount")


@pytest.fixture(scope="module")
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest.fixture
async def view_id():
    """Register a __derived__ view sampling a semantic query, yield its id, clean up."""
    from provisa.api.app import state
    from provisa.core import model_change
    from provisa.core.schema_org import registered_tables, table_columns

    assert state.model_db is not None
    # registered_tables is a model table: it is read through the org's model_db and written inside
    # a model change scope (REQ-1524).
    async with (
        model_change.scope("e2e: register profile test view"),
        state.model_db.acquire() as conn,
    ):
        res = await conn.execute_core(
            insert(registered_tables)
            .values(
                source_id="__derived__",
                domain_id="sales-analytics",
                schema_name="sales-analytics",
                table_name="_profile_test_view",
                view_sql="SELECT id, amount FROM orders",
            )
            .returning(registered_tables.c.id)
        )
        new_id = res.fetchone()[0]
        # A view is served to a reader through the columns registered on it: one with none is
        # served nothing and is refused (V003, REQ-2201), as the admin API's registration never
        # leaves it. Its columns carry the types of the orders columns its SQL selects.
        order_types = dict(
            (
                await conn.execute_core(
                    select(table_columns.c.column_name, table_columns.c.data_type)
                    .join(registered_tables, table_columns.c.table_id == registered_tables.c.id)
                    .where(
                        registered_tables.c.domain_id == "sales-analytics",
                        registered_tables.c.table_name == "orders",
                        table_columns.c.column_name.in_(_VIEW_COLUMNS),
                    )
                )
            ).fetchall()
        )
        assert set(order_types) == set(_VIEW_COLUMNS), order_types
        await conn.execute_core(
            insert(table_columns).values(
                [
                    {
                        "table_id": new_id,
                        "domain_id": "sales-analytics",
                        "column_name": name,
                        "data_type": order_types[name],
                        "visible_to": ["org_admin"],
                    }
                    for name in _VIEW_COLUMNS
                ]
            )
        )
    # A table's profile is read as its caller through the one pipeline, so the view has to be
    # in the model the server holds: the registration is followed by a rebuild, as the admin
    # API's own registration is.
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()

    yield new_id

    assert state.model_db is not None
    async with (
        model_change.scope("e2e: remove profile test view"),
        state.model_db.acquire() as conn,
    ):
        await conn.execute_core(delete(registered_tables).where(registered_tables.c.id == new_id))
    await _rebuild_schemas()


class TestTableProfileEndpoint:
    async def test_view_profile_without_a_role_header_is_the_acting_roles(self, client, view_id):
        # REQ-273: the sample is governed as the role the auth layer established; the header
        # only asks for one. This server has no auth provider, where a request that names no
        # role acts as org_admin (REQ-1327) — so it is answered as the org_admin header is.
        # (That a role the caller does not hold is refused needs an auth provider, which this
        # server has not: tests/unit/test_table_profile_acting_role.py covers it.)
        resp = await client.post(f"/admin/tables/{view_id}/profile")
        assert resp.status_code == 200, resp.text
        named = await client.post(
            f"/admin/tables/{view_id}/profile",
            headers={"X-Provisa-Role": "org_admin"},
        )
        assert named.status_code == 200, named.text
        assert resp.json()["columns"] == named.json()["columns"] == ["id", "amount"]
        assert resp.json()["rows"] == named.json()["rows"]

    async def test_view_profile_returns_sampled_rows(self, client, view_id):
        # Semantic view SQL must resolve through the governed pipeline (regression).
        resp = await client.post(
            f"/admin/tables/{view_id}/profile",
            headers={"X-Provisa-Role": "org_admin"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["columns"] == ["id", "amount"]
        assert body["rowCount"] == len(body["rows"])

    async def test_view_profile_matches_data_sql(self, client, view_id):
        # The profile of a view equals running its SQL through /data/sql.
        prof = await client.post(
            f"/admin/tables/{view_id}/profile",
            headers={"X-Provisa-Role": "org_admin"},
        )
        direct = await client.post(
            "/data/sql",
            json={"sql": "SELECT id, amount FROM orders", "role": "org_admin"},
        )
        assert prof.status_code == 200
        assert direct.status_code == 200
        prof_rows = prof.json()["rows"]
        direct_rows = direct.json()["data"]["sql"]
        assert len(prof_rows) == len(direct_rows)

    async def test_profile_missing_table_404(self, client):
        resp = await client.post(
            "/admin/tables/999999/profile",
            headers={"X-Provisa-Role": "org_admin"},
        )
        assert resp.status_code == 404


class TestGovernedPipeline:
    """The ONE governed pipeline (_govern_and_route → _execute_plan) shared by /data/sql,
    the view-profile endpoint, pgwire, MCP, Flight and airport."""

    async def test_compiles_and_routes_semantic_sql(self, client):
        from provisa.api.app import state
        from provisa.pgwire._pipeline import _execute_plan, _govern_and_route

        assert client is not None  # fixture drives app lifespan → populates `state`
        plan = await _govern_and_route("SELECT id, amount FROM orders", "org_admin")
        result = await _execute_plan(plan, state)
        assert result.column_names == ["id", "amount"]

    async def test_unknown_role_rejected(self, client):
        from provisa.pgwire._pipeline import _govern_and_route

        assert client is not None  # fixture drives app lifespan → populates `state`
        with pytest.raises(PermissionError):
            await _govern_and_route("SELECT 1", "no_such_role")

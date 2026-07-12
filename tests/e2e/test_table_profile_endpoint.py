# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c1a4e-3b6d-4c8a-9e1f-0a2b3c4d5e6f
# (run scripts/canary_stamp.py on this file after creating it)

"""E2E tests: POST /admin/tables/{id}/profile → sampled rows (REQ-452).

Regression: a __provisa__ view's SQL is semantic (domain.field refs) and must be
compiled + governed + routed like an interactive /data/sql query. Handing it raw
to the federation engine 500s because domain refs never resolve.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, insert

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


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
    """Register a __provisa__ view sampling a semantic query, yield its id, clean up."""
    from provisa.api.app import state
    from provisa.core.schema_org import registered_tables

    async with state.tenant_db.acquire() as conn:
        res = await conn.execute_core(
            insert(registered_tables)
            .values(
                source_id="__provisa__",
                domain_id="sales-analytics",
                schema_name="sales-analytics",
                table_name="_profile_test_view",
                governance="pre-approved",
                view_sql="SELECT id, amount FROM orders",
            )
            .returning(registered_tables.c.id)
        )
        new_id = res.fetchone()[0]

    yield new_id

    async with state.tenant_db.acquire() as conn:
        await conn.execute_core(delete(registered_tables).where(registered_tables.c.id == new_id))


class TestTableProfileEndpoint:
    async def test_view_profile_requires_role_header(self, client, view_id):
        # No X-Provisa-Role → cannot pick a governance context; reject, never 500.
        resp = await client.post(f"/admin/tables/{view_id}/profile")
        assert resp.status_code == 400
        assert "role" in resp.json()["detail"].lower()

    async def test_view_profile_returns_sampled_rows(self, client, view_id):
        # Semantic view SQL must resolve through the governed pipeline (regression).
        resp = await client.post(
            f"/admin/tables/{view_id}/profile",
            headers={"X-Provisa-Role": "admin"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["columns"] == ["id", "amount"]
        assert body["rowCount"] == len(body["rows"])

    async def test_view_profile_matches_data_sql(self, client, view_id):
        # The profile of a view equals running its SQL through /data/sql.
        prof = await client.post(
            f"/admin/tables/{view_id}/profile",
            headers={"X-Provisa-Role": "admin"},
        )
        direct = await client.post(
            "/data/sql",
            json={"sql": "SELECT id, amount FROM orders", "role": "admin"},
        )
        assert prof.status_code == 200
        assert direct.status_code == 200
        prof_rows = prof.json()["rows"]
        direct_rows = direct.json()["data"]["sql"]
        assert len(prof_rows) == len(direct_rows)

    async def test_profile_missing_table_404(self, client):
        resp = await client.post(
            "/admin/tables/999999/profile",
            headers={"X-Provisa-Role": "admin"},
        )
        assert resp.status_code == 404


class TestCompileGovernExecute:
    """The extracted core shared by /data/sql and the view-profile endpoint."""

    async def test_compiles_and_routes_semantic_sql(self, client):
        from provisa.api.app import state
        from provisa.api.data.endpoint_dev import _compile_govern_execute

        result, _sources, _default, _decision, _physical = await _compile_govern_execute(
            "SELECT id, amount FROM orders", "admin", state
        )
        assert result.column_names == ["id", "amount"]

    async def test_unknown_role_rejected(self, client):
        from fastapi import HTTPException

        from provisa.api.app import state
        from provisa.api.data.endpoint_dev import _compile_govern_execute

        with pytest.raises(HTTPException) as exc:
            await _compile_govern_execute("SELECT 1", "no_such_role", state)
        assert exc.value.status_code == 400

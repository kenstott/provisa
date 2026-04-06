# Copyright (c) 2026 Kenneth Stott
# Canary: 5a3c861e-356c-4830-8eea-541a3280d31a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E registry flow tests — governance enforcement through HTTP.

Tests run in test mode (default) unless explicitly testing production mode.
"""

import os

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


@pytest.fixture
async def client():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    os.environ["PROVISA_MODE"] = "test"

    from provisa.api.app import create_app

    app = create_app()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


class TestTestMode:
    async def test_arbitrary_query_allowed(self, client):
        """In test mode, arbitrary queries execute with full guards."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__orders { id amount } }", "role": "admin"},
        )
        assert resp.status_code == 200
        assert len(resp.json()["data"]["sales_analytics__orders"]) > 0

    async def test_pre_approved_table_allowed(self, client):
        """Pre-approved tables work without registry."""
        resp = await client.post(
            "/data/graphql",
            json={"query": "{ sales_analytics__customers { id name } }", "role": "admin"},
        )
        assert resp.status_code == 200


class TestCeilingEnforcement:
    def test_ceiling_check_unit(self):
        """Inline ceiling check — extra fields rejected."""
        from provisa.registry.ceiling import CeilingViolationError, check_ceiling

        # Passes: subset of approved fields
        check_ceiling("{ orders { id amount } }", "{ orders { id } }")

        # Fails: extra field not in approved
        with pytest.raises(CeilingViolationError):
            check_ceiling("{ orders { id amount } }", "{ orders { id secret } }")


class TestGovernanceLogic:
    def test_production_rejects_raw_query_on_registry_table(self):
        """Registry-required table rejects raw query in production mode."""
        from provisa.registry.governance import (
            GovernanceError,
            GovernanceMode,
            check_governance,
        )

        with pytest.raises(GovernanceError):
            check_governance(
                GovernanceMode.PRODUCTION,
                [1],
                {1: "registry-required"},
                stable_id=None,
            )

    def test_production_allows_pre_approved(self):
        from provisa.registry.governance import GovernanceMode, check_governance

        check_governance(
            GovernanceMode.PRODUCTION,
            [1],
            {1: "pre-approved"},
            stable_id=None,
        )


class TestGovernedQueryGet:
    """GET /data/graphql?queryId=<stable_id> — governed query execution over GET."""

    @pytest.fixture(autouse=True)
    async def _seed_approved_query(self, client, pg_pool):
        """Insert an approved governed query and clean up after."""
        async with pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO persisted_queries
                    (stable_id, status, query_text, compiled_sql,
                     target_tables, developer_id)
                VALUES ($1, 'approved', $2, $3, $4::int[], $5)
                ON CONFLICT (stable_id) DO UPDATE
                    SET status = 'approved', query_text = EXCLUDED.query_text
                """,
                "test-get-governed",
                "{ sales_analytics__orders { id amount } }",
                "SELECT id, amount FROM orders",
                [],
                "test-developer",
            )
        yield
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM persisted_queries WHERE stable_id = $1",
                "test-get-governed",
            )

    async def test_get_approved_query_returns_200(self, client):
        """GET with a valid approved queryId returns results."""
        resp = await client.get(
            "/data/graphql",
            params={"queryId": "test-get-governed", "role": "admin"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "data" in data
        assert "sales_analytics__orders" in data["data"]

    async def test_get_unknown_query_id_returns_404(self, client):
        """GET with an unrecognised queryId returns 404."""
        resp = await client.get(
            "/data/graphql",
            params={"queryId": "does-not-exist", "role": "admin"},
        )
        assert resp.status_code == 404

    async def test_get_requires_query_id_param(self, client):
        """GET without queryId returns 422 (missing required param)."""
        resp = await client.get("/data/graphql")
        assert resp.status_code == 422

    async def test_get_respects_role(self, client):
        """GET passes role through to the execution pipeline."""
        resp = await client.get(
            "/data/graphql",
            params={"queryId": "test-get-governed", "role": "admin"},
        )
        assert resp.status_code == 200

    async def test_get_accept_header_controls_format(self, client):
        """GET honours the Accept header for content negotiation."""
        resp = await client.get(
            "/data/graphql",
            params={"queryId": "test-get-governed", "role": "admin"},
            headers={"Accept": "text/csv"},
        )
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]


class TestDeprecatedQuery:
    def test_deprecated_query_error(self):
        from provisa.registry.governance import GovernanceError, check_deprecated

        with pytest.raises(GovernanceError, match="deprecated"):
            check_deprecated({
                "status": "deprecated",
                "stable_id": "old-id",
                "deprecated_by": "new-id",
            })

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


class TestDeprecatedQuery:
    def test_deprecated_query_error(self):
        from provisa.registry.governance import GovernanceError, check_deprecated

        with pytest.raises(GovernanceError, match="deprecated"):
            check_deprecated({
                "status": "deprecated",
                "stable_id": "old-id",
                "deprecated_by": "new-id",
            })

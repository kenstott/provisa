# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2a9d13-6c4e-4b81-9d5f-3a1e6c8b4f70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1798: the chat assistant's govdata/Kaggle "subscription sources" MCP tools.

search_kaggle_datasets never takes a raw token or a chosen secret name — it always resolves the
FIXED secret name mcp_tools.KAGGLE_TOKEN_SECRET_NAME via provisa.core.secrets, so a credential
never appears in a tool-call argument, chat history, or log line.
"""

import types
from contextlib import asynccontextmanager

import httpx
import pytest
import respx
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.api.mcp import tools as mcp_tools
from provisa.core.database import Database
from provisa.core.models import GovDataSubject, GovDataSubscription
from provisa.core.schema_admin import secrets_store

pytestmark = pytest.mark.asyncio


def _state(*, govdata_subscriptions=None):
    return types.SimpleNamespace(
        contexts={"analyst": object()},
        config=types.SimpleNamespace(govdata_subscriptions=govdata_subscriptions or []),
    )


class TestSearchGovdataSubjects:
    async def test_reports_unsubscribed_match(self):
        result = await mcp_tools.search_govdata_subjects(_state(), "analyst", "inflation")
        assert any(hit["subject"] == "ECONOMY" for hit in result)
        econ_hit = next(hit for hit in result if hit["subject"] == "ECONOMY")
        assert econ_hit["subscribed"] is False

    async def test_reports_subscribed_when_org_has_the_subject(self):
        subs = [GovDataSubscription(subjects=[GovDataSubject.economy])]
        result = await mcp_tools.search_govdata_subjects(
            _state(govdata_subscriptions=subs), "analyst", "inflation"
        )
        econ_hit = next(hit for hit in result if hit["subject"] == "ECONOMY")
        assert econ_hit["subscribed"] is True

    async def test_all_subscription_covers_every_subject(self):
        subs = [GovDataSubscription(subjects=[GovDataSubject.all])]
        result = await mcp_tools.search_govdata_subjects(
            _state(govdata_subscriptions=subs), "analyst", "crime"
        )
        assert all(hit["subscribed"] for hit in result if hit["subject"])

    async def test_requires_a_known_role(self):
        with pytest.raises(PermissionError):
            await mcp_tools.search_govdata_subjects(_state(), "ghost", "inflation")


@asynccontextmanager
async def _admin_db(tmp_path):
    """A real (SQLite) admin_db holding secrets_store — REQ-1799/1802's `bound_to_request_org`
    always queries this table for the org's vault, regardless of whether resolve_secrets itself
    is mocked, so every test below needs a real one, not a bare mock."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'secrets.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: secrets_store.metadata.create_all(s, tables=[secrets_store]))
    db = Database(engine, name="admin")
    try:
        yield db
    finally:
        await engine.dispose()


class TestSearchKaggleDatasets:
    @pytest.fixture(autouse=True)
    async def _bind_request_org(self, tmp_path, monkeypatch):
        # REQ-1799/1802: search_kaggle_datasets resolves ${secret:...} inside
        # secrets_store.bound_to_request_org(), which needs an installed request-org resolver —
        # exactly the plumbing the real API layer installs at import (provisa.api.app). Without
        # this, resolution fails with "no organization is bound to this context" regardless of
        # whether the secret exists, which is the bug this fixture's own existence proves was
        # previously untested (the tests below used to mock resolve_secrets entirely, papering
        # over the real ${secret:...} resolution path).
        from provisa.core import secrets_store as secrets_store_mod

        async with _admin_db(tmp_path) as db:
            self.admin_db = db
            monkeypatch.setattr(secrets_store_mod, "_request_org", lambda: (db, "test_org"))
            yield

    async def test_resolves_fixed_secret_name_never_a_raw_token_argument(self, monkeypatch):
        seen_reference = {}

        def fake_resolve_secrets(value):
            seen_reference["value"] = value
            return "KGAT_resolved_token"

        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", fake_resolve_secrets)

        route = respx.get("https://www.kaggle.com/api/v1/datasets/list").mock(
            return_value=httpx.Response(
                200, json=[{"ref": "owner/inflation", "title": "Inflation"}]
            )
        )
        with respx.mock:
            respx.route(host="www.kaggle.com").mock(side_effect=route.side_effect)
            result = await mcp_tools.search_kaggle_datasets(_state(), "analyst", "inflation")

        assert seen_reference["value"] == f"${{secret:{mcp_tools.KAGGLE_TOKEN_SECRET_NAME}}}"
        assert result[0]["ref"] == "owner/inflation"

    async def test_missing_secret_fails_closed(self):
        # No mocking of resolve_secrets here — the org vault is real (empty), so this exercises
        # the ACTUAL ${secret:...} resolution path end to end and proves it fails closed rather
        # than silently returning an empty/garbage token.
        with pytest.raises(KeyError, match=mcp_tools.KAGGLE_TOKEN_SECRET_NAME):
            await mcp_tools.search_kaggle_datasets(_state(), "analyst", "inflation")

    async def test_real_secret_resolution_end_to_end(self, monkeypatch):
        # REQ-1799/1802: proves the actual bug (org-context binding, not just the fixed-name
        # convention) is fixed — a real secret, stored the same way the Secrets page stores one,
        # resolves through the real ${secret:...} provider with no mocking of resolve_secrets.
        from provisa.core import secrets_store as secrets_store_mod

        monkeypatch.setattr(
            secrets_store_mod,
            "_cipher",
            lambda: types.SimpleNamespace(encrypt=lambda b: b, decrypt=lambda b: b),
        )
        await secrets_store_mod.put(
            self.admin_db,
            "test_org",
            mcp_tools.KAGGLE_TOKEN_SECRET_NAME,
            "KGAT_stored_token",
            owner_id=secrets_store_mod.ORG_OWNER,
        )

        seen_token = {}

        async def fake_search_datasets(token, *, query):
            seen_token["token"] = token
            return [{"ref": "owner/inflation", "title": "Inflation"}]

        monkeypatch.setattr("provisa.kaggle.client.search_datasets", fake_search_datasets)

        result = await mcp_tools.search_kaggle_datasets(_state(), "analyst", "inflation")

        assert seen_token["token"] == "KGAT_stored_token"
        assert result[0]["ref"] == "owner/inflation"

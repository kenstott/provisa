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

import httpx
import pytest
import respx

from provisa.api.mcp import tools as mcp_tools
from provisa.core.models import GovDataSubject, GovDataSubscription

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


class TestSearchKaggleDatasets:
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

    async def test_missing_secret_fails_closed(self, monkeypatch):
        def fake_resolve_secrets(value):
            raise KeyError("no such secret: kaggle_api_token")

        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", fake_resolve_secrets)

        with pytest.raises(KeyError):
            await mcp_tools.search_kaggle_datasets(_state(), "analyst", "inflation")

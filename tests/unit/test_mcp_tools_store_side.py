# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e9a12-5d48-4f06-a1c9-6e2f8b4d0c75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP tools read the org's model tables through the model store (REQ-1919, REQ-1922).

The glossary, the data products, the metrics and the creation requests are model tables. A
server's two org handles each refuse the other store's tables, so a tool that read one of them
through ``tenant_db`` (the state store) failed with StoreSideViolation: on the Polly bake-off's
server every glossary lookup did, and a model that leaned on it answered nothing. These tests give
the tools the two handles a server has, each guarded, rather than one database as both."""

# Requirements: REQ-1919, REQ-1922

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.api.mcp import tools
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio


def _state(control_plane) -> RoleState:
    return RoleState(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst", "domain_access": ["*"]}},
        model_db=control_plane.org(holds="model"),
        tenant_db=control_plane.org(holds="state"),
        # REQ-2216: a request raised looks up the org's mail channel; none is configured here.
        config=SimpleNamespace(mail=SimpleNamespace(provider="")),
    )


def _request():
    return SimpleNamespace(state=SimpleNamespace(active_org_id="default"))


async def test_the_glossary_is_listed_through_the_model_store(control_plane):
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_read"),
        patch("provisa.api.admin.glossary_router._view_scope", return_value=None),
    ):
        terms = await tools.list_glossary_terms(_state(control_plane), "analyst", _request())
    assert isinstance(terms, list)


async def test_the_glossary_is_searched_through_the_model_store(control_plane):
    assert isinstance(await tools.search_terms(_state(control_plane), "analyst", "revenue"), list)


async def test_a_proposal_is_queued_through_the_model_store(control_plane):
    result = await tools.propose_source(
        _state(control_plane),
        "analyst",
        {"id": "new_pg", "type": "postgresql", "host": "db.internal", "port": 5432},
        "found via network scan",
        requested_by="u-ann",
    )
    assert result["status"] == "pending"

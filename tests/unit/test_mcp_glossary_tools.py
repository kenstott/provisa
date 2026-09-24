# Copyright (c) 2026 Kenneth Stott
# Canary: b9e6010f-9e8d-4827-99a0-8a4f46dc4be9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Polly's glossary management tools (REQ-1835).

Each tool reuses the SAME capability checks and repo calls the /admin/glossary REST router
uses, applied directly to the real, AuthMiddleware-verified request the chat turn carries —
so a permission denial here means the identical router call would also have denied it.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.mcp import tools

pytestmark = pytest.mark.asyncio


def _state():
    return SimpleNamespace(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst"}},
        tenant_db=SimpleNamespace(acquire=lambda: _FakeAcquire(object())),
    )


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _request():
    return SimpleNamespace(state=SimpleNamespace(active_org_id="acme"))


async def test_list_glossary_terms_calls_repo():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_read"),
        patch("provisa.api.admin.glossary_router._view_scope", return_value=None),
        patch(
            "provisa.core.repositories.glossary.list_terms",
            new=AsyncMock(return_value=[{"id": 1, "name": "Revenue"}]),
        ),
    ):
        result = await tools.list_glossary_terms(_state(), "analyst", _request(), q="rev")

    assert result == [{"id": 1, "name": "Revenue"}]


async def test_create_glossary_term_calls_repo_and_notifies():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._declared_domains", return_value=set()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()) as notify,
        patch(
            "provisa.core.repositories.glossary.create_abstract_term",
            new=AsyncMock(return_value=42),
        ),
    ):
        result = await tools.create_glossary_term(_state(), "analyst", _request(), "Churn")

    assert result == {"id": 42}
    notify.assert_awaited_once()


async def test_update_glossary_term_requires_a_found_field():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.rename_term", new=AsyncMock(return_value=False)),
    ):
        with pytest.raises(ValueError):
            await tools.update_glossary_term(_state(), "analyst", _request(), 1, name="New Name")


async def test_delete_glossary_term_is_irreversible_and_checked():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()) as notify,
        patch("provisa.core.repositories.glossary.delete_term", new=AsyncMock(return_value=True)),
    ):
        result = await tools.delete_glossary_term(_state(), "analyst", _request(), 1)

    assert result == {"ok": True}
    notify.assert_awaited_once()


async def test_add_glossary_term_edge_grounds_a_proposed_term():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.add_edge", new=AsyncMock()) as add_edge,
    ):
        result = await tools.add_glossary_term_edge(
            _state(), "analyst", _request(), 1, 2, "broader"
        )

    assert result == {"ok": True}
    called_conn, from_id, to_id, rel_type = add_edge.call_args[0]
    assert (from_id, to_id, rel_type) == (1, 2, "broader")


async def test_remove_glossary_term_edge_not_found_raises():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.remove_edge", new=AsyncMock(return_value=False)),
    ):
        with pytest.raises(ValueError):
            await tools.remove_glossary_term_edge(_state(), "analyst", _request(), 1, 2, "broader")


async def test_glossary_tools_require_role():
    with pytest.raises(ValueError):
        await tools.list_glossary_terms(_state(), "", _request())

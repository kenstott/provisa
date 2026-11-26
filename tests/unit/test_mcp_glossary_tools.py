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
from provisa.api.admin.read_rights import SERVICE_READER
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.mcp import tools
from tests.role_state import RoleState

pytestmark = pytest.mark.asyncio


def _state():
    return RoleState(
        contexts={"analyst": object()},
        roles={"analyst": {"id": "analyst"}},
        model_db=SimpleNamespace(acquire=lambda: _FakeAcquire(object())),
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
        # REQ-2152: the counts are of what the caller is served (Provisa's own reader here).
        patch(
            "provisa.core.repositories.glossary.bindings",
            new=AsyncMock(return_value=[(1, 10, "amount"), (1, 11, None)]),
        ),
        patch("provisa.api.admin.read_rights.reader_for_request", return_value=SERVICE_READER),
    ):
        result = await tools.list_glossary_terms(_state(), "analyst", _request(), q="rev")

    assert result == [{"id": 1, "name": "Revenue", "ref_count": 1, "table_count": 1}]


async def test_create_glossary_term_calls_repo_and_notifies():
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._declared_domains", return_value=set()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()) as notify,
        patch("provisa.api.admin.reference_library_router.embed_terms", new=AsyncMock()) as embed,
        patch(
            "provisa.core.repositories.glossary.create_abstract_term",
            new=AsyncMock(return_value=42),
        ),
        patch(
            "provisa.core.repositories.glossary.get_term",
            new=AsyncMock(return_value=_term(42, "Churn", [], "draft", None)),
        ),
        patch(
            "provisa.core.repositories.glossary.terms_named",
            new=AsyncMock(return_value={42: set()}),
        ),
    ):
        result = await tools.create_glossary_term(_state(), "analyst", _request(), "Churn")

    assert result["id"] == 42
    assert result["term"]["id"] == 42
    notify.assert_awaited_once()
    embed.assert_awaited_once()  # REQ-2185: TERMS ARE EMBEDDED AS THEY CHANGE


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
        patch("provisa.api.admin.reference_library_router.embed_terms", new=AsyncMock()) as embed,
        patch("provisa.core.repositories.glossary.delete_term", new=AsyncMock(return_value=True)),
    ):
        result = await tools.delete_glossary_term(_state(), "analyst", _request(), 1)

    assert result == {"ok": True}
    notify.assert_awaited_once()
    embed.assert_awaited_once()  # REQ-2185: TERMS ARE EMBEDDED AS THEY CHANGE


def _term(term_id: int, name: str, domains: list[str], status: str, definition: str | None):
    """A term as glossary_repo.get_term returns it, the fields Polly reports."""
    return {
        "id": term_id,
        "name": name,
        "domains": domains,
        "status": status,
        "definition": definition,
        "edges_out": [],
        "edges_in": [],
    }


async def test_add_glossary_term_edge_grounds_a_proposed_term():
    term = _term(1, "Patron", ["sales"], "draft", None)
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.add_edge", new=AsyncMock()) as add_edge,
        patch("provisa.core.repositories.glossary.get_term", new=AsyncMock(return_value=term)),
        patch(
            "provisa.core.repositories.glossary.terms_named",
            new=AsyncMock(return_value={1: {"sales"}}),
        ),
    ):
        result = await tools.add_glossary_term_edge(
            _state(), "analyst", _request(), 1, 2, "broader"
        )

    assert result["ok"] is True
    assert result["term"]["id"] == 1
    called_conn, from_id, to_id, rel_type = add_edge.call_args[0]
    assert (from_id, to_id, rel_type) == (1, 2, "broader")


async def test_a_glossary_write_reports_the_term_it_changed_among_others_of_its_name():
    """Two domains each hold a term named neighborhood (REQ-1591). An edge added to the city's
    draft reports that term as it now is -- its id, domains, status and definition, read back by
    id -- and names the other neighborhood by id and domains, never reporting the other's state."""
    written = _term(41, "neighborhood", ["city"], "draft", None)
    written["edges_out"] = [
        {"term_id": 7, "rel_type": "KIND_OF", "name": "location", "domains": ["*"]}
    ]
    get_term = AsyncMock(return_value=written)
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.add_edge", new=AsyncMock()),
        patch("provisa.core.repositories.glossary.get_term", new=get_term),
        patch(
            "provisa.core.repositories.glossary.terms_named",
            new=AsyncMock(return_value={94: {"*"}, 41: {"city"}}),
        ),
    ):
        result = await tools.add_glossary_term_edge(
            _state(), "analyst", _request(), 41, 7, "KIND_OF"
        )

    assert get_term.call_args[0][1] == 41
    assert result["term"] == {
        "id": 41,
        "name": "neighborhood",
        "domains": ["city"],
        "status": "draft",
        "definition": None,
        "edges_out": [{"term_id": 7, "rel_type": "KIND_OF", "name": "location", "domains": ["*"]}],
        "edges_in": [],
        "same_name": [{"id": 94, "domains": ["*"]}],
    }


async def test_update_glossary_term_reports_the_definition_as_read_back():
    term = _term(41, "neighborhood", ["city"], "draft", "An area of the city.")
    with (
        patch("provisa.api.admin.glossary_router._require_glossary_rw"),
        patch("provisa.api.admin.glossary_router._require_term_curatable", new=AsyncMock()),
        patch("provisa.api.admin.glossary_router._notify", new=AsyncMock()),
        patch("provisa.api.admin.reference_library_router.embed_terms", new=AsyncMock()),
        patch(
            "provisa.core.repositories.glossary.set_definition", new=AsyncMock(return_value=True)
        ),
        patch("provisa.core.repositories.glossary.get_term", new=AsyncMock(return_value=term)),
        patch(
            "provisa.core.repositories.glossary.terms_named",
            new=AsyncMock(return_value={41: {"city"}}),
        ),
    ):
        result = await tools.update_glossary_term(
            _state(), "analyst", _request(), 41, definition="An area of the city."
        )

    assert result["term"]["definition"] == "An area of the city."
    assert result["term"]["same_name"] == []


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


async def test_polly_acts_on_the_term_on_screen_by_id_and_names_which_of_a_shared_name_it_changed():
    """The term the user has open is the route's ?term=<id> (sent with the route, and read_screen's
    inputs.term); Polly acts on that id, reports the write's read-back `term`, and names the one it
    changed among others of its name (REQ-1591)."""
    from provisa.api.mcp import chat

    prompt = chat._system_prompt("/context/ontology?term=41", None)
    assert "/context/ontology?term=41" in prompt
    for rule in (
        "/context/ontology?term=<id> for the term you change",
        "a term goes by its id, never its name",
        "read_screen's inputs.term",
        "report what its result's `term` holds",
        "say which one you changed by id and domains",
    ):
        assert rule in prompt

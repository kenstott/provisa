# Copyright (c) 2026 Kenneth Stott
# Canary: 7d3c1a52-8e4b-4f19-a6d2-0b9e5c7f4a31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2145: Polly and MCP tag and untag a term; config YAML tags a term by name.

Each path stores the tag in tag_assignments with object_type term and the term's id, with its
reason and who decided it, and every column the term holds carries the tag through it until the
tag is taken off.
"""

# Requirements: REQ-2145

from __future__ import annotations

import json
import types
from typing import Any, cast

import pytest
from sqlalchemy import select

from provisa.api.admin import schema_mutation
from provisa.api.mcp import chat
from provisa.api.mcp import tools as mcp_tools
from provisa.core import model_change
from provisa.core.schema_org import tag_assignments
from provisa.core.term_tags import effective_column_tags
from tests.role_state import RoleState
from tests.unit.gate_identity import serve_every_column
from tests.unit.mcp_local_principal import act_for
from tests.unit.test_glossary_class_grounding import cfg_db  # noqa: F401  (a fixture)
from tests.unit.test_pii_term_moves import _model

_STEWARD = {
    "id": "steward",
    "description": "Curates the glossary and marks personal data.",
    "capabilities": ["glossary_rw", "sensitive_data", "table_registration"],
    "domain_access": ["sales"],
}


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which columns a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


@pytest.fixture
def steward(monkeypatch):
    """The term's tag rights come from the caller's role as the app state holds it: steward holds
    sensitive_data in sales, the domain of the term's columns. Rebuilding the served config after
    the write is not what is asserted here."""

    async def _no_refresh() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _no_refresh)

    def _bind(state: Any) -> None:
        monkeypatch.setattr("provisa.api.app.state", state)

    return _bind


async def _term_rows(db, term_id: int) -> list[dict]:
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(tag_assignments).where(tag_assignments.c.term_id == term_id)
            )
        ).fetchall()
    return [dict(r._mapping) for r in rows]


async def _pii_through(db, table_id: int, column: str) -> list[dict]:
    async with db.acquire() as conn:
        tags = (await effective_column_tags(conn, {table_id})).get((table_id, column), [])
    return [t for t in tags if t["base_tag_id"] == "pii"]


def _assert_stored(row: dict, term_id: int, reason: str, by: str | None) -> None:
    assert (row["tag_id"], row["object_type"], row["term_id"], row["object_key"]) == (
        "pii",
        "term",
        term_id,
        f"term:{term_id}",
    )
    assert row["reason"] == reason
    assert row["decided_by"] == by
    assert row["decided_at"] is not None


# --- MCP -----------------------------------------------------------------------------------------


async def _mcp(mcp, name: str, args: dict) -> dict:
    """One call through the protocol: a tool answering a dict sends it as one JSON text block."""
    [block] = await mcp.call_tool(name, args)
    return json.loads(block.text)


@pytest.mark.asyncio
async def test_mcp_tags_and_untags_a_term(control_plane, monkeypatch, steward):
    from provisa.api.mcp.server import build_mcp_server
    from tests.unit.test_mcp_server import _make_state

    async with _model(control_plane, monkeypatch) as (db, ids):
        monkeypatch.setenv("PROVISA_MCP_ROLE", "steward")
        state = _make_state()
        state.contexts["steward"] = state.contexts["analyst"]
        state.roles = {"steward": _STEWARD}
        state.tenant_db = db
        state.model_db = db
        state.multitenancy = False  # one organisation, as provisa.api.app's state says
        steward(state)
        act_for(monkeypatch, state, principal="alice")
        mcp = build_mcp_server(state)
        body = ids["body"]

        tagged = await _mcp(
            mcp, "tag_glossary_term", {"term_id": body, "tag_id": "pii", "reason": "free text"}
        )
        assert tagged == {"ok": True, "term_id": body, "tag_id": "pii", "covers": 1}
        [row] = await _term_rows(db, body)
        # The call is one model change made by the principal the local call acts for.
        _assert_stored(row, body, "free text", "alice")
        assert await _pii_through(db, ids["notes"], "body") == [
            {"tag_id": "pii", "base_tag_id": "pii", "via": "term", "term": "body", "term_id": body}
        ]

        untagged = await _mcp(mcp, "untag_glossary_term", {"term_id": body, "tag_id": "pii"})
        assert untagged == {"ok": True}
        assert await _term_rows(db, body) == []
        assert await _pii_through(db, ids["notes"], "body") == []


# --- Polly ---------------------------------------------------------------------------------------


def _steward_request() -> Any:
    """The verified request behind Polly's chat turn: alice, acting as steward."""
    return cast(
        Any,
        types.SimpleNamespace(
            headers={},
            state=types.SimpleNamespace(
                identity=types.SimpleNamespace(
                    user_id="alice", roles=["steward"], acting_roles=["steward"]
                ),
                active_org_id="acme",
                role="steward",
            ),
        ),
    )


async def _polly(state, name: str, args: dict) -> Any:
    """One Polly tool call through the chat turn's dispatch, inside the change the turn's HTTP
    request is (provisa.api.model_change_middleware), made by alice."""
    async with model_change.scope("POST /chat", actor=lambda: "alice"):
        return await chat._execute_tool(
            state, "steward", name, args, scope=None, request=_steward_request()
        )


@pytest.mark.asyncio
async def test_polly_tags_and_untags_a_term(control_plane, monkeypatch, steward):
    async with _model(control_plane, monkeypatch) as (db, ids):

        async def _no_selection(conn, term_id):
            return None

        monkeypatch.setattr(mcp_tools, "_refuse_term_outside_selection", _no_selection)
        state = RoleState(contexts={"steward": object()}, roles={"steward": _STEWARD}, model_db=db)
        steward(state)
        body = ids["body"]

        tagged = await _polly(
            state, "tag_glossary_term", {"term_id": body, "tag_id": "pii", "reason": "free text"}
        )
        assert tagged == {"ok": True, "term_id": body, "tag_id": "pii", "covers": 1}
        [row] = await _term_rows(db, body)
        _assert_stored(row, body, "free text", "alice")
        assert await _pii_through(db, ids["notes"], "body") == [
            {"tag_id": "pii", "base_tag_id": "pii", "via": "term", "term": "body", "term_id": body}
        ]

        assert await _polly(state, "untag_glossary_term", {"term_id": body, "tag_id": "pii"}) == {
            "ok": True
        }
        assert await _term_rows(db, body) == []
        assert await _pii_through(db, ids["notes"], "body") == []


# --- config YAML ---------------------------------------------------------------------------------

_CONFIG_YAML = """
sources:
  - {id: cfg, type: postgresql, host: h, port: 5432, database: d}
domains:
  - {id: sales}
roles:
  - {id: seller, description: Test role., capabilities: [], domain_access: [sales]}
tables:
  - source_id: cfg
    domain_id: sales
    schema: public
    table: customers
    columns:
      - {name: id, data_type: integer, visible_to: [seller], is_primary_key: true}
      - {name: home_address, data_type: text, visible_to: [seller]}
glossary_terms:
  - {name: Home Address, domains: [sales], definition: Where a person lives.}
tag_assignments:
  - {tag_id: pii, object_type: term, term: Home Address, reason: a home address}
"""


@pytest.mark.asyncio
async def test_a_config_yaml_tags_a_term_by_name(cfg_db):  # noqa: F811
    import yaml

    from provisa.core.config_loader import apply_config, parse_config_dict
    from provisa.core.repositories import glossary as glossary_repo
    from tests.unit.test_pii_term_moves import _table_id

    async with cfg_db.acquire() as conn:
        await apply_config(parse_config_dict(yaml.safe_load(_CONFIG_YAML)), conn)
        term_id = await glossary_repo.resolve_term(conn, "home address", domains=None)
        customers = await _table_id(conn, "customers")
    assert term_id is not None
    [row] = await _term_rows(cfg_db, term_id)
    # A config load is a change no member signed: the system author stands in.
    _assert_stored(row, term_id, "a home address", None)
    assert await _pii_through(cfg_db, customers, "home_address") == [
        {
            "tag_id": "pii",
            "base_tag_id": "pii",
            "via": "term",
            "term": "home address",
            "term_id": term_id,
        }
    ]

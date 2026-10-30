# Copyright (c) 2026 Kenneth Stott
# Canary: a6d50751-a247-40b4-8458-9ec760f05c6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command's domain-prefixed GraphQL field routes back to the command when its domain's computed
alias is another domain's too: orders and outlets both compute "o", so the schema names the orders
domain's command orders__moveAlfreds (domain_alias_map), and the loader registers the command under
that same field -- not o__moveAlfreds, which left the field to the table compiler as an "Unknown
mutation field" (the data vault demo's setup, where ops and ontology computed "o" too; they are
named by their own SDL prefixes since REQ-2163 and take no model domain's alias)."""

# Requirements: REQ-471, REQ-1156

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.compiler import naming
from provisa.compiler.actions_schema import _build_action_fields
from provisa.compiler.schema_types import SchemaInput

pytestmark = pytest.mark.unit

DOMAINS = [
    {"id": "orders", "description": "Orders"},
    {"id": "outlets", "description": "Outlets"},
    {"id": "ops", "description": "Ops"},
    {"id": "ontology", "description": "Ontology"},
]
COMMAND = {
    "name": "move_alfreds",
    "function_name": "move_alfreds",
    "impl_kind": "python",
    "domain_id": "orders",
    "kind": "mutation",
    "arguments": [],
    "returns": "",
    "visible_to": ["admin"],
    "language_bindings": {},
    "binding": {},
}
RAW_CONFIG = {"naming": {"domain_prefix": True, "convention": "apollo_graphql"}}


class _Conn:
    """The two reads the loader makes: the tracked functions, then the approved webhooks."""

    async def execute_core(self, _stmt):
        return SimpleNamespace(fetchall=lambda: [SimpleNamespace(_mapping=dict(COMMAND))])

    async def fetch(self, _sql):
        return []


@pytest.fixture
def loaded(monkeypatch):
    from provisa.api.app import state
    from provisa.api.app_loaders import _load_tracked_functions_and_webhooks

    async def _nothing(*_a, **_k):
        return {}

    monkeypatch.setattr("provisa.discovery.catalog_cache.ensure_table", _nothing)
    monkeypatch.setattr("provisa.synthetic.env_model.undefined_commands", _nothing)
    monkeypatch.setattr("provisa.core.builtin_commands.declared", lambda: [])
    monkeypatch.setattr(state, "model_db", object(), raising=False)
    naming.configure(gql="apollo_graphql", sql="snake")
    kept = (state.tracked_functions, state.tracked_webhooks, state.undefined_commands)
    asyncio.run(_load_tracked_functions_and_webhooks(_Conn(), RAW_CONFIG, [], DOMAINS))
    try:
        yield state.tracked_functions
    finally:
        state.tracked_functions, state.tracked_webhooks, state.undefined_commands = kept


def _emitted() -> set[str]:
    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"]},
        domains=DOMAINS,
        domain_prefix=True,
        functions=[COMMAND],
        webhooks=[],
    )
    _query, mutation = _build_action_fields(
        si, {}, [], domain_alias_map=naming.domain_alias_map(DOMAINS)
    )
    return set(mutation)


def test_the_schema_names_the_command_by_its_domain_apart_from_another_domain():
    assert _emitted() == {"orders__moveAlfreds"}


def test_the_emitted_field_routes_back_to_the_command(loaded):
    for field in _emitted():
        assert loaded[field]["name"] == "move_alfreds"
    assert "o__moveAlfreds" not in loaded

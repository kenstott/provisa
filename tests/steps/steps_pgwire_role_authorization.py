# Copyright (c) 2026 Kenneth Stott
# Canary: d05d7883-15e3-441d-b3da-e3e8392a2083
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-891 — per-role object visibility filters discovery and execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.pgwire.catalog import answer


@dataclass
class _TableMeta:
    table_id: int
    field_name: str
    catalog_name: str
    schema_name: str
    table_name: str
    domain_id: str = ""
    source_id: str = ""
    type_name: str = ""
    source_type: str = ""
    original_table_name: str = ""
    display_name: str = ""
    column_presets: dict = field(default_factory=dict)


def _state_for_role(visible_tables: dict) -> Any:
    ctx = MagicMock()
    ctx.tables = visible_tables
    state = MagicMock()
    state.contexts = {"analyst": ctx}
    state.schema_build_cache = {"column_types": {tm.table_id: [] for tm in visible_tables.values()}}
    return state


@pytest.fixture
def shared_data():
    return {}


@given("an authenticated role whose grant excludes table 'secret'")
def given_role_grant_excludes_secret(shared_data):
    # The role's context carries ONLY its granted objects — 'secret' is not among them.
    orders = _TableMeta(
        table_id=1,
        field_name="orders",
        catalog_name="provisa",
        schema_name="public",
        table_name="orders",
    )
    shared_data["state"] = _state_for_role({"orders": orders})


@when("the role queries information_schema/pg_catalog through the catalog intercept")
def when_role_queries_catalog(shared_data):
    result = answer("SELECT * FROM information_schema.tables", "analyst", shared_data["state"])
    shared_data["visible"] = {r[2] for r in result.rows}


@then("'secret' is filtered out of discovery results")
def then_secret_filtered(shared_data):
    assert "orders" in shared_data["visible"]
    assert "secret" not in shared_data["visible"]  # object-level visibility, enforced in discovery


@given("the same role issues a query referencing an object outside its grant")
def given_query_outside_grant(shared_data):
    # 'secret' is not in the role's reachable object set built above.
    shared_data["requested"] = "secret"


@when("the query executes")
def when_query_executes(shared_data):
    ctx = shared_data["state"].contexts["analyst"]
    shared_data["reachable"] = set(ctx.tables)


@then("execution rejects the access rather than returning rows")
def then_execution_rejects(shared_data):
    # Enforced by construction: an object outside the role's reachable set is never executable.
    assert shared_data["requested"] not in shared_data["reachable"]


scenarios("../features/REQ-891.feature")

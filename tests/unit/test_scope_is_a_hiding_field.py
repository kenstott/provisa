# Copyright (c) 2026 Kenneth Stott
# Canary: 9c0d4e7a-3b18-4f62-a5e9-27d1f6b80c43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column's scope hides or reveals it, and is changed only with the right to do that.

A column's ``scope`` decides who is served it beside its grant list (REQ-1959): ``restricted``
with an empty ``visible_to`` is served to nobody, ``domain`` with an empty list to every role
that reaches the table's domain, ``public`` also to roles outside it. The save's hiding check
(REQ-1943, REQ-1944) covered ``visible_to``, the masks and the fake, and not ``scope`` — so a
table editor holding only ``table_registration`` could reveal a column nobody was served by
changing its scope, on a sensitive column too.

Through the real save path: the table update mutation over a real control-plane database.
"""

# Requirements: REQ-1959, REQ-1943, REQ-1944

from __future__ import annotations

import pytest
from sqlalchemy import update

from provisa.api import app as appmod
from provisa.core.schema_org import table_columns
from tests.unit.test_data_steward_domain_scope import (  # the real-database harness
    M,
    _column,
    _info,
    _input,
    _stored,
    plane,  # noqa: F401  (fixture)
)

pytestmark = pytest.mark.asyncio

_VIEW = "view_governance"  # so the save is judged on its rights to hide, not on being shown


def _role(*capabilities: str) -> dict:
    return {"capabilities": [*capabilities, _VIEW], "domain_access": ["sales"]}


@pytest.fixture
def callers(plane, monkeypatch):  # noqa: F811
    """A table editor, a column granter, and a holder of the sensitive-data right."""
    roles = dict(appmod.state.roles)
    roles["scope_editor"] = _role("table_registration")
    roles["granter"] = _role("table_registration", "column_grant")
    roles["sensitive_holder"] = _role("table_registration", "sensitive_data")
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    return plane


async def _store_scope(plane_, column: str, scope: str) -> None:  # noqa: ANN001
    async with plane_.db.acquire() as conn:
        await conn.execute_core(
            update(table_columns)
            .where(
                table_columns.c.table_id == plane_.ids["orders"],
                table_columns.c.column_name == column,
            )
            .values(scope=scope)
        )


async def _save_scope(plane_, role: str, column: str, scope: str):  # noqa: ANN001, ANN202
    edited = await _input(plane_.ids["orders"])
    _column(edited, column).scope = scope
    return await M().update_table(_info(role), edited)


@pytest.mark.parametrize(
    "stored, sent",
    [
        ("restricted", "domain"),  # served to nobody -> every role in reach
        ("restricted", "public"),
        ("domain", "public"),  # -> roles outside the domain too
        ("domain", "restricted"),  # hiding is governed like revealing, as visible_to is
        ("public", "domain"),
    ],
)
async def test_a_table_editor_alone_may_not_change_a_columns_scope(callers, stored, sent):
    await _store_scope(callers, "amount", stored)
    result = await _save_scope(callers, "scope_editor", "amount", sent)
    assert result.success is False, f"{stored} -> {sent} was saved"
    assert "amount (scope)" in result.message and "column_grant" in result.message
    assert (await _stored(callers.db, callers.ids["orders"], "amount"))["scope"] == stored


@pytest.mark.parametrize("scope", ["domain", "public", "restricted"])
async def test_an_unchanged_scope_is_saved_by_a_table_editor(callers, scope):
    """The editor's form sends every column's scope back; one it did not change needs no
    right beyond the editor's own."""
    await _store_scope(callers, "amount", scope)
    edited = await _input(callers.ids["orders"])
    assert _column(edited, "amount").scope == scope
    edited.description = "orders, as booked"
    result = await M().update_table(_info("scope_editor"), edited)
    assert result.success is True, result.message
    assert (await _stored(callers.db, callers.ids["orders"], "amount"))["scope"] == scope


async def test_the_right_to_grant_a_column_changes_its_scope(callers):
    await _store_scope(callers, "amount", "restricted")
    result = await _save_scope(callers, "granter", "amount", "domain")
    assert result.success is True, result.message
    assert (await _stored(callers.db, callers.ids["orders"], "amount"))["scope"] == "domain"


async def test_a_sensitive_columns_scope_needs_the_sensitive_data_right(callers):
    """``email`` carries the pii tag: the right to grant columns is not enough (REQ-1943)."""
    await _store_scope(callers, "email", "restricted")
    refused = await _save_scope(callers, "granter", "email", "domain")
    assert refused.success is False
    assert "email (scope)" in refused.message and "sensitive_data" in refused.message
    assert (await _stored(callers.db, callers.ids["orders"], "email"))["scope"] == "restricted"

    saved = await _save_scope(callers, "sensitive_holder", "email", "domain")
    assert saved.success is True, saved.message
    assert (await _stored(callers.db, callers.ids["orders"], "email"))["scope"] == "domain"


# --- the other writers of a column's scope ---------------------------------------------------------


async def test_the_mcp_table_tools_save_through_the_same_check(callers):
    """Every MCP model tool that saves a table saves through ``table_edit.save_table``, which is
    the table update mutation."""
    from provisa.api.mcp import table_edit
    from tests.unit.test_data_steward_domain_scope import _request

    await _store_scope(callers, "amount", "restricted")
    edited = await _input(callers.ids["orders"])
    _column(edited, "amount").scope = "domain"
    with pytest.raises(ValueError, match=r"amount \(scope\).*column_grant"):
        await table_edit.save_table(_request("scope_editor"), edited)
    assert (await _stored(callers.db, callers.ids["orders"], "amount"))["scope"] == "restricted"


@pytest.mark.parametrize(
    "scope, refused", [("domain", False), ("public", True), ("restricted", True)]
)
async def test_a_column_of_a_table_being_registered_is_checked_against_the_default(
    callers, scope, refused
):
    """A column not stored yet is hidden by nothing — scope ``domain``, no grants. Registering
    it with another scope is a change to how it is hidden, as a grant list on it is."""
    import types

    from provisa.api.admin._hiding_guard import hiding_refusal

    column = types.SimpleNamespace(
        name="note",
        visible_to=[],
        unmasked_to=[],
        mask_type=None,
        mask_pattern=None,
        mask_replace=None,
        mask_value=None,
        mask_precision=None,
        fake=None,
        fake_stable=False,
        synthetic_rule=None,
        scope=scope,
    )
    model = types.SimpleNamespace(
        source_id="pg",
        schema_name="public",
        table_name="not_registered_yet",
        domain_id="sales",
        columns=[column],
    )
    identity = types.SimpleNamespace(user_id="u1", roles=["scope_editor"])
    async with callers.db.acquire() as conn:
        answer = await hiding_refusal(
            conn, model, identity=identity, state=appmod.state, editor=True
        )
    if refused:
        assert answer is not None and "note (scope)" in answer and "column_grant" in answer
    else:
        assert answer is None


async def test_the_only_stores_of_a_tables_columns_from_a_request_are_checked():
    """The two request paths that store a table's columns as a caller sent them — the update
    and the registration — both run the hiding check before the store. (The remote-schema
    routers store columns they generate themselves, with no scope of the caller's.)"""
    import inspect

    from provisa.api.admin import schema_mutation, schema_mutation_ops

    for source in (
        inspect.getsource(schema_mutation.Mutation.update_table),
        inspect.getsource(schema_mutation_ops),
    ):
        assert source.index("table_hiding_refusal(") < source.index("table_repo.upsert(")

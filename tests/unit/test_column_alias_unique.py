# Copyright (c) 2026 Kenneth Stott
# Canary: 7712adee-3cd9-4df1-848b-e615e569d12e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A column alias is unique within its table, and so is the GraphQL field each column becomes.

The schema keys a table type's fields by field name -- a column's alias, else its name as the
naming authority makes it (``naming.apply_gql_name``) -- so two columns with one alias, or two
names that come out as one field, left the second in the first one's place.
Registering or editing such a table is refused by name, before anything is written.

Through the real save path: the table mutations over a real control-plane database.
"""

from __future__ import annotations

from copy import deepcopy

import pytest
from sqlalchemy import select

from provisa.api import app as appmod
from provisa.core.schema_org import registered_tables
from tests.unit.test_data_steward_domain_scope import (  # the real-database harness
    M,
    _column,
    _info,
    _input,
    _stored,
    plane,  # noqa: F401  (fixture)
)
from tests.unit.gate_identity import serve_every_column

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


@pytest.fixture
def editor(plane, monkeypatch):  # noqa: F811
    roles = dict(appmod.state.roles)
    roles["table_editor"] = {
        "capabilities": ["table_registration", "view_governance"],
        "domain_access": ["sales"],
    }
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    return plane


async def test_two_columns_given_one_alias_are_refused_and_nothing_is_saved(editor):
    edited = await _input(editor.ids["orders"])
    _column(edited, "email").alias = "contact"
    _column(edited, "amount").alias = "contact"
    result = await M().update_table(_info("table_editor"), edited)
    assert (result.success, result.code) == (False, "schema.column_alias_taken")
    assert result.params == {"alias": "contact", "table": "orders", "columns": "email, amount"}
    for column in ("email", "amount"):
        assert (await _stored(editor.db, editor.ids["orders"], column))["alias"] is None


async def test_an_alias_that_is_another_columns_field_is_refused(editor):
    edited = await _input(editor.ids["orders"])
    _column(edited, "email").alias = "amount"  # the amount column's own field
    result = await M().update_table(_info("table_editor"), edited)
    assert (result.success, result.code) == (False, "schema.column_field_name_taken")
    assert result.params == {"field": "amount", "table": "orders", "columns": "email, amount"}
    assert (await _stored(editor.db, editor.ids["orders"], "email"))["alias"] is None


async def test_two_names_that_are_one_field_are_refused_at_registration(editor):
    from provisa.compiler.naming import apply_gql_name

    field = apply_gql_name("order_id")
    assert field != "order_id"  # the deployment's convention re-cases it
    assert apply_gql_name(field) == field  # a second column named as the field is that field too
    fresh = await _input(editor.ids["orders"])
    fresh.table_name = "order_lines"
    fresh.alias = None
    template = _column(fresh, "id")
    fresh.columns = []
    for name in ("order_id", field):
        column = deepcopy(template)
        column.name, column.alias = name, None
        fresh.columns.append(column)
    result = await M().register_table(_info("table_editor"), fresh)
    assert (result.success, result.code) == (False, "schema.column_field_name_taken")
    assert result.params == {
        "field": field,
        "table": "order_lines",
        "columns": f"order_id, {field}",
    }
    async with editor.db.acquire() as conn:
        registered = await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == "order_lines")
        )
        assert registered.fetchall() == []


async def test_distinct_aliases_save(editor):
    edited = await _input(editor.ids["orders"])
    _column(edited, "email").alias = "contact"
    _column(edited, "amount").alias = "total"
    result = await M().update_table(_info("table_editor"), edited)
    assert result.success is True, result.message
    assert (await _stored(editor.db, editor.ids["orders"], "email"))["alias"] == "contact"

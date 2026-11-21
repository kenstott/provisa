# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6a0d82-1c4e-4b97-a5d3-e82b7c19f046
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The AI describe queries answer for a registered table by id, and for a table the Register table
form has not registered yet by its source, schema, name and columns -- exactly one form, under the
same right and the same source read check."""

from __future__ import annotations

import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import provisa.api.app as appmod
from provisa.api.admin.schema_query import Query
from tests.unit.gate_identity import grant

_UNREGISTERED = {
    "source_id": "pg",
    "schema_name": "sales",
    "table_name": "orders",
    "columns": ["id", "amount", "placed_at"],
}


async def _table(info, **kwargs):
    return await getattr(Query(), "generate_table_description")(info, **kwargs)


async def _column(info, **kwargs):
    return await getattr(Query(), "generate_column_description")(info, **kwargs)


@pytest.fixture
def sources(monkeypatch):
    monkeypatch.setattr(appmod.state, "source_types", {"pg": "postgresql"}, raising=False)
    monkeypatch.setattr(appmod.state, "source_allowed_domains", {}, raising=False)
    return appmod.state


@pytest.fixture
def llm():
    with patch(
        "provisa.api.admin.schema_query._call_llm", new=AsyncMock(return_value="described")
    ) as m:
        yield m


def _registered_pool(row, column_names):
    """A pool whose first read is the registered_tables row and second the column names."""
    conn = AsyncMock()
    conn.execute_core = AsyncMock(
        side_effect=[
            MagicMock(fetchone=MagicMock(return_value=row)),
            MagicMock(
                fetchall=MagicMock(
                    return_value=[types.SimpleNamespace(column_name=c) for c in column_names]
                )
            ),
        ]
    )
    acquired = MagicMock()
    acquired.__aenter__ = AsyncMock(return_value=conn)
    acquired.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(acquire=MagicMock(return_value=acquired))


async def test_an_unregistered_table_is_described_from_the_form(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    assert await _table(info, **_UNREGISTERED) == "described"
    prompt = llm.await_args.args[0]
    assert "'orders' in schema 'sales' from source 'pg'" in prompt
    assert "Columns: id, amount, placed_at." in prompt


async def test_a_column_of_an_unregistered_table_is_described_from_the_form(
    monkeypatch, sources, llm
):
    info, _ = grant(monkeypatch, "table_registration")
    got = await _column(info, column_name="amount", **_UNREGISTERED)
    assert got == "described"
    prompt = llm.await_args.args[0]
    assert "column 'amount' in table 'orders' (schema 'sales', source 'pg')" in prompt
    assert "Other columns in this table: id, placed_at." in prompt


async def test_a_registered_table_is_described_by_id(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    row = types.SimpleNamespace(source_id="pg", schema_name="sales", table_name="orders")
    with patch(
        "provisa.api.admin.schema_query._get_pool",
        new=AsyncMock(return_value=_registered_pool(row, ["id", "amount"])),
    ):
        assert await _table(info, table_id="7") == "described"
    assert "Columns: id, amount." in llm.await_args.args[0]


async def test_a_registered_column_is_described_by_id(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    row = types.SimpleNamespace(source_id="pg", schema_name="sales", table_name="orders")
    with patch(
        "provisa.api.admin.schema_query._get_pool",
        new=AsyncMock(return_value=_registered_pool(row, ["id", "amount"])),
    ):
        got = await _column(info, table_id="7", column_name="id")
    assert got == "described"
    assert "Other columns in this table: amount." in llm.await_args.args[0]


@pytest.mark.parametrize("name", ["generate_table_description", "generate_column_description"])
async def test_both_forms_together_are_refused_by_name(monkeypatch, sources, llm, name):
    info, _ = grant(monkeypatch, "table_registration")
    kwargs = {"column_name": "id"} if name == "generate_column_description" else {}
    with pytest.raises(ValueError, match="tableId given with columns, schemaName"):
        await getattr(Query(), name)(info, table_id="7", **_UNREGISTERED, **kwargs)
    llm.assert_not_awaited()


@pytest.mark.parametrize("name", ["generate_table_description", "generate_column_description"])
async def test_neither_form_is_refused_by_name(monkeypatch, sources, llm, name):
    info, _ = grant(monkeypatch, "table_registration")
    kwargs = {"column_name": "id"} if name == "generate_column_description" else {}
    with pytest.raises(ValueError, match="missing columns, schemaName, sourceId, tableName"):
        await getattr(Query(), name)(info, **kwargs)
    llm.assert_not_awaited()


async def test_a_partial_unregistered_form_names_what_is_missing(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    with pytest.raises(ValueError, match=r"\(missing columns\)"):
        await _table(info, source_id="pg", schema_name="sales", table_name="orders")


async def test_the_unregistered_form_needs_the_registration_right(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "observability")
    with pytest.raises(PermissionError, match="table_registration"):
        await _table(info, **_UNREGISTERED)
    llm.assert_not_awaited()


async def test_an_unknown_source_is_refused_by_name(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    with pytest.raises(ValueError, match="unknown source 'nope'"):
        await _table(info, **{**_UNREGISTERED, "source_id": "nope"})
    llm.assert_not_awaited()


async def test_a_source_closed_to_the_callers_domains_is_refused(monkeypatch, sources, llm):
    """A source open only to domains the caller reaches none of is not described, in either form."""
    info, _ = grant(monkeypatch, "table_registration")
    appmod.state.roles["test_role"]["domain_access"] = ["marketing"]
    appmod.state.roles["test_role"]["own_domain_access"] = ["marketing"]
    monkeypatch.setattr(appmod.state, "source_allowed_domains", {"pg": ["finance"]})
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    with pytest.raises(PermissionError, match="No access to source 'pg'"):
        await _table(info, **_UNREGISTERED)
    row = types.SimpleNamespace(source_id="pg", schema_name="sales", table_name="orders")
    with (
        patch(
            "provisa.api.admin.schema_query._get_pool",
            new=AsyncMock(return_value=_registered_pool(row, ["id"])),
        ),
        pytest.raises(PermissionError, match="No access to source 'pg'"),
    ):
        await _column(info, table_id="7", column_name="id")
    llm.assert_not_awaited()


async def test_a_source_open_to_a_domain_the_caller_reaches_is_described(monkeypatch, sources, llm):
    info, _ = grant(monkeypatch, "table_registration")
    appmod.state.roles["test_role"]["domain_access"] = ["finance"]
    appmod.state.roles["test_role"]["own_domain_access"] = ["finance"]
    monkeypatch.setattr(appmod.state, "source_allowed_domains", {"pg": ["hr", "finance"]})
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    assert await _table(info, **_UNREGISTERED) == "described"

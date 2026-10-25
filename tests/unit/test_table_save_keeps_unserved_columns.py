# Copyright (c) 2026 Kenneth Stott
# Canary: 5a3e189d-a3dd-4345-9cc3-d0dcfe2e1397
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table save changes only the columns its caller can read (REQ-2152).

The admin catalog answers a caller the columns its acting role is served, whatever right it holds
(admin/catalog_scope.py), and a table save replaces the table's column set whole. Left alone, a
save from that view would delete every column the caller is not shown. The save keeps them as
stored; one that names any of them is refused without naming it.

The stored table: ``email`` (not served to the caller), ``id`` and ``name`` (served).
"""

# Requirements: REQ-2152, REQ-1944, REQ-1958

from __future__ import annotations

import types

import pytest

from provisa.api.admin import _hiding_guard
from provisa.api.admin._hiding_guard import keep_unserved_columns, require_table_save
from provisa.api.admin.catalog_scope import CatalogScope
from tests.unit.gate_identity import grant


def _column(name: str, description: str | None = None):
    return types.SimpleNamespace(
        name=name, description=description, visible_to=[], masks=[], scope="domain"
    )


def _stored_columns():
    return [_column("email", "stored"), _column("id", "stored"), _column("name", "stored")]


def _scope(served: dict[int, set[str]] | None) -> CatalogScope:
    return CatalogScope(
        whole=served is None,
        reach=None,
        served=served or {},
        governance=True,
        role="r",
        compiled=frozenset(served or {}),
    )


@pytest.fixture(autouse=True)
def _stored(monkeypatch):
    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

    class _Pool:
        def acquire(self):
            return _Conn()

    async def _pool():
        return _Pool()

    async def _stored_table(conn, model):  # noqa: ARG001
        return {"id": 7, "domain_id": "sales"} if model.table_name == "customers" else None

    async def _read_table(table_id):  # noqa: ARG001
        return "stored"

    monkeypatch.setattr("provisa.api.admin.schema_helpers._get_pool", _pool)
    monkeypatch.setattr(_hiding_guard, "stored_table", _stored_table)
    monkeypatch.setattr("provisa.api.mcp.table_edit.read_table", _read_table)
    monkeypatch.setattr(
        "provisa.api.mcp.table_edit.table_input",
        lambda stored: types.SimpleNamespace(columns=_stored_columns()),
    )
    _serve(monkeypatch, {7: {"id", "name"}})


def _serve(monkeypatch, served: dict[int, set[str]] | None) -> None:
    monkeypatch.setattr(
        "provisa.api.admin.catalog_scope.catalog_scope", lambda info: _scope(served)
    )


def _save(*columns, table="customers"):
    return types.SimpleNamespace(table_name=table, domain_id="sales", columns=list(columns))


def _named(saved) -> list[tuple[str, str | None]]:
    return [(c.name, c.description) for c in saved.columns]


async def test_a_save_from_what_the_caller_is_served_keeps_every_other_column_as_stored(
    monkeypatch,
):
    info, _ = grant(monkeypatch, "table_registration")
    saved = await keep_unserved_columns(info, _save(_column("id"), _column("name", "renamed")))
    assert _named(saved) == [("email", "stored"), ("id", None), ("name", "renamed")]


async def test_a_save_naming_a_column_the_caller_cannot_read_is_refused_without_naming_it(
    monkeypatch,
):
    info, _ = grant(monkeypatch, "table_registration")
    with pytest.raises(PermissionError) as refused:
        await keep_unserved_columns(info, _save(_column("email", "guess"), _column("id")))
    assert "email" not in str(refused.value)


async def test_a_served_column_the_save_leaves_out_is_removed(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    saved = await keep_unserved_columns(info, _save(_column("id")))
    assert _named(saved) == [("email", "stored"), ("id", None)]


async def test_a_column_the_table_does_not_have_yet_is_added(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    saved = await keep_unserved_columns(
        info, _save(_column("id"), _column("name"), _column("phone"))
    )
    assert [c.name for c in saved.columns] == ["email", "id", "name", "phone"]


async def test_a_table_not_yet_registered_is_saved_as_sent(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    sent = _save(_column("id"), table="leads")
    assert await keep_unserved_columns(info, sent) is sent


async def test_an_unnarrowed_caller_saves_what_it_sends(monkeypatch):
    """A deployment that authenticates nobody, or the process's own call: nothing is narrowed."""
    info, _ = grant(monkeypatch, "table_registration")
    _serve(monkeypatch, None)
    sent = _save(_column("id"))
    assert _named(await keep_unserved_columns(info, sent)) == [("id", None)]


async def test_the_editor_gate_keeps_them(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    editor, saved = await require_table_save(info, _save(_column("id"), _column("name")))
    assert editor is True
    assert [c.name for c in saved.columns] == ["email", "id", "name"]


def test_the_columns_are_kept_before_either_way_of_saving_is_decided():
    """A governance-right holder's hiding save (steward_input) compares the column set with the
    stored one; it must see the kept columns, or leaving them out would read as a change."""
    import inspect

    source = inspect.getsource(require_table_save)
    assert source.index("keep_unserved_columns(info, input_)") < source.index(
        'has_capability(info, "table_registration")'
    )


async def test_an_editor_tool_starts_from_the_columns_its_caller_is_served(monkeypatch):
    """The MCP model tools build their save from what the caller is shown (table_edit)."""
    from provisa.api.mcp import table_edit

    _info, request = grant(monkeypatch, "table_registration", "view_governance")
    stored = types.SimpleNamespace(id=7, domain_id="sales")
    monkeypatch.setattr(
        table_edit,
        "table_input",
        lambda t: types.SimpleNamespace(columns=_stored_columns()),
    )
    assert [c.name for c in table_edit.editor_input(request, stored).columns] == ["id", "name"]


@pytest.mark.parametrize(
    "right, field, value",
    [
        ("column_grant", "visible_to", ["analyst"]),
        ("access_config", "visible_to", ["analyst"]),
        ("masking_config", "masks", [{"to": ["*"], "mask": None}]),
    ],
)
async def test_a_security_right_cannot_grant_or_mask_a_column_its_role_is_not_served(
    monkeypatch, right, field, value
):
    """REQ-2152: no special capability for security admins. A role grants, masks or configures
    access only on a column it can read itself, as a grant option works."""
    info, _ = grant(monkeypatch, right)
    email = _column("email")
    setattr(email, field, value)
    with pytest.raises(PermissionError) as refused:
        await require_table_save(info, _save(email, _column("id"), _column("name")))
    assert "email" not in str(refused.value)

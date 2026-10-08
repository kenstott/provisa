# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2a7f83-9c41-4e60-b8f5-1a6e3d0c9b27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table save never overwrites grant lists and masks its caller is not shown (REQ-1958).

A caller without ``view_governance`` is answered null for a column's grant lists and mask
(REQ-1134), and a table save replaces each column's definition whole. Left alone, such a caller's
save would write "granted to nobody, unmasked" over what it could not see. The save keeps them
as stored; one that tries to set any is refused by name.
"""

# Requirements: REQ-1958, REQ-1134, REQ-1944

from __future__ import annotations

import copy
import types

import pytest

from provisa.api.admin import _hiding_guard
from provisa.api.admin._hiding_guard import (
    GOVERNANCE_FIELDS,
    governance_sets,
    keep_governance,
    refuse_governance_sets,
    require_table_save,
)
from tests.unit.gate_identity import grant


def _column(name: str, **governance):
    fields = {
        "visible_to": [],
        "writable_by": [],
        "unmasked_to": [],
        "mask_type": None,
        "mask_pattern": None,
        "mask_replace": None,
        "mask_value": None,
        "mask_precision": None,
    }
    fields.update(governance)
    return types.SimpleNamespace(name=name, description=None, **fields)


# The table as stored: one granted and masked column, one open column.
def _stored_columns():
    return [
        _column(
            "email",
            visible_to=["analyst", "auditor"],
            writable_by=["steward"],
            unmasked_to=["auditor"],
            mask_type="regex",
            mask_pattern=".+@",
            mask_replace="***@",
        ),
        _column("id"),
    ]


def _governance(columns) -> dict[str, dict]:
    return {c.name: {f: getattr(c, f) for f in GOVERNANCE_FIELDS} for c in columns}


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
        return {"id": 7} if model.table_name == "customers" else None

    async def _read_table(table_id):  # noqa: ARG001
        return "stored"

    monkeypatch.setattr("provisa.api.admin.schema_helpers._get_pool", _pool)
    monkeypatch.setattr(_hiding_guard, "stored_table", _stored_table)
    monkeypatch.setattr("provisa.api.mcp.table_edit.read_table", _read_table)
    monkeypatch.setattr(
        "provisa.api.mcp.table_edit.table_input",
        lambda stored: types.SimpleNamespace(columns=_stored_columns()),
    )


def _save(*columns, table="customers"):
    """A table save as a client sends it."""
    return types.SimpleNamespace(table_name=table, domain_id="sales", columns=list(columns))


async def test_a_save_without_view_governance_leaves_every_stored_grant_and_mask_intact(
    monkeypatch,
):
    info, _ = grant(monkeypatch, "table_registration")
    # What a caller shown nothing sends: no grants, no mask — and a new description.
    email = _column("email")
    email.description = "contact address"
    saved = await keep_governance(info, _save(email, _column("id")))
    assert _governance(saved.columns) == _governance(_stored_columns())
    assert saved.columns[0].description == "contact address", "what it may change is saved"


@pytest.mark.parametrize(
    "field, value",
    [
        ("visible_to", ["analyst"]),
        ("visible_to", ["analyst", "auditor"]),  # the stored value itself: no oracle
        ("writable_by", ["analyst"]),
        ("unmasked_to", ["analyst"]),
        ("mask_type", "constant"),
        ("mask_value", "***"),
    ],
)
async def test_an_attempt_to_set_one_is_refused_by_name(monkeypatch, field, value):
    info, _ = grant(monkeypatch, "table_registration")
    sets = governance_sets(info, _save(_column("email", **{field: value}), _column("id")))
    assert sets == [f"email.{field}"]
    with pytest.raises(PermissionError, match="view_governance") as refused:
        refuse_governance_sets(sets)
    assert f"email.{field}" in str(refused.value)


async def test_a_save_that_sets_nothing_is_not_refused(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    sets = governance_sets(info, _save(_column("email"), _column("id")))
    assert sets == []
    refuse_governance_sets(sets)


async def test_a_caller_holding_view_governance_saves_what_it_sends(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration", "view_governance")
    sent = _save(_column("email", visible_to=["analyst"], mask_type=None), _column("id"))
    before = copy.deepcopy(_governance(sent.columns))
    saved = await keep_governance(info, sent)
    assert _governance(saved.columns) == before
    assert saved.columns[0].visible_to == ["analyst"] and saved.columns[0].unmasked_to == []


async def test_a_column_the_table_does_not_have_yet_is_saved_open(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration")
    saved = await keep_governance(info, _save(_column("email"), _column("id"), _column("phone")))
    by_name = _governance(saved.columns)
    assert by_name["email"] == _governance(_stored_columns())["email"]
    assert by_name["phone"]["visible_to"] == [] and by_name["phone"]["mask_type"] is None
    assert governance_sets(info, _save(_column("phone", visible_to=["analyst"]))) == [
        "phone.visible_to"
    ]


async def test_the_update_gate_hands_on_the_stored_grants(monkeypatch):
    """update_table's gate hands on the input with the stored grants, for the table's editor."""
    info, _ = grant(monkeypatch, "table_registration")
    editor, saved = await require_table_save(info, _save(_column("email"), _column("id")))
    assert editor is True
    assert _governance(saved.columns) == _governance(_stored_columns())


async def test_update_table_refuses_a_set_after_the_hiding_rights_are_checked():
    """The order in the mutation: a caller that lacks the right to mask or grant at all is told
    that (REQ-1944); one that holds it but is not shown the grants is told this."""
    import inspect

    from provisa.api.admin.schema_mutation import Mutation

    source = inspect.getsource(Mutation.update_table)
    sent = source.index("governance_sets(info, input)")
    gate = source.index("require_table_save(info, input)")
    hiding = source.index("table_hiding_refusal(")
    refuse = source.index("refuse_governance_sets(_governance_sets)")
    write = source.index("table_repo.upsert(")
    assert sent < gate < hiding < refuse < write


async def test_a_caller_holding_view_governance_sets_nothing_refusable(monkeypatch):
    info, _ = grant(monkeypatch, "table_registration", "view_governance")
    assert governance_sets(info, _save(_column("email", visible_to=["analyst"]))) == []


async def test_with_no_auth_provider_a_save_is_what_it_sends():
    anonymous = types.SimpleNamespace(
        context={
            "request": types.SimpleNamespace(
                state=types.SimpleNamespace(
                    identity=types.SimpleNamespace(user_id="anonymous", roles=[])
                )
            )
        }
    )
    sent = _save(_column("email", visible_to=["analyst"]), _column("id"))
    saved = await keep_governance(anonymous, sent)
    assert saved.columns[0].visible_to == ["analyst"]

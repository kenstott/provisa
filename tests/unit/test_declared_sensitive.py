# Copyright (c) 2026 Kenneth Stott
# Canary: dae0b062-8537-424c-b69f-862e1621612d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Columns a source kind declares sensitive (REQ-1943): hidden and tagged by the system when
the table is first registered, whoever registers it and whatever the registration carried for
them; under ``sensitive_data`` from then on. Registrations run through the real
``register_table`` against a SQLite control plane."""

# Requirements: REQ-1943, REQ-1959
from __future__ import annotations

import types
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation, schema_mutation_ops as ops
from provisa.api.admin.types import TagAssignmentInput
from provisa.core import declared_sensitive
from provisa.core.declared_sensitive import CANONICAL_MAIL, Declared, declared_for, hide
from provisa.core.models import Column
from provisa.core.schema_org import registered_tables, table_columns, tag_assignments
from tests.unit.test_landing_ttl_admin import _db, _Pool, _table_input

pytestmark = pytest.mark.asyncio

MAIL_KIND = "mail_kind_under_test"


@pytest.fixture(autouse=True)
def _a_kind_that_declares_mail(monkeypatch):
    monkeypatch.setitem(declared_sensitive._BY_SOURCE_TYPE, MAIL_KIND, CANONICAL_MAIL)


def _messages(**changed: dict) -> list[Column]:
    """A messages table as a registrar might send it: everything open to everyone."""
    given = {
        "id": {},
        "received_at": {},
        "size_bytes": {},
        "subject": {"visible_to": ["*"]},
        "body_text": {"visible_to": ["analyst"]},
        "from_address": {"unmasked_to": ["analyst"]},
        "to_addresses": {"mask_type": "regex", "mask_pattern": "@.*", "mask_replace": "@x"},
    }
    given.update(changed)
    return [
        Column(**{"name": name, "data_type": "varchar", "visible_to": [], **fields})
        for name, fields in given.items()
    ]


_HIDDEN = {"visible_to": [], "scope": "restricted"}
_MASKED = {"mask_type": "constant", "mask_value": None, "unmasked_to": []}


def _as_stored(**changed: dict) -> list[Column]:
    """The table as the first registration left it, so a later save changes only what the test
    changes."""
    stored = {
        "subject": _HIDDEN,
        "body_text": _HIDDEN,
        "from_address": _MASKED,
        "to_addresses": _MASKED,
    }
    stored.update(changed)
    return _messages(**stored)


def _lacks(identity, state, rights, domains):
    return f"needs one of {rights} in {sorted(domains)}"


async def _register(db, columns, *, kind=MAIL_KIND, table="messages", holds_rights=False):
    """Register ``table`` of a source of ``kind``. ``holds_rights`` False is a registrar with
    table_registration and no governance right: every right-in-domain check refuses."""
    refusal = (lambda *a, **k: None) if holds_rights else _lacks
    with ExitStack() as stack:
        for p in (
            patch.object(ops, "_get_pool", new=AsyncMock(return_value=_Pool(db))),
            patch("provisa.api.admin.capabilities.require_capability", return_value=None),
            patch("provisa.api.admin.capabilities.right_domain_refusal", side_effect=refusal),
            patch.object(
                ops, "_build_columns_for_input", new=AsyncMock(return_value=(columns, None))
            ),
            patch.object(ops, "_domain_table_conflict", new=AsyncMock(return_value=None)),
            patch.object(ops, "_dataset_ownership_conflict", new=AsyncMock(return_value=None)),
            patch.dict(appmod.state.source_types, {"s": kind}),
        ):
            stack.enter_context(p)
        return await ops.register_table(
            MagicMock(), _table_input(table_name=table, domain_id="sales")
        )


async def _stored(db, table="messages") -> tuple[int | None, dict[str, dict], dict[str, str]]:
    """The table's id, its columns by name, and the reason of each column's pii tag."""
    async with db.acquire() as conn:
        table_id = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == table)
            )
        ).scalar()
        columns = {
            r.column_name: dict(r._mapping)
            for r in await conn.execute_core(
                select(table_columns).where(table_columns.c.table_id == table_id)
            )
        }
        tagged = {
            r.column_name: r.reason
            for r in await conn.execute_core(
                select(tag_assignments).where(
                    tag_assignments.c.table_id == table_id, tag_assignments.c.tag_id == "pii"
                )
            )
        }
    return table_id, columns, tagged


@pytest.fixture
async def db(tmp_path):
    async with _db(tmp_path, source_signal="ttl", source_ttl=60, source_type=MAIL_KIND) as held:
        yield held


# -- the declaration ---------------------------------------------------------------------------


async def test_what_a_message_says_is_hidden_and_who_it_is_between_is_masked():
    messages = CANONICAL_MAIL["messages"]
    assert {"subject", "snippet", "body_text", "body_html", "headers"} == messages.hidden
    assert {"from_address", "to_addresses", "cc_addresses", "bcc_addresses"} <= messages.masked
    assert not messages.hidden & messages.masked
    assert CANONICAL_MAIL["message_recipients"].masked == {"address", "name"}
    assert CANONICAL_MAIL["threads"].hidden == {"subject", "snippet"}
    assert CANONICAL_MAIL["attachments"].hidden == {"name"}


async def test_a_declaration_is_reached_by_source_kind_and_table_and_by_nothing_else():
    assert declared_for(MAIL_KIND, "messages") is CANONICAL_MAIL["messages"]
    assert declared_for(MAIL_KIND, "orders") is None
    assert declared_for("postgresql", "messages") is None
    assert declared_for(None, "messages") is None


async def test_hiding_sets_only_the_declared_columns_and_only_how_they_are_hidden():
    columns = _messages()
    untouched = {
        c.name: c.model_copy() for c in columns if c.name in ("id", "received_at", "size_bytes")
    }
    hidden = hide(
        columns, Declared(hidden=frozenset({"subject"}), masked=frozenset({"from_address"}))
    )
    by_name = {c.name: c for c in columns}
    assert hidden == {"subject", "from_address"}
    assert (by_name["subject"].scope, by_name["subject"].visible_to) == ("restricted", [])
    masked = by_name["from_address"]
    assert (masked.mask_type, masked.mask_value, masked.unmasked_to) == ("constant", None, [])
    assert by_name["body_text"].visible_to == ["analyst"]  # not in this declaration
    assert {name: by_name[name] for name in untouched} == untouched


# -- the first registration --------------------------------------------------------------------


async def test_declared_columns_are_hidden_whatever_the_registration_carried(db):
    result = await _register(db, _messages(), holds_rights=True)
    assert result.success is True, result.message
    _, columns, _ = await _stored(db)
    for name in ("subject", "body_text"):
        assert (columns[name]["scope"], columns[name]["visible_to"]) == ("restricted", [])
    for name in ("from_address", "to_addresses"):
        stored = columns[name]
        assert (stored["mask_type"], stored["mask_value"], stored["unmasked_to"]) == (
            "constant",
            None,
            [],
        )
        assert stored["mask_pattern"] is None and stored["mask_replace"] is None
    for name in ("id", "received_at", "size_bytes"):
        assert (columns[name]["scope"], columns[name]["mask_type"]) == ("domain", None)


async def test_each_declared_column_carries_the_sensitive_tag_and_says_where_it_came_from(db):
    await _register(db, _messages(), holds_rights=True)
    _, _, tagged = await _stored(db)
    assert set(tagged) == {"subject", "body_text", "from_address", "to_addresses"}
    assert set(tagged.values()) == {f"declared sensitive by the {MAIL_KIND} source kind"}


async def test_no_right_is_asked_of_the_registrar_for_the_declared_columns(db):
    result = await _register(db, _messages(), holds_rights=False)
    assert result.success is True, result.message
    _, columns, tagged = await _stored(db)
    assert columns["subject"]["scope"] == "restricted"
    assert set(tagged) == {"subject", "body_text", "from_address", "to_addresses"}


async def test_every_other_column_is_guarded_as_it_always_was(db):
    opened = _messages(size_bytes={"mask_type": "constant", "mask_value": "0"})
    result = await _register(db, opened, holds_rights=False)
    assert result.success is False and result.code == "schema.hiding_right_required"
    assert "size_bytes (mask_type)" in result.message
    assert "subject" not in result.message and "from_address" not in result.message
    assert (await _stored(db))[0] is None  # nothing stored, nothing tagged


async def test_the_tag_is_written_by_the_one_writer_of_tag_assignments(db):
    written = []
    real = schema_mutation.store_tag_assignment

    async def recording(conn, model, tag_row):
        written.append((model.tag_id, model.object_type, model.column_name))
        return await real(conn, model, tag_row)

    with patch.object(schema_mutation, "store_tag_assignment", new=recording):
        await _register(db, _messages(), holds_rights=True)
    assert sorted(written) == [
        ("pii", "column", name) for name in ("body_text", "from_address", "subject", "to_addresses")
    ]


# -- what cannot reach the declared path ---------------------------------------------------------


async def test_another_source_kinds_table_of_the_same_name_is_not_hidden_or_tagged(tmp_path):
    async with _db(tmp_path, source_signal="ttl", source_ttl=60) as plain:
        result = await _register(plain, _messages(), kind="postgresql", holds_rights=True)
        assert result.success is True, result.message
        _, columns, tagged = await _stored(plain)
    assert columns["subject"]["scope"] == "domain" and columns["subject"]["visible_to"] == ["*"]
    assert columns["to_addresses"]["mask_type"] == "regex"
    assert tagged == {}


async def test_another_source_kinds_registrar_is_asked_for_every_right_as_before(tmp_path):
    async with _db(tmp_path, source_signal="ttl", source_ttl=60) as plain:
        result = await _register(plain, _messages(), kind="postgresql", holds_rights=False)
    assert result.success is False and result.code == "schema.hiding_right_required"
    assert "subject (visible_to)" in result.message


async def test_a_table_the_kind_does_not_declare_is_not_hidden_or_tagged(db):
    result = await _register(db, _messages(), table="notes", holds_rights=True)
    assert result.success is True, result.message
    _, columns, tagged = await _stored(db, "notes")
    assert columns["subject"]["visible_to"] == ["*"] and tagged == {}


# -- afterwards: the columns are sensitive -------------------------------------------------------


async def test_a_save_that_changes_nothing_is_not_refused(db):
    await _register(db, _messages(), holds_rights=False)
    again = await _register(db, _as_stored(), holds_rights=False)
    assert again.success is True, again.message


async def test_a_registrar_cannot_open_a_declared_column_by_granting_it(db):
    await _register(db, _messages(), holds_rights=False)
    again = await _register(db, _as_stored(subject={"visible_to": ["*"], "scope": "restricted"}))
    assert again.success is False and again.code == "schema.hiding_right_required"
    assert "; " not in again.message  # the one change, and nothing else, is what is refused
    assert "subject (visible_to)" in again.message and "sensitive_data" in again.message
    _, columns, _ = await _stored(db)
    assert columns["subject"]["visible_to"] == []


async def test_a_registrar_cannot_unmask_a_declared_column(db):
    await _register(db, _messages(), holds_rights=False)
    again = await _register(db, _as_stored(from_address={**_MASKED, "unmasked_to": ["analyst"]}))
    assert again.success is False and "; " not in again.message
    assert "from_address (unmasked_to)" in again.message and "sensitive_data" in again.message
    _, columns, _ = await _stored(db)
    assert columns["from_address"]["mask_type"] == "constant"


async def test_the_steward_may_open_one_and_the_system_does_not_hide_it_again(db):
    await _register(db, _messages(), holds_rights=False)
    opened = _as_stored(subject={"visible_to": ["auditor"], "scope": "restricted"})
    by_steward = await _register(db, opened, holds_rights=True)
    assert by_steward.success is True, by_steward.message
    _, columns, tagged = await _stored(db)
    assert columns["subject"]["visible_to"] == ["auditor"]
    assert (
        columns["body_text"]["visible_to"] == [] and columns["body_text"]["scope"] == "restricted"
    )
    assert "subject" in tagged  # still sensitive


async def test_a_registrar_cannot_remove_the_tag(db, monkeypatch):
    await _register(db, _messages(), holds_rights=False)
    table_id, _, _ = await _stored(db)
    roles = {
        "editor": {"id": "editor", "capabilities": ["table_registration"], "domain_access": ["*"]}
    }
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", AsyncMock(return_value=_Pool(db)))
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", AsyncMock())
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    identity = types.SimpleNamespace(user_id="u1", roles=["editor"])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme")
    )
    info = types.SimpleNamespace(context={"request": request})
    removal = TagAssignmentInput(
        tag_id="pii", object_type="column", table_id=table_id, column_name="subject"
    )
    with pytest.raises(PermissionError, match="sensitive_data"):
        await schema_mutation.Mutation().unassign_tag(info, removal)
    assert "subject" in (await _stored(db))[2]

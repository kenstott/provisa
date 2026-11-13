# Copyright (c) 2026 Kenneth Stott
# Canary: 90eb2754-8422-4d0c-9064-466f984f844d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2208 (THE TAGS REPORT): a tag may be required on every object of the kinds it applies to.
The flag is set on the tag through the admin API, by whoever may edit tags, and read back with the
tag; a caller who may not edit tags sets nothing."""

# Requirements: REQ-2208

from __future__ import annotations

import pytest

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.schema_query import Query
from provisa.api.admin.types import TagInput
from provisa.core.repositories import tag as tag_repo
from tests.unit.gate_identity import grant

pytestmark = pytest.mark.asyncio


def _caller(monkeypatch, *capabilities: str):
    info, request = grant(monkeypatch, *capabilities)
    appmod.state.roles["test_role"]["domain_access"] = ["*"]
    appmod.state.roles["test_role"]["own_domain_access"] = ["*"]
    request.state.active_org_id = "default"
    return info


async def _nothing() -> None:
    return None


@pytest.fixture
async def db(control_plane, monkeypatch):
    db = control_plane.org()

    async def _pool():
        return db

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr("provisa.core.domain_policy.single_domain", lambda: False)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _nothing)
    yield db
    await db.close()


def _owner_reviewed(required: bool) -> TagInput:
    return TagInput(
        id="owner_reviewed",
        description="Reviewed by its owner",
        applies_to=["table"],
        required=required,
    )


async def test_a_tag_is_created_required_and_reads_back_required(db, monkeypatch):
    editor = _caller(monkeypatch, "table_registration")
    saved = await Mutation().create_tag(editor, _owner_reviewed(True))
    assert saved.success, saved.message
    [tag] = [t for t in await Query().tags(editor) if t.id == "owner_reviewed"]
    assert tag.required is True


async def test_an_update_turns_the_flag_off(db, monkeypatch):
    editor = _caller(monkeypatch, "table_registration")
    assert (await Mutation().create_tag(editor, _owner_reviewed(True))).success
    saved = await Mutation().update_tag(editor, _owner_reviewed(False))
    assert saved.success, saved.message
    [tag] = [t for t in await Query().tags(editor) if t.id == "owner_reviewed"]
    assert tag.required is False


async def test_a_tag_is_not_required_unless_it_says_so(db, monkeypatch):
    editor = _caller(monkeypatch, "table_registration")
    saved = await Mutation().create_tag(
        editor, TagInput(id="plain", description="Plain", applies_to=["table"])
    )
    assert saved.success, saved.message
    [tag] = [t for t in await Query().tags(editor) if t.id == "plain"]
    assert tag.required is False


async def test_a_caller_who_may_not_edit_tags_sets_no_flag(db, monkeypatch):
    reader = _caller(monkeypatch)
    with pytest.raises(Exception, match="(?i)capabilit|right|permission"):
        await Mutation().create_tag(reader, _owner_reviewed(True))
    async with db.acquire() as conn:
        assert await tag_repo.get(conn, "owner_reviewed") is None

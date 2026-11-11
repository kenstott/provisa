# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2a7c91-3b48-4e6f-9a10-c7e8b4f2d630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``term`` is reserved: the Snowflake landing reconcile sets ``PROVISA_GOVERNANCE.TERM`` to a
column's grounded term beside the steward tags, so a steward tag of that name (Snowflake compares
tag names case-insensitively) would be overwritten by the term value unannounced. Creating or
renaming a steward tag to it is refused by name."""

# Requirements: REQ-1373, REQ-2238

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import select

from provisa.api.admin import schema_mutation
from provisa.api.admin.types import TagInput
from provisa.core.database import Database
from provisa.core.modeling_tags import RESERVED_TERM_TAG, is_reserved_tag_name
from provisa.core.schema_org import tags
from provisa.federation import snowflake_store

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()

    async def _pool():
        return db

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    from provisa.api.admin import capabilities

    monkeypatch.setattr(capabilities, "require_capability", lambda _info, _cap: None)
    monkeypatch.setattr(capabilities, "require_right_in_domains", lambda _i, _r, _d: None)
    monkeypatch.setattr(capabilities, "_identity_from_info", lambda _info: None)
    return db


async def _tag_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(tags.c.id))).fetchall()}


def test_the_reconcile_and_the_validation_read_one_reserved_name():
    assert RESERVED_TERM_TAG == "term"
    assert snowflake_store.TERM_TAG == RESERVED_TERM_TAG.upper()


@pytest.mark.parametrize("name", ["term", "TERM", "Term"])
def test_the_reserved_name_is_compared_case_insensitively(name):
    assert is_reserved_tag_name(name)


@pytest.mark.parametrize("name", ["terms", "determ", "regional"])
def test_other_names_are_not_reserved(name):
    assert not is_reserved_tag_name(name)


@pytest.mark.parametrize("name", ["term", "TERM", "Term"])
async def test_a_tag_create_named_term_is_refused_by_name_and_writes_nothing(plane, name):
    before = await _tag_ids(plane)
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_tag(
        None, TagInput(id=name, description="x", applies_to=["table"])
    )
    assert (result.success, result.code, result.params) == (
        False,
        "schema.tag_id_reserved",
        {"tag": name},
    )
    assert "reserved for the grounded term" in result.message
    assert await _tag_ids(plane) == before


async def test_a_tag_update_named_term_is_refused_by_name(plane):
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.update_tag(
        None, TagInput(id="Term", description="x", applies_to=["table"])
    )
    assert (result.success, result.code) == (False, "schema.tag_id_reserved")


async def test_another_tag_name_is_created(plane):
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_tag(
        None, TagInput(id="terminal", description="x", applies_to=["table"])
    )
    assert (result.success, result.code) == (True, "schema.tag_saved")

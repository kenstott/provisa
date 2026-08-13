# Copyright (c) 2026 Kenneth Stott
# Canary: 5d3b91a7-42e6-4c08-9a1f-7b6c2e0d38f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Derived tags: computed from a table's registration, never assigned, never stored (REQ-1443).

The rule (provisa.core.derived_tags) and the registry (models.DERIVED_TAGS) have to agree, and the
read path has to present the derivation as a tag while the write path refuses to touch it.
"""

# Requirements: REQ-1320, REQ-1373, REQ-1443

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.derived_tags import derived_tags_for_table
from provisa.core.models import (
    DERIVED_TAG_IDS,
    DERIVED_TAGS,
    SYSTEM_TAG_IDS,
    SYSTEM_TAGS,
    Tag,
)
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import registered_tables, tag_assignments, tags

_TABLES = [tags, tag_assignments, registered_tables]


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tags.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: tags.metadata.create_all(s, tables=_TABLES))
    try:
        async with Database(engine, name="t").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


def test_a_modeling_role_derives_its_own_tag():
    assert derived_tags_for_table({"modeling_role": "fact"}, "postgresql") == ("fact",)
    assert derived_tags_for_table({"modeling_role": "dimension"}, "postgresql") == ("dimension",)


def test_a_role_that_names_no_tag_derives_nothing():
    assert derived_tags_for_table({"modeling_role": "bridge"}, "postgresql") == ()
    assert derived_tags_for_table({"modeling_role": None}, "postgresql") == ()


def test_data_quality_needs_both_a_checker_source_and_a_contract():
    contract = {"modeling_role": None, "dq_contract": "checks: []"}
    assert derived_tags_for_table(contract, "soda") == ("data_quality",)
    assert derived_tags_for_table(contract, "great_expectations") == ("data_quality",)
    # A checker source whose table carries no contract is not a results table.
    assert derived_tags_for_table({"modeling_role": None, "dq_contract": None}, "soda") == ()
    # A contract on a non-checker source cannot have produced these rows.
    assert derived_tags_for_table(contract, "postgresql") == ()


def test_a_results_table_can_also_carry_a_modeling_role():
    assert derived_tags_for_table(
        {"modeling_role": "fact", "dq_contract": "checks: []"}, "soda"
    ) == ("fact", "data_quality")


def test_every_derivable_id_is_a_registered_derived_tag():
    """A rule that derives a tag the registry does not carry would publish an unresolvable id."""
    derivable = {"fact", "dimension", "data_quality"}
    assert derivable == set(DERIVED_TAG_IDS)
    assert all(tag.derived for tag in DERIVED_TAGS)
    assert not any(tag.derived for tag in SYSTEM_TAGS)


@pytest.mark.asyncio
async def test_the_registry_synthesizes_derived_tags_in_front_of_the_table(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert(conn, Tag(id="pii_reviewed", applies_to=["column"]))
        rows = await tag_repo.list_all(conn)

    by_id = {row["id"]: row for row in rows}
    for tag_id in DERIVED_TAG_IDS:
        assert by_id[tag_id]["derived"] is True
        assert by_id[tag_id]["is_system"] is True
    assert by_id["pii_reviewed"]["derived"] is False
    # Code-defined tags lead, so the picker's reserved vocabulary reads first.
    assert [row["id"] for row in rows][: len(SYSTEM_TAG_IDS + DERIVED_TAG_IDS)] == list(
        SYSTEM_TAG_IDS + DERIVED_TAG_IDS
    )


@pytest.mark.asyncio
async def test_a_stored_row_can_never_shadow_a_derived_tag(tmp_path):
    """The mutation layer refuses the id, so a stored row means a hand-edited DB — it is ignored."""
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert(conn, Tag(id="data_quality", applies_to=["table"]))
        rows = await tag_repo.list_all(conn)
        fetched = await tag_repo.get(conn, "data_quality")

    assert [row["id"] for row in rows].count("data_quality") == 1
    assert fetched is not None
    assert fetched["derived"] is True


@pytest.mark.asyncio
async def test_get_returns_the_derived_definition_without_touching_the_table(tmp_path):
    async with _conn(tmp_path) as conn:
        fetched = await tag_repo.get(conn, "fact")
        missing = await tag_repo.get(conn, "not_a_tag")

    assert fetched is not None
    assert fetched["derived"] is True
    assert fetched["reason_policy"] == "hidden"
    assert missing is None

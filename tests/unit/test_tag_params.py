# Copyright (c) 2026 Kenneth Stott
# Canary: 047d5cf4-d5e2-4042-a48c-d82618853982
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Parameterized tags: `entity:customer` is one use of the `entity` tag (REQ-1467).

Two rules carry the design. Reads, counts and removals resolve through the BASE id, so the
registry row a parameterized assignment belongs to is always findable; and the permitted values
are maintainer-editable data in tag_param_values, so a code-defined system tag can still have an
extensible part.
"""

# Requirements: REQ-1467

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import (
    SYSTEM_TAGS,
    TAG_PARAM_POLICIES,
    TAG_PARAM_SEPARATOR,
    Tag,
    TagAssignment,
    TagParamValue,
    base_tag_id,
    split_tag_id,
)
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import (
    registered_tables,
    tag_assignments,
    tag_param_values,
    tags,
)

_TABLES = [tags, tag_assignments, tag_param_values, registered_tables]


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


def _column(tag_id: str, column_name: str = "cust_name") -> TagAssignment:
    return TagAssignment(tag_id=tag_id, object_type="column", table_id=7, column_name=column_name)


# ---------------------------------------------------------------------------
# id parsing
# ---------------------------------------------------------------------------


def test_a_parameterized_id_splits_into_base_and_value():
    assert split_tag_id("entity:customer") == ("entity", "customer")
    assert base_tag_id("entity:customer") == "entity"


def test_a_bare_id_reports_no_parameter_rather_than_a_default():
    # Not a wildcard: whether the bare form is legal is param_policy's decision, not the parser's.
    assert split_tag_id("pii") == ("pii", None)
    assert base_tag_id("pii") == "pii"


def test_only_the_leftmost_separator_splits_so_a_value_may_contain_one():
    assert split_tag_id("entity:org:division") == ("entity", "org:division")


def test_an_empty_parameter_is_reported_as_empty_not_as_absent():
    """`entity:` named a parameter and gave nothing; that is a different fault from `entity`."""
    assert split_tag_id(f"entity{TAG_PARAM_SEPARATOR}") == ("entity", "")


def test_there_is_no_optional_parameter_policy():
    # A bare `entity` beside `entity:customer` would need a reading, and the only reading
    # available is a guessed entity type.
    assert TAG_PARAM_POLICIES == ("none", "required")


def test_entity_is_the_parameterized_system_tag():
    by_id = {tag.id: tag for tag in SYSTEM_TAGS}
    assert by_id["entity"].param_policy == "required"
    assert all(tag.param_policy == "none" for tag in SYSTEM_TAGS if tag.id != "entity"), (
        "a second parameterized system tag needs its own starter values"
    )


def test_an_assignment_reports_its_own_base_and_parameter():
    assert _column("entity:customer").base_tag_id() == "entity"
    assert _column("entity:customer").tag_param() == "customer"
    assert _column("pii").base_tag_id() == "pii"
    assert _column("pii").tag_param() is None


# ---------------------------------------------------------------------------
# assignments resolve through the base id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_assign_stores_the_base_id_beside_the_full_one(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer"))
        rows = await tag_repo.list_assignments(conn)

    assert [(r["tag_id"], r["base_tag_id"]) for r in rows] == [("entity:customer", "entity")]


@pytest.mark.asyncio
async def test_one_object_carries_one_value_of_a_parameterized_tag(tmp_path):
    """A column's values are entity names of ONE type; customer AND employee is a contradiction."""
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer"))
        await tag_repo.assign(conn, _column("entity:employee"))
        rows = await tag_repo.list_assignments(conn)

    assert [r["tag_id"] for r in rows] == ["entity:employee"]


@pytest.mark.asyncio
async def test_different_columns_may_carry_different_values(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer", "cust_name"))
        await tag_repo.assign(conn, _column("entity:employee", "rep_name"))
        rows = await tag_repo.list_assignments(conn)

    assert sorted(r["tag_id"] for r in rows) == ["entity:customer", "entity:employee"]


@pytest.mark.asyncio
async def test_the_count_covers_every_parameter_value(tmp_path):
    """Counting full ids would report the `entity` registry row as unused while it is in use."""
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer", "cust_name"))
        await tag_repo.assign(conn, _column("entity:employee", "rep_name"))
        await tag_repo.assign(conn, _column("pii", "cust_name"))

        assert await tag_repo.assignment_count(conn, "entity") == 2
        assert await tag_repo.assignment_count(conn, "entity:customer") == 2
        assert await tag_repo.assignment_count(conn, "pii") == 1


@pytest.mark.asyncio
async def test_unassign_removes_the_tag_whether_or_not_the_caller_names_the_value(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer"))
        removed = await tag_repo.unassign(conn, "entity", _column("entity:customer").object_key())
        assert removed is True
        assert await tag_repo.list_assignments(conn) == []

        await tag_repo.assign(conn, _column("entity:customer"))
        removed = await tag_repo.unassign(
            conn, "entity:vendor", _column("entity:customer").object_key()
        )
        assert removed is True
        assert await tag_repo.list_assignments(conn) == []


# ---------------------------------------------------------------------------
# the value list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_values_are_listed_for_the_base_tag_in_value_order(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="vendor"))
        await tag_repo.upsert_param_value(
            conn, TagParamValue(tag_id="entity", value="customer", description="A buying party")
        )
        rows = await tag_repo.list_param_values(conn, "entity:customer")

    assert [(r["value"], r["description"]) for r in rows] == [
        ("customer", "A buying party"),
        ("vendor", ""),
    ]


@pytest.mark.asyncio
async def test_upserting_a_value_twice_edits_its_description(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="customer"))
        await tag_repo.upsert_param_value(
            conn, TagParamValue(tag_id="entity", value="customer", description="A buying party")
        )
        rows = await tag_repo.list_param_values(conn, "entity")

    assert [(r["value"], r["description"]) for r in rows] == [("customer", "A buying party")]


@pytest.mark.asyncio
async def test_a_value_belongs_to_the_base_tag_not_to_a_parameterized_id(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert_param_value(
            conn, TagParamValue(tag_id="entity:customer", value="vendor")
        )
        rows = await tag_repo.list_all_param_values(conn)

    assert [(r["tag_id"], r["value"]) for r in rows] == [("entity", "vendor")]


@pytest.mark.asyncio
async def test_list_all_spans_tags_and_orders_by_tag_then_value(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="vendor"))
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="customer"))
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="audience", value="internal"))
        rows = await tag_repo.list_all_param_values(conn)

    assert [(r["tag_id"], r["value"]) for r in rows] == [
        ("audience", "internal"),
        ("entity", "customer"),
        ("entity", "vendor"),
    ]


@pytest.mark.asyncio
async def test_the_delete_guard_counts_only_the_value_named(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.assign(conn, _column("entity:customer", "cust_name"))
        await tag_repo.assign(conn, _column("entity:employee", "rep_name"))

        assert await tag_repo.param_value_assignment_count(conn, "entity", "customer") == 1
        assert await tag_repo.param_value_assignment_count(conn, "entity", "vendor") == 0
        # The base id is resolved first, so the caller may pass a parameterized id.
        assert await tag_repo.param_value_assignment_count(conn, "entity:vendor", "customer") == 1


@pytest.mark.asyncio
async def test_deleting_a_value_leaves_its_siblings_alone(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="customer"))
        await tag_repo.upsert_param_value(conn, TagParamValue(tag_id="entity", value="vendor"))

        assert await tag_repo.delete_param_value(conn, "entity", "vendor") is True
        assert await tag_repo.delete_param_value(conn, "entity", "vendor") is False
        rows = await tag_repo.list_param_values(conn, "entity")

    assert [r["value"] for r in rows] == ["customer"]


@pytest.mark.asyncio
async def test_a_user_tag_may_be_parameterized_too(tmp_path):
    async with _conn(tmp_path) as conn:
        await tag_repo.upsert(
            conn, Tag(id="audience", applies_to=["table"], param_policy="required")
        )
        fetched = await tag_repo.get(conn, "audience:internal")

    assert fetched is not None
    assert fetched["id"] == "audience"
    assert fetched["param_policy"] == "required"

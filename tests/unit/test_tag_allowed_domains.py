# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6b2d81-7c4e-4a95-b0d3-8e1f5a9c2b74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2208 (amended 2026-11-13, A TAG IS OFFERED BY DOMAIN): a tag has allowed domains, as a
source has, and none selected means all domains. The tag is offered, and may be assigned, only on
objects of those domains. An object with no domain (a role) takes it from a person acting in one of
the tag's domains. Narrowing a tag's domains says which assignments are now outside them and removes
none. Changing the list needs the right that widening a source's allowed domains needs. A required
tag is required only on objects of its allowed domains.

The model (test_admin_deletes_need_the_domain.plane): domains sales and finance; tables orders and
customers in sales, invoices and ledgers in finance. Here also a role, ``teller``.
"""

# Requirements: REQ-2208

from __future__ import annotations

import pytest
from sqlalchemy import insert, select, text

import provisa.api.app as appmod
from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api.admin import schema_mutation, schema_query
from provisa.api.admin.types import TagAssignmentInput, TagInput, TagTargetInput
from provisa.core.schema_org import registered_tables, roles, tag_assignments
from tests.unit.test_admin_deletes_need_the_domain import _info, _served, plane  # noqa: F401

TAGGERS = ["table_registration", "user_management", "source_registration"]
ROLES = {
    # Edits tags and sources everywhere.
    "everywhere": {"id": "everywhere", "capabilities": TAGGERS, "domain_access": ["*"]},
    # Acts in sales alone; may register sources there.
    "sales_admin": {"id": "sales_admin", "capabilities": TAGGERS, "domain_access": ["sales"]},
    # Acts in finance alone.
    "finance_admin": {
        "id": "finance_admin",
        "capabilities": TAGGERS,
        "domain_access": ["finance"],
    },
    # Edits tags everywhere, but registers no source.
    "tag_editor": {
        "id": "tag_editor",
        "capabilities": ["table_registration"],
        "domain_access": ["*"],
    },
}


@pytest.fixture
async def model(plane, monkeypatch):  # noqa: F811
    async def _nothing() -> None:
        return None

    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _nothing)

    async def _pool():
        return plane

    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    async with plane.acquire() as conn:
        await conn.execute_core(insert(roles).values(id="teller"))
    return plane


async def _table(db, name: str) -> int:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


def _tag(allowed: list[str] | None, *, required: bool = False) -> TagInput:
    return TagInput(
        id="reviewed",
        description="Reviewed",
        applies_to=["table", "role"],
        required=required,
        allowed_domains=allowed,
    )


async def _define(allowed: list[str] | None) -> None:
    saved = await schema_mutation.Mutation().create_tag(_info("everywhere"), _tag(allowed))
    assert saved.success, saved.message


def _on_table(table_id: int) -> TagAssignmentInput:
    return TagAssignmentInput(tag_id="reviewed", object_type="table", table_id=table_id)


def _on_role(role_id: str) -> TagAssignmentInput:
    return TagAssignmentInput(tag_id="reviewed", object_type="role", role_id=role_id)


async def _assigned(db) -> set[str]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(select(tag_assignments.c.object_key))
        return {r[0] for r in rows.fetchall()}


async def _read_back(role: str = "everywhere"):
    [tag] = [t for t in await schema_query.Query().tags(_info(role)) if t.id == "reviewed"]
    return tag


# --- the field, saved and read back ----------------------------------------------------------------


async def test_a_tags_allowed_domains_are_saved_and_read_back(model):
    await _define(["sales", "finance"])
    assert sorted((await _read_back()).allowed_domains) == ["finance", "sales"]


async def test_a_tag_that_names_none_reads_back_none(model):
    await _define(None)
    assert (await _read_back()).allowed_domains == []


async def test_an_update_that_leaves_the_list_out_keeps_it(model):
    await _define(["sales"])
    saved = await schema_mutation.Mutation().update_tag(_info("everywhere"), _tag(None))
    assert saved.success, saved.message
    assert (await _read_back()).allowed_domains == ["sales"]


# --- assignment: the object's domain ---------------------------------------------------------------


async def test_an_assignment_inside_the_tags_domains_is_made(model):
    await _define(["sales"])
    orders = await _table(model, "orders")
    result = await schema_mutation.Mutation().assign_tag(_info("everywhere"), _on_table(orders))
    assert result.success, result.message
    assert await _assigned(model) == {f"table:{orders}"}


async def test_an_assignment_outside_the_tags_domains_is_refused_by_name(model):
    await _define(["sales"])
    invoices = await _table(model, "invoices")
    result = await schema_mutation.Mutation().assign_tag(_info("everywhere"), _on_table(invoices))
    assert (result.success, result.code) == (False, "schema.tag_domain_not_allowed")
    assert result.params["tag"] == "reviewed"
    assert result.params["allowedDomains"] == ["sales"]
    assert result.params["domains"] == ["finance"]
    assert "finance" in result.message and "sales" in result.message
    assert await _assigned(model) == set()


async def test_a_tag_that_names_no_domain_is_assigned_in_every_domain(model):
    await _define([])
    mutation = schema_mutation.Mutation()
    for name in ("orders", "invoices"):
        result = await mutation.assign_tag(
            _info("everywhere"), _on_table(await _table(model, name))
        )
        assert result.success, result.message
    assert len(await _assigned(model)) == 2


# --- an object with no domain: the assigner's ------------------------------------------------------


async def test_a_role_takes_the_tag_from_someone_acting_in_one_of_its_domains(model):
    await _define(["sales"])
    result = await schema_mutation.Mutation().assign_tag(_info("sales_admin"), _on_role("teller"))
    assert result.success, result.message
    assert await _assigned(model) == {"role:teller"}


async def test_a_role_is_refused_the_tag_by_someone_acting_outside_its_domains(model):
    await _define(["sales"])
    result = await schema_mutation.Mutation().assign_tag(_info("finance_admin"), _on_role("teller"))
    assert (result.success, result.code) == (False, "schema.tag_domain_not_allowed")
    assert result.params["allowedDomains"] == ["sales"]
    assert result.params["domains"] == []
    assert await _assigned(model) == set()


async def test_a_role_takes_a_tag_of_every_domain_from_anyone_who_may_tag_it(model):
    await _define([])
    result = await schema_mutation.Mutation().assign_tag(_info("finance_admin"), _on_role("teller"))
    assert result.success, result.message


# --- the picker: offered per object ----------------------------------------------------------------


async def test_the_picker_is_offered_the_tag_only_on_objects_of_its_domains(model):
    await _define(["sales"])
    query = schema_query.Query()

    async def offered(role: str, target: TagTargetInput) -> list[str]:
        return await query.offered_tags(_info(role), target)

    orders = TagTargetInput(object_type="table", table_id=await _table(model, "orders"))
    invoices = TagTargetInput(object_type="table", table_id=await _table(model, "invoices"))
    teller = TagTargetInput(object_type="role", role_id="teller")
    assert "reviewed" in await offered("everywhere", orders)
    assert "reviewed" not in await offered("everywhere", invoices)
    assert "reviewed" in await offered("sales_admin", teller)
    assert "reviewed" not in await offered("finance_admin", teller)


async def test_the_picker_is_offered_only_tags_that_apply_to_the_objects_kind(model):
    await _define([])
    column = TagTargetInput(
        object_type="column", table_id=await _table(model, "orders"), column_name="id"
    )
    offered = await schema_query.Query().offered_tags(_info("everywhere"), column)
    assert "reviewed" not in offered  # it applies to tables and roles
    assert "pii" in offered  # a system tag that applies to columns


# --- narrowing: which assignments are outside, none removed ----------------------------------------


async def test_narrowing_lists_the_assignments_now_outside_and_removes_none(model):
    await _define(["sales", "finance"])
    mutation = schema_mutation.Mutation()
    orders, invoices = await _table(model, "orders"), await _table(model, "invoices")
    for table_id in (orders, invoices):
        assert (await mutation.assign_tag(_info("everywhere"), _on_table(table_id))).success
    result = await mutation.update_tag(_info("everywhere"), _tag(["sales"]))
    assert result.success, result.message
    assert result.params["outside"] == [
        {
            "tagId": "reviewed",
            "objectType": "table",
            "objectKey": f"table:{invoices}",
            "domains": ["finance"],
        }
    ]
    assert f"table:{invoices}" in result.message
    assert await _assigned(model) == {f"table:{orders}", f"table:{invoices}"}
    assert (await _read_back()).allowed_domains == ["sales"]


async def test_a_save_that_leaves_every_assignment_inside_lists_none(model):
    await _define(["sales", "finance"])
    mutation = schema_mutation.Mutation()
    assert (
        await mutation.assign_tag(_info("everywhere"), _on_table(await _table(model, "orders")))
    ).success
    result = await mutation.update_tag(_info("everywhere"), _tag(["sales"]))
    assert result.success, result.message
    assert result.params["outside"] == []


# --- the right: as widening a source's allowed domains ---------------------------------------------


async def test_changing_the_list_needs_source_registration(model):
    with pytest.raises(PermissionError, match="source_registration"):
        await schema_mutation.Mutation().create_tag(_info("tag_editor"), _tag(["sales"]))


async def test_a_tag_editor_without_it_saves_a_tag_whose_list_does_not_change(model):
    await _define(["sales"])
    saved = await schema_mutation.Mutation().update_tag(_info("tag_editor"), _tag(["sales"]))
    assert saved.success, saved.message


async def test_widening_needs_reach_of_each_domain_added(model):
    await _define(["sales"])
    mutation = schema_mutation.Mutation()
    with pytest.raises(PermissionError, match="No access to domain 'finance'"):
        await mutation.update_tag(_info("sales_admin"), _tag(["sales", "finance"]))
    # Emptying the list opens the tag to every domain: a widening to all of them.
    with pytest.raises(PermissionError, match="No access to domain"):
        await mutation.update_tag(_info("sales_admin"), _tag([]))
    assert (await _read_back()).allowed_domains == ["sales"]


async def test_narrowing_needs_no_reach_of_the_domain_closed(model):
    await _define(["sales", "finance"])
    saved = await schema_mutation.Mutation().update_tag(_info("sales_admin"), _tag(["sales"]))
    assert saved.success, saved.message
    assert (await _read_back()).allowed_domains == ["sales"]


# --- the tags report: a required tag is required only in its domains -------------------------------


async def _missing(db) -> set[tuple[str, str]]:
    async with db.acquire() as conn:
        for name in ("derived_tags", "effective_column_tags", "tag_instances"):
            await conn.execute_core(text(_META_TABLE_VIEWS[name]))
        rows = await conn.execute_core(
            text("SELECT tag_id, object_key FROM tag_instances WHERE via = 'missing'")
        )
        return {(r[0], r[1]) for r in rows.fetchall()}


async def test_a_required_tag_is_missing_only_on_objects_of_its_domains(model):
    saved = await schema_mutation.Mutation().create_tag(
        _info("everywhere"), _tag(["sales"], required=True)
    )
    assert saved.success, saved.message
    orders, customers = await _table(model, "orders"), await _table(model, "customers")
    # Tables in sales lack it; finance's tables and the role, which is in no domain, need not.
    assert await _missing(model) == {
        ("reviewed", f"table:{orders}"),
        ("reviewed", f"table:{customers}"),
    }


async def test_a_required_tag_of_every_domain_is_missing_everywhere(model):
    saved = await schema_mutation.Mutation().create_tag(
        _info("everywhere"), _tag([], required=True)
    )
    assert saved.success, saved.message
    missing = await _missing(model)
    assert ("reviewed", "role:teller") in missing
    assert len([k for k in missing if k[1].startswith("table:")]) == 4

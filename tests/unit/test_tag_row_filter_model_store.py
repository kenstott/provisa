# Copyright (c) 2026 Kenneth Stott
# Canary: 7e2b90c4-51ad-4c66-8f13-9b0d4a6e2c75
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row filter on a tag, in the model store (REQ-1384).

The filter is one row of ``rls_rules`` scoped by ``tag_id``. A tag that carries one is not
deleted; saving one resolves its predicate against every table carrying the tag, and tagging
a table resolves the tag's filters against that table; putting a filtered tag on a table or
taking it off rebuilds the role contexts.
"""

# Requirements: REQ-1384, REQ-1918, REQ-1676

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import RLSRuleInput, TagAssignmentInput
from provisa.core.database import Database
from provisa.core.models import RLSRule, TagAssignment
from provisa.core.repositories import rls as rls_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import (
    domains,
    registered_tables,
    rls_rules,
    roles,
    sources,
    table_columns,
    tags,
)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="tagreader", capabilities=[], domain_access=["sales"])
        )
        for tag_id, applies_to in (("regional", ["table"]), ("column_only", ["column"])):
            await conn.execute_core(insert(tags).values(id=tag_id, applies_to=applies_to))
        for table_id, name, column in ((1, "orders", "region"), (2, "customers", "name")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                )
            )
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=table_id,
                    domain_id="sales",
                    column_name=column,
                    data_type="varchar",
                    visible_to=["tagreader"],
                )
            )

    async def _pool():
        return db

    rebuilds: list[int] = []

    async def _rebuild():
        rebuilds.append(1)

    async def _noop():
        return None

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _noop)
    db.rebuilds = rebuilds  # type: ignore[attr-defined]
    return db


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


def _on_orders(tag_id: str = "regional") -> TagAssignmentInput:
    return TagAssignmentInput(tag_id=tag_id, object_type="table", table_id=1)


async def test_a_tag_filter_is_stored_scoped_by_the_tag_and_replaced_on_resave(plane):
    async with plane.acquire() as conn:
        for predicate in ("region = 'us'", "region = 'eu'"):
            await rls_repo.upsert(
                conn,
                RLSRule(
                    tag_id="regional",
                    role_id="tagreader",
                    filter=predicate,
                    description="Test rule.",
                ),
            )
        held = await rls_repo.list_for_tag(conn, "regional")
        assert await rls_repo.tags_with_filters(conn) == {"regional"}
    assert [(r["tag_id"], r["table_id"], r["domain_id"], r["filter_expr"]) for r in held] == [
        ("regional", None, None, "region = 'eu'")
    ]


async def test_a_tag_that_carries_a_filter_is_not_deleted(plane):
    async with plane.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                tag_id="regional", role_id="tagreader", filter="true", description="Test rule."
            ),
        )
        with pytest.raises(tag_repo.TagDeleteRefused) as err:
            await tag_repo.delete(conn, "regional")
        assert [d.ref.kind for d in err.value.dependents] == ["row_filter"]
        assert "tagreader on tag regional" in str(err.value)
        assert await rls_repo.delete(conn, "tagreader", tag_id="regional") is True
        assert await tag_repo.delete(conn, "regional") is True
    assert await _count(plane, rls_rules) == 0


async def test_the_delete_mutation_refuses_naming_the_filter(plane):
    async with plane.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                tag_id="regional", role_id="tagreader", filter="true", description="Test rule."
            ),
        )
    result = await schema_mutation.Mutation().delete_tag(_info(), "regional")
    assert (result.success, result.code) == (False, "schema.tag_has_dependents")
    assert [d["kind"] for d in result.params["dependents"]] == ["row_filter"]
    assert await _count(plane, tags) == 2 and await _count(plane, rls_rules) == 1


async def test_saving_a_tag_filter_resolves_it_against_every_tagged_table(plane):
    mutation = schema_mutation.Mutation()
    async with plane.acquire() as conn:
        for table_id in (1, 2):
            await tag_repo.assign(
                conn,
                TagAssignment(tag_id="regional", object_type="table", table_id=table_id),
            )
    # `region` is a column of orders and not of customers, and the tag is on both.
    refused = await mutation.upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id="regional",
            role_id="tagreader",
            filter_expr="region = 'us'",
            description="Test rule.",
        ),
    )
    assert (refused.success, refused.code) == (False, "schema.rls_rule_invalid")
    assert await _count(plane, rls_rules) == 0 and plane.rebuilds == []

    saved = await mutation.upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id="regional", role_id="tagreader", filter_expr="1 = 1", description="Test rule."
        ),
    )
    assert (saved.success, saved.code) == (True, "schema.rls_rule_saved_tag")
    assert saved.params == {"tag": "regional", "role": "tagreader"}
    assert await _count(plane, rls_rules) == 1 and plane.rebuilds == [1]


@pytest.mark.parametrize(
    ("tag_id", "code"),
    [
        ("pii", "schema.rls_rule_tag_not_user"),  # a system tag has no registry row
        ("regional:us", "schema.rls_rule_tag_not_user"),  # a rule names the tag, not a value
        ("column_only", "schema.rls_rule_tag_not_table"),
        ("nope", "schema.tag_not_found"),
    ],
)
async def test_only_a_user_tag_that_applies_to_tables_takes_a_filter(plane, tag_id, code):
    result = await schema_mutation.Mutation().upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id=tag_id, role_id="tagreader", filter_expr="1 = 1", description="Test rule."
        ),
    )
    assert (result.success, result.code) == (False, code)
    assert await _count(plane, rls_rules) == 0


async def test_tagging_a_table_the_filter_does_not_fit_is_refused(plane):
    mutation = schema_mutation.Mutation()
    saved = await mutation.upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id="regional",
            role_id="tagreader",
            filter_expr="region = 'us'",
            description="Test rule.",
        ),
    )
    assert saved.success, saved.message
    refused = await mutation.assign_tag(
        _info(), TagAssignmentInput(tag_id="regional", object_type="table", table_id=2)
    )
    assert (refused.success, refused.code) == (False, "schema.tag_filter_misfit")
    assert "tagreader" in refused.message
    async with plane.acquire() as conn:
        assert await tag_repo.assignment_count(conn, "regional") == 0


async def test_assigning_and_removing_a_filtered_tag_rebuilds_the_role_contexts(plane):
    mutation = schema_mutation.Mutation()
    assert (await mutation.assign_tag(_info(), _on_orders())).success
    assert (await mutation.unassign_tag(_info(), _on_orders())).success
    assert plane.rebuilds == []  # the tag carries no filter: it is annotation

    saved = await mutation.upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id="regional",
            role_id="tagreader",
            filter_expr="region = 'us'",
            description="Test rule.",
        ),
    )
    assert saved.success, saved.message
    plane.rebuilds.clear()
    assert (await mutation.assign_tag(_info(), _on_orders())).success
    assert plane.rebuilds == [1]
    assert (await mutation.unassign_tag(_info(), _on_orders())).success
    assert plane.rebuilds == [1, 1]


async def test_deleting_a_tag_filter_through_the_mutation_rebuilds(plane):
    mutation = schema_mutation.Mutation()
    await mutation.upsert_rls_rule(
        _info(),
        RLSRuleInput(
            tag_id="regional", role_id="tagreader", filter_expr="1 = 1", description="Test rule."
        ),
    )
    plane.rebuilds.clear()
    result = await mutation.delete_rls_rule(_info(), role_id="tagreader", tag_id="regional")
    assert (result.success, result.code) == (True, "schema.rls_rule_deleted")
    assert await _count(plane, rls_rules) == 0 and plane.rebuilds == [1]

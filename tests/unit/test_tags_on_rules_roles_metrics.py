# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f3d2e-8a51-4c97-9e2d-5f1a7c4b3e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Row rules, roles and metrics carry tags as sources, tables and columns do (REQ-2208).

A tag goes on a row rule by whoever may change the rule (masking_config in the domains it decides
rows in), on a role by whoever may change roles (user_management), on a metric by a table editor in
the domains its expression reads. A tag on a row rule is read only by a reader of row rules, one on a
role only by whoever may read the role's definition, one on a metric only by a caller the metric is
granted to. The tag goes with its object, and travels with it in a configuration and an
environment's tree.
"""

# Requirements: REQ-2208, REQ-2152, REQ-1377

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import delete, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.admin.schema_query import Query
from provisa.api.admin.types import TagAssignmentInput
from provisa.core.models import RowRuleRef, TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import (
    metrics,
    registered_tables,
    rls_rules,
    roles,
    tag_assignments,
    tags,
    tracked_webhooks,
)
from tests.unit.test_admin_deletes_need_the_domain import plane  # noqa: F401

ALL = ["table_registration", "masking_config", "user_management", "access_config"]
ROLES = {
    "rule_keeper": {"capabilities": ["masking_config"], "domain_access": ["sales"]},
    "role_admin": {"capabilities": ["user_management"], "domain_access": ["*"]},
    "sales_steward": {"capabilities": ["table_registration"], "domain_access": ["sales"]},
    "everyone": {"capabilities": ALL, "domain_access": ["*"]},
    "eu_reader": {"capabilities": [], "domain_access": ["*"]},
}


def _request(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id], acting_roles=[role_id])
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme", role=role_id)
    )


def _info(role_id: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(role_id)})


@pytest.fixture
async def governed(plane, monkeypatch):  # noqa: F811
    """The plane's tables, metrics and domains; the roles above; a rule of eu_reader's on a
    sales table and one on a finance table; and a tag that applies to rules, roles and metrics."""

    async def _refresh() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _refresh)
    monkeypatch.setattr(
        appmod.state,
        "roles",
        {k: {"id": k, **v, "own_domain_access": v["domain_access"]} for k, v in ROLES.items()},
        raising=False,
    )
    rule_ids: dict[str, int] = {}
    async with plane.acquire() as conn:
        for role_id in ROLES:
            await conn.execute_core(insert(roles).values(id=role_id, description=role_id))
        await conn.execute_core(
            insert(tags).values(id="reviewed", applies_to=["rls_rule", "role", "metric"])
        )
        await conn.execute_core(insert(tags).values(id="tables_only", applies_to=["table"]))
        for table_name in ("orders", "invoices"):
            table_id = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(
                        registered_tables.c.table_name == table_name
                    )
                )
            ).scalar_one()
            rule_ids[table_name] = (
                await conn.execute_core(
                    insert(rls_rules)
                    .values(
                        table_id=table_id,
                        role_id="eu_reader",
                        filter_expr=b"region = 'EU'",
                        description="EU rows only",
                        rights="R",
                    )
                    .returning(rls_rules.c.id)
                )
            ).scalar_one()
        await conn.execute_core(
            metrics.update().where(metrics.c.name == "revenue").values(visible_to=["eu_reader"])
        )
    return plane, rule_ids


async def _keys(db) -> set[str]:
    async with db.acquire() as conn:
        return {
            r[0] for r in (await conn.execute_core(select(tag_assignments.c.object_key))).fetchall()
        }


def _on(kind: str, tag: str = "reviewed", **target: Any) -> TagAssignmentInput:
    return TagAssignmentInput(tag_id=tag, object_type=kind, **target)


# --- a row rule ----------------------------------------------------------------------------------


async def test_a_tag_on_a_row_rule_is_assigned_and_removed_by_its_keeper(governed):
    db, rule_ids = governed
    rule = rule_ids["orders"]
    mutation = schema_mutation.Mutation()
    result = await mutation.assign_tag(_info("rule_keeper"), _on("rls_rule", rls_rule_id=rule))
    assert result.success, result.message
    assert await _keys(db) == {f"rls_rule:{rule}"}
    result = await mutation.unassign_tag(_info("rule_keeper"), _on("rls_rule", rls_rule_id=rule))
    assert result.success, result.message
    assert await _keys(db) == set()


async def test_a_tag_on_a_rule_in_another_domain_is_refused(governed):
    db, rule_ids = governed
    with pytest.raises(PermissionError, match="finance"):
        await schema_mutation.Mutation().assign_tag(
            _info("rule_keeper"), _on("rls_rule", rls_rule_id=rule_ids["invoices"])
        )
    assert await _keys(db) == set()


async def test_a_table_editor_may_not_tag_a_row_rule(governed):
    db, rule_ids = governed
    with pytest.raises(PermissionError, match="masking_config"):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_steward"), _on("rls_rule", rls_rule_id=rule_ids["orders"])
        )
    assert await _keys(db) == set()


async def test_a_rule_that_is_not_there_is_not_found(governed):
    result = await schema_mutation.Mutation().assign_tag(
        _info("everyone"), _on("rls_rule", rls_rule_id=999_999)
    )
    assert (result.success, result.code) == (False, "schema.tag_target_not_found")


async def test_a_rules_tag_goes_with_the_rule(governed):
    db, rule_ids = governed
    rule = rule_ids["orders"]
    assert (
        await schema_mutation.Mutation().assign_tag(
            _info("everyone"), _on("rls_rule", rls_rule_id=rule)
        )
    ).success
    async with db.acquire() as conn:
        await conn.execute_core(delete(rls_rules).where(rls_rules.c.id == rule))
    assert await _keys(db) == set()


# --- a role --------------------------------------------------------------------------------------


async def test_a_tag_on_a_role_is_assigned_and_removed_by_a_user_manager(governed):
    db, _ = governed
    mutation = schema_mutation.Mutation()
    result = await mutation.assign_tag(_info("role_admin"), _on("role", role_id="eu_reader"))
    assert result.success, result.message
    assert await _keys(db) == {"role:eu_reader"}
    result = await mutation.unassign_tag(_info("role_admin"), _on("role", role_id="eu_reader"))
    assert result.success, result.message
    assert await _keys(db) == set()


async def test_a_table_editor_may_not_tag_a_role(governed):
    db, _ = governed
    with pytest.raises(PermissionError, match="user_management"):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_steward"), _on("role", role_id="eu_reader")
        )
    assert await _keys(db) == set()


async def test_a_role_that_is_not_there_is_not_found(governed):
    result = await schema_mutation.Mutation().assign_tag(
        _info("role_admin"), _on("role", role_id="nobody")
    )
    assert (result.success, result.code) == (False, "schema.tag_target_not_found")


async def test_a_tag_that_does_not_apply_to_roles_is_refused_on_one(governed):
    result = await schema_mutation.Mutation().assign_tag(
        _info("role_admin"), _on("role", tag="tables_only", role_id="eu_reader")
    )
    assert (result.success, result.code) == (False, "schema.tag_scope_mismatch")


async def test_a_roles_tag_goes_with_the_role(governed):
    db, _ = governed
    assert (
        await schema_mutation.Mutation().assign_tag(
            _info("role_admin"), _on("role", role_id="role_admin")
        )
    ).success
    async with db.acquire() as conn:
        await conn.execute_core(delete(roles).where(roles.c.id == "role_admin"))
    assert await _keys(db) == set()


# --- a metric ------------------------------------------------------------------------------------


async def test_a_tag_on_a_metric_is_assigned_and_removed_in_its_domain(governed):
    db, _ = governed
    mutation = schema_mutation.Mutation()
    result = await mutation.assign_tag(_info("sales_steward"), _on("metric", metric_name="revenue"))
    assert result.success, result.message
    assert await _keys(db) == {"metric:revenue"}
    result = await mutation.unassign_tag(
        _info("sales_steward"), _on("metric", metric_name="revenue")
    )
    assert result.success, result.message
    assert await _keys(db) == set()


async def test_a_tag_on_a_metric_reading_another_domain_is_refused(governed):
    db, _ = governed
    with pytest.raises(PermissionError, match="finance"):
        await schema_mutation.Mutation().assign_tag(
            _info("sales_steward"), _on("metric", metric_name="billed")
        )
    assert await _keys(db) == set()


async def test_a_metric_that_is_not_there_is_not_found(governed):
    result = await schema_mutation.Mutation().assign_tag(
        _info("everyone"), _on("metric", metric_name="nothing")
    )
    assert (result.success, result.code) == (False, "schema.tag_target_not_found")


async def test_a_metrics_tag_goes_with_the_metric(governed):
    db, _ = governed
    assert (
        await schema_mutation.Mutation().assign_tag(
            _info("everyone"), _on("metric", metric_name="revenue")
        )
    ).success
    async with db.acquire() as conn:
        await conn.execute_core(delete(metrics).where(metrics.c.name == "revenue"))
    assert await _keys(db) == set()


# --- who reads them ------------------------------------------------------------------------------


async def _tag_all(db, rule_ids: dict[str, int]) -> None:
    async with db.acquire() as conn:
        for assignment in (
            TagAssignment(
                tag_id="reviewed", object_type="rls_rule", rls_rule_id=rule_ids["orders"]
            ),
            TagAssignment(tag_id="reviewed", object_type="role", role_id="eu_reader"),
            TagAssignment(tag_id="reviewed", object_type="role", role_id="role_admin"),
            TagAssignment(tag_id="reviewed", object_type="metric", metric_name="revenue"),
            TagAssignment(tag_id="reviewed", object_type="metric", metric_name="billed"),
        ):
            await tag_repo.assign(conn, assignment)


async def _read_by(role_id: str, monkeypatch) -> set[str]:
    """The tags on rules, roles and metrics ``role_id`` is listed. Which columns a caller is
    served is REQ-2152's own tests: here the catalog is the acting role's (everyone's whole)."""
    from provisa.api.admin import schema_query
    from provisa.api.admin.catalog_scope import CatalogScope

    async def _pool():
        return appmod.state.tenant_db

    def _scope(info):
        role = info.context["request"].state.role
        return CatalogScope(
            whole=role == "everyone", reach=None, served={}, governance=False, role=role
        )

    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "catalog_scope", _scope)
    seen = await Query().tag_assignments(_info(role_id))
    return {
        f"{a.object_type}:{a.rls_rule_id or a.role_id or a.metric_name}"
        for a in seen
        if a.object_type in ("rls_rule", "role", "metric")
    }


async def test_a_reader_of_everything_reads_every_tag(governed, monkeypatch):
    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    assert await _read_by("everyone", monkeypatch) == {
        f"rls_rule:{rule_ids['orders']}",
        "role:eu_reader",
        "role:role_admin",
        "metric:revenue",
        "metric:billed",
    }


async def test_a_rules_tag_is_read_only_by_a_reader_of_rules(governed, monkeypatch):
    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    assert not any(k.startswith("rls_rule:") for k in await _read_by("eu_reader", monkeypatch))
    assert not any(k.startswith("rls_rule:") for k in await _read_by("role_admin", monkeypatch))


async def test_a_roles_tag_is_read_by_a_user_manager_and_by_the_roles_holder(governed, monkeypatch):
    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    assert {k for k in await _read_by("role_admin", monkeypatch) if k.startswith("role:")} == {
        "role:eu_reader",
        "role:role_admin",
    }
    assert {k for k in await _read_by("eu_reader", monkeypatch) if k.startswith("role:")} == {
        "role:eu_reader"
    }


async def test_a_metrics_tag_is_read_only_by_whom_the_metric_is_granted_to(governed, monkeypatch):
    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    assert {k for k in await _read_by("eu_reader", monkeypatch) if k.startswith("metric:")} == {
        "metric:revenue"
    }


# --- a configuration and an environment carry them -----------------------------------------------


async def test_a_rules_tag_names_the_rule_by_role_target_and_rights(governed):
    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    async with db.acquire() as conn:
        listed = {
            r["object_key"]: r for r in await tag_repo.list_assignments(conn) if r["rls_rule"]
        }
        ref = listed[f"rls_rule:{rule_ids['orders']}"]["rls_rule"]
        assert ref == {"role_id": "eu_reader", "table_id": "orders", "rights": ["R"]}
        assert await tag_repo.resolve_rls_rule_id(conn, RowRuleRef(**ref)) == rule_ids["orders"]
        assert (
            await tag_repo.resolve_rls_rule_id(
                conn, RowRuleRef(role_id="eu_reader", table_id="orders", rights=["R", "U"])
            )
            is None
        )


async def test_the_tags_ride_in_their_objects_files_and_come_back_on_them(governed):
    from provisa.core.env_deploy import _decompose, _existing_ids, _roles_after
    from provisa.core.env_project import project

    db, rule_ids = governed
    await _tag_all(db, rule_ids)
    async with db.acquire() as conn:
        # The plane names a function and a webhook alike, which one tree cannot hold.
        await conn.execute_core(delete(tracked_webhooks))
        tree = await project(conn, "org_default")
        existing = await _existing_ids(conn, "org_default")
        known_roles = await _roles_after(conn, "org_default", tree)
    entry = {"tag_id": "reviewed", "base_tag_id": "reviewed"}
    assert tree["roles/eu_reader.yaml"]["tags"] == [entry]
    assert tree["metrics/revenue.yaml"]["tags"] == [entry]
    orders = next(p for p, b in tree.items() if p.endswith("/orders.yaml"))
    assert {**entry, "on": "#rule:eu_reader:R"} in tree[orders]["tags"]
    rows = _decompose(tree, existing, known_roles)
    placed = {
        (r["object_type"], r["object_key"])
        for r in rows["tag_assignments"]
        if r["object_type"] in ("rls_rule", "role", "metric")
    }
    assert placed == {
        ("rls_rule", f"rls_rule:{rule_ids['orders']}"),
        ("role", "role:eu_reader"),
        ("role", "role:role_admin"),
        ("metric", "metric:revenue"),
        ("metric", "metric:billed"),
    }
    # The rule keeps its serial, which its tag names.
    assert rule_ids["orders"] in {r["id"] for r in rows["rls_rules"]}

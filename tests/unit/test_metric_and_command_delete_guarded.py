# Copyright (c) 2026 Kenneth Stott
# Canary: 77aa38fc-b1a0-445f-b462-091d71d44eb8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A metric's, a command's and a webhook's delete go through the dependency guard (REQ-1918).

A metric is blocked by the views that use it: one composed from it (``view_metrics``) or whose
SQL reads it as ``metrics.<name>``. Nothing in the model refers to a command or a webhook; what
the delete adds for them is their parts — the row filters defined on them and their tag
assignments — which were left behind, on every control plane, because no foreign key names them.
"""

# Requirements: REQ-1918, REQ-1919, REQ-1317, REQ-205, REQ-209

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import function as function_repo
from provisa.core.repositories import metric as metric_repo
from provisa.core.schema_org import (
    domains,
    metrics,
    registered_tables,
    rls_rules,
    roles,
    sources,
    tag_assignments,
    tracked_functions,
    tracked_webhooks,
)


@pytest.fixture
async def plane(monkeypatch) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="metric-delete-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["*"])
        )
        for name in ("revenue", "margin", "unused"):
            await conn.execute_core(insert(metrics).values(name=name, expression="SUM(orders.a)"))
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", {}, raising=False)
    return db


async def _view(db: Database, name: str, **values: Any) -> int:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="views",
                table_name=name,
                **values,
            )
        )
        return (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def _names(db: Database, table) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(table.c.name))).fetchall()}


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


# --- metric --------------------------------------------------------------------------------------


async def test_a_metric_a_view_is_composed_from_is_refused_naming_the_view(plane):
    view = await _view(
        plane,
        "revenue_by_region",
        view_sql="SELECT 1 AS id",
        view_metrics={"metrics": ["revenue"], "dimensions": ["orders.region"]},
    )
    async with plane.acquire() as conn:
        with pytest.raises(metric_repo.MetricDeleteRefused) as err:
            await metric_repo.delete(conn, "revenue")
    assert [(d.ref.kind, d.ref.id, d.via) for d in err.value.dependents] == [
        ("table", view, ("registered_tables.view_metrics",))
    ]
    assert "revenue" in await _names(plane, metrics)


async def test_a_metric_a_views_sql_reads_is_refused_naming_the_view(plane):
    view = await _view(plane, "margins", view_sql="SELECT m.total FROM metrics.margin m")
    async with plane.acquire() as conn:
        with pytest.raises(metric_repo.MetricDeleteRefused) as err:
            await metric_repo.delete(conn, "margin")
    assert [(d.ref.kind, d.ref.id, d.via) for d in err.value.dependents] == [
        ("table", view, ("registered_tables.view_sql",))
    ]


async def test_a_metric_nothing_uses_is_deleted(plane):
    async with plane.acquire() as conn:
        assert await metric_repo.delete(conn, "unused") is True
        assert await metric_repo.delete(conn, "unused") is False
    assert await _names(plane, metrics) == {"revenue", "margin"}


async def test_a_declared_set_of_metrics_is_removed_without_the_guard(plane):
    await _view(plane, "margins", view_sql="SELECT m.total FROM metrics.margin m")
    async with plane.acquire() as conn:
        await metric_repo.remove_where(conn, metrics.c.name.not_in(["revenue"]))
    assert await _names(plane, metrics) == {"revenue"}


async def test_the_mutation_refuses_with_the_list(plane, monkeypatch):
    view = await _view(plane, "margins", view_sql="SELECT m.total FROM metrics.margin m")
    rebuilds: list[int] = []

    async def _pool():
        return plane

    async def _rebuild() -> None:
        rebuilds.append(1)

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    info: Any = types.SimpleNamespace(context={"request": request})

    refused = await schema_mutation.Mutation().delete_metric(info, "margin")
    assert (refused.success, refused.code) == (False, "schema.metric_has_dependents")
    assert refused.params == {
        "metric": "margin",
        "dependents": [
            {"kind": "table", "id": view, "name": "margins", "via": ["registered_tables.view_sql"]}
        ],
    }
    assert rebuilds == []
    deleted = await schema_mutation.Mutation().delete_metric(info, "unused")
    assert (deleted.success, deleted.code) == (True, "schema.metric_deleted") and rebuilds == [1]


# --- command and webhook -------------------------------------------------------------------------

KINDS = [
    pytest.param(function_repo.delete_function, tracked_functions, id="command"),
    pytest.param(function_repo.delete_webhook, tracked_webhooks, id="webhook"),
]


@pytest.mark.parametrize(("delete", "table"), KINDS)
async def test_its_row_filters_and_tags_go_with_it(plane, delete, table):
    async with plane.acquire() as conn:
        for name in ("refund", "other"):
            await conn.execute_core(insert(table).values(name=name, domain_id="sales"))
            await conn.execute_core(
                insert(rls_rules).values(role_id="seller", action_name=name, filter_expr=b"1=1")
            )
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="command",
                    object_key=name,
                    command_name=name,
                )
            )
        assert await delete(conn, "refund") is True
        assert await delete(conn, "refund") is False
        left_rules = (await conn.execute_core(select(rls_rules.c.action_name))).fetchall()
        left_tags = (await conn.execute_core(select(tag_assignments.c.command_name))).fetchall()
    assert await _names(plane, table) == {"other"}
    assert [r[0] for r in left_rules] == ["other"] and [t[0] for t in left_tags] == ["other"]


async def test_a_full_replace_removes_every_command_and_webhook(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(insert(tracked_functions).values(name="a", domain_id="sales"))
        await conn.execute_core(insert(tracked_webhooks).values(name="b", domain_id="sales"))
        await function_repo.remove_all(conn)
    assert (
        await _count(plane, tracked_functions) == 0 and await _count(plane, tracked_webhooks) == 0
    )

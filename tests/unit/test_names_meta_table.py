# Copyright (c) 2026 Kenneth Stott
# Canary: 6e2b9d4a-8c31-4f7e-a5d0-3b1c7e9f2a48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-194 (amended 2026-11-07, THE NAMES TABLE IS A META TABLE WITH A REPORT): the names table
of the last model build is a meta table, read where the table each row names is, with a
standard report listing every name, its problems, and the clashes the last build refused."""

from __future__ import annotations

import pytest
from sqlalchemy import insert, select, text

from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS
from provisa.api.app_loaders import _META_TABLE_ALIAS, _META_TABLES
from provisa.compiler import naming
from provisa.compiler.names_table import (
    FrozenAlias,
    NameClash,
    NameRow,
    build_names_table,
)
from provisa.core.repositories.model_names import (
    freeze_aliases,
    write_names_table,
    write_refused_clashes,
)
from provisa.security.rights import DEFAULT_TIER_META_TABLES, META_ROW_SCOPED_VIEWS


@pytest.fixture(autouse=True)
def _convention():
    naming.configure(gql="apollo_graphql", sql="snake")
    yield
    naming.configure(gql="apollo_graphql", sql="snake")


def _names():
    return build_names_table(
        [
            NameRow(
                "table",
                "1",
                "sql",
                "orders",
                "orders",
                "crm.public.orders",
                domain_id="sales",
                table_id=1,
                scope=("sql", "sales"),
            ),
            NameRow(
                "column",
                "1:order",
                "sql",
                "order",
                "order",
                "crm.public.orders.order",
                domain_id="sales",
                table_id=1,
                scope=("sql_column", "1"),
            ),
            NameRow(
                "command",
                "top",
                "graphql_field",
                "top",
                "top",
                "command top",
                scope=("graphql_query",),
            ),
        ]
    )


def test_the_names_table_is_a_row_scoped_meta_table():
    assert "model_names" in _META_TABLES
    assert _META_TABLE_ALIAS["model_names"] == "model_names_meta"
    assert META_ROW_SCOPED_VIEWS["model_names_meta"] == ("table_id",)
    assert "model_names_meta" in DEFAULT_TIER_META_TABLES
    assert "names_report" in _OPS_REPORT_VIEWS


@pytest.mark.asyncio
async def test_the_meta_table_reads_the_last_build(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(text(_META_TABLE_VIEWS["model_names"]))
            await write_names_table(conn, _names().rows)
            rows = (
                await conn.execute_core(
                    text(
                        "SELECT object_kind, object_id, surface, name, table_id, warnings "
                        "FROM model_names_meta ORDER BY object_kind, object_id"
                    )
                )
            ).fetchall()
            # A second build replaces every row.
            await write_names_table(conn, _names().rows[:1])
            after = (
                await conn.execute_core(text("SELECT object_id FROM model_names_meta"))
            ).fetchall()
    finally:
        await db.close()
    assert [tuple(r)[:5] for r in rows] == [
        ("column", "1:order", "sql", "order", 1),
        ("command", "top", "graphql_field", "top", None),
        ("table", "1", "sql", "orders", 1),
    ]
    assert rows[0][5] == [
        {
            "code": "reserved",
            "message": "crm.public.orders.order: 'order' is a reserved word on sql: it must be "
            "quoted",
        }
    ]
    assert [tuple(r) for r in after] == [("1",)]


@pytest.mark.asyncio
async def test_the_report_lists_names_problems_and_refused_clashes(control_plane):
    db = control_plane.org()
    clash = NameClash("sql", "status", "crm.public.orders.Status", "crm.public.orders.status")
    try:
        async with db.acquire() as conn:
            await conn.execute_core(text(_OPS_REPORT_VIEWS["names_report"]))
            await write_names_table(conn, _names().rows)
            await write_refused_clashes(conn, [clash])
            report = (
                await conn.execute_core(
                    text(
                        "SELECT object_kind, object_id, surface, name, status, problems "
                        "FROM names_report ORDER BY id"
                    )
                )
            ).fetchall()
            # A build that stores its names table refused none.
            await write_names_table(conn, _names().rows)
            statuses = (
                await conn.execute_core(text("SELECT DISTINCT status FROM names_report"))
            ).fetchall()
    finally:
        await db.close()
    assert [tuple(r) for r in report] == [
        ("clash", "crm.public.orders.status", "sql", "status", "refused", clash.message()),
        (
            "column",
            "1:order",
            "sql",
            "order",
            "warning",
            "crm.public.orders.order: 'order' is a reserved word on sql: it must be quoted",
        ),
        ("command", "top", "graphql_field", "top", "ok", None),
        ("table", "1", "sql", "orders", "ok", None),
    ]
    assert sorted(r[0] for r in statuses) == ["ok", "warning"]


@pytest.mark.asyncio
async def test_a_frozen_alias_is_kept_and_never_replaces_a_stewards(control_plane):
    from provisa.core.schema_org import domains, registered_tables, sources, table_columns

    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(domains).values(id="sales", description="sales"))
            await conn.execute_core(insert(sources).values(id="crm", type="postgres"))
            for tid, alias in ((1, None), (2, "kept")):
                await conn.execute_core(
                    insert(registered_tables).values(
                        id=tid,
                        source_id="crm",
                        domain_id="sales",
                        schema_name="public",
                        table_name=f"t{tid}",
                        alias=alias,
                    )
                )
            for column in ("Status", "status"):
                await conn.execute_core(
                    insert(table_columns).values(
                        table_id=1, domain_id="sales", column_name=column, data_type="varchar"
                    )
                )
            await freeze_aliases(
                conn,
                [
                    FrozenAlias("column", "1:status", "sql", "status_2"),
                    FrozenAlias("table", "1", "sql", "crm_t1"),
                    FrozenAlias("table", "2", "sql", "crm_t2"),
                ],
            )
            columns = (
                await conn.execute_core(
                    select(table_columns.c.column_name, table_columns.c.alias).order_by(
                        table_columns.c.column_name
                    )
                )
            ).fetchall()
            tables = (
                await conn.execute_core(
                    select(registered_tables.c.id, registered_tables.c.alias).order_by(
                        registered_tables.c.id
                    )
                )
            ).fetchall()
    finally:
        await db.close()
    assert sorted(tuple(r) for r in columns) == [("Status", None), ("status", "status_2")]
    assert [tuple(r) for r in tables] == [(1, "crm_t1"), (2, "kept")]


@pytest.mark.asyncio
async def test_a_kind_with_no_stored_alias_is_refused(control_plane):
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            with pytest.raises(ValueError, match="a command has no stored alias"):
                await freeze_aliases(conn, [FrozenAlias("command", "top", "graphql_field", "x")])
    finally:
        await db.close()


# ── the clash policy setting ──


def test_clash_policy_defaults_to_refuse_and_reads_the_org_override():
    from provisa.compiler.names_table import clash_policy

    assert clash_policy({}) == "refuse"
    assert clash_policy({"names": {"clash_policy": "disambiguate"}}) == "disambiguate"
    with pytest.raises(ValueError, match="unknown clash policy"):
        clash_policy({"names": {"clash_policy": "rename"}})


def test_the_settings_block_validates_the_policy():
    from provisa.api.admin.settings_router import _names_block
    from provisa.api.errors import ApiError

    updated: list = []
    assert _names_block({"clash_policy": "disambiguate"}, {}, updated) == {
        "clash_policy": "disambiguate"
    }
    assert updated == ["names.clash_policy"]
    assert _names_block({"clash_policy": None}, {"names": {"clash_policy": "x"}}, []) is None
    with pytest.raises(ApiError):
        _names_block({"clash_policy": "rename"}, {}, [])
    with pytest.raises(ApiError):
        _names_block({"other": 1}, {}, [])

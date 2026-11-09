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

import asyncio
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from sqlalchemy import insert, select, text

from provisa.api._meta_views import _META_TABLE_VIEWS, _OPS_REPORT_VIEWS
from provisa.api.app_loaders import _META_TABLE_ALIAS, _META_TABLES
from provisa.compiler import naming
from provisa.compiler.names_table import (
    FrozenAlias,
    NameClash,
    NameRow,
    NameWarning,
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


def _many_names(count: int) -> list[NameRow]:
    warning = NameWarning("column", "1:c0", "sql", "reserved", "crm.public.orders.c0: reserved")
    return [
        NameRow(
            "column",
            f"1:c{i}",
            "sql",
            f"c{i}",
            f"c{i}",
            f"crm.public.orders.c{i}",
            domain_id="sales",
            table_id=1,
            warnings=(warning,) if i == 0 else (),
        )
        for i in range(count)
    ]


def _many_clashes(count: int) -> list[NameClash]:
    return [NameClash("sql", f"c{i}", f"holder{i}", f"newcomer{i}") for i in range(count)]


class _RecordingConn:
    """Keeps the statements a writer sends the model store, and the rows sent beside each."""

    def __init__(self) -> None:
        self.statements: list[Any] = []

    async def execute_core(self, stmt: Any) -> None:
        self.statements.append(stmt)

    async def execute_core_many(self, stmt: Any, rows: list[dict[str, Any]]) -> None:
        self.statements.append(stmt)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        yield

    async def advisory_xact_lock(self, key: int) -> None:
        pass


@pytest.mark.asyncio
async def test_a_model_with_more_names_than_a_statement_can_bind_is_stored(control_plane):
    """A PostgreSQL statement binds at most 65535 parameters, and a stored name takes ten: a
    model of 284 tables stopped every demo start here, past 6553 names."""
    names = _many_names(7000)
    db = control_plane.org()
    try:
        async with db.acquire() as conn:
            await write_names_table(conn, names)
            stored = (
                await conn.execute_core(text("SELECT count(*) FROM model_names"))
            ).scalar_one()
            warned = (
                await conn.execute_core(
                    text("SELECT warnings FROM model_names WHERE object_id = '1:c0'")
                )
            ).scalar_one()
    finally:
        await db.close()
    assert stored == 7000
    assert warned == [{"code": "reserved", "message": "crm.public.orders.c0: reserved"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("write", "many"),
    [(write_names_table, _many_names), (write_refused_clashes, _many_clashes)],
    ids=["names", "clashes"],
)
async def test_what_a_build_sends_does_not_grow_with_its_names(write, many):
    """However many names a model has, the same statements with the same binds: the rows travel
    beside the statement."""

    async def sent(count: int) -> list[int]:
        conn = _RecordingConn()
        await write(conn, many(count))
        return [len(s.compile().params) for s in conn.statements]

    assert await sent(2) == await sent(500)


@pytest.mark.asyncio
async def test_a_build_writing_while_another_replaces_the_names_waits_for_it(control_plane):
    """Every worker builds the model it is told changed: a second build arriving between the first
    one's delete and its insert took the same keys, and the first was refused on the primary key
    (REQ-194). The second waits, and the table holds the last build's names alone."""
    db = control_plane.org()
    names = _names().rows
    finished = threading.Event()
    failures: list[BaseException] = []

    async def second_build() -> None:
        async with db.acquire() as conn:
            await write_names_table(conn, names[:1])

    def run_second() -> None:
        try:
            asyncio.run(second_build())
        except BaseException as exc:  # noqa: BLE001 -- reported by the assertion below
            failures.append(exc)
        finally:
            finished.set()

    second = threading.Thread(target=run_second)
    try:
        async with db.acquire() as conn:
            await conn.execute_core(text(_META_TABLE_VIEWS["model_names"]))
            insert_rows = conn.execute_core_many

            async def second_build_arrives(stmt: Any, rows: list[dict[str, Any]]) -> None:
                second.start()
                finished.wait(timeout=3)
                await insert_rows(stmt, rows)

            conn.execute_core_many = second_build_arrives  # type: ignore[method-assign]
            await write_names_table(conn, names)
            conn.execute_core_many = insert_rows  # type: ignore[method-assign]
        second.join(timeout=30)
        assert not second.is_alive(), "the second build never finished"
        assert failures == []
        async with db.acquire() as conn:
            kept = (
                await conn.execute_core(text("SELECT object_id FROM model_names_meta"))
            ).fetchall()
    finally:
        await db.close()
    assert [tuple(r) for r in kept] == [("1",)]


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

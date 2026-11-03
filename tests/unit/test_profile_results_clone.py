# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4ffacd-fc52-4235-9dff-5f15fb301a1e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2052: a demo sandbox carries its template's profile runs.

The runs are relations of the template org's schema of their own, not model tables, so the model
copy (clone_model) does not move them: they are copied beside it, each under its own name."""

# Requirements: REQ-2052

from __future__ import annotations

from sqlalchemy import insert, select, text
from sqlalchemy.schema import CreateTable

from provisa.core.database import Database
from provisa.core.env_copy import copy_profile_results
from provisa.profiler.schema import result_sa_table


async def test_a_profile_run_is_carried_whole_and_nothing_else_is(control_plane):
    engine = control_plane.engine()
    db = Database(engine, "model", search_path="tmpl_src", holds="model")
    columns = result_sa_table("customers", 1, "columns")
    columns.schema = "tmpl_src"
    async with db.acquire() as conn, conn.transaction():
        for schema in ("tmpl_src", "sbx_dst"):
            await conn.execute_core(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            await conn.execute_core(text(f'CREATE SCHEMA "{schema}"'))
        # The source's model: a relation is carried while a table it registers names it.
        await conn.execute_core(
            text('CREATE TABLE "tmpl_src"."registered_tables" (id int, table_name text)')
        )
        await conn.execute_core(
            text('INSERT INTO "tmpl_src"."registered_tables" VALUES (1, \'customers\')')
        )
        await conn.execute_core(CreateTable(columns))
        await conn.execute_core(
            insert(columns).values(run_id="r1", column_name="email", null_count=227)
        )
        await conn.execute_core(text('CREATE TABLE "tmpl_src"."unrelated" (id int)'))

        copied = await copy_profile_results(conn, "tmpl_src", "sbx_dst")

        assert copied == ["customers_1_profile_columns"]
        columns.schema = "sbx_dst"
        rows = (await conn.execute_core(select(columns.c.run_id, columns.c.null_count))).all()
        assert [tuple(r) for r in rows] == [("r1", 227)]
        held = (
            await conn.execute_core(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'sbx_dst'"
                )
            )
        ).fetchall()
        assert [r[0] for r in held] == ["customers_1_profile_columns"]
        await conn.execute_core(text('DROP SCHEMA "tmpl_src" CASCADE'))
        await conn.execute_core(text('DROP SCHEMA "sbx_dst" CASCADE'))

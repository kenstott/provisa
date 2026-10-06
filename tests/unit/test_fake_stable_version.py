# Copyright (c) 2026 Kenneth Stott
# Canary: 0419a04f-04ea-4ab5-9bcc-98e6fdaa4262
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A stable fake is pinned to the portable definition version it was saved under (REQ-1494: a
release never changes an existing stable column's fakes silently). The pin is kept while the
column stays stable with the same fake, taken anew when the fake changes or becomes stable, and
cleared when it is no longer stable."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import select

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import table_columns
from provisa.fakes import portable
from provisa.fakes.portable import CURRENT_VERSION, pinned_version


def test_the_pin_is_kept_while_the_fake_is_and_taken_anew_when_it_changes():
    assert pinned_version("email()", False, None) is None
    assert pinned_version("email()", True, None) == CURRENT_VERSION
    assert pinned_version("email()", True, ("email()", True, 7)) == 7
    assert pinned_version("name()", True, ("email()", True, 7)) == CURRENT_VERSION
    assert pinned_version("email()", True, ("email()", False, None)) == CURRENT_VERSION
    assert pinned_version("email()", False, ("email()", True, 7)) is None


@pytest.fixture
def tenant_db(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'tenant.db'}")
    db = Database(engine, name="org")
    asyncio.run(init_schema(db, "", org_id="default"))
    yield db
    engine.dispose()


def _table(fake: str, stable: bool) -> Table:
    return Table(
        source_id="ui_pg",
        domain_id="d",
        schema="public",
        table="people",
        columns=[
            Column(name="id", data_type="integer", visible_to=["*"], is_primary_key=True),
            Column(
                name="email", data_type="varchar", visible_to=["*"], fake=fake, fake_stable=stable
            ),
        ],
    )


async def _save(db: Database, fake: str, stable: bool) -> int | None:
    async with db.acquire() as conn:
        await table_repo.upsert(conn, _table(fake, stable), origin="admin")
        row = (
            await conn.execute_core(
                select(table_columns.c.fake_stable_version).where(
                    table_columns.c.column_name == "email"
                )
            )
        ).fetchone()
    return row[0]


def test_a_release_with_a_newer_definition_keeps_a_saved_stable_column_on_its_version(
    tenant_db, monkeypatch
):
    assert asyncio.run(_save(tenant_db, "email()", True)) == 1
    monkeypatch.setattr(portable, "CURRENT_VERSION", 2)
    # Saved again unchanged under the newer release: still version 1.
    assert asyncio.run(_save(tenant_db, "email()", True)) == 1
    # Its fake changed: the operator chose anew, so it takes the current version.
    assert asyncio.run(_save(tenant_db, "name()", True)) == 2
    assert asyncio.run(_save(tenant_db, "name()", False)) is None

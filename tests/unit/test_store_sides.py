# Copyright (c) 2026 Kenneth Stott
# Canary: f6c6dc59-f531-472d-b485-08a3916c3e6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model store and the state store are two handles (REQ-1919, REQ-1920, REQ-1922).

With regions an org's model lives in one database shared by its regions and each region keeps
its state (replica builds, events, the request record) in its own. Every org has the two handles —
``model_db`` and ``tenant_db`` — even when both reach one database, and a handle refuses a
statement on the other's tables, naming it: so a read that would cross between them in a region
deployment fails in every deployment, and in every test, instead of only where it would break."""

# Requirements: REQ-1919, REQ-1920, REQ-1922

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core.database import Database, StoreSideViolation, create_engine_from_url
from provisa.core.schema_org import metadata, registered_tables, replica_state


@pytest.fixture
def handles(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'org.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    return Database(engine, "model", holds="model"), Database(engine, "state", holds="state")


async def test_each_handle_reads_and_writes_its_own_tables(handles):
    model_db, tenant_db = handles
    async with model_db.acquire() as conn:
        await conn.execute_core(select(registered_tables.c.id))
        await conn.fetch("SELECT id FROM registered_tables")
    async with tenant_db.acquire() as conn:
        await conn.execute_core(select(replica_state.c.source_id))
        await conn.fetch("SELECT * FROM replica_state")


async def test_the_state_handle_refuses_a_model_table_by_name(handles):
    _, tenant_db = handles
    async with tenant_db.acquire() as conn:
        with pytest.raises(
            StoreSideViolation, match="registered_tables is a model table, use the org.s model_db"
        ):
            await conn.execute_core(select(registered_tables.c.id))
        with pytest.raises(StoreSideViolation, match="registered_tables"):
            await conn.fetch("SELECT t.id FROM org_x.registered_tables t JOIN events e ON true")


async def test_the_model_handle_refuses_a_state_table_by_name(handles):
    model_db, _ = handles
    async with model_db.acquire() as conn:
        with pytest.raises(StoreSideViolation, match="replica_state is a state table"):
            await conn.execute_core(select(replica_state.c.source_id))
        with pytest.raises(StoreSideViolation, match="query_audit_log"):
            await conn.execute("DELETE FROM query_audit_log WHERE 1 = 0")


async def test_a_handle_that_holds_no_side_refuses_nothing(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'any.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    async with Database(engine, "any").acquire() as conn:
        await conn.execute_core(select(registered_tables.c.id))
        await conn.execute_core(select(replica_state.c.source_id))


def test_every_org_table_is_kept_in_one_store_or_named_as_in_both():
    from provisa.core.store_sides import BOTH, PLATFORM_STATE_SIDE, TABLES

    org = set(metadata.tables)
    assert not TABLES[PLATFORM_STATE_SIDE] & org  # the deployment's, not an org's
    sides = [tables for side, tables in TABLES.items() if side != PLATFORM_STATE_SIDE]
    for i, one in enumerate(sides):
        for other in sides[i + 1 :]:
            assert not one & other
    kept = frozenset().union(*sides) | BOTH
    assert kept == org, org - kept


async def test_the_org_handles_are_one_of_each(tmp_path):
    from provisa.core.database import org_store_handles
    from provisa.core.model_change import ModelPlane

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'org.db'}")
    model_db, tenant_db, record_db = org_store_handles(
        "org_acme",
        ModelPlane("acme", None),
        model_engine=engine,
        state_engine=engine,
        record_engine=engine,
    )
    assert (model_db.holds, tenant_db.holds, record_db.holds) == ("model", "state", "record")
    assert model_db.model is not None and tenant_db.model is None and record_db.model is None


async def test_a_schema_statement_is_not_refused(handles):
    """A store's schema is laid out whole, naming every table; only row reads and writes are
    checked."""
    _, tenant_db = handles
    async with tenant_db.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS registered_tables_x (id INTEGER)")
        await conn.execute("DROP TABLE IF EXISTS registered_tables_x")
        with pytest.raises(StoreSideViolation):
            await conn.fetch("-- a comment first\nSELECT id FROM registered_tables")


async def test_the_record_is_kept_apart_from_the_state(tmp_path):
    """REQ-1922: a region names the store its record is kept in, so the record handle refuses
    the region's state and the state handle refuses the record."""
    from provisa.core.schema_org import query_audit_log

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'org.db'}")
    with engine.begin() as raw:
        metadata.create_all(raw)
    record_db = Database(engine, "record", holds="record")
    tenant_db = Database(engine, "state", holds="state")
    async with record_db.acquire() as conn:
        await conn.execute_core(select(query_audit_log.c.id))
        with pytest.raises(StoreSideViolation, match="replica_state is a state table"):
            await conn.execute_core(select(replica_state.c.source_id))
    async with tenant_db.acquire() as conn:
        with pytest.raises(StoreSideViolation, match="query_audit_log is a record table"):
            await conn.execute_core(select(query_audit_log.c.id))


def test_no_foreign_key_crosses_from_one_store_to_another():
    """REQ-1922: in a region deployment the stores are separate databases, where a foreign key
    between them cannot hold."""
    from provisa.core.store_sides import BOTH, side_of

    crossing = [
        f"{t.name}.{fk.parent.name} -> {fk.column.table.name}"
        for t in metadata.sorted_tables
        for fk in t.foreign_keys
        if t.name not in BOTH
        and fk.column.table.name not in BOTH
        and side_of(t.name) != side_of(fk.column.table.name)
    ]
    assert crossing == []

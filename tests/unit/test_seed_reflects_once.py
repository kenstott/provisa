# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8c1d27-5a94-4e0b-9b6d-c2e7a41f0d53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006, REQ-2190: the boot reads the control plane's own catalog once a schema, not once a
table.

The meta domain registers each of its views with the columns the view has, read from the control
plane's catalog. With the pg_catalog-shaped views (REQ-2190) that is over two hundred views, and a
reflection per view repeated the catalog-wide lookups every reflection makes (PostgreSQL's domains
and enums, each a scan of ``pg_type``, which grows with every org schema's views): on the hosted
control plane, a boot that answered /health in about two minutes took five to seven. The seed and
the boot's declared-keys pass now read a schema's views in one pass, so the number of catalog
reads is fixed whatever the number of views.
"""

# Requirements: REQ-2006, REQ-2190

from __future__ import annotations

import pytest
from sqlalchemy import event, insert

# What a fixed number of catalog reads may be: a pass reads columns, keys, domains and enums.
_CATALOG_READS_A_PASS = 8


def _catalog_reads(conn):
    """Count the statements that read PostgreSQL's catalog for a reflection (the seed's own view
    DDL names pg_catalog too, and is not counted)."""
    reads = {"n": 0}

    def _count(_c, _cursor, statement, _params, _context, _executemany):
        text = statement.lstrip().upper()
        if text.startswith(("SELECT", "WITH")) and "PG_CATALOG.PG_" in text:
            reads["n"] += 1

    event.listen(conn._sc.engine, "before_cursor_execute", _count)
    return reads, lambda: event.remove(conn._sc.engine, "before_cursor_execute", _count)


@pytest.mark.asyncio
async def test_the_meta_seed_reads_the_catalog_a_fixed_number_of_times(control_plane):
    from provisa.api.app_loaders import _META_TABLES
    from provisa.api.startup_seed import _seed_meta_domain
    from provisa.core.schema_org import sources

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        reads, stop = _catalog_reads(conn)
        try:
            await _seed_meta_domain(conn, org_id="default")
        finally:
            stop()
    assert len(_META_TABLES) > 100, "the views this pins are the pg_catalog-shaped ones"
    assert reads["n"] <= _CATALOG_READS_A_PASS, (
        f"{reads['n']} catalog reads seeding {len(_META_TABLES)} meta views"
    )


@pytest.mark.asyncio
async def test_the_boot_key_pass_reads_the_meta_schema_once(monkeypatch, control_plane):
    from provisa.api import startup_seed
    from provisa.api.app import state
    from provisa.api.startup_seed import _seed_meta_domain
    from provisa.core.schema_org import registered_tables, sources, table_columns
    from provisa.federation import replica_routing
    from sqlalchemy import select

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await _seed_meta_domain(conn, org_id="default")

    async def _none_kept(_state):
        return frozenset()

    monkeypatch.setattr(replica_routing, "kept_sources", _none_kept)
    monkeypatch.setattr(state, "model_db", db, raising=False)
    async with db.acquire() as conn:
        reads, stop = _catalog_reads(conn)
    try:
        await startup_seed._resolve_pk_from_sources()
    finally:
        stop()
    assert reads["n"] <= _CATALOG_READS_A_PASS, f"{reads['n']} catalog reads in the key pass"

    # The pass still records what the catalog says: a meta table keyed by its base table's id.
    async with db.acquire() as conn:
        keyed = (
            await conn.execute_core(
                select(table_columns.c.column_name)
                .select_from(
                    table_columns.join(
                        registered_tables, registered_tables.c.id == table_columns.c.table_id
                    )
                )
                .where(
                    registered_tables.c.table_name == "registered_tables",
                    table_columns.c.is_primary_key.is_(True),
                )
            )
        ).fetchall()
    assert [r[0] for r in keyed] == ["id"]

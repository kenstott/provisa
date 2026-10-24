# Copyright (c) 2026 Kenneth Stott
# Canary: 0389ff53-a2ee-4f23-a54c-57e5c2af12ea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A new org's schema is created from nothing on PostgreSQL, both ways an org schema is laid
out: schema.sql (provision_org, bind_region_stores, the sandbox and demo paths) and the org
metadata (create_all). registered_tables.class_term_id references glossary_terms (REQ-2130), so
either order must create glossary_terms before that reference is made."""

# Requirements: REQ-697, REQ-2130

import pytest
from sqlalchemy import text

from provisa.core.database import Database
from provisa.core.db import init_schema
from provisa.core.schema_org import metadata as org_metadata
from tests.pg_control_plane import SCHEMA_SQL

_FK = """
    SELECT ccu.table_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :schema
      AND tc.table_name = 'registered_tables' AND kcu.column_name = 'class_term_id'
"""


@pytest.mark.asyncio
async def test_schema_sql_lays_out_a_new_org_from_nothing(control_plane):
    db = Database(control_plane.engine(), name="fresh")
    await init_schema(db, SCHEMA_SQL, org_id="fresh_sql")
    async with db.acquire() as conn:
        rows = await conn.fetch(_FK.replace(":schema", "$1"), "org_fresh_sql")
    assert [r[0] for r in rows] == ["glossary_terms"]


def test_the_org_metadata_lays_out_a_new_org_from_nothing(control_plane):
    engine = control_plane.engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA org_fresh_md"))
        conn.execute(text("SET search_path TO org_fresh_md"))
        org_metadata.create_all(conn)
        rows = conn.execute(text(_FK), {"schema": "org_fresh_md"}).fetchall()
    assert [r[0] for r in rows] == ["glossary_terms"]

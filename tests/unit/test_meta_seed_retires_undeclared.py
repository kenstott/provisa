# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2e5f14-c3a7-4d91-a6e0-71f9d2c84b3e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2163: the meta seed registers its tables as a SET, and retires the ones it no longer
declares.

REQ-2163 moved the glossary's views out of meta (meta.glossary_term_refs -> ontology._term_refs)
and renamed their seeded relationships (meta:registered_tables:glossary_term_refs ->
meta:registered_tables:_term_refs, both GraphQL field ``glossaryRefs`` of registered_tables). A
control plane seeded before that kept the old registrations, and with them the old relationships,
next to the new: two relationships named ``glossaryRefs`` on M__RegisteredTables, and the schema
build refused with RelationshipFieldTaken on every boot of that org (J627, alpha.500).
"""

# Requirements: REQ-2163

from __future__ import annotations

import pytest
from sqlalchemy import func, insert, select

from provisa.core.schema_org import registered_tables, relationships, sources

_SCHEMA = "org_default"


async def _register(conn, table_name: str) -> int:
    return (
        await conn.execute_core(
            insert(registered_tables)
            .values(
                source_id="provisa-admin",
                domain_id="meta",
                schema_name=_SCHEMA,
                table_name=table_name,
            )
            .returning(registered_tables.c.id)
        )
    ).scalar_one()


async def _boot_seeds(conn) -> None:
    from provisa.api.startup_seed import (
        _seed_meta_domain,
        _seed_meta_relationships,
        _seed_ontology_domain,
    )

    await _seed_meta_domain(conn, org_id="default")
    await _seed_ontology_domain(conn, org_id="default")
    await _seed_meta_relationships(conn)


@pytest.mark.asyncio
async def test_a_reseed_retires_the_glossary_registrations_meta_no_longer_declares(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await _boot_seeds(conn)
        # What a pre-REQ-2163 seed left behind: the glossary's meta registration and its edge from
        # registered_tables, under the field name the ontology edge now carries.
        rt_id = (
            await conn.execute_core(
                select(registered_tables.c.id).where(
                    registered_tables.c.source_id == "provisa-admin",
                    registered_tables.c.table_name == "registered_tables",
                )
            )
        ).scalar_one()
        old_refs = await _register(conn, "glossary_term_refs")
        await conn.execute_core(
            insert(relationships).values(
                id="meta:registered_tables:glossary_term_refs",
                source_table_id=rt_id,
                target_table_id=old_refs,
                source_column="id",
                target_column="table_id",
                cardinality="one-to-many",
                graphql_alias="glossaryRefs",
            )
        )

        await _boot_seeds(conn)

        remaining = (
            await conn.execute_core(
                select(registered_tables.c.table_name).where(
                    registered_tables.c.domain_id == "meta",
                    registered_tables.c.table_name == "glossary_term_refs",
                )
            )
        ).fetchall()
        fields = (
            await conn.execute_core(
                select(relationships.c.id).where(
                    relationships.c.source_table_id == rt_id,
                    relationships.c.graphql_alias == "glossaryRefs",
                )
            )
        ).fetchall()
        # Every seeded field of a table is one relationship (what the schema build asserts).
        clashes = (
            await conn.execute_core(
                select(relationships.c.source_table_id, relationships.c.graphql_alias)
                .where(relationships.c.graphql_alias.is_not(None))
                .group_by(relationships.c.source_table_id, relationships.c.graphql_alias)
                .having(func.count() > 1)
            )
        ).fetchall()
    assert remaining == []
    assert [r[0] for r in fields] == ["meta:registered_tables:_term_refs"]
    assert clashes == []


@pytest.mark.asyncio
async def test_a_reseed_keeps_the_org_registry_it_does_not_write_itself(control_plane):
    """The root org's registry view is meta's too (REQ-1301), registered by
    seed_org_registry_view after this seed: retiring what the meta seed no longer declares must
    not take it."""
    from provisa.core.org_registry_view import VIEW_NAME

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await _register(conn, VIEW_NAME)
        await _boot_seeds(conn)
        kept = (
            await conn.execute_core(
                select(registered_tables.c.table_name).where(
                    registered_tables.c.domain_id == "meta",
                    registered_tables.c.table_name == VIEW_NAME,
                )
            )
        ).fetchall()
    assert [r[0] for r in kept] == [VIEW_NAME]

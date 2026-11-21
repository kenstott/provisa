# Copyright (c) 2026 Kenneth Stott
# Canary: 5d1a8c37-2e94-4b6f-a0c8-7f3e9b1d4a52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tag follows its object's environment class (REQ-2208): a tag on a reference library document or
one of its sections is the environment's own, as the document is, so a copy, a merge or a deploy of
the model from another environment neither brings that environment's document tags nor removes this
one's. Tags on the model's objects (tag_assignments) travel with the model, as before.
"""

# Requirements: REQ-2208, REQ-2185, REQ-1548

from __future__ import annotations

import pytest
from sqlalchemy import select

from provisa.core import env_copy, env_merge
from provisa.core.database import Database
from provisa.core.env_classes import CARRIED, NEVER_RUNTIME, UNBOUND
from provisa.core.env_deploy import deploy_tree
from provisa.core.env_project import project
from provisa.core.environments import org_schema
from provisa.core.models import TagAssignment
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import metadata
from provisa.reference_library import store

pytestmark = pytest.mark.asyncio

ORG = "default"
DEV = "dev"


def test_a_library_tag_is_the_environments_own_and_a_model_tag_travels():
    assert "reference_document_tags" in NEVER_RUNTIME  # as reference_documents is
    assert "reference_documents" in NEVER_RUNTIME
    assert "tag_assignments" in CARRIED


async def _document(db: Database, title: str, tag: str) -> int:
    async with db.acquire() as conn:
        document = await store.insert_document(
            conn,
            title=title,
            source_kind=store.URL,
            source=f"https://example.com/{title}",
            media_type="text/markdown",
            content_sha256=title.encode().hex().ljust(64, "0")[:64],
            status=store.KEPT,
            embedding_model="m",
            added_by="org_admin",
            domains=["*"],
            passages=[
                store.StoredPassage(
                    ordinal=0, section="Personal data", page=None, text="t", vector=[0.1, 0.9]
                )
            ],
        )
        for heading in (None, "Personal data"):  # the document, and its section
            await tag_repo.assign(
                conn,
                TagAssignment(
                    tag_id=tag,
                    object_type="document" if heading is None else "section",
                    document_id=document,
                    section_heading=heading,
                ),
            )
    return document


async def _library_tags(db: Database) -> set[tuple[str, str]]:
    """Every tag on a document or a section the environment holds, in any table that holds tags."""
    found: set[tuple[str, str]] = set()
    async with db.acquire() as conn:
        for name in ("tag_assignments", "reference_document_tags"):
            if name not in metadata.tables:
                continue
            held = metadata.tables[name]
            rows = (
                await conn.execute_core(
                    select(held.c.tag_id, held.c.object_type).where(
                        held.c.object_type.in_(["document", "section"])
                    )
                )
            ).fetchall()
            found |= {(r.tag_id, r.object_type) for r in rows}
    return found


@pytest.fixture
async def envs(control_plane, monkeypatch):
    """prod (the org's schema) and dev laid out as schema.sql lays out an environment, each holding
    a library document tagged in it: ``prod-only`` in prod, ``dev-only`` in dev."""
    prod_schema, dev_schema = org_schema(ORG), org_schema(ORG, DEV)
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'CREATE SCHEMA "{dev_schema}"')
        for table in metadata.sorted_tables:
            c.exec_driver_sql(
                f'CREATE TABLE "{dev_schema}"."{table.name}" '
                f'(LIKE "{prod_schema}"."{table.name}" INCLUDING ALL)'
            )
    prod = control_plane.org(ORG)
    dev = Database(
        control_plane.engine(f"{control_plane.url}?options=-csearch_path%3D{dev_schema}"),
        name="org-model",
        search_path=dev_schema,
    )
    await _document(prod, "prod-policy", "prod-only")
    await _document(dev, "dev-policy", "dev-only")
    # A merge names its source commit and the commit both lines last shared: no repository here.
    monkeypatch.setattr(env_merge, "_source_ref", lambda org_id, env: env or "prod")
    monkeypatch.setattr(env_merge, "_base", lambda org_id, source, target: None)
    return prod, dev


DEV_ONLY = {("dev-only", "document"), ("dev-only", "section")}


async def test_a_copy_neither_brings_nor_removes_a_document_tag(envs):
    prod, dev = envs
    await env_copy.copy_model(prod, ORG, None, DEV, landing=UNBOUND)
    assert await _library_tags(dev) == DEV_ONLY


async def test_a_merge_neither_brings_nor_removes_a_document_tag(envs):
    prod, dev = envs
    await env_merge.merge_model(prod, ORG, None, DEV, removals=True)
    assert await _library_tags(dev) == DEV_ONLY


async def test_a_deploy_neither_brings_nor_removes_a_document_tag(envs):
    prod, dev = envs
    async with prod.acquire() as conn:
        tree = await project(conn, org_schema(ORG))
    await deploy_tree(prod, ORG, DEV, tree, ref="test")
    assert await _library_tags(dev) == DEV_ONLY

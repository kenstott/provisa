# Copyright (c) 2026 Kenneth Stott
# Canary: 05c3ece8-40f0-495f-b2cd-8aba60e66985
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The source setup stepper's record (REQ-2262, amended 2026-11-26): what became of each step of
a source's setup -- tables registered or skipped; relationships found, none found or skipped;
query opened, done or skipped -- held with the source on the server, shared by every user and
environment. setSourceSetupOutcomes replaces it and stamps when every step was decided; a form
save keeps it; the sources query (and so Polly, through the admin GraphQL) reads it; and it
travels with the source when an environment is copied, merged or deployed, and in a configuration.
"""

# Requirements: REQ-2262

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.core import env_copy, env_merge, model_change
from provisa.core.database import Database
from provisa.core.env_classes import UNBOUND
from provisa.core.env_deploy import deploy_tree
from provisa.core.env_project import project
from provisa.core.environments import org_schema
from provisa.core.models import Source, SourceSetupOutcomes, SourceType
from provisa.core.repositories import source as source_repo
from provisa.core.schema_org import metadata, sources

ORG = "default"
DEV = "dev"


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organisation, as the API layer says at import (provisa.api.app)."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


@pytest.fixture
async def plane(control_plane):
    db = control_plane.org()
    async with model_change.scope("save", actor=lambda: "ada"), db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="crm", type="postgresql"))
    return db


async def _held(db: Database, source_id: str = "crm") -> dict:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(sources.c.setup_outcomes).where(sources.c.id == source_id)
            )
        ).scalar_one()


async def _set(db: Database, outcomes: SourceSetupOutcomes, source_id: str = "crm"):
    async with model_change.scope("save", actor=lambda: "ada"), db.acquire() as conn:
        return await source_repo.set_setup_outcomes(conn, source_id, outcomes)


def test_each_step_takes_only_its_own_outcomes():
    SourceSetupOutcomes(tables="registered", relationships="none_found", query="opened")
    for bad in ({"tables": "found"}, {"relationships": "done"}, {"query": "registered"}):
        with pytest.raises(ValidationError):
            SourceSetupOutcomes(**bad)
    with pytest.raises(ValidationError):
        SourceSetupOutcomes(step="skipped")


async def test_a_new_source_has_nothing_decided(plane):
    assert SourceSetupOutcomes.model_validate(await _held(plane)) == SourceSetupOutcomes()


async def test_a_write_replaces_the_record_and_stamps_when_all_is_decided(plane):
    partial = await _set(plane, SourceSetupOutcomes(tables="skipped"))
    assert partial == SourceSetupOutcomes(tables="skipped")
    assert SourceSetupOutcomes.model_validate(await _held(plane)) == partial

    whole = await _set(
        plane, SourceSetupOutcomes(tables="registered", relationships="found", query="done")
    )
    assert whole is not None and whole.decided_at is not None
    held = SourceSetupOutcomes.model_validate(await _held(plane))
    assert (held.tables, held.relationships, held.query) == ("registered", "found", "done")
    assert held.decided_at == whole.decided_at

    # Run setup again: every step open, no decision time.
    again = await _set(plane, SourceSetupOutcomes())
    assert again == SourceSetupOutcomes()
    assert SourceSetupOutcomes.model_validate(await _held(plane)) == SourceSetupOutcomes()


async def test_no_such_source_records_nothing(plane):
    assert await _set(plane, SourceSetupOutcomes(tables="skipped"), "nope") is None


async def test_a_form_save_keeps_the_record(plane):
    await _set(plane, SourceSetupOutcomes(tables="skipped"))
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await source_repo.upsert(conn, Source(id="crm", type=SourceType.postgresql, port=5433))
    assert SourceSetupOutcomes.model_validate(await _held(plane)).tables == "skipped"


async def test_a_configuration_carries_the_record(plane):
    """Read into a configuration (store_config) and applied again, the record is as held."""
    await _set(
        plane, SourceSetupOutcomes(tables="registered", relationships="skipped", query="opened")
    )
    async with plane.acquire() as conn:
        row = await source_repo.get(conn, "crm")
    written = source_repo.source_from_row(row)
    assert written.setup_outcomes.query == "opened"
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await conn.execute_core(sources.delete().where(sources.c.id == "crm"))
        await source_repo.upsert(conn, written, setup_outcomes=written.setup_outcomes)
    held = SourceSetupOutcomes.model_validate(await _held(plane))
    assert (held.tables, held.relationships, held.query) == ("registered", "skipped", "opened")


async def test_the_mutation_records_it_and_the_sources_query_reads_it(plane, monkeypatch):
    import provisa.api.app as app_mod
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.admin.types import (
        SetupQueryOutcome,
        SetupRelationshipsOutcome,
        SetupTablesOutcome,
    )
    from tests.unit.gate_identity import grant

    async def pool():
        return plane

    monkeypatch.setattr(schema_mutation, "_get_pool", pool)
    info = grant(monkeypatch, "source_registration", state=app_mod.state)[0]
    async with model_change.scope("save", actor=lambda: "ada"):
        result = await Mutation().set_source_setup_outcomes(
            info,
            source_id="crm",
            tables=SetupTablesOutcome.registered,
            relationships=SetupRelationshipsOutcome.none_found,
            query=SetupQueryOutcome.done,
        )
    assert result.success, result.message
    async with model_change.scope("save", actor=lambda: "ada"):
        missing = await Mutation().set_source_setup_outcomes(
            info, source_id="nope", tables=SetupTablesOutcome.skipped
        )
    assert (missing.success, missing.code) == (False, "schema.source_not_found")

    from provisa.api.admin._row_mappers import _source_from_row

    async with plane.acquire() as conn:
        row = await source_repo.get(conn, "crm")
    for connection in (True, False):  # not a connection detail: every reader of sources sees it
        read = _source_from_row(row, connection=connection).setup_outcomes
        assert (read.tables, read.relationships, read.query) == (
            SetupTablesOutcome.registered,
            SetupRelationshipsOutcome.none_found,
            SetupQueryOutcome.done,
        )
        assert read.decided_at is not None


async def test_the_mutation_needs_source_registration(plane, monkeypatch):
    import provisa.api.app as app_mod
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.schema_mutation import Mutation
    from provisa.api.admin.types import SetupTablesOutcome
    from tests.unit.gate_identity import grant

    async def pool():
        return plane

    monkeypatch.setattr(schema_mutation, "_get_pool", pool)
    info = grant(monkeypatch, "usage", state=app_mod.state)[0]
    with pytest.raises(PermissionError):
        await Mutation().set_source_setup_outcomes(
            info, source_id="crm", tables=SetupTablesOutcome.skipped
        )
    assert SourceSetupOutcomes.model_validate(await _held(plane)) == SourceSetupOutcomes()


def test_polly_reads_it_through_the_admin_graphql():
    """Polly's admin tools answer from the admin GraphQL schema (api.mcp.admin_api)."""
    from provisa.api.mcp.admin_api import schema_type

    fields = {f["name"] for f in schema_type("SourceType")["fields"]}
    assert "setupOutcomes" in fields
    outcomes = {f["name"] for f in schema_type("SourceSetupOutcomesType")["fields"]}
    assert outcomes == {"tables", "relationships", "query", "decidedAt"}


@pytest.fixture
async def envs(control_plane, monkeypatch):
    """prod (the org's schema) holding source ``crm`` with its setup decided, and dev laid out as
    schema.sql lays out an environment."""
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
    async with model_change.scope("save", actor=lambda: "ada"), prod.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="crm", type="postgresql"))
    await _set(
        prod, SourceSetupOutcomes(tables="registered", relationships="none_found", query="done")
    )
    # A merge names its source commit and the commit both lines last shared: no repository here.
    monkeypatch.setattr(env_merge, "_source_ref", lambda org_id, env: env or "prod")
    monkeypatch.setattr(env_merge, "_base", lambda org_id, source, target: None)
    return prod, dev


async def _decided_in(db: Database) -> tuple:
    held = SourceSetupOutcomes.model_validate(await _held(db))
    return held.tables, held.relationships, held.query


DECIDED = ("registered", "none_found", "done")


async def test_a_copy_carries_the_record_into_the_environment(envs):
    prod, dev = envs
    await env_copy.copy_model(prod, ORG, None, DEV, landing=UNBOUND)
    assert await _decided_in(dev) == DECIDED


async def test_a_merge_carries_the_record_into_the_environment(envs):
    prod, dev = envs
    await env_merge.merge_model(prod, ORG, None, DEV, removals=True)
    assert await _decided_in(dev) == DECIDED


async def test_a_deploy_carries_the_record_into_the_environment(envs):
    prod, dev = envs
    async with prod.acquire() as conn:
        tree = await project(conn, org_schema(ORG))
    await deploy_tree(prod, ORG, DEV, tree, ref="test")
    assert await _decided_in(dev) == DECIDED

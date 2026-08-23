# Copyright (c) 2026 Kenneth Stott
# Canary: 54b1adb3-196f-44fe-baa8-3f943b314e88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What one environment's model carries into another, run against two real schemas (REQ-1489…91).

The copy is only meaningful against PostgreSQL: it reads and writes two schemas in one statement
stream, carries primary keys verbatim and then advances the target's sequences past them, and
relies on server defaults to land an unbound source row. None of that is observable against a
metadata double, so both environments here are real schemas built from the authoritative
``schema.sql``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from provisa.core.env_copy import MERGE, REPLACE, copy_model, plan_copy
from provisa.core.environments import org_schema
from provisa.core.schema_org import (
    domains,
    metadata as org_metadata,
    org_settings,
    registered_tables,
    roles,
    sources,
    user_role_assignments,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ENV = "dev"


def _scoped(table, schema):
    """The same table addressed in one particular environment's schema."""
    from sqlalchemy import MetaData

    return table.to_metadata(MetaData(), schema=schema)


@pytest.fixture
async def envs(docker_postgres):
    """An org holding a populated ``prod`` and an empty ``dev``, plus helpers to read either."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"envcopy{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = create_engine_from_url(url, pool_size=2)
    db = Database(engine, name="org", search_path=org_schema(org_id))
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(db, schema_sql, org_id=org_id)
    await init_schema(db, schema_sql, org_id=org_id, env=ENV)

    async def rows(table, env=None, order_by="id"):
        scoped = _scoped(table, org_schema(org_id, env))
        async with db.acquire() as conn:
            result = await conn.execute_core(select(scoped).order_by(scoped.c[order_by]))
            return [dict(r._mapping) for r in result.fetchall()]

    async def insert(table, env=None, **values):
        scoped = _scoped(table, org_schema(org_id, env))
        async with db.acquire() as conn:
            await conn.execute_core(scoped.insert().values(**values))

    async def update(table, key, env=None, **values):
        scoped = _scoped(table, org_schema(org_id, env))
        pk = next(iter(scoped.primary_key.columns))
        async with db.acquire() as conn:
            await conn.execute_core(scoped.update().where(pk == key).values(**values))

    # schema.sql seeds the meta domains and roles into EVERY schema it builds, so both
    # environments already share them before any copy runs. Tests assert on what the copy moved,
    # which is the difference from this baseline rather than the whole table.
    baseline = {d["id"] for d in await rows(domains, ENV)}

    async def added_domains(env=ENV):
        return sorted({d["id"] for d in await rows(domains, env)} - baseline)

    yield type(
        "Envs",
        (),
        {
            "org_id": org_id,
            "db": db,
            "rows": staticmethod(rows),
            "insert": staticmethod(insert),
            "update": staticmethod(update),
            "added_domains": staticmethod(added_domains),
        },
    )
    await engine.dispose()


@pytest.fixture
async def seeded(envs):
    """prod holds a bound source, a domain, a registered table and two settings."""
    await envs.insert(
        sources,
        id="warehouse",
        type="postgres",
        host="prod-db.internal",
        port=5432,
        database="analytics",
        username="prod_reader",
        dialect="postgresql",
        description="the warehouse",
    )
    await envs.insert(domains, id="sales", description="revenue", steward="ana")
    await envs.insert(
        registered_tables,
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
    )
    await envs.insert(org_settings, key="naming.style", value={"case": "snake"})
    await envs.insert(org_settings, key="cache.redis_url", value={"url": "redis://prod:6379"})
    return envs


class TestCreationCopy:
    """REPLACE — what a new environment is born holding."""

    async def test_the_governed_model_travels_whole(self, seeded):
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert await seeded.added_domains() == ["sales"]
        registered = await seeded.rows(registered_tables, ENV)
        assert [(r["source_id"], r["table_name"]) for r in registered] == [("warehouse", "orders")]

    async def test_a_row_naming_the_source_schema_is_rebased_onto_the_target_schema(self, seeded):
        # REQ-1301's registry view is registered against the schema it lives in. Carried verbatim,
        # the new environment would claim its registry sits in prod's schema, and its own seed —
        # keyed on (source_id, schema_name, table_name) — would add a SECOND registration of
        # meta.org_registry, which every request to that runtime then fails on.
        await seeded.insert(
            registered_tables,
            source_id="warehouse",
            domain_id="sales",
            schema_name=org_schema(seeded.org_id),
            table_name="org_registry",
        )
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        copied = {
            r["table_name"]: r["schema_name"] for r in await seeded.rows(registered_tables, ENV)
        }
        assert copied["org_registry"] == org_schema(seeded.org_id, ENV)
        # a real source schema is not a schema of this org and is left exactly as it is
        assert copied["orders"] == "public"

    async def test_the_source_row_arrives_without_the_connection_it_pointed_at(self, seeded):
        # REQ-1491: the row must exist — registered_tables references it — but where prod points is
        # prod's, and a copied environment inheriting it would write to production.
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        (copied,) = await seeded.rows(sources, ENV)
        assert copied["id"] == "warehouse"
        assert copied["type"] == "postgres"
        assert copied["description"] == "the warehouse"
        assert (copied["host"], copied["database"], copied["username"]) == ("", "", "")
        assert copied["port"] == 0

    async def test_an_unbound_row_is_marked_rather_than_left_blank(self, seeded):
        # An empty host is not an absent one — the connection builder reads it as localhost:5432 —
        # so boundness is a column, not something inferred from the emptiness of another column.
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        (copied,) = await seeded.rows(sources, ENV)
        assert copied["bound"] is False
        (original,) = await seeded.rows(sources)
        assert original["bound"] is True

    async def test_settings_naming_a_runtime_stay_with_the_environment_that_set_them(self, seeded):
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        keys = [s["key"] for s in await seeded.rows(org_settings, ENV, order_by="key")]
        assert keys == ["naming.style"]

    async def test_a_secret_is_not_a_thing_a_copy_can_carry(self, seeded):
        # REQ-1489 is an allow-list: org_secrets is in no carried class, so no code path reads it.
        from provisa.core.schema_org import org_secrets

        await seeded.insert(org_secrets, key="anthropic", value_enc=b"ciphertext")
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert await seeded.rows(org_secrets, ENV, order_by="key") == []

    async def test_the_target_sequence_is_advanced_past_the_ids_the_copy_carried(self, seeded):
        # Keys travel verbatim because the model references them; a target sequence still sitting
        # at 1 would hand the next insert an id the copy already used.
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        await seeded.insert(
            registered_tables,
            ENV,
            source_id="warehouse",
            domain_id="sales",
            schema_name="public",
            table_name="returns",
        )
        ids = [r["id"] for r in await seeded.rows(registered_tables, ENV)]
        assert len(ids) == len(set(ids)) == 2

    async def test_a_replace_drops_what_the_target_holds_and_the_source_does_not(self, seeded):
        await seeded.insert(domains, ENV, id="stale", description="from an older load")
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert await seeded.added_domains() == ["sales"]


class TestMerge:
    """REQ-1490 — a merge matches by identity and leaves the rest of the target alone."""

    @pytest.fixture(autouse=True)
    async def _created(self, seeded):
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)

    async def test_a_binding_the_environment_established_survives_the_merge(self, seeded):
        # A checkout carries no bindings, so it has nothing to overwrite this with (REQ-1491).
        await seeded.update(
            sources, "warehouse", ENV, host="dev-db.internal", database="scratch", bound=True
        )
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE)
        (row,) = await seeded.rows(sources, ENV)
        assert (row["host"], row["database"], row["bound"]) == ("dev-db.internal", "scratch", True)

    async def test_governance_on_a_source_row_still_arrives(self, seeded):
        await seeded.update(sources, "warehouse", description="renamed upstream")
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE)
        (row,) = await seeded.rows(sources, ENV)
        assert row["description"] == "renamed upstream"

    async def test_an_object_the_target_added_is_left_alone(self, seeded):
        await seeded.insert(domains, ENV, id="experiment", description="only in dev")
        report = await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE)
        assert await seeded.added_domains() == ["experiment", "sales"]
        assert report.as_dict()["removed"] == 0

    async def test_removals_are_a_separate_decision(self, seeded):
        await seeded.insert(domains, ENV, id="experiment", description="only in dev")
        report = await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE, removals=True)
        assert await seeded.added_domains() == ["sales"]
        removed = [k for t in report.tables if t.table == "domains" for k in t.removed]
        assert removed == ["experiment"]

    async def test_a_source_row_is_never_removed_by_a_merge(self, seeded):
        # REQ-1491: a binding is the environment's own deliberate fact, and nothing arriving from
        # another environment is evidence that it should go.
        await seeded.insert(sources, ENV, id="local_files", type="file", bound=True)
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE, removals=True)
        assert [s["id"] for s in await seeded.rows(sources, ENV)] == ["local_files", "warehouse"]

    async def test_a_changed_object_is_updated_in_place(self, seeded):
        await seeded.update(domains, "sales", description="revenue and refunds")
        report = await copy_model(seeded.db, seeded.org_id, None, ENV, mode=MERGE)
        (row,) = [d for d in await seeded.rows(domains, ENV) if d["id"] == "sales"]
        assert row["description"] == "revenue and refunds"
        assert report.as_dict()["changed"] == 1
        assert report.as_dict()["added"] == 0


class TestSeededClasses:
    """REQ-1539: roles and assignments are seeded by a creation and never carried again."""

    async def test_a_creation_seeds_the_roles_of_the_environment_it_came_from(self, seeded):
        await seeded.insert(roles, id="lab_reviewer", capabilities=["write"], domain_access=[])
        await seeded.insert(
            user_role_assignments, user_id="ana", role_id="lab_reviewer", domain_id="sales"
        )
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE, seed=True)
        assert "lab_reviewer" in {r["id"] for r in await seeded.rows(roles, ENV)}
        assert [
            (a["user_id"], a["role_id"])
            for a in await seeded.rows(user_role_assignments, ENV, order_by="user_id")
        ] == [("ana", "lab_reviewer")]

    async def test_a_creation_that_does_not_seed_leaves_them_behind(self, seeded):
        # The default. Only the create endpoint asks for a seed; nothing else may.
        await seeded.insert(roles, id="lab_reviewer", capabilities=["write"], domain_access=[])
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert "lab_reviewer" not in {r["id"] for r in await seeded.rows(roles, ENV)}

    async def test_a_merge_cannot_carry_a_branch_role_into_the_base(self, seeded):
        # The escalation this class exists to prevent: dev's `developer` is unrestricted and
        # prod's holds nothing, and a merge is a statement about the MODEL, not about who may act.
        await seeded.update(roles, "developer", capabilities=[])
        await seeded.update(roles, "developer", env=ENV, capabilities=["write"])
        await copy_model(seeded.db, seeded.org_id, ENV, None, mode=MERGE, removals=True)
        prod = {r["id"]: r["capabilities"] for r in await seeded.rows(roles)}
        assert prod["developer"] == []

    async def test_a_merge_cannot_carry_an_assignment_into_the_base(self, seeded):
        await seeded.insert(
            user_role_assignments, env=ENV, user_id="ana", role_id="developer", domain_id="sales"
        )
        await copy_model(seeded.db, seeded.org_id, ENV, None, mode=MERGE, removals=True)
        assert await seeded.rows(user_role_assignments) == []


class TestPlan:
    async def test_a_plan_changes_nothing(self, seeded):
        report = await plan_copy(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert report.as_dict()["added"] > 0
        assert await seeded.added_domains() == []

    async def test_the_plan_and_the_copy_agree(self, seeded):
        planned = await plan_copy(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        applied = await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        assert planned.as_dict() == applied.as_dict()


class TestRefusals:
    async def test_an_environment_cannot_be_copied_onto_itself(self, envs):
        with pytest.raises(ValueError, match="onto itself"):
            await copy_model(envs.db, envs.org_id, None, None, mode=REPLACE)

    async def test_an_unknown_mode_is_refused_rather_than_defaulted(self, envs):
        with pytest.raises(ValueError, match="unknown copy mode"):
            await copy_model(envs.db, envs.org_id, None, ENV, mode="sync")

    async def test_no_table_outside_the_carried_classes_is_touched(self, seeded):
        # The copy walks org_metadata; this asserts the walk is filtered by the allow-list rather
        # than by a list of exclusions somebody has to keep complete.
        from provisa.core.env_classes import CARRIED, IDENTITY_ONLY

        report = await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE)
        touched = {t.table for t in report.tables}
        assert touched <= CARRIED | IDENTITY_ONLY | {"org_settings"}
        assert touched <= set(org_metadata.tables)


class TestConflicts:
    """REQ-1555: which of the target's own work the merge carries away.

    Real commits, because the answer is measured against the one both branches last held -- a
    double that returned a base would be testing the double. The two branches are given a shared
    baseline the way an environment gets one (REQ-1543), then each side is edited in its own schema
    and re-projected, which is exactly the shape two people working at once produce.
    """

    @staticmethod
    async def _commit(envs, env, message):
        """Project one environment's schema into its branch, as the write-through does."""
        from provisa.core.env_files import dump
        from provisa.core.env_project import project
        from provisa.core.env_repo import commit_files, ensure_repo

        async with envs.db.acquire() as conn:
            files = dump(await project(conn, org_schema(envs.org_id, env)))
        return commit_files(ensure_repo(envs.org_id), env or "prod", files, message, "ana")

    @pytest.fixture
    async def parted(self, seeded):
        """prod and dev holding the same model, each with the commit both lines share."""
        from provisa.core.env_repo import ensure_repo, start_branch

        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE, seed=True)
        await self._commit(seeded, None, "provisioned")
        # REQ-1543: a branch is SEEDED at its source's tip rather than rooted beside it, which is
        # what makes a commit both lines held exist at all.
        start_branch(ensure_repo(seeded.org_id), ENV, "prod")
        await self._commit(seeded, ENV, "created from prod")
        return seeded

    async def test_an_object_both_lines_edited_is_named(self, parted):
        await parted.update(domains, "sales", env=ENV, description="branch revenue")
        await self._commit(parted, ENV, "dev edits sales")
        await parted.update(domains, "sales", description="prod revenue")
        await self._commit(parted, None, "prod edits sales")

        report = await plan_copy(parted.db, parted.org_id, ENV, None, mode=MERGE)
        assert report.compared
        assert [(c.path, c.source, c.target) for c in report.conflicts] == [
            ("sales/domain.yaml", "changed", "changed")
        ]

    async def test_work_only_the_source_did_is_the_merge_doing_its_job(self, parted):
        await parted.update(domains, "sales", env=ENV, description="branch revenue")
        await self._commit(parted, ENV, "dev edits sales")

        report = await plan_copy(parted.db, parted.org_id, ENV, None, mode=MERGE)
        assert report.compared
        assert report.conflicts == []
        assert report.as_dict()["changed"] > 0

    async def test_the_merge_still_applies_and_says_what_it_carried_away(self, parted):
        """Reported, not refused: a merge into a target IS the source winning. What was missing
        was the sentence naming whose work went."""
        await parted.update(domains, "sales", env=ENV, description="branch revenue")
        await self._commit(parted, ENV, "dev edits sales")
        await parted.update(domains, "sales", description="prod revenue")
        await self._commit(parted, None, "prod edits sales")

        report = await copy_model(parted.db, parted.org_id, ENV, None, mode=MERGE)
        assert [c["path"] for c in report.as_dict()["conflicts"]] == ["sales/domain.yaml"]
        prod = {d["id"]: d["description"] for d in await parted.rows(domains)}
        assert prod["sales"] == "branch revenue"

    async def test_a_checkout_asks_no_such_question(self, parted):
        """REPLACE is the operator stating which model the environment runs, so the target's
        divergence is the point of the act rather than a collision inside it."""
        await parted.update(domains, "sales", description="prod revenue")
        report = await plan_copy(parted.db, parted.org_id, ENV, None, mode=REPLACE)
        assert report.conflicts == [] and not report.compared

    async def test_lines_that_share_no_ancestor_report_that_nothing_was_compared(self, seeded):
        # Each branch rooted on its own baseline rather than branched from the other: there is no
        # commit both held, so the question cannot be asked -- which is not the same answer as a
        # clean merge, and the report distinguishes them.
        await self._commit(seeded, None, "provisioned")
        await copy_model(seeded.db, seeded.org_id, None, ENV, mode=REPLACE, seed=True)
        await seeded.update(domains, "sales", env=ENV, description="branch revenue")
        await self._commit(seeded, ENV, "dev's own root")

        report = await plan_copy(seeded.db, seeded.org_id, ENV, None, mode=MERGE)
        assert not report.compared
        assert report.conflicts == []
        assert report.as_dict()["base"] is None

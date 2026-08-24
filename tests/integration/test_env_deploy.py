# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2b4e71-3c05-4a8f-b16d-52f7a9c0e483
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A tree made into a model, or refused whole, against real schemas (REQ-1496).

The deploy is the inverse of the projection, so the test that matters is the round trip: a model
projected out of one schema and loaded into another must project back byte-identically, with every
surrogate re-minted on the way in and no reference left pointing at an integer from the schema it
came from. None of that is observable against a metadata double.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select

from provisa.core.env_files import dump, load
from provisa.core.env_deploy import DeployError, deploy_tree, plan_deploy
from provisa.core.env_project import project
from provisa.core.environments import org_schema
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    roles,
    sources,
    table_columns,
    tag_assignments,
    tracked_functions,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ENV = "dev"


def _scoped(table, schema):
    from sqlalchemy import MetaData

    return table.to_metadata(MetaData(), schema=schema)


@pytest.fixture
async def org(docker_postgres):
    """An org with a populated ``prod`` and an empty ``dev``, and helpers to read either."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"envload{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = create_engine_from_url(url, pool_size=2)
    db = Database(engine, name="org", search_path=org_schema(org_id))
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(db, schema_sql, org_id=org_id)
    await init_schema(db, schema_sql, org_id=org_id, env=ENV)

    async def insert(table, env=None, **values):
        scoped = _scoped(table, org_schema(org_id, env))
        async with db.acquire() as conn:
            result = await conn.execute_core(
                scoped.insert().values(**values).returning(*scoped.primary_key.columns)
            )
            row = result.fetchone()
        return row[0] if row is not None else None

    async def update(table, key, env=None, **values):
        scoped = _scoped(table, org_schema(org_id, env))
        pk = next(iter(scoped.primary_key.columns))
        async with db.acquire() as conn:
            await conn.execute_core(scoped.update().where(pk == key).values(**values))

    async def rows(table, env=None, order_by="id"):
        scoped = _scoped(table, org_schema(org_id, env))
        async with db.acquire() as conn:
            result = await conn.execute_core(select(scoped).order_by(scoped.c[order_by]))
            return [dict(r._mapping) for r in result.fetchall()]

    async def tree(env=None):
        async with db.acquire() as conn:
            return await project(conn, org_schema(org_id, env))

    yield type(
        "Org",
        (),
        {
            "id": org_id,
            "db": db,
            "insert": staticmethod(insert),
            "update": staticmethod(update),
            "rows": staticmethod(rows),
            "tree": staticmethod(tree),
        },
    )
    await engine.dispose()


async def _seed(org, env=None, *, burn_serials: int = 0):
    """A small model with every kind of reference the deployer has to resolve."""
    await org.insert(sources, env, id="warehouse", type="postgres", host="db.internal")
    await org.insert(domains, env, id="sales", description="revenue")
    for i in range(burn_serials):
        await org.insert(
            registered_tables,
            env,
            source_id="warehouse",
            domain_id="sales",
            schema_name="scratch",
            table_name=f"burn{i}",
        )
    customer = await org.insert(
        registered_tables,
        env,
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="customers",
        alias="Customer",
    )
    order = await org.insert(
        registered_tables,
        env,
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        alias="Order",
    )
    await org.insert(table_columns, env, table_id=order, column_name="total")
    await org.insert(table_columns, env, table_id=order, column_name="customer_id")
    await org.insert(
        relationships,
        env,
        id="orders_customer",
        source_table_id=order,
        target_table_id=customer,
        source_column="customer_id",
        target_column="id",
        cardinality="many-to-one",
        alias="customer",
    )
    return {"customer": customer, "order": order}


class TestTheRoundTrip:
    """Project, deploy, project again: the model is what survives, not the keys that held it."""

    async def test_a_tree_loaded_into_another_schema_projects_back_identically(self, org):
        await _seed(org)
        prod = await org.tree()
        await deploy_tree(org.db, org.id, ENV, prod, ref="deadbeef")
        assert await org.tree(ENV) == prod

    async def test_the_target_mints_its_own_surrogates(self, org):
        # dev's sequences are deliberately out of step, which is the whole reason the tree carries
        # paths rather than serials — a deploy that wrote prod's integers would collide here.
        await _seed(org)
        await _seed(org, ENV, burn_serials=3)
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        loaded = {r["table_name"]: r["id"] for r in await org.rows(registered_tables, ENV)}
        original = {r["table_name"]: r["id"] for r in await org.rows(registered_tables)}
        assert set(loaded) == set(original)
        assert loaded["orders"] != original["orders"]

    async def test_a_relationship_arrives_pointing_at_the_right_table(self, org):
        await _seed(org)
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        (edge,) = await org.rows(relationships, ENV)
        customers = next(
            r["id"]
            for r in await org.rows(registered_tables, ENV)
            if r["table_name"] == "customers"
        )
        assert edge["target_table_id"] == customers

    async def test_an_unchanged_file_leaves_the_entity_on_the_key_it_already_had(self, org):
        # Rows outside the deployer's scope reference a registered table, so a deploy is not licence to
        # renumber the thing a file describes.
        await _seed(org)
        before = {r["table_name"]: r["id"] for r in await org.rows(registered_tables)}
        await deploy_tree(org.db, org.id, None, await org.tree(), ref="deadbeef")
        after = {r["table_name"]: r["id"] for r in await org.rows(registered_tables)}
        assert after == before


class TestTheTreeIsRefusedWhole:
    async def test_a_reference_to_a_file_the_tree_does_not_hold_is_refused(self, org):
        # What a textual three-way merge produces when one side deletes a table the other still
        # points at. It fails here, naming the path, rather than landing as a dangling edge.
        await _seed(org)
        tree = await org.tree()
        del tree["sales/tables/Customer.yaml"]
        with pytest.raises(DeployError, match="sales/tables/Customer.yaml"):
            await deploy_tree(org.db, org.id, ENV, tree, ref="deadbeef")

    async def test_a_refused_tree_leaves_the_environment_exactly_as_it_was(self, org):
        await _seed(org)
        await _seed(org, ENV)
        before = await org.tree(ENV)
        tree = await org.tree()
        del tree["sales/tables/Customer.yaml"]
        with pytest.raises(DeployError):
            await deploy_tree(org.db, org.id, ENV, tree, ref="deadbeef")
        assert await org.tree(ENV) == before


class TestWhatALoadDoesNotCarry:
    async def test_the_roles_of_the_tree_are_not_applied(self, org):
        # REQ-1539: on their own desktop a developer can grant themselves anything, and the branch
        # they push carries the projected roles. A deploy that applied them would make that the way
        # into whatever loaded the branch.
        await _seed(org)
        await org.update(roles, "developer", capabilities=["write", "read"])
        await org.update(roles, "developer", env=ENV, capabilities=[])
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        loaded = {r["id"]: r["capabilities"] for r in await org.rows(roles, ENV)}
        assert loaded["developer"] == []

    async def test_a_load_that_seeds_does_apply_them(self, org):
        # The creation path, and only it: a new environment needs roles or it opens with nobody
        # able to act.
        await _seed(org)
        await org.update(roles, "developer", capabilities=["write", "read"])
        await org.update(roles, "developer", env=ENV, capabilities=[])
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef", seed=True)
        loaded = {r["id"]: r["capabilities"] for r in await org.rows(roles, ENV)}
        assert loaded["developer"] == ["write", "read"]

    async def test_a_role_hand_written_into_the_tree_is_inert(self, org):
        # REQ-1539: the crudest attempt is to open the projected role file in an editor and write
        # the rights you want. It is refused POSITIONALLY rather than by inspecting the file --
        # ``roles`` is not in the scope a deploy applies, so a hand-written role and a projected one
        # are equally ignored.
        await _seed(org)
        await _seed(org, ENV)
        await org.update(roles, "developer", env=ENV, capabilities=[])
        files = dump(await org.tree())
        hand_written = "capabilities:\n- write\n- org_settings\ndomain_access:\n- '*'\n"
        assert files["roles/developer.yaml"] != hand_written  # the edit really is an edit
        edited = {**files, "roles/developer.yaml": hand_written}
        await deploy_tree(org.db, org.id, ENV, load(edited), ref="deadbeef")
        loaded = {r["id"]: r["capabilities"] for r in await org.rows(roles, ENV)}
        assert loaded["developer"] == []

    async def test_a_source_already_bound_here_keeps_its_binding(self, org):
        # REQ-1491: where a source points is this environment's own fact, and a tree has nothing to
        # say about it — a deploy must not blank a credential somebody established deliberately.
        await _seed(org)
        await _seed(org, ENV)
        await org.update(sources, "warehouse", env=ENV, host="dev-db.internal", bound=True)
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        (source,) = await org.rows(sources, ENV)
        assert source["host"] == "dev-db.internal"
        assert source["bound"] is True

    async def test_a_source_the_tree_introduces_arrives_unbound(self, org):
        await _seed(org)
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        (source,) = await org.rows(sources, ENV)
        assert source["host"] == ""
        assert source["bound"] is False

    async def test_a_source_the_tree_stopped_naming_is_left_where_it_is(self, org):
        # Dropping it would destroy the binding, and a model that no longer names the source is not
        # evidence that the credential should go.
        await _seed(org)
        await _seed(org, ENV)
        tree = await org.tree()
        del tree["sources/warehouse.yaml"]
        for path in [p for p in tree if p.startswith("sales/tables/")]:
            del tree[path]
        await deploy_tree(org.db, org.id, ENV, tree, ref="deadbeef")
        assert [s["id"] for s in await org.rows(sources, ENV)] == ["warehouse"]


class TestThePlan:
    async def test_a_plan_names_what_would_arrive_and_writes_nothing(self, org):
        await _seed(org)
        report = await plan_deploy(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        assert "sales/tables/Order.yaml" in report.delta.added
        assert "sales/domain.yaml" in report.delta.added
        assert await org.rows(registered_tables, ENV) == []

    async def test_a_plan_of_a_tree_that_does_not_hold_refuses_before_it_reports(self, org):
        await _seed(org)
        tree = await org.tree()
        del tree["sales/tables/Customer.yaml"]
        with pytest.raises(DeployError):
            await plan_deploy(org.db, org.id, ENV, tree, ref="deadbeef")

    async def test_removing_a_file_reads_as_a_removal(self, org):
        await _seed(org)
        await _seed(org, ENV)
        tree = await org.tree()
        del tree["sales/tables/Customer.yaml"]
        del tree["sales/tables/Order.yaml"]
        report = await plan_deploy(org.db, org.id, ENV, tree, ref="deadbeef")
        assert sorted(report.delta.removed) == [
            "sales/tables/Customer.yaml",
            "sales/tables/Order.yaml",
        ]


class TestWhatAPullOverwrites:
    """REQ-1556: a fast-forward pull is not refused, and it can still carry local work away.

    The apply diffs the incoming tree against the environment's CURRENT PROJECTION, so an edit
    sitting in the schema which never reached a commit reads as an ordinary change -- and that is
    exactly the DRIFTED environment (REQ-1524), the one case with no divergence recorded anywhere
    to warn about it. What separates it is the commit the branch actually stands at.
    """

    @staticmethod
    async def _committed(org):
        """ENV's model, committed, so the branch stands where the schema does."""
        from provisa.core.env_files import dump
        from provisa.core.env_repo import commit_files, ensure_repo

        return commit_files(
            ensure_repo(org.id), ENV, dump(await org.tree(ENV)), "provisioned", "ana"
        )

    async def test_an_edit_no_commit_holds_is_named_rather_than_carried_away_quietly(self, org):
        await _seed(org, ENV)
        base = await self._committed(org)
        # The remote's version of the same object, and a local edit to it that never got committed.
        incoming = await org.tree(ENV)
        incoming["sales/domain.yaml"]["description"] = "revenue, from the remote"
        await org.update(domains, "sales", ENV, description="revenue, edited here")

        report = await deploy_tree(org.db, org.id, ENV, incoming, ref="deadbeef", base_sha=base)
        assert [(c.path, c.source, c.target) for c in report.conflicts] == [
            ("sales/domain.yaml", "changed", "changed")
        ]
        assert report.compared
        # Reported, never resolved: the pull still applies and the remote's version is what stands.
        rows = await org.rows(domains, ENV)
        assert [r["description"] for r in rows if r["id"] == "sales"] == [
            "revenue, from the remote"
        ]

    async def test_work_only_the_remote_did_is_the_pull_doing_its_job(self, org):
        await _seed(org, ENV)
        base = await self._committed(org)
        incoming = await org.tree(ENV)
        incoming["sales/domain.yaml"]["description"] = "revenue, from the remote"
        report = await deploy_tree(org.db, org.id, ENV, incoming, ref="deadbeef", base_sha=base)
        assert report.conflicts == []
        assert report.compared

    async def test_two_people_arriving_at_one_answer_is_agreement(self, org):
        await _seed(org, ENV)
        base = await self._committed(org)
        incoming = await org.tree(ENV)
        incoming["sales/domain.yaml"]["description"] = "revenue, restated"
        await org.update(domains, "sales", ENV, description="revenue, restated")
        report = await deploy_tree(org.db, org.id, ENV, incoming, ref="deadbeef", base_sha=base)
        assert report.conflicts == []

    async def test_a_deploy_that_asked_no_such_question_says_it_compared_nothing(self, org):
        # A checkout of a ref is the operator stating which model the environment runs, so its
        # divergence from what stood there is the point of the act rather than a collision in it.
        await _seed(org, ENV)
        report = await deploy_tree(org.db, org.id, ENV, await org.tree(ENV), ref="deadbeef")
        assert not report.compared
        assert report.as_dict()["base"] is None
        assert report.as_dict()["conflicts"] == []


class TestATagOnACommandSurvivesTheTrip:
    """The projection puts a command's tags in the command's file; the deploy has to read them
    back as command assignments. Neither half is any use without the other: a tag the tree can
    carry but the deployer drops is deleted by the first deploy that reaches the target."""

    async def _seed_tagged_command(self, org):
        await _seed(org)
        await org.insert(tracked_functions, None, name="refund_order", source_id="warehouse")
        await org.insert(
            tag_assignments,
            None,
            tag_id="deprecated",
            base_tag_id="deprecated",
            object_type="command",
            command_name="refund_order",
            object_key="command:refund_order",
            reason="superseded by refund_line",
        )

    async def test_the_tag_arrives_against_the_command_it_names(self, org):
        await self._seed_tagged_command(org)
        await deploy_tree(org.db, org.id, ENV, await org.tree(), ref="deadbeef")
        (row,) = await org.rows(tag_assignments, ENV)
        assert row["object_type"] == "command"
        assert row["command_name"] == "refund_order"
        assert row["object_key"] == "command:refund_order"
        assert row["reason"] == "superseded by refund_line"

    async def test_the_round_trip_is_identical(self, org):
        await self._seed_tagged_command(org)
        prod = await org.tree()
        await deploy_tree(org.db, org.id, ENV, prod, ref="deadbeef")
        assert await org.tree(ENV) == prod

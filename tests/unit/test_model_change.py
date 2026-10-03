# Copyright (c) 2026 Kenneth Stott
# Canary: c46b035c-e670-4806-bf89-77a365919866
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every model change commits (REQ-1524).

The control-plane connection records each write to a projected table of an environment's model;
the change scope commits each written environment once when it ends. Run against a SQLite
control plane with the projection's commit captured: what is asserted is WHICH commits are asked
for, with what message and author. That a commit of an unchanged tree writes nothing is the
repository's own property (``env_repo.commit_files``), tested there.
"""

# Requirements: REQ-1524

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import insert, update

from provisa.core import model_change
from provisa.core.env_deploy import PROJECTED
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.model_change import ModelChangeOutsideScope, ModelPlane
from provisa.core.schema_org import metadata, roles
from tests.unit.test_every_kind_records_its_origin import KINDS, plane  # noqa: F401 — the fixture
from tests.unit.test_load_manages_what_config_declared import (  # noqa: F401 — the fixture
    _file,
    _load,
    no_vault,
    seed_the_view_source,
)

PLANE = ModelPlane("acme", "prod")


@pytest.fixture
def commits() -> Iterator[list[dict]]:
    """Every commit the change scopes ask for, with the platform plane attached."""
    asked: list[dict] = []

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor):
        asked.append(
            {"org": org_id, "env": env, "schema": schema, "message": message, "actor": actor}
        )
        return "sha"

    model_change.attach(object(), commit=_write_through, projected=PROJECTED)  # type: ignore[arg-type]
    yield asked
    model_change.detach()


@pytest.fixture
async def model(plane) -> Database:  # noqa: F811 — the origin scenarios' plane
    """The origin scenarios' plane, made the model of acme/prod once it is seeded."""
    plane.model = PLANE
    return plane


@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_a_write_of_each_kind_is_one_commit_named_by_its_writer(model, commits, kind):
    write, _table, _where = KINDS[kind]
    async with model_change.scope("POST /admin/graphql", actor=lambda: "pat"):
        async with model.acquire() as conn:
            await write(conn, "admin")
    assert len(commits) == 1
    commit = commits[0]
    assert (commit["org"], commit["env"], commit["schema"], commit["actor"]) == (
        "acme",
        "prod",
        "org_acme",
        "pat",
    )
    assert commit["message"].split(" ")[0] in ("upsert", "assign")


async def test_a_scope_with_several_named_writes_lists_them_under_its_label(model, commits):
    async with model_change.scope("POST /admin/graphql"):
        model_change.label("registerTables")
        for name in ("metric", "tag"):
            write, _t, _w = KINDS[name]
            async with model.acquire() as conn:
                await write(conn, "admin")
    assert [c["message"] for c in commits] == [
        "registerTables\n\n- upsert metric revenue\n- upsert tag finance"
    ]
    assert commits[0]["actor"] is None  # no member signed it: the system author stands in


async def test_an_unnamed_write_is_named_by_the_scope_and_the_tables_it_wrote(model, commits):
    async with model_change.scope("PUT /admin/roles/seller"):
        async with model.acquire() as conn:
            await conn.execute_core(
                update(roles).where(roles.c.id == "seller").values(capabilities=["usage"])
            )
    assert [c["message"] for c in commits] == ["PUT /admin/roles/seller (roles)"]


async def test_raw_sql_writes_are_recorded_by_the_table_they_write(model, commits):
    async with model_change.scope("raw"):
        async with model.acquire() as conn:
            await conn.execute("UPDATE roles SET capabilities = '[]' WHERE id = $1", "seller")
    assert [c["message"] for c in commits] == ["raw (roles)"]


async def test_a_write_that_changed_no_row_asks_for_no_commit(model, commits):
    async with model_change.scope("noop"):
        async with model.acquire() as conn:
            await conn.execute_core(
                update(roles).where(roles.c.id == "nobody").values(capabilities=[])
            )
    assert commits == []


async def test_a_nested_scope_is_part_of_the_outer_change(model, commits):
    async with model_change.scope("POST /admin/import/apply"):
        async with model_change.scope("config load"):
            write, _t, _w = KINDS["metric"]
            async with model.acquire() as conn:
                await write(conn, "admin")
        assert commits == []  # not yet: the outer scope is the one change
    assert len(commits) == 1


async def test_a_write_no_scope_owns_raises(model, commits):
    write, _t, _w = KINDS["metric"]
    async with model.acquire() as conn:
        with pytest.raises(ModelChangeOutsideScope, match="metrics of acme/prod"):
            await write(conn, "admin")


async def test_with_no_repository_attached_nothing_is_recorded(model, commits):
    model_change.detach()
    write, _t, _w = KINDS["metric"]
    async with model.acquire() as conn:
        await write(conn, "admin")  # no scope, and no raise: there is no projection to keep


async def test_a_path_that_commits_itself_is_not_committed_again(model, commits):
    async with model_change.scope("POST /admin/environments/dev/undo"):
        async with model_change.committed_by_caller():
            write, _t, _w = KINDS["metric"]
            async with model.acquire() as conn:
                await write(conn, "admin")
    assert commits == []


async def test_a_scope_whose_body_raised_still_commits_what_was_written(model, commits):
    """A statement that committed before the raise changed the model; the projection records
    the model as it is."""
    with pytest.raises(RuntimeError, match="after the write"):
        async with model_change.scope("POST /admin/graphql"):
            write, _t, _w = KINDS["metric"]
            async with model.acquire() as conn:
                await write(conn, "admin")
            raise RuntimeError("after the write")
    assert len(commits) == 1


async def test_a_table_outside_the_projection_is_not_recorded(model, commits):
    async with model.acquire() as conn:  # no scope: a recorded write would raise
        await conn.execute_core(
            insert(metadata.tables["admin_audit_log"]).values(
                action="role.edited", actor_id="pat", subject_id="seller", detail={}
            )
        )
    assert commits == []


@pytest.fixture
async def scenario_model() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="load-test")
    await _init_schema_portable(db)
    await seed_the_view_source(db)
    db.model = PLANE
    return db


async def test_a_whole_config_load_is_one_commit(scenario_model, commits):
    async with model_change.scope("boot"):
        await _load(scenario_model, _file())
    assert [c["message"] for c in commits] == [commits[0]["message"]]
    assert commits[0]["message"].startswith("config load\n\n- upsert ")


async def test_a_config_load_outside_any_other_scope_is_its_own_change(scenario_model, commits):
    await _load(scenario_model, _file())
    assert len(commits) == 1
    assert commits[0]["message"].startswith("config load")

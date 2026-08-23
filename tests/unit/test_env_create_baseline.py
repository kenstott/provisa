# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1f8d24-57ae-4c93-a0e5-2d7c41f9b830
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A new environment starts its history where the one it was created from is standing (REQ-1543).

Without that the branch has no ref, so the first edit somebody makes IS the first commit and undo
has no parent to step back to -- the change is unundoable precisely because it was the first one.
The ref is seeded from the source rather than rooted, so the new line continues the source's and a
later merge between them has a base; the position is recorded against the seeded sha, because the
model was just copied and an identical tree writes no commit at all (REQ-1526).
"""

# Requirements: REQ-1487, REQ-1524, REQ-1543

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.core import env_repo

pytestmark = pytest.mark.asyncio

ORG = "acme"
ACTOR = "uid-ada"


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


class _Pool:
    dialect = "postgresql"


@pytest.fixture
def created(monkeypatch):
    """Every collaborator of the create path answered, collecting the write-throughs it asked for."""
    commits: list[tuple[str, str, str | None]] = []
    source_tip: list[str | None] = ["a" * 40]
    tree_unchanged: list[bool] = [False]
    started: list[tuple[str, str]] = []
    positions: list[tuple[str, str | None]] = []
    origins: list[tuple[str, str]] = []

    async def _member(request, org_id, *rights):  # REQ-1573: rights the endpoint accepts
        return ACTOR

    async def _known(org_id, name):
        return {"name": name}

    async def _reserve_env(*args, **kwargs):
        return None

    async def _provision_org(*args, **kwargs):
        return None

    class _Report:
        def as_dict(self):
            return {"added": 1, "changed": 0, "removed": 0}

    async def _copy_model(*args, **kwargs):
        return _Report()

    class _Conn:
        async def __aenter__(self):
            return "conn"

        async def __aexit__(self, *exc):
            return False

    class _Db:
        def acquire(self):
            return _Conn()

    async def _org_tenant_db(org_id):
        return _Db()

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor):
        commits.append((env, message, actor))
        return None if tree_unchanged[0] else "d" * 40

    def _start_branch(repo, env, from_env):
        started.append((env, from_env))
        return source_tip[0]

    async def _set_position(db, org_id, name, *, deployed_sha, redo_sha):
        positions.append((name, deployed_sha))

    async def _set_origin(db, org_id, name, origin_sha):
        origins.append((name, origin_sha))

    async def _audit(*args, **kwargs):
        return None

    async def _get_env(db, org_id, name):
        return {"name": name}

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "_guard", _member)
    monkeypatch.setattr(er, "_known", _known)
    monkeypatch.setattr(er, "reserve_env", _reserve_env)
    monkeypatch.setattr(er, "copy_model", _copy_model)
    monkeypatch.setattr(er, "_audit", _audit)
    monkeypatch.setattr(er, "get_env", _get_env)
    monkeypatch.setattr(er, "_pool", lambda: _Pool())
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(er, "_state", lambda: "state")
    monkeypatch.setattr("provisa.core.org_provisioning.provision_org", _provision_org)
    monkeypatch.setattr("provisa.api.admin.orgs_router._org_tenant_db", _org_tenant_db)
    monkeypatch.setattr(env_repo, "write_through", _write_through)
    monkeypatch.setattr(env_repo, "start_branch", _start_branch)
    monkeypatch.setattr(env_repo, "ensure_repo", lambda org_id: "repo")
    monkeypatch.setattr("provisa.core.env_store.set_position", _set_position)
    monkeypatch.setattr("provisa.core.env_store.set_origin", _set_origin)
    return type(
        "Created",
        (),
        {
            "commits": commits,
            "started": started,
            "positions": positions,
            "origins": origins,
            "source_tip": source_tip,
            "tree_unchanged": tree_unchanged,
        },
    )


async def _create(name: str, from_env: str = "prod"):
    body = er.CreateEnvBody(name=name, from_env=from_env, inherit_connections=True)
    return await er.create_environment(_Request(), ORG, body)


class TestBaseline:
    async def test_the_new_environment_gets_a_commit_of_its_own(self, created):
        await _create("dev")
        assert [c[0] for c in created.commits] == ["dev"]

    async def test_the_commit_names_where_the_model_came_from(self, created):
        await _create("dev", from_env="prod")
        assert created.commits[0][1] == "created from prod"

    async def test_it_is_authored_by_the_person_who_created_it(self, created):
        await _create("dev")
        assert created.commits[0][2] == ACTOR


class TestAncestry:
    async def test_the_branch_is_started_from_the_environment_it_was_created_from(self, created):
        await _create("dev", from_env="staging")
        assert created.started == [("dev", "staging")]

    async def test_it_is_started_before_anything_is_committed_into_it(self, created):
        """A commit written first would be the root the seeding was supposed to prevent."""
        order: list[str] = []
        await _create("dev")
        order = [*["start" for _ in created.started], *["commit" for _ in created.commits]]
        assert order == ["start", "commit"]

    async def test_the_environment_stands_where_its_branch_was_started(self, created):
        """REQ-1526: the copied model is the source's model, so the tree matches and no commit is
        written -- and a position left unwritten would say the environment is nowhere."""
        created.tree_unchanged[0] = True
        await _create("dev")
        assert created.positions == [("dev", "a" * 40)]

    async def test_the_seeded_sha_is_the_floor_of_the_new_line(self, created):
        """Everything at or below it is the source's history, so an undo stops there: a branch
        created and then changed once offers one undo, not two."""
        await _create("dev")
        assert created.origins == [("dev", "a" * 40)]

    async def test_a_source_with_no_branch_leaves_nothing_to_stand_on(self, created):
        """Not a failure: an org whose prod was never committed has no sha to record, and the first
        write-through is what gives the environment its position."""
        created.source_tip[0] = None
        await _create("dev")
        assert (created.positions, created.origins) == ([], [])

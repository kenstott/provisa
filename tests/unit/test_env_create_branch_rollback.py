# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A failed creation leaves no branch; a branch with no environment is refused by name (REQ-1554).

A branch is the history of exactly one environment. The creation's compensating rollback dropped the
environment's schemas and its registry row and kept the branch it had just started, so the name's
next creation found a branch with no environment and failed with an untyped RepositoryError (a 500).
The rollback now takes the branch it started. A branch the creation did not start -- one with no
environment behind it -- is refused as ``environments.branch_without_environment`` (409) and is
left as it is.
"""

# Requirements: REQ-1543, REQ-1554

from __future__ import annotations

import pytest

from provisa.core import env_create, env_repo
from provisa.core.env_repo import commit_files, ensure_repo, has_branch
from tests.helpers import ready_org_folder

pytestmark = pytest.mark.asyncio

ORG = "acme"


@pytest.fixture
def plane(tmp_path, monkeypatch):
    """A real org repository whose prod branch has a commit; every database step answered."""
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder(ORG)
    commit_files(ensure_repo(ORG), "prod", {"model/a.yaml": "a: 1\n"}, "seed", None)
    forgotten: list[str] = []

    async def _nothing(*args, **kwargs):
        return None

    class _Report:
        pass

    async def _copy_model(*args, **kwargs):
        return _Report()

    async def _forget_env(db, org_id, name):
        forgotten.append(name)

    async def _write_through(*args, **kwargs):
        return None

    monkeypatch.setattr(env_create, "reserve_env", _nothing)
    monkeypatch.setattr(env_create, "copy_model", _copy_model)
    monkeypatch.setattr(env_create, "forget_env", _forget_env)
    monkeypatch.setattr("provisa.core.org_provisioning.provision_org", _nothing)
    monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _nothing)
    monkeypatch.setattr("provisa.core.env_store.set_position", _nothing)
    monkeypatch.setattr("provisa.core.env_store.set_origin", _nothing)
    monkeypatch.setattr(env_repo, "write_through", _write_through)
    return forgotten


class _Db:
    def acquire(self):
        return self

    async def __aenter__(self):
        return "conn"

    async def __aexit__(self, *exc):
        return False


async def _no_position(*args, **kwargs):
    return None


async def _create(name: str):
    return await env_create.create_environment(
        "state",
        "admin-db",
        "tenant-pool",
        _Db(),
        ORG,
        name,
        from_env="prod",
        created_by="uid-ada",
        expires_at=None,
        data_mode="inherit",
        mutation_handling="refused",
        note="created from prod",
    )


async def test_a_creation_that_fails_after_its_branch_started_takes_the_branch(plane, monkeypatch):
    async def _position_refused(*args, **kwargs):
        raise RuntimeError("the admin plane refused the position")

    monkeypatch.setattr("provisa.core.env_store.set_position", _position_refused)
    with pytest.raises(RuntimeError, match="refused the position"):
        await _create("synthetic")
    assert plane == ["synthetic"]  # the registry row went
    assert not has_branch(ensure_repo(ORG), "synthetic")  # and so did the branch
    assert has_branch(ensure_repo(ORG), "prod")


async def test_the_name_can_be_created_again_after_a_failed_creation(plane, monkeypatch):
    async def _position_refused(*args, **kwargs):
        raise RuntimeError("the admin plane refused the position")

    monkeypatch.setattr("provisa.core.env_store.set_position", _position_refused)
    with pytest.raises(RuntimeError):
        await _create("synthetic")
    monkeypatch.setattr("provisa.core.env_store.set_position", _no_position)
    await _create("synthetic")
    assert has_branch(ensure_repo(ORG), "synthetic")


async def test_a_branch_with_no_environment_is_refused_by_name_and_left(plane):
    repo = ensure_repo(ORG)
    commit_files(repo, "synthetic", {"model/a.yaml": "a: 2\n"}, "an earlier life", None)
    tip = repo.refs[b"refs/heads/synthetic"]
    with pytest.raises(env_create.HistoryBranchWithoutEnvironment) as refused:
        await _create("synthetic")
    assert refused.value.code == "environments.branch_without_environment"
    assert "'synthetic' already has a model-history branch with no environment" in str(
        refused.value
    )
    assert plane == ["synthetic"]  # the reservation this creation made went
    assert ensure_repo(ORG).refs[b"refs/heads/synthetic"] == tip  # the branch is not this one's


async def test_the_endpoint_answers_the_refusal_as_a_409(plane, monkeypatch):
    from provisa.api.admin import environments_router as er
    from provisa.api.errors import ApiError

    commit_files(ensure_repo(ORG), "synthetic", {"model/a.yaml": "a: 2\n"}, "earlier", None)

    async def _member(request, org_id, *rights):
        return "uid-ada"

    async def _org_model_db(org_id):
        return _Db()

    async def _known(org_id, name):
        return {"name": name}

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "_guard", _member)
    monkeypatch.setattr(er, "_known", _known)
    monkeypatch.setattr(er, "_pool", lambda: type("P", (), {"dialect": "postgresql"})())
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(er, "_state", lambda: "state")
    monkeypatch.setattr("provisa.api.admin.orgs_router._org_model_db", _org_model_db)

    class _Request:
        class state:  # noqa: N801 -- mirrors starlette's attribute name
            identity = None

    body = er.CreateEnvBody(name="synthetic", from_env="prod", data_mode="inherit")
    with pytest.raises(ApiError) as answered:
        await er.create_environment(_Request(), ORG, body)
    assert answered.value.status_code == 409
    assert answered.value.code == "environments.branch_without_environment"

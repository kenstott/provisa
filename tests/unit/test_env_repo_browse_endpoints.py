# Copyright (c) 2026 Kenneth Stott
# Canary: 5a3c9e71-2b8d-4f60-9c14-7e0a6d3b8f52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The doors BROWSE is reachable through, and who each one asks (REQ-1524, REQ-1528).

``env_repo``'s verbs are covered against a real repository in ``tests/integration``; what is under
test here is the HTTP layer over them -- which guard each ref reaches, that a ref the repository
cannot resolve leaves as a 404 rather than a traceback, and that listing a tree does not carry the
whole model with it.
"""

# Requirements: REQ-1524, REQ-1528

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core import env_repo

pytestmark = pytest.mark.asyncio

ORG = "acme"
TREE = {
    "domains/sales.yaml": "id: sales\n",
    "tables/orders.yaml": "name: orders\n",
}


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


@pytest.fixture
def repo(monkeypatch):
    """The repository answers, and every guard records the name it was asked about."""
    asked: dict[str, list] = {"member": [], "guard": [], "within": []}

    async def _member(request, org_id):
        asked["member"].append(org_id)
        return "uid-member"

    async def _guard(request, org_id):
        asked["guard"].append(org_id)
        return "uid-admin"

    async def _guard_within(request, org_id, name):
        asked["within"].append(name)
        return "uid-owner"

    async def _get_env(admin_db, org_id, name):
        # ``dev`` is an environment of this org; a sha is not, and neither is a branch whose
        # environment was deleted.
        return {"name": name} if name in ("prod", "dev") else None

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "_guard", _guard)
    monkeypatch.setattr(er, "_guard_within", _guard_within)
    monkeypatch.setattr(er, "get_env", _get_env)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(env_repo, "branches", lambda org_id: ["dev", "prod"])
    monkeypatch.setattr(
        env_repo,
        "history",
        lambda org_id, ref, limit=100: [
            {"sha": "a" * 40, "author": "u <u@x>", "message": "edit", "committed_at": 1},
        ][:limit],
    )
    monkeypatch.setattr(env_repo, "files_at", lambda org_id, ref: dict(TREE))
    return asked


class TestWhoMayReadARef:
    async def test_a_ref_that_names_an_environment_is_guarded_as_that_environment(self, repo):
        """REQ-1528: the person who made the branch reads it; nobody else gains anything."""
        await er.repo_history(_Request(), ORG, ref="dev")
        assert repo["within"] == ["dev"]
        assert repo["guard"] == []

    async def test_a_sha_belongs_to_no_environment_so_it_is_an_org_admin_act(self, repo):
        await er.repo_history(_Request(), ORG, ref="b" * 40)
        assert repo["guard"] == [ORG]
        assert repo["within"] == []

    async def test_listing_branches_asks_membership(self, repo):
        """A member picks what to branch from, so a member must be able to see the refs."""
        out = await er.list_repo_branches(_Request(), ORG)
        assert out == {"branches": ["dev", "prod"]}
        assert repo["member"] == [ORG]


class TestWhatBrowseReturns:
    async def test_history_carries_the_ref_it_was_asked_for(self, repo):
        out = await er.repo_history(_Request(), ORG, ref="dev")
        assert out["ref"] == "dev"
        assert out["commits"][0]["sha"] == "a" * 40

    async def test_the_limit_reaches_the_repository(self, repo):
        out = await er.repo_history(_Request(), ORG, ref="dev", limit=0)
        assert out["commits"] == []

    async def test_listing_a_tree_returns_paths_and_not_the_model(self, repo):
        out = await er.repo_files(_Request(), ORG, ref="dev")
        assert out["paths"] == sorted(TREE)
        assert "text" not in out and "files" not in out

    async def test_one_file_is_read_by_path(self, repo):
        out = await er.repo_file(_Request(), ORG, ref="dev", path="tables/orders.yaml")
        assert out == {"ref": "dev", "path": "tables/orders.yaml", "text": "name: orders\n"}


class TestWhatCannotBeAnswered:
    async def test_an_unresolvable_ref_is_a_404(self, repo, monkeypatch):
        def _raise(org_id, ref, limit=100):
            raise env_repo.RepositoryError(f"{ref!r} is neither a branch nor a commit")

        monkeypatch.setattr(env_repo, "history", _raise)
        with pytest.raises(ApiError) as exc:
            await er.repo_history(_Request(), ORG, ref="c" * 40)
        assert exc.value.status_code == 404
        assert exc.value.code == "environments.unknown_ref"

    async def test_a_path_not_in_the_tree_is_a_404(self, repo):
        with pytest.raises(ApiError) as exc:
            await er.repo_file(_Request(), ORG, ref="dev", path="tables/missing.yaml")
        assert exc.value.status_code == 404
        assert exc.value.code == "environments.unknown_path"

    async def test_the_guard_runs_before_the_repository_is_touched(self, repo, monkeypatch):
        """A refusal must not depend on whether the ref happened to exist."""

        async def _refuse(request, org_id):
            raise ApiError(403, "environments.forbidden", "no")

        monkeypatch.setattr(er, "_guard", _refuse)
        monkeypatch.setattr(
            env_repo, "files_at", lambda org_id, ref: pytest.fail("read before the guard")
        )
        with pytest.raises(ApiError) as exc:
            await er.repo_files(_Request(), ORG, ref="d" * 40)
        assert exc.value.status_code == 403

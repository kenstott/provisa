# Copyright (c) 2026 Kenneth Stott
# Canary: b71e4a09-3c26-4f58-9d0b-5e7a2c18f634
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The org's repository and the three verbs it offers: BRANCH, BROWSE and LOAD (REQ-1524).

Everything here is exercised against a real on-disk bare repository, because the claim being made
is that it is an ORDINARY one -- a tree a git client could read, refs a git client could list. A
double that recorded calls would confirm the calls and not the claim.
"""

from __future__ import annotations

import pytest

from provisa.core import env_repo
from provisa.core.env_repo import (
    RepositoryError,
    branch,
    branches,
    commit_files,
    ensure_repo,
    files_at,
    history,
    repo_path,
)

pytestmark = pytest.mark.unit

ORG = "acme"


@pytest.fixture(autouse=True)
def repo_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    return tmp_path


MODEL = {
    "sales/domain.yaml": "id: sales\n",
    "sales/tables/Order.yaml": "table_name: orders\n",
    "sources/warehouse.yaml": "type: postgres\n",
}


class TestTheRepositoryExistsBeforeTheFirstEdit:
    def test_it_is_created_on_demand_and_is_bare(self):
        ensure_repo(ORG)
        assert (repo_path(ORG) / "objects").is_dir()
        assert not (repo_path(ORG) / ".git").exists()

    def test_asking_twice_gives_the_same_repository(self):
        first = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        assert history(ORG, "prod")[0]["sha"] == first

    def test_each_org_gets_its_own_object_store(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        assert branches("other") == []


class TestTheTreeIsAnOrdinaryGitTree:
    def test_a_path_becomes_nested_trees_and_round_trips(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        assert files_at(ORG, "prod") == MODEL

    def test_a_removed_entity_is_removed_from_the_tree(self):
        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        remaining = {k: v for k, v in MODEL.items() if "Order" not in k}
        commit_files(repo, "prod", remaining, "drop Order", "ken@example.com")
        assert files_at(ORG, "prod") == remaining


class TestCommittingIsNotAVerb:
    def test_the_acting_user_is_the_author(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", "ken@example.com")
        assert "ken@example.com" in history(ORG, "prod")[0]["author"]

    def test_an_act_with_no_actor_is_attributed_to_the_system(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "provision", None)
        assert history(ORG, "prod")[0]["author"] == env_repo.SYSTEM_AUTHOR

    def test_an_unchanged_projection_writes_no_commit(self):
        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        assert commit_files(repo, "prod", dict(MODEL), "binding edited", None) is None
        assert len(history(ORG, "prod")) == 1

    def test_history_is_newest_first_so_undo_is_loading_an_earlier_sha(self):
        repo = ensure_repo(ORG)
        first = commit_files(repo, "prod", MODEL, "seed", None)
        commit_files(repo, "prod", {**MODEL, "sales/tables/Line.yaml": "x: 1\n"}, "add", None)
        shas = [c["sha"] for c in history(ORG, "prod")]
        assert shas[-1] == first
        assert files_at(ORG, first) == MODEL


class TestBranch:
    def test_an_environment_is_a_branch_within_the_orgs_one_repository(self):
        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        branch(ORG, "prod", "dev")
        assert branches(ORG) == ["dev", "prod"]
        assert files_at(ORG, "dev") == MODEL

    def test_a_branch_may_be_taken_from_a_sha(self):
        repo = ensure_repo(ORG)
        first = commit_files(repo, "prod", MODEL, "seed", None)
        commit_files(repo, "prod", {**MODEL, "sales/tables/Line.yaml": "x: 1\n"}, "add", None)
        branch(ORG, first, "rewind")
        assert files_at(ORG, "rewind") == MODEL

    def test_an_existing_branch_is_never_repositioned(self):
        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        branch(ORG, "prod", "dev")
        with pytest.raises(RepositoryError, match="already exists"):
            branch(ORG, "prod", "dev")


class TestBrowseIsByRefAndNeverByPath:
    def test_a_ref_that_is_neither_a_branch_nor_a_sha_is_refused(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        with pytest.raises(RepositoryError, match="neither a branch nor a commit"):
            files_at(ORG, "../../etc/passwd")

    def test_a_sha_from_another_org_is_not_reachable(self, tmp_path):
        sha = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        with pytest.raises(RepositoryError, match="neither a branch nor a commit"):
            files_at("other", sha)

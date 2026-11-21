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

from tests.helpers import ready_org_folder

from provisa.core import env_repo
from provisa.core.env_repo import (
    RepositoryError,
    branch,
    branches,
    commit_files,
    ensure_repo,
    files_at,
    history,
    parent_of,
    repo_path,
)

pytestmark = pytest.mark.unit

ORG = "acme"


@pytest.fixture(autouse=True)
def repo_dir(tmp_path, monkeypatch):
    # REQ-1992: the org's repository is in its folder on the node, made ready as its runtime
    # makes it.
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    ready_org_folder(ORG)
    ready_org_folder("other")
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

    def test_two_first_uses_at_once_both_get_the_repository(self, monkeypatch):
        """Every request runs on its own thread and a write-through and a read can both be the
        org's first use. Two callers that both found no repository both initialised it in place;
        the second was refused ("FileExistsError: … e2e.git/branches") and its request answered
        500 (UI e2e swap lane on v0.1.0-alpha.477, GET …/repo-integration/sync). Here the second
        caller arrives while the first is in the middle of creating."""
        from dulwich.repo import Repo

        real_init = Repo.init_bare
        arrived: list[Repo] = []

        def _init_while_another_caller_arrives(path, *args, **kwargs):
            if not arrived:
                arrived.append(None)  # type: ignore[arg-type]
                arrived[0] = ensure_repo(ORG)
            return real_init(path, *args, **kwargs)

        monkeypatch.setattr(Repo, "init_bare", staticmethod(_init_while_another_caller_arrives))
        first = ensure_repo(ORG)

        assert arrived[0].path == first.path == str(repo_path(ORG))
        sha = commit_files(first, "prod", MODEL, "seed", None)
        assert history(ORG, "prod")[0]["sha"] == sha
        # Nothing of the caller that lost is left beside the repository.
        assert [p.name for p in repo_path(ORG).parent.iterdir()] == [repo_path(ORG).name]

    def test_a_repository_is_never_seen_half_made(self, monkeypatch):
        """A caller that looks while another is creating finds no repository yet, never one with
        an object store and no HEAD."""
        from dulwich.repo import Repo

        real_init = Repo.init_bare
        seen: list[bool] = []

        def _init(path, *args, **kwargs):
            repo = real_init(path, *args, **kwargs)
            seen.append(repo_path(ORG).exists())
            return repo

        monkeypatch.setattr(Repo, "init_bare", staticmethod(_init))
        ensure_repo(ORG)
        assert seen == [False]
        assert (repo_path(ORG) / "HEAD").is_file()

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


class TestAProjectionMayNotHoldTheCommitTheControlPlaneNames:
    """REQ-1524: the repository is a projection, so a sha this store never received is a BROWSE
    refusal like any other unresolvable ref -- never a raw dulwich KeyError escaping to a 500."""

    def test_walking_back_from_a_sha_this_store_does_not_hold_is_refused(self):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        absent = "23f2cfb3db0b2e1a20717a2ba8a7f2fa9be94093"
        with pytest.raises(RepositoryError, match="neither a branch nor a commit"):
            parent_of(ORG, absent)

    def test_the_first_commit_of_a_line_has_nothing_behind_it(self):
        first = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        assert parent_of(ORG, first) is None


class TestLandingOnACommitThatCameFromElsewhere:
    """REQ-1547: a pull applies the fetched commit, and the branch moves onto THAT commit -- the
    model now equals it -- rather than onto a new commit beside it, which would leave the two lines
    diverged by the very act that reconciled them."""

    @staticmethod
    def _fetched(org: str, files: dict[str, str]) -> str:
        """A commit ahead of ``prod`` that arrived the way a fetch brings one: on another ref."""
        from provisa.core.env_repo import start_branch

        repo = ensure_repo(org)
        start_branch(repo, "elsewhere", "prod")
        sha = commit_files(repo, "elsewhere", files, "reviewed on the host", None)
        assert sha is not None
        return sha

    def test_a_model_equal_to_the_commit_moves_the_branch_onto_it(self):
        from provisa.core.env_repo import tip

        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        changed = {**MODEL, "sales/domain.yaml": "id: sales\ndescription: reviewed\n"}
        fetched = self._fetched(ORG, changed)
        assert commit_files(repo, "prod", changed, "pull", None, onto=fetched) == fetched
        assert tip(ORG, "prod") == fetched

    def test_a_model_that_differs_from_it_is_committed_on_top_of_it(self):
        from provisa.core.env_repo import tip

        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        changed = {**MODEL, "sales/domain.yaml": "id: sales\ndescription: reviewed\n"}
        fetched = self._fetched(ORG, changed)
        here = {**changed, "roles/analyst.yaml": "id: analyst\n"}
        landed = commit_files(repo, "prod", here, "pull", None, onto=fetched)
        assert landed not in (None, fetched)
        assert tip(ORG, "prod") == landed
        assert parent_of(ORG, landed) == fetched

    def test_a_commit_the_branch_is_not_behind_is_refused(self):
        from provisa.core.env_repo import RepositoryError

        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        fetched = self._fetched(ORG, {**MODEL, "x.yaml": "a: 1\n"})
        commit_files(repo, "prod", {**MODEL, "y.yaml": "b: 2\n"}, "local edit", None)
        with pytest.raises(RepositoryError, match="diverged"):
            commit_files(repo, "prod", MODEL, "pull", None, onto=fetched)


class TestAMergeRecordsWhatItMerged:
    """REQ-1555: a merge is measured against the commit both lines last held, and after a merge of
    the whole model that commit is the SOURCE's -- the target now holds everything the source did.
    A merge commit with the target's line alone would leave the base at the original branch point
    for ever, and every later merge would re-report edits the earlier one had already carried."""

    def test_the_merge_commit_names_the_source_it_merged(self):
        from provisa.core.env_repo import merge_base, start_branch, tip

        repo = ensure_repo(ORG)
        commit_files(repo, "prod", MODEL, "seed", None)
        start_branch(repo, "dev", "prod")
        worked = {**MODEL, "sales/domain.yaml": "id: sales\ndescription: dev\n"}
        dev_tip = commit_files(repo, "dev", worked, "dev edits sales", None)
        before = tip(ORG, "prod")

        merged = commit_files(repo, "prod", worked, "merge dev", None, merged_from=dev_tip)

        assert merged is not None
        assert parent_of(ORG, merged) == before  # the target's own line stays first
        assert merge_base(ORG, dev_tip, merged) == dev_tip

    def test_the_next_merge_is_measured_from_the_last_one(self):
        from provisa.core.env_repo import merge_base, start_branch

        repo = ensure_repo(ORG)
        seed = commit_files(repo, "prod", MODEL, "seed", None)
        start_branch(repo, "dev", "prod")
        first = {**MODEL, "a.yaml": "a: 1\n"}
        dev_one = commit_files(repo, "dev", first, "one", None)
        commit_files(repo, "prod", first, "merge dev", None, merged_from=dev_one)
        dev_two = commit_files(repo, "dev", {**first, "b.yaml": "b: 2\n"}, "two", None)
        prod_tip = history(ORG, "prod")[0]["sha"]

        assert merge_base(ORG, dev_two, prod_tip) == dev_one != seed


def test_two_commits_of_one_repository_do_not_write_its_objects_at_once(monkeypatch):
    """REQ-2006: a template's build and a change of it committed together, two threads writing one
    object: FileExistsError on its .lock, and the environment marked drifted. One repository's
    commits are taken in turn."""
    import threading
    import time

    repo = ensure_repo(ORG)
    inside: dict[str, int] = {}
    most: dict[str, int] = {}
    lock = threading.Lock()
    build = env_repo._build_tree

    def _counted(on, files):
        key = str(on.path)
        with lock:
            inside[key] = inside.get(key, 0) + 1
            most[key] = max(most.get(key, 0), inside[key])
        time.sleep(0.05)
        with lock:
            inside[key] -= 1
        return build(on, files)

    monkeypatch.setattr(env_repo, "_build_tree", _counted)
    threads = [
        threading.Thread(target=commit_files, args=(on, "prod", {"a.yaml": n}, n, "me"))
        for on, n in ((repo, "1"), (repo, "2"), (repo, "3"))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert most[str(repo.path)] == 1

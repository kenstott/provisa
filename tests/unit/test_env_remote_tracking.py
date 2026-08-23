# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6ac21e-7b48-4d05-9a1c-82e4f0d5b96c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Bringing back what the org's git host holds, and deploying it (REQ-1541).

The whole point of the feature is a round trip through a REAL second repository: Provisa pushes a
projection out, a review and a merge happen over there, and a fetch brings the merged branch back
under a name a deploy can be pointed at. A double standing in for the remote would confirm that
dulwich was called and none of that.
"""

# Requirements: REQ-1541, REQ-1527, REQ-125, REQ-1496

from __future__ import annotations

import pytest
from dulwich.repo import Repo

from provisa.core import env_ci
from provisa.core.env_repo import (
    REMOTE_PREFIX,
    _resolve,
    commit_files,
    ensure_repo,
    files_at,
    remote_branches,
    repo_path,
    resolve_sha,
)

pytestmark = pytest.mark.unit

ORG = "acme"


@pytest.fixture(autouse=True)
def repo_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    return tmp_path


@pytest.fixture
def remote(tmp_path):
    """A bare repository standing in for the org's git host, addressed as a path."""
    path = tmp_path / "origin.git"
    path.mkdir()
    Repo.init_bare(str(path))
    return str(path)


def _seed_remote(remote_path: str, branch: str, files: dict[str, str]) -> str:
    """Commit ``files`` onto ``branch`` of the remote, the way a person's merge would leave it."""
    from provisa.core import env_repo

    ref = f"refs/heads/{branch}".encode()
    repo = Repo(remote_path)
    tree = env_repo._build_tree(repo, files)
    from dulwich.objects import Commit

    commit = Commit()
    commit.tree = tree
    commit.author = commit.committer = b"Someone <someone@example.com>"
    commit.commit_time = commit.author_time = 1700000000
    commit.commit_timezone = commit.author_timezone = 0
    commit.encoding = b"UTF-8"
    commit.message = f"merge into {branch}".encode()
    parent = repo.refs.read_ref(ref)
    if parent is not None:
        commit.parents = [parent]
    repo.object_store.add_object(commit)
    repo.refs[ref] = commit.id
    return commit.id.decode()


class TestAFetchBringsTheRemoteBranchesBack:
    def test_they_arrive_as_remote_tracking_refs(self, remote):
        sha = _seed_remote(remote, "main", {"sales/domain.yaml": "id: sales\n"})

        fetched = env_ci.fetch(ORG, remote)

        assert fetched == {"main": sha}
        assert remote_branches(ORG) == {"main": sha}

    def test_the_environment_branches_are_left_alone(self, remote):
        commit_files(
            ensure_repo(ORG), "prod", {"sales/domain.yaml": "id: ours\n"}, "seed", "someone"
        )
        mine = resolve_sha(ORG, "prod")
        _seed_remote(remote, "prod", {"sales/domain.yaml": "id: theirs\n"})

        env_ci.fetch(ORG, remote)

        assert resolve_sha(ORG, "prod") == mine
        assert files_at(ORG, "prod")["sales/domain.yaml"] == "id: ours\n"

    def test_a_branch_deleted_on_the_remote_stops_being_offered(self, remote):
        _seed_remote(remote, "main", {"a.yaml": "a: 1\n"})
        _seed_remote(remote, "feature/x", {"a.yaml": "a: 2\n"})
        env_ci.fetch(ORG, remote)
        assert set(remote_branches(ORG)) == {"main", "feature/x"}

        del Repo(remote).refs[b"refs/heads/feature/x"]
        env_ci.fetch(ORG, remote)

        assert set(remote_branches(ORG)) == {"main"}
        assert REMOTE_PREFIX + b"feature/x" not in ensure_repo(ORG).refs.as_dict()


class TestWhatWasFetchedCanBeDeployed:
    def test_origin_slash_name_resolves_to_the_fetched_commit(self, remote):
        sha = _seed_remote(remote, "main", {"sales/domain.yaml": "id: sales\n"})
        env_ci.fetch(ORG, remote)

        assert resolve_sha(ORG, "origin/main") == sha
        assert files_at(ORG, "origin/main") == {"sales/domain.yaml": "id: sales\n"}

    def test_it_is_a_picture_and_not_a_live_lookup(self, remote):
        _seed_remote(remote, "main", {"a.yaml": "a: 1\n"})
        env_ci.fetch(ORG, remote)
        pinned = resolve_sha(ORG, "origin/main")
        _seed_remote(remote, "main", {"a.yaml": "a: 2\n"})

        assert resolve_sha(ORG, "origin/main") == pinned

        env_ci.fetch(ORG, remote)
        assert resolve_sha(ORG, "origin/main") != pinned

    def test_an_unfetched_remote_name_is_refused_rather_than_guessed(self):
        from provisa.core.env_repo import RepositoryError

        with pytest.raises(RepositoryError):
            _resolve(ensure_repo(ORG), "origin/main")


class TestTheCredentialDoesNotSurviveTheFetch:
    def test_nothing_writes_the_resolved_remote_into_the_repository(self, remote, monkeypatch):
        _seed_remote(remote, "main", {"a.yaml": "a: 1\n"})
        monkeypatch.setenv("GIT_TOKEN", "s3cret")

        env_ci.fetch(ORG, remote)

        config = repo_path(ORG) / "config"
        text = config.read_text() if config.exists() else ""
        assert "s3cret" not in text
        assert remote not in text

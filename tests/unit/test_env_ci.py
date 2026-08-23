# Copyright (c) 2026 Kenneth Stott
# Canary: c81f5a26-7d94-4b30-9e12-6f0ac37b5d84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's half of the org's CI: a push to their remote and a status they read (REQ-1527).

The push is exercised against a REAL second bare repository on disk rather than a recorded call,
because the claim is that an ordinary git client finds the branch there afterwards.
"""

from __future__ import annotations

import pytest
from dulwich.repo import Repo

from provisa.core import env_ci
from provisa.core.env_ci import RepoIntegration, announce, push
from provisa.core.env_repo import commit_files, ensure_repo

pytestmark = pytest.mark.unit

ORG = "acme"
MODEL = {"sales/domain.yaml": "id: sales\n"}


@pytest.fixture(autouse=True)
def repo_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_REPO_DIR", str(tmp_path / "repos"))
    return tmp_path


@pytest.fixture
def remote(tmp_path):
    """A bare repository standing in for the org's git host."""
    path = tmp_path / "remote.git"
    Repo.init_bare(str(path), mkdir=True)
    return path


@pytest.fixture
def posted(monkeypatch):
    """Every status this test posted, in order. The transport itself is covered by httpx."""
    sent: list[tuple[str, dict]] = []

    async def _post(url, payload):
        sent.append((url, payload))

    monkeypatch.setattr(env_ci, "post_status", _post)
    return sent


def _integration(monkeypatch, **kwargs):
    async def _read(_admin_db, _org_id):
        return RepoIntegration(**kwargs)

    monkeypatch.setattr(env_ci, "read_integration", _read)


class TestTheProjectionReachesTheOrgsOwnRemote:
    def test_the_branch_is_there_afterwards(self, remote):
        sha = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        push(ORG, "prod", str(remote))
        assert Repo(str(remote)).refs[b"refs/heads/prod"].decode() == sha

    def test_only_the_environment_that_changed_is_pushed(self, remote):
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        commit_files(ensure_repo(ORG), "dev", MODEL, "seed", None)
        push(ORG, "dev", str(remote))
        refs = Repo(str(remote)).refs.keys()
        assert b"refs/heads/dev" in refs
        assert b"refs/heads/prod" not in refs

    def test_the_remote_is_resolved_through_the_secrets_provider(self, remote, monkeypatch):
        monkeypatch.setenv("TEST_GIT_REMOTE", str(remote))
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        push(ORG, "prod", "${env:TEST_GIT_REMOTE}")
        assert b"refs/heads/prod" in Repo(str(remote)).refs.keys()


class TestTheStatusSaysWhatHappened:
    @pytest.mark.asyncio
    async def test_a_landed_projection_carries_the_sha(self, monkeypatch, posted):
        _integration(monkeypatch, status_webhook="https://ci.acme/hook")
        await announce(None, ORG, "dev", "abc123", False)
        url, payload = posted[0]
        assert url == "https://ci.acme/hook"
        assert payload["environment"] == "dev"
        assert payload["sha"] == "abc123"
        assert payload["drifted"] is False

    @pytest.mark.asyncio
    async def test_a_failed_projection_says_the_environment_is_drifted(self, monkeypatch, posted):
        _integration(monkeypatch, status_webhook="https://ci.acme/hook")
        await announce(None, ORG, "dev", None, True)
        _url, payload = posted[0]
        assert payload["sha"] is None
        assert payload["drifted"] is True

    @pytest.mark.asyncio
    async def test_it_reports_whether_the_remote_actually_took_it(
        self, monkeypatch, posted, remote
    ):
        _integration(monkeypatch, remote=str(remote), status_webhook="https://ci.acme/hook")
        sha = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        await announce(None, ORG, "prod", sha, False)
        assert posted[0][1]["pushed"] is True
        assert Repo(str(remote)).refs[b"refs/heads/prod"].decode() == sha


class TestADeploymentWithNoRemoteAndNoCI:
    @pytest.mark.asyncio
    async def test_nothing_is_pushed_and_nothing_is_posted(self, monkeypatch, posted):
        _integration(monkeypatch)
        await announce(None, ORG, "dev", "abc123", False)
        assert posted == []

    @pytest.mark.asyncio
    async def test_a_remote_alone_posts_no_status(self, monkeypatch, posted, remote):
        _integration(monkeypatch, remote=str(remote))
        sha = commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        await announce(None, ORG, "prod", sha, False)
        assert posted == []
        assert Repo(str(remote)).refs[b"refs/heads/prod"].decode() == sha


class TestReportingNeverFailsTheChange:
    @pytest.mark.asyncio
    async def test_an_unreachable_remote_still_posts_the_status(
        self, monkeypatch, posted, tmp_path
    ):
        _integration(
            monkeypatch, remote=str(tmp_path / "absent.git"), status_webhook="https://ci.acme/hook"
        )
        commit_files(ensure_repo(ORG), "prod", MODEL, "seed", None)
        await announce(None, ORG, "prod", "abc123", False)
        assert posted[0][1]["pushed"] is False

    @pytest.mark.asyncio
    async def test_a_receiver_that_is_down_raises_nothing(self, monkeypatch):
        _integration(monkeypatch, status_webhook="https://ci.acme/hook")

        async def _explode(_url, _payload):
            raise RuntimeError("connection refused")

        monkeypatch.setattr(env_ci, "post_status", _explode)
        await announce(None, ORG, "dev", "abc123", False)

    @pytest.mark.asyncio
    async def test_an_unreadable_integration_raises_nothing(self, monkeypatch, posted):
        async def _explode(_admin_db, _org_id):
            raise RuntimeError("control plane is down")

        monkeypatch.setattr(env_ci, "read_integration", _explode)
        await announce(None, ORG, "dev", "abc123", False)
        assert posted == []


class TestNothingChangedIsNotAnEvent:
    @pytest.mark.asyncio
    async def test_an_unchanged_model_announces_nothing(self, monkeypatch, posted, remote):
        """``write_through`` returns None for an unchanged tree, and that is not a status."""
        from provisa.core import env_repo

        _integration(monkeypatch, remote=str(remote), status_webhook="https://ci.acme/hook")
        announced: list[tuple] = []

        async def _spy(admin_db, org_id, env, sha, drifted):
            announced.append((org_id, env, sha, drifted))

        monkeypatch.setattr(env_ci, "announce", _spy)

        async def _set_drifted(_admin_db, _org_id, _env, _value):
            return None

        async def _set_position(_admin_db, _org_id, _env, **_values):
            return None

        monkeypatch.setattr("provisa.core.env_store.set_drifted", _set_drifted)
        monkeypatch.setattr("provisa.core.env_store.set_position", _set_position)
        monkeypatch.setattr(env_repo, "project", _model)
        monkeypatch.setattr(env_repo, "dump", lambda _model: MODEL)
        first = await env_repo.write_through(None, None, ORG, "prod", "org_acme", "one", None)
        second = await env_repo.write_through(None, None, ORG, "prod", "org_acme", "two", None)
        assert first is not None
        assert second is None
        assert announced == [(ORG, "prod", first, False)]


async def _model(_conn, _schema):
    return MODEL

# Copyright (c) 2026 Kenneth Stott
# Canary: 8aea219e-5415-4fb3-aeef-c7ea2516e720
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the sync read tells, and who it tells it to (REQ-1546, REQ-1552).

``sync_state`` itself is covered against a real repository elsewhere; what is under test here is
the endpoint over it -- which guard it asks, that a branch existing on only one side still appears,
and that the answer says whether there is a remote at all, since "in sync" and "mirrored nowhere"
are otherwise the same picture.
"""

# Requirements: REQ-1546, REQ-1552, REQ-1525

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from provisa.api.admin import environments_router as er
from provisa.core import env_ci, env_repo, secrets_store
from provisa.core.env_ci import RepoIntegration

pytestmark = pytest.mark.asyncio

ORG = "acme"


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


@pytest.fixture
def repo(monkeypatch):
    """Refs on both sides, the guards each recording that they were asked."""
    asked: dict[str, list] = {"member": [], "guard": []}
    integration = {"value": RepoIntegration(remote="https://git.example/acme.git")}

    async def _member(request, org_id, *rights):  # REQ-1573: rights the endpoint accepts
        asked["member"].append(org_id)
        return "uid-member"

    async def _guard(request, org_id):
        asked["guard"].append(org_id)
        return "uid-admin"

    async def _read_integration(admin_db, org_id):
        return integration["value"]

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "_guard", _guard)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(env_ci, "read_integration", _read_integration)
    monkeypatch.setattr(env_repo, "branches", lambda org_id: ["dev", "prod"])
    monkeypatch.setattr(env_repo, "remote_branches", lambda org_id: ["prod", "hotfix"])
    monkeypatch.setattr(
        env_repo,
        "sync_state",
        lambda org_id, env: {
            "local": "a" * 40,
            "remote": "a" * 40,
            "ahead": 0,
            "behind": 0,
            "diverged": False,
            "unsynced": False,
        },
    )
    return asked, integration


class TestWhoMayReadIt:
    async def test_a_member_reads_it(self, repo):
        """REQ-1552: a member who may push has to be able to see that a push is owed."""
        asked, _ = repo
        await er.repo_sync_state(_Request(), ORG)
        assert asked["member"] == [ORG]
        assert asked["guard"] == []


class TestWhatItCarries:
    async def test_a_branch_on_either_side_alone_still_appears(self, repo):
        """A branch never pushed and one only on the remote are both states worth seeing."""
        answer = await er.repo_sync_state(_Request(), ORG)
        assert sorted(answer["branches"]) == ["dev", "hotfix", "prod"]

    async def test_it_says_whether_there_is_a_remote_at_all(self, repo):
        """REQ-1552: without this, an org that has never configured one reads as in sync."""
        _, integration = repo
        assert (await er.repo_sync_state(_Request(), ORG))["remote_configured"] is True
        integration["value"] = RepoIntegration()
        assert (await er.repo_sync_state(_Request(), ORG))["remote_configured"] is False

    async def test_a_status_webhook_is_not_a_mirror(self, repo):
        """The question is whether the model reaches a remote, not whether CI is wired up."""
        _, integration = repo
        integration["value"] = RepoIntegration(status_webhook="https://ci.example/hook")
        assert (await er.repo_sync_state(_Request(), ORG))["remote_configured"] is False

    async def test_the_remote_url_stays_behind_the_admin_read(self, repo):
        """REQ-1525: it carries secret references, and this read is open to every member."""
        answer = await er.repo_sync_state(_Request(), ORG)
        assert "remote" not in answer
        assert "git.example" not in str(answer)


class TestWhatARefusedPullSays:
    """REQ-1556: the refusal names the objects, because the divergence names nothing.

    Whoever now has to decide whose work survives is deciding about particular objects, and "the
    two lines both hold commits the other does not" is not a statement about any of them. What is
    under test here is that the endpoint asks the question and carries the answer out on the
    response; the comparison itself is covered against real schemas elsewhere.
    """

    @pytest.fixture
    def diverged(self, repo, monkeypatch):
        from provisa.core.env_conflicts import Conflict

        asked: dict[str, tuple] = {}

        async def _guard_within(request, org_id, name):
            return "uid-admin"

        async def _known(org_id, name):
            return None

        async def _remote_of(org_id):
            return "https://git.example/acme.git"

        async def _collisions(org_id, name, base_sha, sha):
            asked["collisions"] = (org_id, name, base_sha, sha)
            return [Conflict("sales/domain.yaml", "changed", "removed")]

        # REQ-1557: the fetch resolves the remote's secret references, so the router binds the
        # org around it. Standing in for the binding keeps this a unit test of the refusal;
        # ``tests/integration/test_secrets_store.py`` drives the real binding.
        @asynccontextmanager
        async def _bound(_admin_db, _org_id, *, user_id=None):
            yield

        monkeypatch.setattr(secrets_store, "bound", _bound)
        monkeypatch.setattr(er, "_guard_within", _guard_within)
        monkeypatch.setattr(er, "_known", _known)
        monkeypatch.setattr(er, "_remote_of", _remote_of)
        monkeypatch.setattr(er, "_collisions", _collisions)
        monkeypatch.setattr(env_ci, "fetch", lambda org_id, remote: None)
        monkeypatch.setattr(env_repo, "merge_base", lambda org_id, a, b: "base123")
        monkeypatch.setattr(
            env_repo,
            "sync_state",
            lambda org_id, env: {
                "local": "a" * 40,
                "remote": "b" * 40,
                "ahead": 1,
                "behind": 1,
                "diverged": True,
                "unsynced": False,
            },
        )
        return asked

    async def test_the_refusal_carries_the_objects_both_lines_moved(self, diverged):
        from provisa.api.errors import ApiError

        with pytest.raises(ApiError) as raised:
            await er.pull_environment(_Request(), ORG, "dev")
        assert raised.value.status_code == 409
        assert raised.value.code == "environments.diverged"
        assert raised.value.params["conflicts"] == [
            {"path": "sales/domain.yaml", "source": "changed", "target": "removed"}
        ]

    async def test_it_names_the_commit_the_two_lines_last_shared(self, diverged):
        """An empty list under a base of None means NOTHING WAS COMPARED, so the base travels."""
        from provisa.api.errors import ApiError

        with pytest.raises(ApiError) as raised:
            await er.pull_environment(_Request(), ORG, "dev")
        assert raised.value.params["base"] == "base123"
        assert diverged["collisions"] == (ORG, "dev", "base123", "b" * 40)

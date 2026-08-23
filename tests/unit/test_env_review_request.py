# Copyright (c) 2026 Kenneth Stott
# Canary: 3d90b7a4-1f6e-4c81-92da-6b0f2c5ae174
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Asking the git host to review a branch, when the host is where the rule lives (REQ-1551).

The pull request itself is somebody else's API and is not called here; what this file pins is the
router's half of it -- that the branch is PUSHED before the request is opened, that the target
defaults to the environment a branch came from (REQ-1549) and is refused rather than guessed when
there is none, and that the comment REQ-1550 requires reaches the host as the body reviewers read.
"""

# Requirements: REQ-1551, REQ-1549, REQ-1550, REQ-1546

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from provisa.api.admin import environments_router as er
from provisa.api.errors import ApiError
from provisa.core import env_ci, env_remote, secrets_store

pytestmark = pytest.mark.asyncio

ORG = "acme"
HEAD_SHA = "a10c0de000000000000000000000000000000000"
REMOTE = "https://${env:GIT_TOKEN}@github.com/acme/model.git"


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


@pytest.fixture
def wired(monkeypatch):
    """Every seam recorded in ORDER, because the order is the requirement: a host cannot review
    refs it has never received, so the push has to precede the request."""
    calls: list[tuple] = []
    rows = {
        # A feature environment that knows where it came from, and a root that does not.
        "feature": {"name": "feature", "branched_from": "staging"},
        "staging": {"name": "staging", "branched_from": "prod"},
        "prod": {"name": "prod", "branched_from": None},
    }

    async def _guard_within(request, org_id, name):
        calls.append(("guard_within", org_id, name))
        return "uid-dev"

    async def _known(org_id, name):
        if name not in rows:
            raise ApiError(404, "environments.unknown", name, org=org_id, env=name)
        return rows[name]

    async def _audit(org_id, actor, action, name, detail):
        calls.append(("audit", action, name, detail))

    async def _remote_of(org_id):
        return REMOTE

    def push(org_id, env, remote):
        calls.append(("push", org_id, env, remote))

    def open_pull_request(remote, *, head, base, title, body):
        calls.append(("pr", remote, head, base, title, body))
        return {"url": "https://github.com/acme/model/pull/4", "number": 4, "new": True}

    # REQ-1557: a git call that resolves ``${secret:...}`` runs with the org bound, so the router
    # reaches for the control plane before it reaches for git. Standing in for the binding keeps
    # this a unit test of the router while still recording that the org WAS bound around the call;
    # ``tests/integration/test_secrets_store.py`` drives the real binding.
    @asynccontextmanager
    async def _bound(_admin_db, org_id):
        calls.append(("bound", org_id))
        yield

    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(secrets_store, "bound", _bound)

    monkeypatch.setattr(er, "_guard_within", _guard_within)
    monkeypatch.setattr(er, "_known", _known)
    monkeypatch.setattr(er, "_audit", _audit)
    monkeypatch.setattr(er, "_remote_of", _remote_of)
    monkeypatch.setattr(er, "_track_pushed", lambda org_id, env, sha: calls.append(("track", sha)))
    monkeypatch.setattr(env_ci, "push", push)
    monkeypatch.setattr(env_remote, "open_pull_request", open_pull_request)
    monkeypatch.setattr("provisa.core.env_repo.tip", lambda org_id, env: HEAD_SHA)
    monkeypatch.setattr("provisa.core.env_repo.sync_state", lambda org_id, env: {"unsynced": False})
    return calls


class TestRequestReview:
    async def test_the_branch_is_pushed_before_the_request_is_opened(self, wired):
        """REQ-1551: a pull request names two refs on the host, and one of them has just been made
        here. Opening it first would report a missing branch as the host's error."""
        out = await er.request_review(
            _Request(), ORG, "feature", er.ReviewBody(message="adds tags")
        )
        kinds = [c[0] for c in wired]
        assert kinds.index("push") < kinds.index("pr")
        assert out["pull_request"]["url"] == "https://github.com/acme/model/pull/4"
        assert out["pushed"] == HEAD_SHA

    async def test_the_target_defaults_to_the_branch_it_came_from(self, wired):
        """REQ-1549: the environment records it, so nobody has to choose it."""
        await er.request_review(_Request(), ORG, "feature", er.ReviewBody(message="adds tags"))
        pr = next(c for c in wired if c[0] == "pr")
        assert (pr[2], pr[3]) == ("feature", "staging")

    async def test_a_named_target_wins_over_the_default(self, wired):
        await er.request_review(
            _Request(), ORG, "feature", er.ReviewBody(into="prod", message="hotfix")
        )
        pr = next(c for c in wired if c[0] == "pr")
        assert pr[3] == "prod"

    async def test_the_comment_is_what_the_reviewers_read(self, wired):
        await er.request_review(_Request(), ORG, "feature", er.ReviewBody(message="  adds tags  "))
        pr = next(c for c in wired if c[0] == "pr")
        assert pr[5] == "adds tags"

    async def test_an_environment_with_no_parent_has_to_name_the_target(self, wired):
        """REQ-1549: refused rather than guessed. Proposing into prod because nothing else was
        recorded is not a default."""
        with pytest.raises(ApiError) as exc:
            await er.request_review(_Request(), ORG, "prod", er.ReviewBody(message="anything"))
        assert exc.value.status_code == 400
        assert exc.value.code == "environments.no_merge_target"
        assert not [c for c in wired if c[0] == "push"]

    async def test_a_blank_comment_is_refused_before_anything_is_pushed(self, wired):
        """REQ-1550: and the push does not happen, because the request that would have used it
        was never made."""
        with pytest.raises(ApiError) as exc:
            await er.request_review(_Request(), ORG, "feature", er.ReviewBody(message="   "))
        assert exc.value.status_code == 400
        assert exc.value.code == "environments.message_required"
        assert not [c for c in wired if c[0] in ("push", "pr")]

    async def test_reviewing_an_environment_into_itself_is_refused(self, wired):
        with pytest.raises(ApiError) as exc:
            await er.request_review(
                _Request(), ORG, "feature", er.ReviewBody(into="feature", message="x")
            )
        assert exc.value.code == "environments.same_environment"

    async def test_a_host_that_cannot_be_asked_arrives_as_a_readable_refusal(
        self, wired, monkeypatch
    ):
        """REQ-1551/REQ-1537: a Gitea or a bare mirror is told plainly, not tracebacked at."""

        def refuse(remote, *, head, base, title, body):
            raise env_remote.RemoteError("Provisa opens pull requests on GitHub and GitLab")

        monkeypatch.setattr(env_remote, "open_pull_request", refuse)
        with pytest.raises(ApiError) as exc:
            await er.request_review(_Request(), ORG, "feature", er.ReviewBody(message="adds tags"))
        assert exc.value.status_code == 400
        assert exc.value.code == "environments.review_unavailable"

    async def test_the_link_is_audited_against_the_environment_that_asked(self, wired):
        await er.request_review(_Request(), ORG, "feature", er.ReviewBody(message="adds tags"))
        audit = next(c for c in wired if c[0] == "audit")
        assert audit[1] == "environment.review_requested"
        assert audit[2] == "feature"
        assert audit[3]["into"] == "staging"
        assert audit[3]["url"] == "https://github.com/acme/model/pull/4"

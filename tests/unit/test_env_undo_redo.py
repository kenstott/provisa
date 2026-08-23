# Copyright (c) 2026 Kenneth Stott
# Canary: 15a22f23-c7a2-4603-95d4-ee6cb79aac26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which way an environment can step, answered where it is listed (REQ-1543, REQ-1553).

The move itself -- deploy the neighbouring tree, then move the cursor -- is covered against a real
repository in the integration lane. What is under test here is the claim the list makes about each
end of the line, because that claim is what decides whether a person is offered a control that can
only be refused.
"""

# Requirements: REQ-1543, REQ-1553

from __future__ import annotations

import pytest

from provisa.api.admin import environments_router as er
from provisa.core import env_repo

pytestmark = pytest.mark.asyncio

ORG = "acme"
FIRST = "a" * 40
SECOND = "b" * 40
THIRD = "c" * 40
LINE = {SECOND: FIRST, THIRD: SECOND, FIRST: None}


class _Request:
    class state:  # noqa: N801 -- mirrors starlette's attribute name
        identity = None


def _row(
    name: str,
    *,
    deployed_sha: str | None,
    redo_sha: str | None = None,
    origin_sha: str | None = None,
) -> dict:
    return {
        "name": name,
        "branched_from": None if name == "prod" else "prod",
        "deployed_sha": deployed_sha,
        "redo_sha": redo_sha,
        "origin_sha": origin_sha,
    }


@pytest.fixture
def listing(monkeypatch):
    """The registry answers with whatever rows a test sets, over a three-commit line."""
    rows: list[dict] = []

    async def _member(request, org_id):
        return "uid-member"

    async def _list_envs(admin_db, org_id):
        return [dict(r) for r in rows]

    monkeypatch.setattr(er, "_member", _member)
    monkeypatch.setattr(er, "list_envs", _list_envs)
    monkeypatch.setattr(er, "_admin_pool", lambda: "admin-db")
    monkeypatch.setattr(env_repo, "parent_of", lambda org_id, sha: LINE[sha])
    return rows


async def _listed(name: str) -> dict:
    answer = await er.list_environments(_Request(), ORG)
    return next(e for e in answer["environments"] if e["name"] == name)


class TestSteppingBack:
    async def test_an_environment_in_the_middle_of_its_line_can_step_back(self, listing):
        listing.append(_row("dev", deployed_sha=SECOND))
        assert (await _listed("dev"))["can_undo"] is True

    async def test_the_first_commit_has_nothing_behind_it(self, listing):
        """REQ-1553: the refusal exists, so the control must not be offered into it."""
        listing.append(_row("dev", deployed_sha=FIRST))
        assert (await _listed("dev"))["can_undo"] is False

    async def test_an_environment_that_has_never_deployed_cannot_step_at_all(self, listing):
        """No position is not a position at the start of the line -- it is no line yet."""
        listing.append(_row("dev", deployed_sha=None))
        answer = await _listed("dev")
        assert (answer["can_undo"], answer["can_redo"]) == (False, False)


class TestTheEnvironmentsOwnBeginning:
    """A branch is seeded at its source's tip, so a parent exists below its first own commit."""

    async def test_the_environments_first_commit_has_nothing_behind_it_either(self, listing):
        """One change made after a branch is created is one undo, not two: the step below the
        environment's own first commit lands on the SOURCE's model, a tree it never held."""
        listing.append(_row("dev", deployed_sha=SECOND, origin_sha=FIRST))
        assert (await _listed("dev"))["can_undo"] is False

    async def test_above_its_beginning_it_steps_back_as_usual(self, listing):
        listing.append(_row("dev", deployed_sha=THIRD, origin_sha=FIRST))
        assert (await _listed("dev"))["can_undo"] is True


class TestSteppingForward:
    async def test_forward_is_open_only_where_something_was_stepped_back_from(self, listing):
        """The cursor is the whole of it: redo walks back from the position an undo departed."""
        listing.append(_row("dev", deployed_sha=SECOND, redo_sha=THIRD))
        assert (await _listed("dev"))["can_redo"] is True

    async def test_an_environment_at_the_top_has_nothing_ahead(self, listing):
        listing.append(_row("dev", deployed_sha=THIRD, redo_sha=None))
        assert (await _listed("dev"))["can_redo"] is False

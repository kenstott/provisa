# Copyright (c) 2026 Kenneth Stott
# Canary: 2b9c4e08-51df-4a37-8c6e-71f0a3d95b42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Which of the target's own work a merge is about to carry away (REQ-1555).

The merge itself is unchanged -- the source wins, which is what a merge into a target is. What is
asserted here is that the report SAYS SO: an object both lines moved away from the commit they last
shared was edited twice, independently, and one of those edits is about to disappear.
"""

# Requirements: REQ-1490, REQ-1539, REQ-1555

from __future__ import annotations

import pytest

from provisa.core import env_conflicts
from provisa.core.env_conflicts import ADDED, CHANGED, REMOVED, compare


def _paths(conflicts) -> list[tuple[str, str, str]]:
    return [(c.path, c.source, c.target) for c in conflicts]


class TestCompare:
    def test_both_lines_edited_the_same_object(self):
        base = {"sales/orders.yaml": {"description": "orders"}}
        source = {"sales/orders.yaml": {"description": "sales orders"}}
        target = {"sales/orders.yaml": {"description": "customer orders"}}
        assert _paths(compare(base, source, target)) == [("sales/orders.yaml", CHANGED, CHANGED)]

    def test_only_the_source_moved_so_the_merge_is_doing_its_job(self):
        base = {"a.yaml": {"v": 1}}
        assert compare(base, {"a.yaml": {"v": 2}}, dict(base)) == []

    def test_only_the_target_moved_so_nothing_contradicts_it(self):
        """The source still overwrites it -- but with the base's own content, which is the value
        the target's editor started from, not another person's answer to the same question."""
        base = {"a.yaml": {"v": 1}}
        assert compare(base, dict(base), {"a.yaml": {"v": 2}}) == []

    def test_two_people_arriving_at_the_same_answer_is_agreement(self):
        base = {"a.yaml": {"v": 1}}
        same = {"a.yaml": {"v": 2}}
        assert compare(base, dict(same), dict(same)) == []

    def test_one_side_deleted_what_the_other_edited(self):
        """The loudest case: the merge removes an object somebody was still working on."""
        base = {"a.yaml": {"v": 1}}
        assert _paths(compare(base, {}, {"a.yaml": {"v": 2}})) == [("a.yaml", REMOVED, CHANGED)]

    def test_both_sides_created_the_same_path_differently(self):
        assert _paths(compare({}, {"a.yaml": {"v": 1}}, {"a.yaml": {"v": 2}})) == [
            ("a.yaml", ADDED, ADDED)
        ]

    def test_both_sides_deleted_it_which_is_not_a_collision(self):
        assert compare({"a.yaml": {"v": 1}}, {}, {}) == []

    def test_an_object_untouched_by_either_line(self):
        base = {"a.yaml": {"v": 1}}
        assert compare(base, dict(base), dict(base)) == []

    def test_every_colliding_object_is_named_not_counted(self):
        base = {"a.yaml": {"v": 0}, "b.yaml": {"v": 0}, "c.yaml": {"v": 0}}
        source = {"a.yaml": {"v": 1}, "b.yaml": {"v": 1}, "c.yaml": {"v": 0}}
        target = {"a.yaml": {"v": 2}, "b.yaml": {"v": 2}, "c.yaml": {"v": 0}}
        assert [c.path for c in compare(base, source, target)] == ["a.yaml", "b.yaml"]


class TestTheBase:
    """Without a commit both lines held there is no question to ask, and that is reported."""

    @pytest.mark.asyncio
    async def test_lines_that_share_no_ancestor_are_not_reported_as_clean(self, monkeypatch):
        import provisa.core.env_repo as env_repo

        monkeypatch.setattr(env_repo, "tip", lambda org_id, env: "a" * 40)
        monkeypatch.setattr(env_repo, "merge_base", lambda org_id, a, b: None)
        base, conflicts = await env_conflicts.detect(None, "acme", "dev", "prod", "s", "d")
        assert base is None and conflicts == []

    @pytest.mark.asyncio
    async def test_an_environment_whose_line_has_not_started_holds_no_base(self, monkeypatch):
        import provisa.core.env_repo as env_repo

        monkeypatch.setattr(env_repo, "tip", lambda org_id, env: None)
        base, conflicts = await env_conflicts.detect(None, "acme", "dev", "prod", "s", "d")
        assert base is None and conflicts == []

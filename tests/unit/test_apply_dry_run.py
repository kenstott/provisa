# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8c41de-07f3-4a96-8e15-c3d9f6a2701b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The parts of a dry run of an apply that need no database (REQ-2222): what it reports, and the
domain policy it must not leave behind."""

# Requirements: REQ-2222

import asyncio

import pytest

from provisa.core import domain_policy
from provisa.core.store_config import item_identity, model_changes


@pytest.fixture(autouse=True)
def _reset_policy():
    domain_policy.reset()
    yield
    domain_policy.reset()


def _store(**sections) -> dict:
    return {"sources": [], "roles": [], "naming_rules": [], **sections}


class TestDomainPolicyTrial:
    def test_a_trial_sees_what_it_configures(self):
        with domain_policy.trial():
            domain_policy.configure(False, "sales")
            assert domain_policy.snapshot() == (False, "sales")

    def test_the_change_ends_with_the_trial(self):
        domain_policy.configure(True, "default")
        with domain_policy.trial():
            domain_policy.configure(False, "sales")
        assert domain_policy.snapshot() == (True, "default")

    def test_a_trial_starts_from_the_policy_in_force(self):
        domain_policy.configure(True, "hr")
        with domain_policy.trial():
            assert domain_policy.snapshot() == (True, "hr")

    def test_the_change_ends_when_the_trial_raises(self):
        with pytest.raises(ValueError, match="refused"):
            with domain_policy.trial():
                domain_policy.configure(False, "sales")
                raise ValueError("refused")
        assert domain_policy.snapshot() == (None, "default")

    def test_another_task_does_not_see_the_trial(self):
        async def scenario() -> tuple:
            entered, leave = asyncio.Event(), asyncio.Event()

            async def in_trial() -> None:
                with domain_policy.trial():
                    domain_policy.configure(False, "sales")
                    entered.set()
                    await leave.wait()

            running = asyncio.create_task(in_trial())
            await entered.wait()
            seen = domain_policy.snapshot()
            leave.set()
            await running
            return seen

        assert asyncio.run(scenario()) == (None, "default")


class TestModelChanges:
    def test_nothing_changed(self):
        store = _store(sources=[{"id": "pg", "host": "a"}])
        assert model_changes(store, store) == {"changes": False, "unchanged": 1, "sections": {}}

    def test_an_added_item_is_named(self):
        report = model_changes(_store(), _store(sources=[{"id": "pg"}]))
        assert report["changes"] is True
        assert report["sections"]["sources"] == {"add": ["pg"], "change": [], "remove": []}

    def test_a_changed_item_names_its_fields_and_no_values(self):
        before = _store(sources=[{"id": "pg", "password": "hunter2", "pool": {"max": 1}}])
        after = _store(sources=[{"id": "pg", "password": "s3cret", "pool": {"max": 2}}])
        report = model_changes(before, after)
        assert report["sections"]["sources"]["change"] == [
            {"item": "pg", "fields": ["password", "pool.max"]}
        ]
        assert "hunter2" not in str(report) and "s3cret" not in str(report)

    def test_a_removed_item_is_named(self):
        report = model_changes(_store(roles=[{"id": "analyst"}]), _store())
        assert report["sections"]["roles"]["remove"] == ["analyst"]

    def test_unchanged_counts_across_sections(self):
        store = _store(sources=[{"id": "pg"}], roles=[{"id": "a"}, {"id": "b"}])
        assert model_changes(store, store)["unchanged"] == 3

    @pytest.mark.parametrize(
        ("section", "item", "named"),
        [
            ("tables", {"source_id": "pg", "schema": "public", "table": "o"}, "pg/public/o"),
            ("calendars", {"name": "fiscal", "version": 2}, "fiscal@2"),
            ("naming_rules", {"pattern": "^x", "replace": "y"}, "^x"),
            ("naming.rules", {"pattern": "^x", "replace": "y"}, "^x"),
            ("sources", {"id": "pg"}, "pg"),
        ],
    )
    def test_an_item_is_named_as_a_file_names_it(self, section, item, named):
        assert item_identity(section, item) == named

# Copyright (c) 2026 Kenneth Stott
# Canary: 6c6c910f-541b-44b5-b72d-a0c4b4243213
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Walking a branch chain, including the two shapes a chain must never have (REQ-1529).

``branched_from`` is acyclic by construction, so both refusals here describe a corrupted registry
rather than a tree an org could build. They are still checked, because the walk runs on every
binding resolution: a chain that closes on itself would otherwise be an unbounded loop holding an
admin-plane connection, and that is a defect that takes the plane down rather than one request.
"""

# Requirements: REQ-1491, REQ-1529

from __future__ import annotations

import pytest

from provisa.core import env_bindings as eb

pytestmark = pytest.mark.asyncio

ORG = "acme"


@pytest.fixture
def registry(monkeypatch):
    """A ``name -> branched_from`` map standing in for the environments table."""
    parents: dict[str, str | None] = {}

    async def _row(admin_db, org_id, env):
        assert org_id == ORG
        if env not in parents:
            raise eb.BindingError(f"organization {org_id!r} has no environment {env!r}")
        return {"name": env, "branched_from": parents[env]}

    monkeypatch.setattr(eb, "_row", _row)
    return parents


class TestLineage:
    async def test_a_base_is_its_own_whole_lineage(self, registry):
        registry["prod"] = None
        assert await eb.lineage(None, ORG, "prod") == ["prod"]
        assert await eb.base_of(None, ORG, "prod") == "prod"

    async def test_a_chain_is_nearest_first(self, registry):
        registry.update({"prod": None, "base": "prod", "feature": "base"})
        assert await eb.lineage(None, ORG, "feature") == ["feature", "base", "prod"]
        assert await eb.base_of(None, ORG, "feature") == "prod"

    async def test_an_unknown_environment_has_no_lineage(self, registry):
        with pytest.raises(eb.BindingError, match="has no environment 'ghost'"):
            await eb.lineage(None, ORG, "ghost")

    async def test_a_chain_naming_a_missing_parent_is_refused(self, registry):
        """Not silently truncated to what was reachable: a lineage that stops early would resolve a
        binding against fewer environments than the branch actually inherits from."""
        registry["feature"] = "vanished"
        with pytest.raises(eb.BindingError, match="has no environment 'vanished'"):
            await eb.lineage(None, ORG, "feature")


class TestCycleGuard:
    async def test_an_environment_branched_from_itself_is_named_as_a_cycle(self, registry):
        registry["loop"] = "loop"
        with pytest.raises(eb.BindingError, match="cyclic branch chain: loop -> loop"):
            await eb.lineage(None, ORG, "loop")

    async def test_a_ring_reports_the_environment_that_closed_it(self, registry):
        registry.update({"a": "c", "b": "a", "c": "b"})
        with pytest.raises(eb.BindingError, match=r"cyclic branch chain: a -> c -> b -> a"):
            await eb.lineage(None, ORG, "a")

    async def test_a_cycle_is_not_reported_as_a_deep_chain(self, registry):
        """The depth limit would also stop this, and would describe a tree nobody has (REQ-1529)."""
        registry.update({"a": "b", "b": "a"})
        with pytest.raises(eb.BindingError) as exc:
            await eb.lineage(None, ORG, "a")
        assert "cyclic" in str(exc.value) and "deeper than" not in str(exc.value)

    async def test_the_cycle_check_costs_a_correct_chain_nothing(self, registry):
        """A name repeated across DIFFERENT organizations is not a cycle -- the walk never leaves
        one org, so ``seen`` holds names and needs no org in the key."""
        registry.update({"prod": None, "dev": "prod"})
        assert await eb.lineage(None, ORG, "dev") == ["dev", "prod"]


class TestDepthLimit:
    async def test_an_acyclic_chain_past_the_limit_is_refused(self, registry):
        registry["e0"] = None
        for i in range(1, eb.MAX_DEPTH + 5):
            registry[f"e{i}"] = f"e{i - 1}"
        deepest = f"e{eb.MAX_DEPTH + 4}"
        with pytest.raises(eb.BindingError, match=f"deeper than {eb.MAX_DEPTH}"):
            await eb.lineage(None, ORG, deepest)

    async def test_a_chain_at_the_limit_still_resolves(self, registry):
        registry["e0"] = None
        for i in range(1, eb.MAX_DEPTH):
            registry[f"e{i}"] = f"e{i - 1}"
        chain = await eb.lineage(None, ORG, f"e{eb.MAX_DEPTH - 1}")
        assert chain[0] == f"e{eb.MAX_DEPTH - 1}" and chain[-1] == "e0"

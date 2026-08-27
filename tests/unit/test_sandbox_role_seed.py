# Copyright (c) 2026 Kenneth Stott
# Canary: 7f2c40e9-1b56-4d83-9ea7-5c08b3d61a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1597: sandbox is org_admin minus a denylist, and it is the subtraction that is asserted.

A stranger holding a "Try it Out" link is meant to be able to do everything the product does inside
an environment that expires (REQ-1595). Defining the role by what it may do would make every
capability added later invisible to visitors until somebody remembered this one row -- so the seed
subtracts, and this file asserts the subtraction rather than the resulting list: a new right lands
in the sandbox automatically, and the six that are withheld stay withheld.
"""

# Requirements: REQ-1595, REQ-1596, REQ-1597

from __future__ import annotations

import json
import re

import pytest

from provisa.core.db import _ORG_ADMIN_CAPABILITIES, _SANDBOX_DENIED, _SEED_ROLES

SANDBOX = "sandbox"


def _caps(role_id: str) -> set[str]:
    return set(next(caps for rid, caps in _SEED_ROLES if rid == role_id))


class TestWhatIsWithheld:
    @pytest.mark.parametrize("right", sorted(_SANDBOX_DENIED))
    def test_a_denied_right_is_absent(self, right):
        assert right not in _caps(SANDBOX)

    def test_the_visitor_cannot_leave_the_environment_they_were_seated_in(self):
        """REQ-1596: the membership pin confines them, and the pin is pointless against a role that
        could name another environment -- the two halves only contain a stranger together."""
        assert "environment_switch" not in _caps(SANDBOX)

    def test_it_carries_nothing_from_the_platform_plane(self):
        assert not _caps(SANDBOX) & {"admin", "superadmin", "cross_org", "platform_settings"}


class TestWhatIsKept:
    def test_it_is_everything_else_org_admin_holds(self):
        """The subtraction itself: a capability added to org_admin reaches the sandbox with no
        second edit, which is the only way the demo keeps demonstrating the whole product."""
        assert _caps(SANDBOX) == set(_ORG_ADMIN_CAPABILITIES) - _SANDBOX_DENIED

    @pytest.mark.parametrize(
        "right",
        ["source_registration", "table_registration", "create_relationship", "write"],
    )
    def test_the_visitor_can_actually_build_something(self, right):
        # A sandbox that only reads is a screenshot; these are the rights that make it a trial.
        assert right in _caps(SANDBOX)


class TestPlaneParity:
    """schema.sql is PostgreSQL-only DDL and db.py is its portable mirror; a role seeded into one
    plane and not the other is a role that exists on SaaS and not on a laptop."""

    def test_the_postgres_seed_says_the_same_thing(self):
        sql = open("provisa/core/schema.sql").read()
        match = re.search(r"'sandbox',\s*'(\[.*?\])'::jsonb", sql, re.S)
        assert match is not None, "no sandbox row in the PostgreSQL seed"
        assert set(json.loads(re.sub(r"\s+", "", match.group(1)))) == _caps(SANDBOX)

    def test_the_environment_rights_re_assertion_never_names_it(self):
        """The seams that add environment_management/environment_switch to pre-existing role rows
        name org_admin and developer only -- naming sandbox there would hand back what the denylist
        took away, on the next init_schema."""
        sql = open("provisa/core/schema.sql").read()
        for line in sql.splitlines():
            if "environment_switch" in line and line.lstrip().upper().startswith(
                ("UPDATE", "WHERE")
            ):
                assert "sandbox" not in line

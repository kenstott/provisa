# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1058: an unauthenticated visitor is bound to the org's built-in read-only 'public' role.

The role is a standard role (REQ-2155), seeded in every org with the others. It holds no write
capability, and any attempt to give it one -- a capability on the role, or a write grant on a
table naming it -- is refused by name. A visitor acting as it is refused every write.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.core.db import SEEDED_ROLE_IDS, STANDARD_ROLE_RIGHTS, STANDARD_ROLES
from provisa.core.models import Role, Table, WriteGrant
from provisa.security.rights import (
    PUBLIC_READ_RIGHTS,
    PUBLIC_ROLE,
    Capability,
    has_capability,
)

_READ_ONLY = r"Role 'public' is read-only"


# --- seeded as a standard role --------------------------------------------------------------------


def test_public_is_a_standard_role_seeded_in_every_org():
    assert PUBLIC_ROLE in STANDARD_ROLES
    assert PUBLIC_ROLE in SEEDED_ROLE_IDS


def test_public_starts_with_zero_rights_and_holds_no_write_capability():
    # REQ-1059: nothing is public by default; what the org publishes to it is a grant.
    assert STANDARD_ROLE_RIGHTS[PUBLIC_ROLE] == frozenset()
    assert STANDARD_ROLES[PUBLIC_ROLE].parents == ()
    assert Capability.WRITE.value not in PUBLIC_READ_RIGHTS


# --- a write capability is refused by name --------------------------------------------------------


@pytest.mark.parametrize("capability", ["write", "data_product_rw", "create_view", "usage"])
def test_giving_public_a_capability_beyond_reading_is_refused_by_name(capability):
    with pytest.raises(ValidationError, match=_READ_ONLY) as refused:
        Role(id=PUBLIC_ROLE, description="Visitors", capabilities=[capability], domain_access=["*"])
    assert capability in str(refused.value)


def test_public_may_be_given_the_read_rights():
    role = Role(
        id=PUBLIC_ROLE,
        description="Visitors",
        capabilities=sorted(PUBLIC_READ_RIGHTS),
        domain_access=["*"],
    )
    assert set(role.capabilities) == PUBLIC_READ_RIGHTS


def test_another_role_may_hold_write():
    role = Role(id="loader", description="Loads", capabilities=["write"], domain_access=["*"])
    assert role.capabilities == ["write"]


# --- a write grant naming it is refused by name ---------------------------------------------------


@pytest.mark.parametrize(
    "grant",
    [
        {"operation": "insert", "columns": ["id"]},
        {"operation": "update", "columns": ["id"], "rows": "TRUE"},
        {"operation": "delete", "rows": "TRUE"},
    ],
)
def test_a_write_grant_naming_public_is_refused_by_name(grant):
    with pytest.raises(ValidationError, match=_READ_ONLY) as refused:
        WriteGrant(role=PUBLIC_ROLE, **grant)
    assert grant["operation"] in str(refused.value)


def test_a_table_naming_public_in_its_write_grants_is_refused_by_name():
    with pytest.raises(ValidationError, match=_READ_ONLY):
        Table.model_validate(
            {
                "source_id": "pg",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "columns": [{"name": "id", "visible_to": [PUBLIC_ROLE]}],
                "write_grants": [{"role": PUBLIC_ROLE, "operation": "insert", "columns": ["id"]}],
            }
        )


def test_a_table_may_be_published_to_public_for_reading():
    table = Table.model_validate(
        {
            "source_id": "pg",
            "domain_id": "sales",
            "schema": "public",
            "table": "orders",
            "columns": [{"name": "id", "visible_to": [PUBLIC_ROLE]}],
        }
    )
    assert table.columns[0].visible_to == [PUBLIC_ROLE]


# --- a public visitor's write is refused ----------------------------------------------------------


def _visitor_gov() -> GovernanceContext:
    """Governance for the visitor: the public role's standard rights, as stage 2 reads them."""
    gov = GovernanceContext()
    gov.role_id = PUBLIC_ROLE
    gov.can_write = has_capability(
        {"capabilities": sorted(STANDARD_ROLE_RIGHTS[PUBLIC_ROLE])}, Capability.WRITE
    )
    gov.table_map = {"sales.orders": 1}
    gov.all_columns = {1: [("id", "integer"), ("region", "varchar")]}
    gov.write_grants = {}
    gov.rls_rules = {}
    gov.write_ops = {1: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {1: True}
    gov.write_refused_forms = {1: frozenset()}
    return gov


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO sales.orders (id, region) VALUES (1, 'east')",
        "UPDATE sales.orders SET region = 'x' WHERE id = 1",
        "DELETE FROM sales.orders WHERE id = 1",
    ],
)
def test_a_public_visitors_write_is_refused(sql):
    with pytest.raises(WriteNotAdmitted, match="role 'public' does not hold the 'write' right"):
        apply_governance(sql, _visitor_gov())

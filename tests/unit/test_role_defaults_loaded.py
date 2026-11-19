# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9c2a71-5e84-4b0d-9a6e-1c7d2b8e4f05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: a role's default entitlements reach a holder in a real model build. The build writes
the role rows to the entitlement store and reads each role's defaults back from it, so the roles it
installs carry them and a holder with no list of their own gets the role's default."""

# Requirements: REQ-2219

from __future__ import annotations

import pytest

from provisa.core.models import Role
from provisa.core.repositories import role as role_repo
from provisa.entitlements import store
from provisa.entitlements.lists import ROLE, HeldEntitlements, effective_lists
from provisa.entitlements.runtime import role_defaults
from provisa.security.inheritance import flatten_role_dicts, with_own_domains

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def conn(control_plane):
    db = control_plane.org()
    async with db.acquire() as c:
        yield c


async def _role(conn, role_id, domain_access, defaults):
    await role_repo.upsert(
        conn,
        Role(
            id=role_id,
            description="Test role.",
            capabilities=["query_development"],
            domain_access=domain_access,
            default_entitlements=defaults,
        ),
        org_id="default",
    )


async def _built_roles(conn) -> dict[str, dict]:
    """The roles as the model build installs them: the role rows written, the roles read, and
    folded as the build folds them (own domains kept, then the inheritance chains)."""
    from provisa.api.app import _load_roles

    await store.write_role_rows(conn)
    return {r["id"]: r for r in flatten_role_dicts(with_own_domains(await _load_roles(conn)))}


async def test_a_roles_default_list_applies_to_a_holder_with_no_list(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"], "relationships": []})
    roles = await _built_roles(conn)

    defaults = role_defaults(roles["seller"])
    assert defaults == {"tables": ["sales.orders"], "relationships": []}
    lists = effective_lists("seller", {}, HeldEntitlements(), defaults)
    assert lists["tables"].identifiers == ("sales.orders",)
    assert lists["tables"].origin == ROLE
    assert lists["relationships"].identifiers == ()


async def test_a_deny_by_default_role_gives_a_holder_nothing(conn):
    await _role(conn, "auditor", ["hr"], {"domains": []})
    roles = await _built_roles(conn)

    lists = effective_lists("auditor", {}, HeldEntitlements(), role_defaults(roles["auditor"]))
    assert lists["domains"].identifiers == ()
    assert lists["domains"].origin == ROLE


async def test_a_default_naming_the_roles_own_domains_narrows_nothing(conn):
    """Setting a role's domains writes the same default (the shortcut): that default is the role
    itself, however the build widens its domain access."""
    await _role(conn, "seller", ["sales"], None)
    roles = await _built_roles(conn)

    assert role_defaults(roles["seller"]) == {}


async def test_a_changed_default_reaches_the_next_build(conn):
    await _role(conn, "seller", ["sales"], {"tables": ["sales.orders"]})
    await _built_roles(conn)
    await _role(conn, "seller", ["sales"], {"tables": ["sales.*"]})
    roles = await _built_roles(conn)

    assert role_defaults(roles["seller"]) == {"tables": ["sales.*"]}

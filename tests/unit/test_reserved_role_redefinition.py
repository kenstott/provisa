# Copyright (c) 2026 Kenneth Stott
# Canary: 30d45a7f-609e-43af-b884-107631ec25d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1349 / REQ-1297: a config file (or the admin surface) may not define the reserved admin roles
org_admin and platform_admin — their definitions are the deployment's own. The write used to be
dropped SILENTLY, so a config that declared one loaded as if honoured while doing nothing. It is now
refused by name, before any row is written, so the config fails to load instead."""

# Requirements: REQ-1349, REQ-1297

from __future__ import annotations

import pytest

from provisa.core.models import Role
from provisa.core.repositories import role as role_repo
from provisa.core.repositories.role import ReservedRoleRedefined


@pytest.mark.parametrize("role_id", ["org_admin", "platform_admin"])
async def test_a_reserved_admin_role_cannot_be_redefined(role_id):
    # The refusal fires before any DB write, so no connection is needed (None would be used only
    # after the check passes).
    role = Role(id=role_id, capabilities=["table_registration"], domain_access=["*"])
    with pytest.raises(ReservedRoleRedefined) as exc:
        await role_repo.upsert(None, role, org_id=None)  # type: ignore[arg-type]
    assert exc.value.role_id == role_id
    assert exc.value.code == "config.reserved_role_redefined"
    assert role_id in str(exc.value)


async def test_an_ordinary_role_is_not_refused_by_the_reserved_check():
    # A non-reserved role passes the reserved-role gate and proceeds (it then needs a connection,
    # which None is not — so an AttributeError/TypeError here, NOT ReservedRoleRedefined, confirms
    # the gate let it through).
    role = Role(id="eu_resident", capabilities=["table_registration"], domain_access=["*"])
    with pytest.raises(Exception) as exc:
        await role_repo.upsert(None, role, org_id=None)  # type: ignore[arg-type]
    assert not isinstance(exc.value, ReservedRoleRedefined)

# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b0a11-7c05-4a3d-9d18-6ee0a5c4b7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1573 (amended by REQ-2155): who holds the two environment rights, asserted at the standard
roles.

``environment_management`` is creating or deleting an environment and reaching the environments
admin surface; ``environment_switch`` is being served by one other than prod. org_admin holds both;
every other data role holds environment_switch alone, through viewer (REQ-2155 amended 2026-11-20:
which environments a person sees is decided by the roles assigned in each, REQ-1539).
"""

from __future__ import annotations

import pytest

from provisa.core.db import STANDARD_ROLE_RIGHTS
from provisa.security.rights import Capability

_RIGHTS = {"environment_management", "environment_switch"}


def _seeded(role_id: str) -> set[str]:
    return set(STANDARD_ROLE_RIGHTS[role_id])


def test_org_admin_holds_both_rights():
    assert _RIGHTS <= _seeded("org_admin")


@pytest.mark.parametrize("role_id", ["viewer", "analyst", "data_writer", "developer", "modeler"])
def test_a_member_switches_but_never_manages(role_id: str):
    assert "environment_switch" in _seeded(role_id)
    assert "environment_management" not in _seeded(role_id)


def test_only_org_admin_manages_environments():
    from provisa.core.db import STANDARD_ROLES

    holders = {r for r in STANDARD_ROLES if "environment_management" in _seeded(r)}
    assert holders == {"org_admin"}


def test_the_platform_admin_holds_neither():
    # REQ-1327: the control plane alone; no environment of any org's data.
    assert not (_RIGHTS & _seeded("platform_admin"))


def test_both_rights_are_real_capabilities():
    assert _RIGHTS <= {c.value for c in Capability}


class TestEnvGateCapabilities:
    """REQ-1573: which capability set the environment gate reads, and when it reads none."""

    def _state(self):
        from types import SimpleNamespace

        return SimpleNamespace(
            roles={
                "analyst": {"id": "analyst", "capabilities": ["usage", "query_development"]},
                "developer": {
                    "id": "developer",
                    "capabilities": ["query_development", "environment_switch"],
                },
            }
        )

    def _identity(self, user_id, roles):
        from types import SimpleNamespace

        return SimpleNamespace(user_id=user_id, roles=roles, acting_roles=roles)

    def test_dev_no_auth_is_exempt(self):
        from provisa.api.admin.capabilities import env_gate_capabilities

        # The anonymous dev principal is what an unsecured deployment resolves for every request —
        # the same skip require_capability makes. None means "no gate", not "no rights".
        assert (
            env_gate_capabilities(self._identity("anonymous", ["org_admin"]), self._state()) is None
        )
        assert env_gate_capabilities(None, self._state()) is None

    def test_a_real_user_is_gated_on_the_rights_their_roles_carry(self):
        from provisa.api.admin.capabilities import env_gate_capabilities

        state = self._state()
        assert "environment_switch" not in env_gate_capabilities(
            self._identity("u1", ["analyst"]), state
        )
        assert "environment_switch" in env_gate_capabilities(
            self._identity("u2", ["developer"]), state
        )

    def test_an_empty_set_is_not_the_exemption(self):
        from provisa.api.admin.capabilities import env_gate_capabilities
        from provisa.api.env_routing import may_switch

        caps = env_gate_capabilities(self._identity("u3", ["nosuchrole"]), self._state())
        assert caps == set()
        assert not may_switch(caps)

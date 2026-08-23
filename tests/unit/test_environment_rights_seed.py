# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b0a11-7c05-4a3d-9d18-6ee0a5c4b7f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1573: who holds the two environment rights, asserted at the seed.

``environment_management`` is creating or deleting an environment and reaching the environments
admin surface; ``environment_switch`` is being served by one other than prod. org_admin and
developer hold both, analyst and modeler hold neither, and the three places that say so — the
Capability enum, the Python seed, and schema.sql — have to agree or a deployment gets a right its
UI does not gate on.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from provisa.core.db import _SEED_ROLES
from provisa.security.rights import Capability

_RIGHTS = {"environment_management", "environment_switch"}
_SCHEMA_SQL = Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"


def _seeded(role_id: str) -> set[str]:
    return set(next(caps for rid, caps in _SEED_ROLES if rid == role_id))


def _schema_sql_seeded(role_id: str) -> set[str]:
    sql = _SCHEMA_SQL.read_text()
    start = sql.index(f"    '{role_id}',")
    block = sql[start : sql.index("ON CONFLICT", start)]
    return set(re.findall(r'"([a-z_]+)"', block[block.index("'[") : block.index("]'")]))


@pytest.mark.parametrize("role_id", ["org_admin", "developer"])
def test_the_working_roles_hold_both_rights(role_id: str):
    assert _RIGHTS <= _seeded(role_id)
    assert _RIGHTS <= _schema_sql_seeded(role_id)


@pytest.mark.parametrize("role_id", ["analyst", "modeler"])
def test_a_reader_holds_neither(role_id: str):
    # The default the user asked for: an analyst works in prod, and nothing about switching
    # environments arrives with a role whose subject is querying the model.
    assert not (_RIGHTS & _seeded(role_id))
    assert not (_RIGHTS & _schema_sql_seeded(role_id))


def test_both_rights_are_real_capabilities():
    assert _RIGHTS <= {c.value for c in Capability}


@pytest.mark.parametrize("role_id", ["org_admin", "developer", "analyst", "modeler"])
def test_the_python_seed_and_schema_sql_say_the_same_thing(role_id: str):
    # Two seeds exist because two planes are created two ways (portable and PG); a right present in
    # one and absent from the other is a deployment where the gate depends on how it was installed.
    assert _seeded(role_id) & _RIGHTS == _schema_sql_seeded(role_id) & _RIGHTS


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

        return SimpleNamespace(user_id=user_id, roles=roles)

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

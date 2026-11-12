# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A SQL statement reads a command as a governed relation only when its role may call it.

REQ-870: a command the role may not use answers exactly as a name never registered does. The
validator's V006 check passes the table functions it is handed; handing it every registered
command (REQ-2226's minted-token fix) let an unassigned call through to the engine, which
answered "Table Function with name region_count does not exist" where a name never registered
gets V006 (test_command_admission_every_surface, release/v0.1.0-alpha.509)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.pgwire._pipeline import governed_table_functions
from provisa.security.inheritance import OWN_DOMAIN_ACCESS
from provisa.security.rights import Capability

pytestmark = pytest.mark.unit

_REGION_COUNT = {
    "name": "region_count",
    "kind": "query",
    "domain_id": "sales",
    "visible_to": ["reader"],
}


def _role(rid: str, caps: list[str]) -> dict:
    return {
        "id": rid,
        "description": rid,
        "capabilities": caps,
        "domain_access": ["*"],
        "parents": [],
        OWN_DOMAIN_ACCESS: ["*"],
    }


def _state() -> SimpleNamespace:
    run = Capability.COMMAND_EXECUTION.value
    return SimpleNamespace(
        roles={
            "reader": _role("reader", ["query_development", run]),
            "outsider": _role("outsider", ["query_development", "write", run]),
        },
        tracked_functions={"region_count": _REGION_COUNT},
        tracked_webhooks={},
    )


def test_a_command_assigned_to_the_role_is_a_governed_table_function():
    assert "region_count" in governed_table_functions(_state(), "reader")


def test_a_command_the_role_is_not_assigned_is_no_governed_table_function():
    assert "region_count" not in governed_table_functions(_state(), "outsider")

# Copyright (c) 2026 Kenneth Stott
# Canary: b2696336-fe92-4c9f-905e-09f5018288b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2155: the admin role type carries what a role holds with its ancestors folded in, so the
client builds a caller's rights from the roles it holds without reading their ancestors'
definitions (which a caller without user_management is not shown)."""

from __future__ import annotations

from provisa.api.admin._row_mappers import _role_from_row
from provisa.core.db import standard_role_dicts
from provisa.security.inheritance import effective_capabilities


def _row(role_id: str) -> dict:
    spec = next(r for r in standard_role_dicts() if r["id"] == role_id)
    return {
        **spec,
        "description": "Test role.",
        "domain_access": ["*"],
        "demonstrated": [],
        "rate_limit": None,
        "residency_values": [],
        "session_vars": {},
        "inactivity_days": None,
    }


def test_a_shown_role_carries_its_inherited_rights():
    rows = standard_role_dicts()
    held = _role_from_row(
        _row("data_steward"), detail=True, effective=effective_capabilities("data_steward", rows)
    )
    assert "access_config" not in held.capabilities
    assert {"access_config", "glossary_read", "query_development"} <= set(
        held.effective_capabilities
    )


def test_a_withheld_role_carries_no_rights():
    hidden = _role_from_row(_row("data_steward"), detail=False, effective=["access_config"])
    assert hidden.effective_capabilities is None

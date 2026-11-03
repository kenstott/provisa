# Copyright (c) 2026 Kenneth Stott
# Canary: 4f7c2a19-8d3e-4b61-a5c0-2e9b7d14f8a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A draft's preview read as "Role: All" -- the meta-role of the set the user acts as -- is read
with the union of the set's rights, as every other surface reads under it (REQ-1620, REQ-1921).

A set one of whose members may see the draft answers rows; a set none of whose members may see
it is refused by name, as a single role without the right is.
"""

# Requirements: REQ-1620, REQ-1921

from __future__ import annotations

import pytest

from provisa.api.admin import table_read
from provisa.security.meta_role import ensure_meta_role, is_meta_role_id
from tests.unit.test_empty_domain_access_reaches_nothing import _built_roles, _tables
from tests.unit.test_table_read_as_caller import (
    _REFUNDS,
    _Routed,
    _refunds,
    multi_domain,  # noqa: F401  # pyright: ignore[reportUnusedImport]
    routing,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)

# "r" may see the draft's columns; "s" and "t" read the same domain and see none of them.
_ROLES = [
    {"id": "r", "description": "Test role.", "capabilities": ["usage"], "domain_access": ["sales"]},
    {"id": "s", "description": "Test role.", "capabilities": ["usage"], "domain_access": ["sales"]},
    {"id": "t", "description": "Test role.", "capabilities": ["usage"], "domain_access": ["sales"]},
]


@pytest.fixture
def state(multi_domain, monkeypatch):  # noqa: F811
    """The real role build of three roles, ``refunds`` registered as a draft only ``r`` sees."""
    import provisa.api.app as appmod

    runtime = appmod.state._active_runtime()
    for name in ("meta_roles", "meta_resolved_at", "meta_qualifiers", "masking_rules"):
        monkeypatch.setattr(runtime, name, {}, raising=False)
    built = _built_roles(monkeypatch, [dict(r) for r in _ROLES])
    monkeypatch.setattr(built, "tables", _tables(), raising=False)
    built.role_build_inputs["draft_tables"] = [_refunds()]
    return built


async def test_a_draft_previewed_as_a_set_one_member_may_see_answers(state, routing):  # noqa: F811
    meta = ensure_meta_role(state, ["r", "s"])
    assert is_meta_role_id(meta)
    with pytest.raises(_Routed) as routed:
        await table_read.read_as_caller(state, _REFUNDS, meta, limit=5)
    routing.assert_awaited_once()
    assert "refunds" in routed.value.sql


async def test_a_draft_previewed_as_a_set_no_member_may_see_is_refused_by_name(
    state,
    routing,  # noqa: F811
):
    # A single role without the right is refused naming the table ...
    with pytest.raises(PermissionError) as single:
        await table_read.read_as_caller(state, _REFUNDS, "s", limit=5)
    assert "refunds" in str(single.value)
    # ... and so is a set none of whose members holds it.
    meta = ensure_meta_role(state, ["s", "t"])
    with pytest.raises(PermissionError) as refused:
        await table_read.read_as_caller(state, _REFUNDS, meta, limit=5)
    assert "refunds" in str(refused.value)
    routing.assert_not_awaited()

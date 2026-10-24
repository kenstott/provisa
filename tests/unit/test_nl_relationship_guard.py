# Copyright (c) 2026 Kenneth Stott
# Canary: dfa6030b-21cf-4ce5-9c64-97b947757659
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-603: non-strict NL's SQL carries the relationship guard's opt-out, so a role whose flag allows
the bypass runs a join outside the approved relationships and a role whose flag does not is still
refused V002 -- one bypass rule, no NL-only one. Strict NL never carries it. Governed for real:
the pipeline's own V002 check over a two-table domain with no approved join."""

# Requirements: REQ-603

from __future__ import annotations

import pytest

from provisa.nl.runner import GUARD_OPT_OUT, effective_strict, nl_may_bypass, non_strict_sql
from tests.unit.test_high_security_relationship_guard import (
    _GUARD_OFF,
    _UNAPPROVED_JOIN,
    _ctx,
    _route,
    _state,
)

pytestmark = pytest.mark.asyncio

_GUARDED = {"id": "modeler", "capabilities": [], "domain_access": ["*"]}


async def _governs(monkeypatch, role: dict, sql: str) -> str:
    """'' when the statement clears governance, else the refusal (this state stops short of a
    route, so a statement that clears governance still raises -- with no V002)."""
    with pytest.raises(Exception) as raised:  # noqa: PT011 -- asserting which refusal it is
        await _route(monkeypatch, role=role, security_high=False, sql=sql)
    return str(raised.value)


async def test_non_strict_sql_opts_out_of_the_guard():
    sql = non_strict_sql(_UNAPPROVED_JOIN, _ctx())
    assert sql.splitlines()[0] == GUARD_OPT_OUT


async def test_non_strict_runs_an_unapproved_join_for_a_role_whose_flag_allows_it(monkeypatch):
    refusal = await _governs(monkeypatch, _GUARD_OFF, non_strict_sql(_UNAPPROVED_JOIN, _ctx()))
    assert "V002" not in refusal


async def test_non_strict_is_refused_v002_for_a_role_whose_flag_does_not(monkeypatch):
    refusal = await _governs(monkeypatch, _GUARDED, non_strict_sql(_UNAPPROVED_JOIN, _ctx()))
    assert "V002" in refusal


@pytest.mark.parametrize("role", [_GUARD_OFF, _GUARDED], ids=["bypass-allowed", "guarded"])
async def test_strict_sql_carries_no_opt_out_and_is_refused_v002(monkeypatch, role):
    refusal = await _governs(monkeypatch, role, _UNAPPROVED_JOIN)
    assert "V002" in refusal


async def test_only_a_role_that_may_bypass_runs_non_strict():
    allowed = _state(role=_GUARD_OFF, security_high=False)
    guarded = _state(role=_GUARDED, security_high=False)
    high = _state(role=_GUARD_OFF, security_high=True)
    assert nl_may_bypass("modeler", allowed) and not nl_may_bypass("modeler", guarded)
    assert not nl_may_bypass("modeler", high)
    assert effective_strict(False, "modeler", allowed) is False
    assert effective_strict(False, "modeler", guarded) is True
    assert effective_strict(True, "modeler", allowed) is True

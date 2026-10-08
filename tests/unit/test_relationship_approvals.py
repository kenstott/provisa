# Copyright (c) 2026 Kenneth Stott
# Canary: 0c5e1b7a-93d4-4f26-8a1e-6b2d7f40c9e3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship request is decided by the domains it touches (REQ-1948): the rule itself."""

# Requirements: REQ-1948, REQ-1944

from __future__ import annotations

import types

import pytest

from provisa.api.admin import relationship_approvals as ra
from provisa.api.admin.capabilities import right_reach

BOTH = frozenset({"sales", "finance"})
SALES = frozenset({"sales"})


def _approval(user: str, *domains: str) -> dict:
    return {"approver": user, "approved_at": "now", "domains": sorted(domains)}


# --- the reach of the right (REQ-1944's pairing, reused) ---------------------------------------

_ROLES = {
    "sales_modeler": {"capabilities": ["create_relationship"], "domain_access": ["sales"]},
    "finance_reader": {"capabilities": ["query_development"], "domain_access": ["finance"]},
    "org_modeler": {"capabilities": ["create_relationship"], "domain_access": ["*"]},
}


def _state():
    return types.SimpleNamespace(roles=_ROLES)


def _identity(user: str, *roles: str):
    return types.SimpleNamespace(user_id=user, roles=list(roles))


@pytest.fixture(autouse=True)
def _many_domains(monkeypatch):
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


def test_the_right_reaches_only_the_domains_of_the_roles_carrying_it():
    # The finance role carries no create_relationship, so finance is not reached.
    reach = right_reach(_identity("u", "sales_modeler", "finance_reader"), _state(), ra.RIGHT)
    assert reach == SALES


def test_a_role_reaching_every_domain_reaches_every_domain():
    assert right_reach(_identity("u", "org_modeler"), _state(), ra.RIGHT) is None


def test_a_user_without_the_right_reaches_nothing():
    assert right_reach(_identity("u", "finance_reader"), _state(), ra.RIGHT) == frozenset()


def test_single_domain_mode_drops_the_domain_half_and_keeps_the_right(monkeypatch):
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: True)
    assert right_reach(_identity("u", "sales_modeler"), _state(), ra.RIGHT) is None
    assert right_reach(_identity("u", "finance_reader"), _state(), ra.RIGHT) == frozenset()


def test_reach_within_a_request_is_the_overlap():
    assert ra.reached(SALES, BOTH) == SALES
    assert ra.reached(None, BOTH) == BOTH
    assert ra.reached(frozenset({"hr"}), BOTH) == frozenset()


# --- who may approve ---------------------------------------------------------------------------


def test_an_approver_reaching_none_of_the_domains_is_refused_naming_them():
    refusal = ra.approval_refusal(
        user_id="hr", requested_by="req", approvals=[], involved=BOTH, reach=frozenset({"hr"})
    )
    assert refusal is not None
    assert refusal.code == "requests.approver_outside_domains"
    assert refusal.params == {"domains": "finance, sales"}
    assert "finance, sales" in refusal.message


def test_the_requester_is_refused_even_when_their_right_reaches_a_domain():
    refusal = ra.approval_refusal(
        user_id="req", requested_by="req", approvals=[], involved=BOTH, reach=SALES
    )
    assert refusal is not None and refusal.code == "requests.own_request"


def test_a_second_approval_by_the_same_user_is_refused():
    refusal = ra.approval_refusal(
        user_id="a",
        requested_by="req",
        approvals=[_approval("a", "sales")],
        involved=BOTH,
        reach=SALES,
    )
    assert refusal is not None and refusal.code == "requests.already_approved"


def test_an_approval_with_no_user_behind_it_is_refused():
    refusal = ra.approval_refusal(
        user_id=None, requested_by="req", approvals=[], involved=BOTH, reach=None
    )
    assert refusal is not None and refusal.code == "requests.approver_unidentified"


def test_an_eligible_approver_is_not_refused():
    assert (
        ra.approval_refusal(
            user_id="a", requested_by="req", approvals=[], involved=BOTH, reach=SALES
        )
        is None
    )


# --- who may reject ----------------------------------------------------------------------------


def test_a_rejection_comes_from_anyone_who_could_approve():
    kw = {"requested_by": "req", "involved": BOTH}
    assert ra.rejection_refusal(user_id="a", reach=SALES, **kw) is None
    outside = ra.rejection_refusal(user_id="hr", reach=frozenset({"hr"}), **kw)
    assert outside is not None and outside.code == "requests.approver_outside_domains"
    own = ra.rejection_refusal(user_id="req", reach=SALES, **kw)
    assert own is not None and own.code == "requests.own_request"


# --- when it is executable ---------------------------------------------------------------------


def test_two_approvals_from_one_side_of_a_cross_domain_request_leave_it_waiting():
    approvals = [_approval("a", "sales"), _approval("b", "sales")]
    assert ra.waiting_on(BOTH, approvals) == ["finance"]
    assert ra.executable(BOTH, approvals, "req") is False


def test_one_approval_from_each_side_makes_it_executable():
    approvals = [_approval("a", "sales"), _approval("f", "finance")]
    assert ra.waiting_on(BOTH, approvals) == []
    assert ra.executable(BOTH, approvals, "req") is True


def test_one_approver_reaching_both_sides_is_still_one_approval():
    approvals = [_approval("org", "finance", "sales")]
    assert ra.waiting_on(BOTH, approvals) == []
    assert ra.executable(BOTH, approvals, "req") is False


def test_a_same_domain_request_needs_two_approvers_of_that_domain():
    assert ra.executable(SALES, [_approval("a", "sales")], "req") is False
    assert ra.executable(SALES, [_approval("a", "sales"), _approval("b", "sales")], "req") is True


def test_the_requester_and_a_repeated_approver_never_count():
    assert ra.executable(SALES, [_approval("a", "sales"), _approval("a", "sales")], "req") is False
    assert (
        ra.executable(SALES, [_approval("a", "sales"), _approval("req", "sales")], "req") is False
    )


def test_a_request_with_no_resolved_domain_is_never_executable():
    approvals = [_approval("a"), _approval("b")]
    assert ra.executable(frozenset(), approvals, "req") is False


# --- what a user sees --------------------------------------------------------------------------


def test_a_user_sees_what_they_can_decide_and_what_they_made():
    assert ra.visible_to(user_id="a", requested_by="req", involved=BOTH, reach=SALES) is True
    assert ra.visible_to(user_id="req", requested_by="req", involved=BOTH, reach=frozenset())
    assert not ra.visible_to(
        user_id="hr", requested_by="req", involved=BOTH, reach=frozenset({"hr"})
    )


def test_can_decide_is_false_for_the_requester():
    assert ra.can_decide(user_id="a", requested_by="req", involved=BOTH, reach=SALES) is True
    assert ra.can_decide(user_id="req", requested_by="req", involved=BOTH, reach=SALES) is False


# --- a request whose tables are gone -------------------------------------------------------------


def test_a_request_with_no_registered_table_cannot_be_approved_by_anyone():
    for reach in (None, SALES):
        refusal = ra.approval_refusal(
            user_id="a", requested_by="req", approvals=[], involved=frozenset(), reach=reach
        )
        assert refusal is not None and refusal.code == "requests.tables_not_registered"


def test_it_is_rejected_only_by_a_right_reaching_every_domain():
    kw = {"requested_by": "req", "involved": frozenset()}
    assert ra.rejection_refusal(user_id="org", reach=None, **kw) is None
    narrow = ra.rejection_refusal(user_id="a", reach=SALES, **kw)
    assert narrow is not None and narrow.code == "requests.tables_not_registered"
    own = ra.rejection_refusal(user_id="req", reach=None, **kw)
    assert own is not None and own.code == "requests.own_request"


def test_the_refusal_names_the_tables_that_are_gone():
    refusal = ra.approval_refusal(
        user_id="a",
        requested_by="req",
        approvals=[],
        involved=frozenset(),
        reach=None,
        tables=("orders", "customers"),
    )
    assert refusal is not None and refusal.params == {"tables": "orders, customers"}
    assert "orders, customers" in refusal.message
    assert ra.tables_named({"source_table_id": "orders", "target_table_id": ""}) == ("orders",)

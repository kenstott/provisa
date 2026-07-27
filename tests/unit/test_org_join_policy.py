# Copyright (c) 2026 Kenneth Stott
# Canary: 91c4e7a2-0f36-4d58-b1a9-6e83d2c50f47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1268/REQ-1269: org join-policy primitives.

``email_matches_rule`` is the single predicate both gates share (invite redemption and
auto-join); its truth table — NULL rule admits everything, a rule demands a non-empty
matching email, a corrupt stored rule fails CLOSED. ``_validate_org_policy`` is the
write-time guard: only compilable rules are ever stored (which is why the read-side
re.error guard is a hand-edit backstop, not a code path), and auto_join without a
default role is rejected — membership without a role would grant nothing.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from provisa.api.admin.orgs_router import _validate_org_policy
from provisa.core.org_membership import email_matches_rule

# ---------------------------------------------------------------------------
# email_matches_rule truth table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("email", "rule", "expected"),
    [
        # NULL rule imposes no restriction — even a missing email passes.
        ("alice@acme.com", None, True),
        (None, None, True),
        ("", None, True),
        # Anchored domain rule.
        ("alice@acme.com", r"@acme\.com$", True),
        ("alice@example.com", r"@acme\.com$", False),
        # Unanchored substring would over-match without the anchor — the rule text decides.
        ("alice@acme.com.evil.org", r"@acme\.com$", False),
        ("alice@acme.com.evil.org", r"@acme\.com", True),
        # A non-NULL rule requires a non-empty email.
        (None, r"@acme\.com$", False),
        ("", r"@acme\.com$", False),
        # Corrupt stored rule (hand-edited DB) fails closed: no match, never an exception.
        ("alice@acme.com", r"[unclosed", False),
    ],
)
def test_email_matches_rule(email, rule, expected):
    assert email_matches_rule(email, rule) is expected


# ---------------------------------------------------------------------------
# _validate_org_policy write-time guard
# ---------------------------------------------------------------------------


def test_policy_accepts_valid_rule_and_no_auto_join():
    _validate_org_policy(r"@acme\.com$", False, None)


def test_policy_accepts_auto_join_with_role():
    _validate_org_policy(r"@acme\.com$", True, "analyst")


def test_policy_accepts_all_defaults():
    _validate_org_policy(None, False, None)


def test_policy_rejects_uncompilable_rule():
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(r"[unclosed", False, None)
    assert exc.value.status_code == 400
    assert "Invalid email rule" in exc.value.detail


def test_policy_rejects_auto_join_without_role():
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(None, True, None)
    assert exc.value.status_code == 400
    assert "auto_join_role" in exc.value.detail

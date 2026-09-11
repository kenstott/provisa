# Copyright (c) 2026 Kenneth Stott
# Canary: cf59ecc9-f721-4fe0-bda3-6ae653fd23fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the invitation expiry the org_admin chooses (REQ-1696).

Source coverage:
  - provisa/api/admin/invites_router.py — CreateInviteBody.expires_in_days

The span is the caller's, bounded to what an invitation can mean: at least a day, at most a
century. A week is only what an omitted value falls to, so a caller that never asked keeps the
invitation it always got.
"""

import pytest
from pydantic import ValidationError

from provisa.api.admin.invites_router import CreateInviteBody


def test_omitted_span_is_a_week() -> None:
    assert CreateInviteBody(org_id="acme").expires_in_days == 7


@pytest.mark.parametrize("days", [1, 30, 36500])
def test_caller_span_is_kept(days: int) -> None:
    assert CreateInviteBody(org_id="acme", expires_in_days=days).expires_in_days == days


@pytest.mark.parametrize("days", [0, -1, 36501])
def test_span_outside_a_day_to_a_century_is_refused(days: int) -> None:
    with pytest.raises(ValidationError):
        CreateInviteBody(org_id="acme", expires_in_days=days)

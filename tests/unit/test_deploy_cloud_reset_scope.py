# Copyright (c) 2026 Kenneth Stott
# Canary: 249c2997-d261-4ac3-a681-1daa54e3cbb7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Only `reset` wipes cloud-dev's accounts and orgs; every arm that deploys code keeps them.

On 2026-11-16 a code deploy ran `deploy-cloud.sh all`, whose arm called reset_state: the claimed
platform admin, every account, membership, invite and tenant org went with it. Deploying code and
returning the deployment to its first start are two acts, and only the one named `reset` is the
second.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "deploy-cloud.sh"


def _arms() -> dict[str, str]:
    """Each arm of the script's final dispatch, its comment lines dropped."""
    text = _SCRIPT.read_text()
    dispatch = text[text.rindex('case "$TARGET" in') :]
    arms: dict[str, str] = {}
    for name, body in re.findall(r"^  ([\w-]+)\)\n(.*?);;", dispatch, re.S | re.M):
        arms[name] = "\n".join(
            line for line in body.splitlines() if not line.strip().startswith("#")
        )
    return arms


def _calls(body: str) -> set[str]:
    return set(re.findall(r"\b[a-z_]+\b", body))


def test_the_dispatch_has_the_arms_the_usage_names():
    assert set(_arms()) == {"ui", "api", "cfg", "all", "sandbox-reset", "reset", "patch"}


def test_reset_wipes_accounts_and_orgs():
    assert "reset_state" in _calls(_arms()["reset"])


@pytest.mark.parametrize("arm", ["ui", "api", "cfg", "all", "patch", "sandbox-reset"])
def test_no_other_arm_wipes_them(arm):
    calls = _calls(_arms()[arm])
    assert "reset_state" not in calls
    # verify_demo asserts zero accounts: a statement about the reset, so it runs only after one.
    assert "verify_demo" not in calls


def test_the_header_says_only_reset_wipes():
    header = _SCRIPT.read_text().split("set -euo pipefail", 1)[0]
    assert "api/all/reset also wipe" not in header
    assert "Only `reset` wipes" in header

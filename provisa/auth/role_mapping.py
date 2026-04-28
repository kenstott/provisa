# Copyright (c) 2026 Kenneth Stott
# Canary: c8cd15b0-50e9-4174-b0cb-f2ecec4c4fd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map auth identity claims to Provisa role IDs."""

from __future__ import annotations

from provisa.auth.models import AuthIdentity


def resolve_role(
    identity: AuthIdentity,
    mapping_rules: list[dict],
    default_role: str,
) -> str:
    """Return the first matching Provisa role_id for the identity.

    Rule types:
      - exact: claim == value
      - contains: value in claim_list
    """
    for rule in mapping_rules:
        rule_type = rule.get("type", "exact")
        claim_key = rule["claim"]
        expected = rule["value"]
        role_id = rule["role"]

        claim_value = identity.raw_claims.get(claim_key)
        if claim_value is None:
            continue

        if rule_type == "exact" and claim_value == expected:
            return role_id
        if rule_type == "contains" and isinstance(claim_value, list):
            if expected in claim_value:
                return role_id

    return default_role

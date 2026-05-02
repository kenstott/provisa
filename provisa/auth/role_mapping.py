# Copyright (c) 2026 Kenneth Stott
# Canary: c8cd15b0-50e9-4174-b0cb-f2ecec4c4fd5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Map auth identity claims to Provisa role IDs and role:domain assignments."""

from __future__ import annotations

from provisa.auth.models import AuthIdentity, RoleAssignment


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


def resolve_assignments(identity: AuthIdentity) -> list[RoleAssignment]:
    """Parse structured role claims into RoleAssignment pairs.

    Each claim in identity.roles can be:
      - "role_id:domain_id"  → RoleAssignment(role_id, domain_id)
      - "role_id"            → RoleAssignment(role_id, "*")

    Enterprise IdPs emit lists of claims (e.g. ["analyst:trading_ops", "steward:trading_risk"]).
    Plain role names (no colon) are treated as global (all-domain) assignments.
    """
    result: list[RoleAssignment] = []
    for claim in identity.roles:
        claim = claim.strip()
        if not claim:
            continue
        if ":" in claim:
            role_id, domain_id = claim.split(":", 1)
            result.append(RoleAssignment(role_id=role_id.strip(), domain_id=domain_id.strip()))
        else:
            result.append(RoleAssignment(role_id=claim, domain_id="*"))
    return result

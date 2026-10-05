# Copyright (c) 2026 Kenneth Stott
# Canary: c483bee5-38e4-4fe1-b67e-0f7269fdfd23
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The identity a region's own work is done as (REQ-1921, REQ-1922).

Where the platform declares regions, what a region keeps — each copy of a view it builds, each
row it fetches by key into its store — is governed as the organisation's administrator with that
region as its region attribute: the ``org_admin`` role's rules, resolved against the role's own
session constants and this node's region, never against the values of whichever caller's request
happened to set the work off. That is one identity, used by every such piece of work; with no
platform regions none of it applies and the work is done as before.
"""

# Requirements: REQ-1921, REQ-1922

from __future__ import annotations

from typing import Any

from provisa.security.rights import ORG_ADMIN_ROLE

#: The role a region's own work is governed as.
ROLE = ORG_ADMIN_ROLE


def governs_region_work() -> bool:
    """Whether the platform declares regions: only then is a region's work governed as its
    administrator (REQ-1922)."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION

    return process_region.region() != DEFAULT_REGION


def session_vars(state: Any) -> dict[str, str]:
    """The administrator's session variables in this region: the role's own constants, and the
    node's region over them — never a request's bound values."""
    from provisa.core import process_region
    from provisa.core.request_context import REGION_VAR

    role = state.roles.get(ROLE) or {}
    out = {str(k): str(v) for k, v in (role.get("session_vars") or {}).items()}
    out[REGION_VAR] = process_region.region()
    return out


def row_rule(state: Any, table: Any) -> str | None:
    """The administrator's row rule on ``table`` (its own, else its domain's), None for none."""
    rls = state.rls_contexts.get(ROLE)
    if rls is None:
        return None
    rule = rls.rules.get(table.id)
    if rule is None and getattr(table, "domain_id", None):
        rule = rls.domain_rules.get(table.domain_id)
    return rule or None

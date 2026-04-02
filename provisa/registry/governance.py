# Copyright (c) 2025 Kenneth Stott
# Canary: aa15b4e4-33cd-4b4a-955b-4f3acd7c4b2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Production governance gate (REQ-001, REQ-002, REQ-003, REQ-004).

Registry-required tables: query must match approved registry entry.
Pre-approved tables: user rights only, no registry needed.
Test mode: environment-gated, allows arbitrary queries.
"""

from __future__ import annotations

import os
from enum import Enum


class GovernanceMode(str, Enum):
    TEST = "test"
    PRODUCTION = "production"


class GovernanceError(Exception):
    """Raised when a query violates governance rules."""

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)


def get_mode() -> GovernanceMode:
    """Determine current governance mode from environment."""
    env = os.environ.get("PROVISA_MODE", "test")
    if env.lower() in ("production", "prod"):
        return GovernanceMode.PRODUCTION
    return GovernanceMode.TEST


def check_governance(
    mode: GovernanceMode,
    target_table_ids: list[int],
    table_governance: dict[int, str],
    stable_id: str | None = None,
) -> None:
    """Check if a query is allowed under the current governance mode.

    Args:
        mode: Current governance mode.
        target_table_ids: Table IDs involved in the query.
        table_governance: {table_id: governance_level} mapping.
        stable_id: Approved query stable ID (None if raw query).

    Raises GovernanceError if not allowed.
    """
    if mode == GovernanceMode.TEST:
        return  # All queries allowed in test mode

    # Production mode
    for table_id in target_table_ids:
        gov = table_governance.get(table_id, "registry-required")
        if gov == "pre-approved":
            continue  # No registry needed (REQ-003)
        if gov == "registry-required":
            if stable_id is None:
                raise GovernanceError(
                    f"Table {table_id} requires an approved query. "
                    f"Raw queries not allowed in production mode (REQ-001)."
                )


def check_deprecated(query: dict) -> None:
    """Raise if a query is deprecated (REQ-026)."""
    if query.get("status") == "deprecated":
        replacement = query.get("deprecated_by")
        msg = f"Query {query.get('stable_id')!r} is deprecated."
        if replacement:
            msg += f" Use replacement: {replacement!r}"
        raise GovernanceError(msg)


def check_output_type(
    query: dict,
    requested_output: str,
) -> None:
    """Check that the requested output type is within the approved ceiling (REQ-046)."""
    permitted = query.get("permitted_outputs", ["json"])
    if requested_output not in permitted:
        raise GovernanceError(
            f"Output type {requested_output!r} not permitted. "
            f"Allowed: {permitted}"
        )

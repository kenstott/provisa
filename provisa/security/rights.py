# Copyright (c) 2026 Kenneth Stott
# Canary: dfc5dbe7-500f-4850-980a-123862c96eaf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Distinct rights model — independently configured per role (REQ-042).

Each capability gates a specific operation. Missing capability → rejection.
"""

from __future__ import annotations

from enum import Enum


class Capability(str, Enum):
    SOURCE_REGISTRATION = "source_registration"
    TABLE_REGISTRATION = "table_registration"
    RELATIONSHIP_REGISTRATION = "relationship_registration"
    SECURITY_CONFIG = "security_config"
    QUERY_DEVELOPMENT = "query_development"
    QUERY_APPROVAL = "query_approval"
    FULL_RESULTS = "full_results"  # bypass sampling mode
    ADMIN = "admin"


class InsufficientRightsError(Exception):
    """Raised when a role lacks the required capability."""

    def __init__(self, role_id: str, required: Capability):
        self.role_id = role_id
        self.required = required
        super().__init__(
            f"Role {role_id!r} lacks required capability: {required.value}"
        )


def check_capability(
    role: dict,
    required: Capability,
) -> None:
    """Check that a role has the required capability.

    Raises InsufficientRightsError if not.
    """
    capabilities = role.get("capabilities", [])
    if required.value not in capabilities and Capability.ADMIN.value not in capabilities:
        raise InsufficientRightsError(role["id"], required)


def has_capability(role: dict, capability: Capability) -> bool:
    """Check without raising."""
    capabilities = role.get("capabilities", [])
    return capability.value in capabilities or Capability.ADMIN.value in capabilities

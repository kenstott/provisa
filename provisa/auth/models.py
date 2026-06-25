# Copyright (c) 2026 Kenneth Stott
# Canary: 3de609ff-6421-4f6e-9d77-5c7c93e20416
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auth identity and provider base classes."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

# Requirements: REQ-120


@dataclass
class AuthIdentity:  # REQ-120
    """Authenticated user identity extracted from a token.

    roles: list of structured claims in 'role_id:domain_id' or plain 'role_id' format.
    A list supports enterprise IdPs that emit multiple role claims per user.
    """

    user_id: str
    email: str | None
    display_name: str | None
    roles: list[str]
    raw_claims: dict = field(default_factory=dict)
    active_org_id: str | None = None


@dataclass
class RoleAssignment:  # REQ-120
    """A resolved (role_id, domain_id) pair for a user.

    domain_id == '*' means the role applies across all domains.
    """

    role_id: str
    domain_id: str


class AuthProvider(ABC):  # REQ-120
    """Abstract base for authentication providers."""

    @property
    def auth_scheme(self) -> str:
        return "bearer"

    @abstractmethod
    async def validate_token(self, token: str) -> AuthIdentity:
        """Validate a bearer token and return the identity."""
        ...

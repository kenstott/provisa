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


@dataclass
class AuthIdentity:
    """Authenticated user identity extracted from a token."""

    user_id: str
    email: str | None
    display_name: str | None
    roles: list[str]
    raw_claims: dict = field(default_factory=dict)


class AuthProvider(ABC):
    """Abstract base for authentication providers."""

    @abstractmethod
    async def validate_token(self, token: str) -> AuthIdentity:
        """Validate a bearer token and return the identity."""
        ...

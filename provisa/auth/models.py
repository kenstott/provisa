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
from typing import Any

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

    # Stable provider identifier (e.g. "firebase", "basic"), set by each concrete
    # provider. Recorded on user_profiles; a missing one is a wiring bug.
    provider_name: str

    # REQ-1263: the personal-access-token store, attached by ``build_auth_provider`` when the
    # platform control plane is available. None on a deployment without one, in which case a
    # PAT-shaped credential is simply not a PAT and goes to the provider like any other token.
    pat_store: Any = None

    @property
    def auth_scheme(self) -> str:
        return "bearer"

    @abstractmethod
    async def validate_token(self, token: str) -> AuthIdentity:  # REQ-486
        """Validate a bearer token and return the identity."""
        ...

    async def validate_credential(self, token: str) -> AuthIdentity:  # REQ-1263
        """Resolve any bearer credential — a personal access token or a provider token.

        Every surface calls this rather than ``validate_token`` so a PAT works everywhere
        without each protocol learning what a PAT is. The token's own prefix decides which
        path runs, so this is a dispatch on credential type, never a try-one-then-the-other
        chain that would turn a rejected credential into a second guess.
        """
        return await self._with_pat(token, self.validate_token)

    async def _with_pat(self, token: str, fallback) -> AuthIdentity:
        from provisa.auth.pat import is_personal_access_token

        if self.pat_store is not None and is_personal_access_token(token):
            return await self.pat_store.validate(token)
        return await fallback(token)

    @property
    def token_validators(self) -> dict:
        """Credential presentation → the validator that accepts it.

        Read by AuthMiddleware and by every non-HTTP surface. One scheme by default; a
        provider accepting more than one presentation overrides this (REQ-124).
        """
        return {self.auth_scheme: self.validate_credential}


def validator_for_scheme(provider: AuthProvider, scheme: str):
    """The provider's validator for one credential presentation, or None if it accepts no such.

    A surface that can only carry one presentation — MCP and Flight carry a bearer token, and
    nothing else — asks for that scheme by name instead of calling ``validate_token`` directly.
    Calling ``validate_token`` would hand a bearer token to whatever the provider's *primary*
    validator is, which for the basic provider is base64 ``user:password``: every bearer
    credential, including a PAT, fails there.
    """
    validators = getattr(provider, "token_validators", None) or {
        getattr(provider, "auth_scheme", "bearer"): provider.validate_token
    }
    return validators.get(scheme.lower())

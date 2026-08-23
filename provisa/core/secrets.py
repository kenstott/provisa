# Copyright (c) 2026 Kenneth Stott
# Canary: 6773fcaa-c61e-4fc2-b7e3-cc9ab9ab3d01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reference grammar every secret is written in: ``${provider:reference}``.

Three provider names matter to a person writing config. ``env`` is the process environment -- the
deployment's own configuration, and the only thing a central secrets backend's own credential may
be written against. ``secret`` is the SECRETS SERVICE, whichever one this deployment is wired to
(``provisa/core/secrets_registry.py``): Provisa's own encrypted per-org store by default, a central
service when one is configured. The reference does not change when the backend does. ``user`` is
the same service and the PERSONAL vault inside it (REQ-1560): it resolves against whoever is
acting, so the same reference hands each person their own credential and hands a person who has
stored none an error. There is no third name for "somebody else's secret", which is what stops one
member of an org using another's.

Resolution is FAIL-CLOSED throughout: an unknown provider, an unset name and an unreachable backend
all raise. A secret that could not be resolved is never an empty string and never falls through to
another provider (REQ-1557).
"""

# Requirements: REQ-125, REQ-251, REQ-320, REQ-557, REQ-1557, REQ-1560

import os
import re
from abc import ABC, abstractmethod

_SECRET_PATTERN = re.compile(r"\$\{(\w+):([^}]+)\}")


class SecretsProvider(ABC):  # REQ-125, REQ-320
    @abstractmethod
    def resolve(self, reference: str) -> str: ...

    def resolve_user(self, reference: str) -> str:  # REQ-1560
        """Resolve ``${user:NAME}`` from the acting person's vault in this backend.

        Fail-closed by default: a backend that has no personal vault says so rather than answering
        with the org's value, which would hand one member another member's credential.
        """
        raise ValueError(
            f"The configured secrets service ({type(self).__name__}) holds no personal vault, so "
            f"${{user:{reference}}} cannot be resolved. Store it as an organization secret, or "
            "select a secrets service that supports personal vaults."
        )


class EnvSecretsProvider(SecretsProvider):  # REQ-125
    def resolve(self, reference: str) -> str:
        if ":-" in reference:
            var, default = reference.split(":-", 1)
            return os.environ.get(var, default)
        value = os.environ.get(reference)
        if value is None:
            raise KeyError(f"Environment variable not set: {reference}")
        return value


_PROVIDERS: dict[str, SecretsProvider] = {
    "env": EnvSecretsProvider(),
}


def register_provider(name: str, provider: SecretsProvider) -> None:  # REQ-557
    _PROVIDERS[name] = provider


def _provider_for(name: str) -> SecretsProvider:
    """The provider ``name`` refers to.

    ``secret`` is resolved through the runtime rather than the table above, because WHICH backend
    it means is deployment configuration that can be installed after this module is imported, and
    because its default -- Provisa's own store -- must not be constructed until something actually
    asks for a secret (REQ-1557).
    """
    if name in ("secret", "user"):
        from provisa.core.secrets_runtime import secrets_backend

        return secrets_backend()
    provider = _PROVIDERS.get(name)
    if provider is None:
        raise ValueError(f"Unknown secrets provider: {name}")
    return provider


def resolve_secrets(  # REQ-125, REQ-251, REQ-320, REQ-1557
    value: str, *, providers: tuple[str, ...] | None = None
) -> str:
    """Replace ${provider:reference} patterns with resolved secret values.

    ``providers`` narrows which provider names this particular call will honour. The caller that
    uses it is the one building a central backend from its config: that backend's own credential
    cannot come out of the store it opens, so its resolution is restricted to ``env``.
    """

    def _replace(match: re.Match) -> str:
        provider_name = match.group(1)
        reference = match.group(2)
        if providers is not None and provider_name not in providers:
            raise ValueError(
                f"Secrets provider {provider_name!r} is not permitted here; "
                f"expected one of {', '.join(providers)}."
            )
        provider = _provider_for(provider_name)
        # REQ-1560: the NAME in the reference decides the vault. ``user`` never reads the org's
        # names and ``secret`` never reads a person's -- neither scope stands in for the other.
        if provider_name == "user":
            return provider.resolve_user(reference)
        return provider.resolve(reference)

    return _SECRET_PATTERN.sub(_replace, value)


def resolve_secrets_in_dict(data: dict) -> dict:  # REQ-251, REQ-320
    """Recursively resolve secret references in a dict."""
    result = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = resolve_secrets(value)
        elif isinstance(value, dict):
            result[key] = resolve_secrets_in_dict(value)
        elif isinstance(value, list):
            result[key] = [
                resolve_secrets_in_dict(item)
                if isinstance(item, dict)
                else resolve_secrets(item)
                if isinstance(item, str)
                else item
                for item in value
            ]
        else:
            result[key] = value
    return result

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


#: The names :class:`ScopeProvider` answers, and how. Each is read from the context the resolution
#: is happening in, never from configuration -- that is the whole point of the provider.
_SCOPE_NAMES = ("ENV", "ORG")


class ScopeProvider(SecretsProvider):  # REQ-1622
    """``${scope:ENV}`` / ``${scope:ORG}`` -- WHERE this resolution is happening.

    Not a secret, but the same grammar, because an author writing a source's path or a store's DSN
    is writing one string and should not have to know which half of it is templated by which syntax.
    ``${env:...}`` is the DEPLOYMENT's process environment and ``${scope:ENV}`` is the PROVISA
    environment the request is bound to; the two words collide in English, so the provider names
    keep them apart.

    What this buys is REQ-1622's rule: a source or a store whose address carries ``${scope:ENV}``
    resolves somewhere the environment owns alone, and what an environment owns alone is what
    ``retire_environment`` may remove without asking anything else.

    Fail-closed like every other provider: an unknown name raises rather than resolving empty.
    ``ORG`` with no org bound raises for the same reason. ``ENV`` never does -- REQ-1487 settles
    that a context naming no environment IS prod, so ``active_env()`` is an answer, not a hole.
    """

    def resolve(self, reference: str) -> str:
        from provisa.api.org_runtime import active_env, current_org

        if reference == "ENV":
            return active_env()
        if reference == "ORG":
            org_id = current_org.get()
            if org_id is None:
                raise KeyError(
                    "${scope:ORG} was resolved with no organization bound to this context"
                )
            return org_id
        raise ValueError(f"Unknown scope variable: {reference}. Known: {', '.join(_SCOPE_NAMES)}.")


_PROVIDERS: dict[str, SecretsProvider] = {
    "env": EnvSecretsProvider(),
    "scope": ScopeProvider(),
}

#: Matches ONLY the scope provider. :func:`expand_scope` uses it so a caller can template a value
#: with where-it-is without also forcing every ``${env:...}`` and ``${secret:...}`` in the same
#: string to resolve -- those resolve at their own use points, and pulling them forward here would
#: raise on a name that was never going to be read.
_SCOPE_PATTERN = re.compile(r"\$\{scope:(\w+)\}")


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


def expand_scope(value: str) -> str:  # REQ-1622
    """Replace ``${scope:NAME}`` with where this resolution is happening, and nothing else.

    The narrow counterpart to :func:`resolve_secrets`, for the places that hold a value which is
    part address and part credential -- a source's path, a store's DSN. Those two halves are read at
    different moments by different code, and expanding the address half here leaves the credential
    half written exactly as the author wrote it, for its own use point to resolve.
    """
    return _SCOPE_PATTERN.sub(lambda m: _PROVIDERS["scope"].resolve(m.group(1)), value)


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

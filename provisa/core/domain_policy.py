# Copyright (c) 2026 Kenneth Stott
# Canary: 8f9f5490-f36a-47e2-bf9e-338c6805c12d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolved domain policy — single source of truth for the `naming.use_domains` feature.

Tri-state via ``use_domains``:
  * ``None``  — inert: every new branch falls through to the default (pre-feature) behavior.
  * ``False`` — single-domain mode: all registrations stored under ``default_domain``;
                explicit foreign domains are a hard error; domain hidden from names/UI/access.
  * ``True``  — namespaced mode: ``domain_id`` required on every registration.

Set at config load via :func:`configure`. ``core`` must not import ``api`` state, so this module
is the shared policy that ``config_loader``, repositories, and compilers all read.

PER-ORG (REQ-1266): the mode is a tenant setting, so the policy is keyed by the org whose request
is running. ``core`` cannot import the ``current_org`` ContextVar (it lives in ``api``), so the API
layer installs a resolver via :func:`set_scope_resolver` at startup and every read below resolves
through it. With no resolver installed — an installed single-tenant deployment, the CLI, unit
tests — every read and write lands on the one unscoped policy, which is what those callers mean.
An org that has never been configured reads the unscoped policy too: that is the deployment
default it was created under, not a guess.
"""

# Requirements: REQ-154, REQ-367, REQ-418, REQ-432, REQ-433, REQ-1266

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

_SYSTEM_DOMAIN_IDS = ["", "meta", "ops"]


@dataclass
class _DomainPolicy:
    use_domains: bool | None = None
    default_domain: str = "default"


# Scope key None is the deployment-wide policy: the only one a single-tenant install has, and the
# one a multi-tenant org inherits until it configures its own.
_policies: dict[str | None, _DomainPolicy] = {None: _DomainPolicy()}
_scope_resolver: Callable[[], str | None] | None = None


def set_scope_resolver(resolver: Callable[[], str | None] | None) -> None:  # REQ-1266
    """Install the function that names the org whose policy the current context reads.

    Called once by the API layer with the ``current_org`` ContextVar getter. Passing ``None``
    uninstalls it, returning the module to its unscoped, single-tenant behavior.
    """
    global _scope_resolver
    _scope_resolver = resolver


def _scope() -> str | None:
    return _scope_resolver() if _scope_resolver is not None else None


def _current() -> _DomainPolicy:
    """The policy for the current scope, or the deployment-wide one it was created under."""
    policy = _policies.get(_scope())
    return policy if policy is not None else _policies[None]


def configure(use_domains: bool | None, default_domain: str) -> None:  # REQ-154, REQ-432, REQ-433
    """Set the resolved policy for the current scope, before any registration."""
    _policies[_scope()] = _DomainPolicy(use_domains, default_domain)


def snapshot() -> tuple[bool | None, str]:  # REQ-1266
    """The current scope's resolved policy, for a caller that must carry it forward unchanged."""
    policy = _current()
    return policy.use_domains, policy.default_domain


def reset() -> None:
    """Restore the inert policy: the current scope, and the deployment-wide one it falls back to.

    Test isolation. A scoped entry left behind would outlive the test that wrote it, and the
    deployment-wide entry is what an unconfigured scope reads.
    """
    _policies.pop(_scope(), None)
    _policies[None] = _DomainPolicy()


def reset_all() -> None:  # REQ-1266
    """Drop every org's policy. Only a process teardown between deployments means this."""
    _policies.clear()
    _policies[None] = _DomainPolicy()


def use_domains() -> bool | None:  # REQ-471
    return _current().use_domains


def default_domain() -> str:  # REQ-471
    return _current().default_domain


def active() -> bool:  # REQ-471
    """True when the feature is engaged; gates every new code path."""
    return _current().use_domains is not None


def single_domain() -> bool:  # REQ-471
    return _current().use_domains is False


def resolve_domain_id(requested: str | None) -> str:  # REQ-367, REQ-418, REQ-432, REQ-433
    """Resolve the domain_id to store for a registration.

    Inert: returns ``requested or ""`` — the default (pre-feature) behavior.
    Namespaced (True): ``requested`` required.
    Single-domain (False): falsy ``requested`` coerces to ``default_domain``; a truthy
    value other than ``default_domain`` is a hard error.
    """
    policy = _current()
    if policy.use_domains is None:
        return requested or ""
    if policy.use_domains:
        if not requested:
            raise ValueError("domain_id is required when naming.use_domains=true")
        return requested
    if requested and requested != policy.default_domain:
        raise ValueError(
            f"naming.use_domains=false: cannot register domain {requested!r}; "
            f"only {policy.default_domain!r} is permitted"
        )
    return policy.default_domain


def import_default() -> str:  # REQ-471
    """Domain id for dynamic importers (hasura/fk introspection) that carry no domain info.

    Inert preserves the ``"default"`` literal these paths used; once the
    feature is engaged they fall under the configured ``default_domain``.
    """
    if not active():
        return "default"
    return _current().default_domain


def system_domain_ids() -> list[str]:  # REQ-471
    """Domain ids always preserved across replace-mode reloads."""
    ids = list(_SYSTEM_DOMAIN_IDS)
    policy = _current()
    if policy.use_domains is False:
        ids.append(policy.default_domain)
    return ids

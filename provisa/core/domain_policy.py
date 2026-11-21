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

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Callable

#: Provisa's own domains, which hold its catalog, operations and ontology rather than a model's
#: data (provisa-ui/src/lib/internalDomains.ts names the same).
INTERNAL_DOMAIN_IDS = ("meta", "ops", "ontology")
_SYSTEM_DOMAIN_IDS = ["", *INTERNAL_DOMAIN_IDS]


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


@dataclass
class _Window:
    """A policy that the context which opened it reads alone, while it is open (:func:`trial`,
    :func:`staged`). Configuring inside the window changes this one object, so work that the context
    starts inside it (a task, a thread, each carrying a copy of the context) reads the same policy.
    Once the window closes, that work reads the scope's policy again: a copy of a context made
    inside the window does not keep a policy that has since gone."""

    policy: _DomainPolicy
    #: The scope the window was opened in. Work of another org, bound inside it, reads its own.
    scope: str | None
    open: bool = True
    #: Whether :func:`configure` was called inside the window.
    configured: bool = False


# REQ-2222: the policy of a dry run in progress, seen by that dry run alone.
_window: ContextVar[_Window | None] = ContextVar("provisa_domain_policy_window", default=None)


@contextmanager
def _opened(*, keep: bool) -> Iterator[None]:
    held = _Window(replace(_current()), _scope())
    token = _window.set(held)
    try:
        yield
        if keep and held.configured:
            _policies[held.scope] = held.policy
    finally:
        held.open = False
        _window.reset(token)


@contextmanager
def trial() -> Iterator[None]:  # REQ-2222
    """Within this block :func:`configure` changes the policy for the current context only, and
    the change ends with the block: an apply configures the policy before it writes anything, and
    a dry run of one must not change what the org's other requests read."""
    with _opened(keep=False):
        yield


@contextmanager
def staged() -> Iterator[None]:  # REQ-1266, REQ-2006
    """A runtime build's window: as :func:`trial`, and when the block ends normally the policy it
    configured last becomes the scope's, in one write.

    A build sets the deployment's naming, reads its model under it, then sets the org's own
    (``app._build_org_runtime``). Two builds of one org's environments run at once (a sandbox's
    open, REQ-2006), and each reads its own window's policy, never the other's in-between value.
    Neither do the org's requests meanwhile. A block that configures nothing writes nothing (the
    scope keeps inheriting the deployment's), and one that raises leaves the scope's as it was."""
    with _opened(keep=True):
        yield


def _open_window() -> _Window | None:
    held = _window.get()
    if held is not None and held.open and held.scope == _scope():
        return held
    return None


def _current() -> _DomainPolicy:
    """The policy for the current scope, or the deployment-wide one it was created under."""
    held = _open_window()
    if held is not None:
        return held.policy
    policy = _policies.get(_scope())
    return policy if policy is not None else _policies[None]


def configure(use_domains: bool | None, default_domain: str) -> None:  # REQ-154, REQ-432, REQ-433
    """Set the resolved policy for the current scope, before any registration."""
    held = _open_window()
    if held is not None:
        held.policy = _DomainPolicy(use_domains, default_domain)
        held.configured = True
        return
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


class TableDomainRequired(ValueError):  # REQ-1531, REQ-433
    """A table registration names no domain. Every table sits in a domain; refused by name."""

    code = "schema.table_domain_required"

    def __init__(self, table: str) -> None:
        super().__init__(f"Table {table!r} names no domain: every table is registered in a domain")
        self.table = table


def table_domain_id(requested: str | None, table: str) -> str:  # REQ-1531, REQ-433
    """The domain a table is registered into. Every table sits in a domain: an empty domain is
    refused (:class:`TableDomainRequired`) on every path that saves one -- a single or bulk
    registration, a source's register step, the Wikidata starter and class tables, a
    configuration import or apply. In single-domain mode the one domain is used."""
    resolved = resolve_domain_id(requested)
    if not resolved:
        raise TableDomainRequired(table)
    return resolved


def command_domain_id(requested: str | None, name: str) -> str:  # REQ-1531
    """The domain a command or webhook is saved into. Every one sits in a domain: an empty
    domain is refused, by every path that saves one. In single-domain mode the one domain is
    used, as for a table."""
    resolved = resolve_domain_id(requested)
    if not resolved:
        raise ValueError(f"{name!r} names no domain: a command or webhook sits in a domain")
    return resolved


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

"""Resolved domain policy — single source of truth for the `naming.use_domains` feature.

Tri-state via ``use_domains``:
  * ``None``  — legacy/inert: every new branch falls through to pre-feature behavior.
  * ``False`` — single-domain mode: all registrations stored under ``default_domain``;
                explicit foreign domains are a hard error; domain hidden from names/UI/access.
  * ``True``  — namespaced mode: ``domain_id`` required on every registration.

Set once at config load via :func:`configure`. ``core`` must not import ``api`` state, so
this module is the shared policy that ``config_loader``, repositories, and compilers all read.
"""

# Requirements: REQ-154, REQ-367, REQ-418, REQ-432, REQ-433

from __future__ import annotations

from dataclasses import dataclass

_SYSTEM_DOMAIN_IDS = ["", "meta", "ops"]


@dataclass
class _DomainPolicy:
    use_domains: bool | None = None
    default_domain: str = "default"


_policy = _DomainPolicy()


def configure(use_domains: bool | None, default_domain: str) -> None:  # REQ-154, REQ-432, REQ-433
    """Set the resolved policy. Called once at config load before any registration."""
    _policy.use_domains = use_domains
    _policy.default_domain = default_domain


def reset() -> None:
    """Restore the inert legacy policy (test isolation)."""
    _policy.use_domains = None
    _policy.default_domain = "default"


def use_domains() -> bool | None:  # REQ-471
    return _policy.use_domains


def default_domain() -> str:  # REQ-471
    return _policy.default_domain


def active() -> bool:  # REQ-471
    """True when the feature is engaged; gates every new code path."""
    return _policy.use_domains is not None


def single_domain() -> bool:  # REQ-471
    return _policy.use_domains is False


def resolve_domain_id(requested: str | None) -> str:  # REQ-367, REQ-418, REQ-432, REQ-433
    """Resolve the domain_id to store for a registration.

    Legacy (inert): returns ``requested or ""`` — identical to pre-feature behavior.
    Namespaced (True): ``requested`` required.
    Single-domain (False): falsy ``requested`` coerces to ``default_domain``; a truthy
    value other than ``default_domain`` is a hard error.
    """
    if not active():
        return requested or ""
    if _policy.use_domains:
        if not requested:
            raise ValueError("domain_id is required when naming.use_domains=true")
        return requested
    if requested and requested != _policy.default_domain:
        raise ValueError(
            f"naming.use_domains=false: cannot register domain {requested!r}; "
            f"only {_policy.default_domain!r} is permitted"
        )
    return _policy.default_domain


def import_default() -> str:  # REQ-471
    """Domain id for dynamic importers (hasura/fk introspection) that carry no domain info.

    Legacy (inert) preserves the historical ``"default"`` literal these paths used; once the
    feature is engaged they fall under the configured ``default_domain``.
    """
    if not active():
        return "default"
    return _policy.default_domain


def system_domain_ids() -> list[str]:  # REQ-471
    """Domain ids always preserved across replace-mode reloads."""
    ids = list(_SYSTEM_DOMAIN_IDS)
    if _policy.use_domains is False:
        ids.append(_policy.default_domain)
    return ids

# Copyright (c) 2026 Kenneth Stott
# Canary: b1f92ec3-2c80-42f3-b547-8a49ee6aa3a5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A request's mask: what every surface calls (REQ-2219).

A transport that authenticates a person binds them once per call
(:func:`bind_entitlements`, or :func:`entitlements_scope`), beside its session variables. Then:

* :func:`request_mask` is the person's mask over a role's schema, or None for work no person is
  doing (a model build, a scheduled job), which is not masked;
* :func:`current_mask_hash` is what a cache key includes, empty when nothing is narrowed;
* :func:`govern_for_request` narrows a role's governance context to the request's mask;
* :func:`warnings` is what a sign-in or the names report shows of a person's lists.
"""

# Requirements: REQ-2219

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from provisa.entitlements.index import Finding, build_index, check_lists
from provisa.entitlements.lists import (
    HeldEntitlements,
    IdpLists,
    effective_lists,
    held_version,
    idp_lists_hash,
    parse_idp_entitlements,
)
from provisa.entitlements.mask import EntitlementMask, MemberInput, compute_mask

log = logging.getLogger(__name__)

_NOTHING_HELD = HeldEntitlements()


@dataclass(frozen=True)
class EntitlementRequest:
    """One person's lists, as bound for a call."""

    idp: IdpLists
    idp_hash: str
    held: HeldEntitlements
    held_version: str


_current: ContextVar[EntitlementRequest | None] = ContextVar("provisa_entitlements", default=None)


def _request_for(state: Any, identity: Any) -> EntitlementRequest:
    idp = parse_idp_entitlements(identity.raw_claims)
    held = state.held_entitlements.get(identity.user_id, _NOTHING_HELD)
    return EntitlementRequest(idp, idp_lists_hash(idp), held, held_version(held))


def bind_entitlements(state: Any, identity: Any) -> Token:
    """Bind ``identity``'s lists for the call being served in this context. A token whose lists
    cannot be read raises :class:`~provisa.entitlements.lists.EntitlementClaimError`."""
    return _current.set(_request_for(state, identity))


def reset_entitlements(token: Token) -> None:
    _current.reset(token)


@contextmanager
def entitlements_scope(state: Any, identity: Any) -> Iterator[None]:
    """:func:`bind_entitlements` for the block."""
    token = bind_entitlements(state, identity)
    try:
        yield
    finally:
        reset_entitlements(token)


def _role_context(state: Any, role_id: str) -> Any:
    from provisa.api.role_surface import role_surface  # noqa: PLC0415

    surface = role_surface(state, role_id)
    if surface is None:
        raise RuntimeError(f"role {role_id!r} has no surface to compute entitlements against")
    return surface.context


def _role_defaults(role: Mapping[str, Any]) -> Mapping[str, list[str]]:
    """The role's default lists. Defaults naming exactly the domains the role is offered to are
    the role itself, so a plain role grant is unchanged; they narrow nothing."""
    defaults = dict(role.get("default_entitlements") or {})
    domains = defaults.get("domains")
    offered = role.get("domain_access")
    if domains is not None and offered is not None and set(domains) == set(offered):
        defaults.pop("domains")
    return defaults


def _members(role_id: str) -> tuple[str, ...]:
    from provisa.security.meta_role import members_of  # noqa: PLC0415

    return members_of(role_id)


def _compute(
    state: Any, role_id: str, request: EntitlementRequest, version: tuple, now: datetime
) -> EntitlementMask:
    cache = state.entitlement_masks
    names = state.names_table
    products = tuple(state.data_product_refs)

    def index_of(role: str) -> Any:
        return cache.index(
            (version, role), lambda: build_index(names, _role_context(state, role), products)
        )

    model_index = cache.index((version, "*"), lambda: build_index(names, None, products))
    members: list[MemberInput] = []
    findings: list[Finding] = []
    for member in _members(role_id):
        role = state.roles[member]
        lists = effective_lists(member, request.idp, request.held, _role_defaults(role))
        findings.extend(check_lists(member, role, lists, model_index))
        expiries = tuple(r for r in request.held.expiries if r.role_id == member)
        members.append(MemberInput(member, lists, index_of(member), expiries))
    return compute_mask(role_id, index_of(role_id), members, version, now, tuple(findings))


def request_mask(state: Any, role_id: str) -> EntitlementMask | None:
    """The bound person's mask over ``role_id``'s schema; None when no person is bound, or when
    there is no model yet and they carry no lists. Lists with no names table to expand them
    against raise: they must not widen to the whole role."""
    request = _current.get()
    if request is None:
        return None
    if state.names_table is None:
        if request.idp or request.held.lists:
            raise RuntimeError(
                "entitlement lists are bound but this model has no names table to expand them"
            )
        return None
    version = (state.schema_boot_id, state.schema_version)
    key = (role_id, request.idp_hash, request.held_version, version)
    now = datetime.now(UTC)

    def build() -> EntitlementMask:
        mask = _compute(state, role_id, request, version, now)
        for finding in mask.findings:
            log.warning("entitlements: %s (%s)", finding.message, finding.code)
        return mask

    return state.entitlement_masks.get_or_build(key, build, now=now)


def warnings(state: Any, role_id: str) -> tuple[Finding, ...]:
    """The flags on the bound person's lists for ``role_id``, for a sign-in or the names report."""
    mask = request_mask(state, role_id)
    return () if mask is None else mask.findings


def current_mask_hash(state: Any, role_id: str) -> str:
    """The hash a cache key includes: empty when no person is bound or the mask narrows nothing."""
    mask = request_mask(state, role_id)
    return "" if mask is None else mask.hash


def govern_for_request(governance: Any, state: Any, role_id: str) -> Any:
    """``governance`` (a per-read copy) narrowed to the bound person's mask over ``role_id``."""
    from provisa.compiler.entitlement_rule import narrow_governance  # noqa: PLC0415

    return narrow_governance(governance, request_mask(state, role_id))

# Copyright (c) 2026 Kenneth Stott
# Canary: e7e8dec4-2da5-490b-a122-4374882219a1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pseudo-roles: roles a user holds because of what their attributes say (REQ-2057).

The org declares which claim types become roles (``claim_roles``). For each declared type ``t``
whose value ``v`` the user's attributes give (the org's record over the identity provider's
claims, :mod:`provisa.auth.attributes`), the user holds the parent role ``t`` and its sub-role
``t__v``. No value: neither. A pseudo-role carries no rights and is never granted by hand: a grant
list names it like any role, and a request acts as it only by naming it, as with any role.
"""

# Requirements: REQ-2057, REQ-1382

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from provisa.auth.attributes import effective, idp_attributes, normalize_name

#: Between a claim type and its value in a sub-role's id, as in a GraphQL path.
SEPARATOR = "__"


def sub_role_id(claim_type: str, value: str) -> str:
    return f"{claim_type}{SEPARATOR}{value}"


def held_pseudo_roles(declared: Iterable[str], attributes: Mapping[str, str]) -> dict[str, str]:
    """pseudo-role id -> its parent ('' for a parent), for every declared claim type the user's
    resolved ``attributes`` give a value."""
    held: dict[str, str] = {}
    for claim_type in declared:
        name = normalize_name(claim_type)
        value = attributes.get(name)
        if value is None or value == "":
            continue
        held[name] = ""
        held[sub_role_id(name, value)] = name
    return held


def named_unsecured(
    names: Iterable[str], declared: Iterable[str]
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """An unsecured request's named roles split (REQ-2057, REQ-2035): the data roles, the declared
    pseudo-roles it names -- held, each with its parent, as unsecured mode takes any role it is
    named at its word -- and the attributes those imply (``region__eu``: region=eu), which bind its
    session variables. A signed-in request holds a pseudo-role only by its claims, never so."""
    types = {normalize_name(t) for t in declared}
    roles: list[str] = []
    held: dict[str, str] = {}
    attributes: dict[str, str] = {}
    for name in names:
        claim_type, sep, value = name.partition(SEPARATOR)
        if sep and value and claim_type in types:
            held[name] = claim_type
            held.setdefault(claim_type, "")
            attributes[claim_type] = value
        elif name in types:
            held.setdefault(name, "")
        else:
            roles.append(name)
    return roles, held, attributes


def declared_claim_roles(state) -> list[str]:
    """The claim types the bound org declares: its own ``claim_roles`` setting (org_settings)
    where it has one, which replaces the deployment config's as every org-owned key does
    (REQ-1349, ``org_settings.merge_org_overrides``)."""
    overrides = state.settings_overrides
    if "claim_roles" in overrides:
        return [str(t) for t in overrides["claim_roles"]]
    return list(state.config.claim_roles)


def principal_bindings(
    raw_claims: Mapping | None, org_attributes: Mapping[str, str], declared: Iterable[str]
) -> tuple[dict[str, str], dict[str, str]]:
    """What one authenticated principal binds, on every transport: its resolved attributes (the
    session variables a row filter reads, before ``user_id`` and ``role``) and the pseudo-roles
    they make it hold."""
    attributes = effective(idp_attributes(raw_claims), org_attributes)
    return attributes, held_pseudo_roles(declared, attributes)


def principal_of(state: Any, identity: Any) -> tuple[dict[str, str], dict[str, str]]:
    """:func:`principal_bindings` for ``identity`` in the org bound now: the attributes the org
    records for it (its runtime build, REQ-1382) over its provider's claims, and the claim types
    the org declares. For a transport that authenticates its own caller (Flight, gRPC, Bolt,
    pgwire, MCP); HTTP binds the same from its middleware."""
    org = state.user_attributes.get(identity.user_id, {})
    return principal_bindings(identity.raw_claims, org, declared_claim_roles(state))


def principal_session_vars(
    user_id: str | None, attributes: Mapping[str, str], role_id: str | None
) -> dict[str, str]:
    """The session variables a principal binds (REQ-1682): its resolved attributes, then
    ``user_id`` and ``role`` from the acting identity."""
    out = dict(attributes)
    if user_id and user_id != "anonymous":
        out["user_id"] = str(user_id)
    if role_id:
        out["role"] = str(role_id)
    return out


@contextmanager
def principal_scope(state: Any, identity: Any, role_id: str | None) -> Iterator[None]:
    """Bind ``identity``'s session variables, acting as ``role_id``, for one call of a transport
    that authenticates its own caller, as the HTTP middleware binds them for a request."""
    from provisa.core.request_context import reset_session_vars, set_session_vars

    attributes, _held = principal_of(state, identity)
    token = set_session_vars(principal_session_vars(identity.user_id, attributes, role_id))
    try:
        yield
    finally:
        reset_session_vars(token)


def bind_principal_session_vars(state: Any, identity: Any, role_id: str | None) -> None:
    """Bind ``identity``'s session variables, acting as ``role_id``, for the call being served in
    this context -- for a transport that binds each call's principal in a context of the call's
    own (gRPC's RPC context, an Airport handler thread), where every call binds its own before
    it runs a statement."""
    from provisa.core.request_context import set_session_vars

    attributes, _held = principal_of(state, identity)
    set_session_vars(principal_session_vars(identity.user_id, attributes, role_id))


#: The claim type a regional node's own region is carried as, on its service calls.
REGION_CLAIM = "region"


def service_acting_role(state: Any, role_id: str) -> str:
    """The role a service call this node makes on its own behalf acts as (REQ-2057): ``role_id``
    with the node's region pseudo-role -- ``region`` and ``region__<node region>`` -- in its set,
    so a grant to the region reaches the node's own work (a trigger run). A node with no region
    acts as ``role_id`` alone. Never for a user's request: a user's region never comes from the
    node."""
    from provisa.core import process_region
    from provisa.core.regions import DEFAULT_REGION
    from provisa.security.meta_role import ensure_meta_role

    node_region = process_region.region()
    if node_region == DEFAULT_REGION:
        return role_id
    return ensure_meta_role(
        state, [role_id], (REGION_CLAIM, sub_role_id(REGION_CLAIM, node_region))
    )

# Copyright (c) 2026 Kenneth Stott
# Canary: 0b23aa0b-6d6e-4275-8001-b28fd2afb210
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A user's attributes: the identity provider's claims and the org's own record (REQ-1382,
REQ-1383), resolved in one place.

- an attribute only the provider supplies is the provider's;
- an attribute only the org records is the org's;
- an attribute both name is the ORG's: the org's record of its own member is the more specific
  statement, and the provider's value is reported as overridden. It is never merged.

Both land in the one namespace a row filter reads, ``current_setting('provisa.<name>')``, so a
predicate does not know where an attribute came from. The source is kept beside each value so an
administrator can see why an attribute has the value it has.
"""

# Requirements: REQ-1382, REQ-1383, REQ-1682

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

IDP = "idp"
ORG = "org"

#: Bound from the acting identity on every request: who the caller is and the role they act as. A
#: recorded attribute of either name would be an administrator writing a user's identity.
RESERVED_NAMES = frozenset({"user_id", "role"})

#: Claims that describe the TOKEN rather than the user. They differ at every sign-in, so the record
#: of what a provider supplies for a user (REQ-1383) leaves them out: it is rewritten only when what
#: the provider says about the user changes.
TOKEN_CLAIMS = frozenset(
    {"iat", "exp", "nbf", "jti", "auth_time", "nonce", "at_hash", "c_hash", "sid", "session_state"}
)

_HASURA_PREFIX = "x-hasura-"
# What ``current_setting('provisa.<name>')`` can spell: a plain identifier.
_NAME = re.compile(r"[a-z_][a-z0-9_]*")


def normalize_name(key: str) -> str:
    """The one spelling of an attribute name: lower-cased, ``-`` as ``_``. A Hasura session
    variable keeps its own name: ``X-Hasura-User-Id`` is ``hasura_user_id``, the app's user key an
    imported rule reads -- never ``user_id``, which is the verified identity."""
    name = key.strip().lower()
    if name.startswith(_HASURA_PREFIX):
        name = "hasura_" + name[len(_HASURA_PREFIX) :]
    return name.replace("-", "_")


def idp_attributes(raw_claims: Mapping | None) -> dict[str, str]:
    """The attributes an identity provider supplies: every SCALAR claim, under its normalized
    name, as text. A list or an object is not a value a predicate can compare with -- except the
    object a Hasura token carries its session variables in (``https://hasura.io/jwt/claims`` by
    default, any name the token gives it): its ``x-hasura-*`` claims are read from there, as
    Hasura read them."""
    out: dict[str, str] = {}

    def _scalar(key: str, val: object) -> None:
        if isinstance(val, bool):
            out[normalize_name(key)] = "true" if val else "false"
        elif isinstance(val, (str, int, float)):
            out[normalize_name(key)] = str(val)

    for key, val in (raw_claims or {}).items():
        if isinstance(val, Mapping):
            for inner, inner_val in val.items():
                if str(inner).lower().startswith(_HASURA_PREFIX):
                    _scalar(str(inner), inner_val)
        else:
            _scalar(str(key), val)
    return out


def recorded_idp_attributes(raw_claims: Mapping | None) -> dict[str, str]:
    """What is kept of a provider's attributes for the provenance display (REQ-1383):
    :func:`idp_attributes` without the claims that describe the token."""
    return {k: v for k, v in idp_attributes(raw_claims).items() if k not in TOKEN_CLAIMS}


def attribute_name_problem(name: str) -> tuple[str, str] | None:
    """Why ``name`` cannot be recorded for a user, as (code, message), or None when it can. The
    code names the problem for a client (``users.attribute_<code>``)."""
    normalized = normalize_name(name)
    if not normalized:
        return "name_missing", "An attribute needs a name"
    if normalized in RESERVED_NAMES:
        return (
            "name_reserved",
            f"'{normalized}' is bound from the acting identity on every request and cannot be "
            "recorded as an attribute",
        )
    if not _NAME.fullmatch(normalized):
        return (
            "name_invalid",
            f"Attribute name '{name}' must be letters, digits and underscores, and must not "
            "start with a digit",
        )
    return None


def session_constant_problem(name: str) -> tuple[str, str] | None:  # REQ-1682
    """Why ``name`` cannot be a role's configured session value, as (code, message), or None when
    it can. A request binds ``user_id`` and ``role``, and the node sets the region and the org over
    whatever else is bound (``request_context.session_vars_for``): a constant of any of those names
    would never be read, so it is refused rather than kept."""
    from provisa.core.request_context import ORG_VAR, REGION_VAR

    normalized = normalize_name(name)
    if not normalized:
        return "name_missing", "A session value needs a name"
    if normalized in RESERVED_NAMES | {REGION_VAR, ORG_VAR}:
        return (
            "name_reserved",
            f"'{normalized}' is bound by the request or the node and cannot be a role's "
            "session value",
        )
    if not _NAME.fullmatch(normalized):
        return (
            "name_invalid",
            f"Session value name '{name}' must be letters, digits and underscores, and must "
            "not start with a digit",
        )
    return None


@dataclass(frozen=True)
class ResolvedAttribute:
    """One attribute of a user as it resolved: its value, where it comes from, and -- for a
    provider value the org also records -- that it is overridden and so not used."""

    name: str
    value: str
    source: str  # IDP | ORG
    overridden: bool = False


def resolve(idp: Mapping[str, str], org: Mapping[str, str]) -> list[ResolvedAttribute]:
    """Every attribute of a user with its source, by name, the org's before the provider's of the
    same name. ``idp`` and ``org`` are keyed by normalized name."""
    out: list[ResolvedAttribute] = []
    for name in sorted({*idp, *org}):
        if name in org:
            out.append(ResolvedAttribute(name, org[name], ORG))
        if name in idp:
            out.append(ResolvedAttribute(name, idp[name], IDP, overridden=name in org))
    return out


def effective(idp: Mapping[str, str], org: Mapping[str, str]) -> dict[str, str]:
    """name -> the value in force: the provider's attributes overlaid by the org's record."""
    return {**idp, **org}

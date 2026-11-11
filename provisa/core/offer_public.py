# Copyright (c) 2026 Kenneth Stott
# Canary: 7f87a5de-bb93-4de6-8422-83e5fc554cd1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An open offer is public data (REQ-2180, REQUESTS GO THROUGH HOOKS).

An offer can be open only when its data is already public: readable by every member through a
role every member holds. Every member holds a role when every role of the org has it in its chain
(the role itself or an ancestor, REQ-2155): whatever a member is granted, they hold it through
inheritance. Its data is readable by every member when the role reads each of the product's
datasets, and no one's grant of the role narrows its ``tables`` list past one of them -- neither
the role's own default list, nor a list Provisa holds for a person, nor one their identity provider
sends.

An open offer is no request at all: the requester sees Query now and no hook fires. Marking an
offer open is refused naming what is not public; when the data stops being public, the offer is
listed for review.
"""

# Requirements: REQ-2180

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from typing import Any

from provisa.entitlements.index import WILDCARD

#: The entitlement list an offer's datasets are reached through (provisa.core.offer_grant.TABLES).
TABLES = "tables"
#: Why an open offer is listed for review.
REVIEW_NOT_PUBLIC = "this open offer's data is no longer public"


def _narrows(identifiers: Collection[str], names: Iterable[str]) -> list[str]:
    """The dataset ``names`` a ``tables`` list of ``identifiers`` does not reach."""
    if WILDCARD in identifiers:
        return []
    return [n for n in names if n not in identifiers]


def not_public(
    *,
    role_id: str,
    role_chains: Mapping[str, list[str]],
    roles: Mapping[str, Any],
    role_defaults: Mapping[str, Collection[str]],
    held: Mapping[str, Any],
    idp: Mapping[str, Mapping[str, Mapping[str, Collection[str]]]],
    datasets: Collection[str],
    unread: Collection[str],
) -> list[str]:
    """What keeps an offer of ``role_id`` on ``datasets`` (by SQL name) from being public, each in
    words; empty when it is public. ``unread`` are the datasets the role reads no column of;
    ``role_defaults`` its default lists; ``held`` what Provisa holds per person
    (``state.held_entitlements``); ``idp`` the lists each person's identity provider sends
    (``state.idp_entitlements``)."""
    found: list[str] = []
    outside = sorted(r for r in roles if role_id not in role_chains.get(r, [r]))
    if outside:
        found.append(
            f"not every member holds role {role_id!r}: {', '.join(outside)} "
            f"{'does' if len(outside) == 1 else 'do'} not inherit it"
        )
    if unread:
        found.append(f"role {role_id!r} reads nothing of {', '.join(sorted(unread))}")
    default = role_defaults.get(TABLES)
    if default is not None:
        missed = _narrows(default, datasets)
        if missed:
            found.append(f"role {role_id!r}'s default tables list leaves out {', '.join(missed)}")
    for principal, holding in sorted(held.items()):
        for listed in holding.lists:
            if listed.role_id != role_id or listed.kind != TABLES or listed.capability is not None:
                continue
            missed = _narrows(listed.identifiers, datasets)
            if missed:
                found.append(f"{principal}'s grant of {role_id!r} leaves out {', '.join(missed)}")
    for principal, lists in sorted(idp.items()):
        sent = (lists.get(role_id) or {}).get(TABLES)
        if sent is None:
            continue
        missed = _narrows(sent, datasets)
        if missed:
            found.append(
                f"{principal}'s identity provider narrows {role_id!r} past {', '.join(missed)}"
            )
    return found
